#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <sstream>
#include <ctime>
#include <unistd.h>
#include <queue>

#include <grpc/support/log.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/alarm.h>

#ifdef BAZEL_BUILD
#include "examples/protos/grpcHandler.grpc.pb.h"
#else
#include "grpcHandler.grpc.pb.h"
#endif

using namespace std;
using grpc::Alarm;
using grpc::Channel;
using grpc::ClientAsyncReaderWriter;
using grpc::ClientContext;
using grpc::CompletionQueue;
using grpc::Status;
using grpcHandler::GrpcHandler;
using grpcHandler::NdrResponse;

int replyCount = 0;
int messageCount = 0;

class GrpcHandlerClient
{

   enum class StreamStatus
   {
      CONNECT = 1,
      PROCESS = 2,
      PUSH_TO_BACK = 3,
      FINISH = 4
   };

   // struct for keeping state and data information
   struct AsyncClientCall
   {
      // Context for the client. It could be used to convey extra information to
      // the server and/or tweak certain RPC behaviors.
      ClientContext context;

      // Finish status when the client is done with the stream.
      Status finish_status_ = Status::OK;

      // Status of client
      StreamStatus status;

      // queue of messages to be sent
      std::queue<std::string> localQueue;

      // Container for the data we send to the server.
      NdrResponse request;

      // Container for the data we expect from the server.
      NdrResponse reply;

      // Uniquely identify the message
      std::string mac;

      // Whether the client is connected to the server
      bool serverConnected = false;

      // Whether the stream has an active READ on the completion queue
      bool activeRead = false;

      // Used to push call's to the back of the completion queue
      std::unique_ptr<Alarm> alarm_;

      // The bidirectional, asynchronous stream for sending/receiving messages.
      std::unique_ptr<ClientAsyncReaderWriter<NdrResponse, NdrResponse>> stream_;
   };

public:
   std::queue<AsyncClientCall *> streamQueue;

   bool serverConnected;

   explicit GrpcHandlerClient(std::shared_ptr<Channel> channel, int streamCount)
       : stub_(GrpcHandler::NewStub(channel))
   {
      // ensure the client is connected to the server
      std::chrono::time_point<std::chrono::system_clock> _deadline = std::chrono::system_clock::now() + std::chrono::hours(24);
      auto state = channel->GetState(true);
      while (state != GRPC_CHANNEL_READY)
      {
         if (!channel->WaitForStateChange(state, _deadline))
         {
            serverConnected = false;
         }
         state = channel->GetState(true);
      }
      
      serverConnected = true;
      establishConnectionToStreams(streamCount);
   }

   // used to round-robin between streams
   AsyncClientCall *getNextContext()
   {
      AsyncClientCall *call = streamQueue.front();
      streamQueue.pop();
      streamQueue.push(call);
      return call;
   }

   // Adds a message from given mac address to given call's local queue
   void sendToQueue(std::string mac, int startNum, AsyncClientCall *call)
   {
      call->mac = mac + std::to_string(startNum);
      std::string text;
      text = "testMessage from : " + mac + std::to_string(startNum);
      call->localQueue.push(text);
   }

   // Loop while listening for completed responses.
   void AsyncCompleteRpc(std::shared_ptr<grpc::Channel> channelName)
   {
      void *got_tag;
      bool ok = false;
      // Loop sending and receiving messages
      while (true)
      {
         // TODO - might want to make timeout deadline configurable
         std::chrono::time_point<std::chrono::system_clock> deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(1000);

         cq_.AsyncNext(&got_tag, &ok, deadline);
         // The tag in this example is the memory location of the call object
         AsyncClientCall *call = static_cast<AsyncClientCall *>(got_tag);

         // Ensure the client is connected to the server
         auto state = channelName->GetState(true);
         if (state != GRPC_CHANNEL_READY)
         {
            std::cout << "Connection to server lost." << std::endl;
            // Loop until the connection is re-established
            while (channelName->GetState(true) != GRPC_CHANNEL_READY)
            {
            }
            std::cout << "Connection to server re-established." << std::endl;
            // TODO - # of streams should be config value, not streamQueue.size()
            establishConnectionToStreams(streamQueue.size());
         }

         switch (call->status)
         {
            case StreamStatus::CONNECT:
               // Stream has connected, start processing messages
               std::cout << "**** Stream Tag: " << got_tag << "   CONNECT: Stream connected." << std::endl;
               call->serverConnected = true;
               call->stream_->Read(&call->reply, (void *)call);
               call->activeRead = true;
               call->status = StreamStatus::PROCESS;
               PutTaskBackToQueue(call);
               break;
            case StreamStatus::PROCESS:
               if (call->reply.has_message())
               {
                  // Process the reply from the server
                  std::cout << "**** Stream Tag: " << got_tag << "   READ: Read a new message:" << call->reply.message() << std::endl;
                  replyCount++;
                  // uncomment_to_DEBUG std::cout << replyCount << std::endl;

                  // Prepare to read another reply, but don't start a READ (prioritize WRITE's)
                  call->reply.clear_message();
                  call->activeRead = false;
               } else {
                  if (call->activeRead)
                  {
                     std::cout << "READ timed out." << std::endl;
                  }
               }

               if (!call->localQueue.empty())
               {
                  // WRITE a message from the queue
                  std::cout << "**** Stream Tag: " << got_tag << "   WRITE: Sending message: " << call->localQueue.front() << std::endl;
                  messageCount++;
                  call->request.set_message(call->localQueue.front());
                  call->localQueue.pop();
                  call->stream_->Write(call->request, (void *)call);
               }
               else
               {
                  // Start a READ if one isn't already active
                  if (!call->activeRead)
                  {
                     call->activeRead = true;
                     call->stream_->Read(&call->reply, (void *)call);
                  }
               }
               // Push the call to the back of the completion queue
               call->status = StreamStatus::PUSH_TO_BACK;
               break;

            case StreamStatus::PUSH_TO_BACK:
               // Pushing the call to the back of the completion queue ensures
               // all streams are handled in a round-robin fashion
               call->status = StreamStatus::PROCESS;
               PutTaskBackToQueue(call);
               break;

            case StreamStatus::FINISH:
               std::cout << "**** Stream Tag: " << got_tag << "   FINISH: Streaming complete" << std::endl;
               // TODO - actually delete call on FINISH
               // delete call;
               break;

            default:
               std::cout << "ERROR: Unrecognized status: " << int(call->status) << std::endl;
         }
      }
   }

private:
   // uses the GRPC alarm to put tasks to the back of the completion queue
   void PutTaskBackToQueue(AsyncClientCall *call)
   {
      call->alarm_.reset(new Alarm);
      call->alarm_->Set(&cq_, gpr_now(gpr_clock_type::GPR_CLOCK_REALTIME), (void *)call);
   }

   // creates and stores streams
   void establishConnectionToStreams(int streamCount)
   {
      for (int i = 1; i <= streamCount; i++)
      {
         // create new AsyncClientCall
         AsyncClientCall *call = new AsyncClientCall();
         call->status = StreamStatus::CONNECT;
         call->stream_ = stub_->AsyncSayHelloNew(&call->context, &cq_, (void *)call);
         if (streamQueue.size() == streamCount)
         {
            std::cout << "Re-establishing stream " << call << " with server." << std::endl;
            // re-establish/replace existing streams with server
            call->localQueue = streamQueue.front()->localQueue;
            streamQueue.pop();
            // TODO - delete calls?
         }
         streamQueue.push(call);
      }
   }

   // Out of the passed in Channel comes the stub, stored here, our view of the
   // server's exposed services.
   std::unique_ptr<GrpcHandler::Stub> stub_;

   // The producer-consumer queue we use to communicate asynchronously with the
   // gRPC runtime.
   CompletionQueue cq_;
};

const char *ParseCmdPara(char *argv, const char *para)
{
   auto p_target = std::strstr(argv, para);
   if (p_target == nullptr)
   {
      printf("para error argv[%s] should be %s \n", argv, para);
      return nullptr;
   }
   p_target += std::strlen(para);
   return p_target;
}

int main(int argc, char **argv)
{

   if (argc != 5)
   {
      std::cout << "Usage:./program --streamCount=xx --messageCount=xx --messageInterval=xx --port=xx";
      return 0;
   }

   int streamCount = std::atoi(ParseCmdPara(argv[1], "--streamCount="));
   int g_message_num = std::atoi(ParseCmdPara(argv[2], "--messageCount="));
   int messageInterval = std::atoi(ParseCmdPara(argv[3], "--messageInterval="));
   int g_port = std::atoi(ParseCmdPara(argv[4], "--port="));

   // Instantiate the client. It requires a channel, out of which the actual RPCs
   // are created. This channel models a connection to an endpoint (in this case,
   // localhost at port 50055). We indicate that the channel isn't authenticated
   // (use of InsecureChannelCredentials()).
   std::shared_ptr<grpc::Channel> channelName = grpc::CreateChannel(
       "localhost:" + std::to_string(g_port), grpc::InsecureChannelCredentials());
   GrpcHandlerClient greeter(channelName,
                             streamCount);

   if (!greeter.serverConnected)
   {
      std::cout << "Server not connected by deadline of 24 hours" << std::endl;
      return 1;
   }

   // Spawn reader thread that loops indefinitely
   std::thread thread_ = std::thread(&GrpcHandlerClient::AsyncCompleteRpc, &greeter, channelName);

   // uncomment_to_DEBUG time_t now = time(0);

   // round robin messages between streams
   std::string macString = "00a0bc";
   for (int i = 0; i < g_message_num; i++)
   {
      greeter.sendToQueue(macString, i, greeter.getNextContext());
      // simulate time between messages
      if (messageInterval)
      {
         if (i%streamCount == 0) {
            sleep(messageInterval);
         }
      }
      else
      {
         usleep(500);
      }
   }

   std::cout << "Queue processing complete" << std::endl;
   for (int i = 0; i < greeter.streamQueue.size(); i++)
   {
      std::cout << "Pending size of contextID is " << greeter.getNextContext()->localQueue.size() << std::endl;
   }

   // uncomment_to_DEBUG time_t then = time(0);
   // uncomment_to_DEBUG std::cout << "Time difference for processing is all messages is " << then - now << " seconds" << std::endl;
   // uncomment_to_DEBUG std::cout << "Reply's received: " << replyCount << std::endl;

   // Below is the block for AsyncComplete queue which blocks for ever until we have a client restart..
   thread_.join(); // blocks forever

   // Verify client context is closed at the end of the run and there no bidirectional streams anymore
   while (!greeter.streamQueue.empty())
   {
      greeter.streamQueue.front()->stream_->Finish(&greeter.streamQueue.front()->finish_status_, (void *)greeter.streamQueue.front());
      std::cout << "Client " << greeter.streamQueue.front() << " no more after calling finish" << std::endl;
   }
   std::cout << "All bidirectional streams closed successfully" << std::endl;

   return 0;
}
