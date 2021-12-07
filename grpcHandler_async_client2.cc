#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <sstream>
#include <ctime>

#include <grpc/support/log.h>
#include <grpcpp/grpcpp.h>

#ifdef BAZEL_BUILD
#include "examples/protos/grpcHandler.grpc.pb.h"
#else
#include "grpcHandler.grpc.pb.h"
#endif

using namespace std;
using grpc::Channel;
using grpc::ClientAsyncReaderWriter;
using grpc::ClientContext;
using grpc::CompletionQueue;
using grpc::Status;
using grpcHandler::GrpcHandler;
using grpcHandler::NdrResponse;

class GrpcHandlerClient
{

   enum class Type
   {
      READ = 1,
      WRITE = 2,
      CONNECT = 3,
      WRITES_DONE = 4,
      FINISH = 5
   };

   // struct for keeping state and data information
   struct AsyncClientCall
   {
      // Context for the client. It could be used to convey extra information to
      // the server and/or tweak certain RPC behaviors.
      ClientContext context;

      // Finish status when the client is done with the stream.
      Status finish_status_ = Status::OK;

      // Container for the data we send to the server.
      NdrResponse request;

      // Container for the data we expect from the server.
      NdrResponse reply;

      // Uniquely identify the message
      std::string mac;

      bool serverConnected = false;

      // The bidirectional, asynchronous stream for sending/receiving messages.
      std::unique_ptr<ClientAsyncReaderWriter<NdrResponse, NdrResponse>> stream_;
   };

   // struct MessageFormat
   // {
   //    // Container for the data we send to the server.
   //    NdrResponse request;

   //    // Container for the data we expect from the server.
   //    NdrResponse reply;

   //    // Uniquely identify the message
   //    std::string mac;

   //    AsyncClientCall *call;
   // };

public:
   unordered_map<int, AsyncClientCall *> hashMap;

   explicit GrpcHandlerClient(std::shared_ptr<Channel> channel, int count)
       : stub_(GrpcHandler::NewStub(channel))
   {
      for (int i = 1; i <= count; i++)
      {
         // Create Context here for this unique contextID
         // MessageFormat *msg = new MessageFormat();
         AsyncClientCall *call = new AsyncClientCall();
         // msg->call = call;

         // stub_->PrepareAsyncSayHello() creates an RPC object, returning
         // an instance to store in "call" but does not actually start the RPC
         // Because we are using the asynchronous API, we need to hold on to
         // the "call" instance in order to get updates on the ongoing RPC.
         call->stream_ =
             stub_->AsyncSayHelloNew(&call->context, &cq_, (void *)call);
         hashMap[i] = call;
      }
   }

   // Assembles the client's payload and sends it to the server.
   void sendMessage(std::string mac, int startNum, AsyncClientCall *call)
   {
      // Call object to store rpc data
      // AsyncClientCall *call = new AsyncClientCall;

      // stub_->PrepareAsyncSayHello() creates an RPC object, returning
      // an instance to store in "call" but does not actually start the RPC
      // Because we are using the asynchronous API, we need to hold on to
      // the "call" instance in order to get updates on the ongoing RPC.
      // call->stream_ =
      //    stub_->AsyncSayHelloNew(&call->context, &cq_, (void *)this);

      // StartCall initiates the RPC call
      // call->stream_->StartCall(reinterpret_cast<void *>(Type::CONNECT));

      // call->stream_->Write(call->request, reinterpret_cast<void *>(Type::WRITE));

      // Get call context ID

      std::cout << "Sending testMessage via Call context ID: " << call << std::endl;

      int i = startNum;
      while (i <= startNum + 900) // true
      {

         // MessageFormat *msg = new MessageFormat();
         call->mac = mac + std::to_string(i);
         // msg->call = call;

         std::string text;
         std::stringstream ss;
         ss << call;
         text = "testMessage from : " + mac + std::to_string(i);
         /*if (i == 3) {
            text = "quit";
         }*/
         if (!evaluateMessage(text, call))
         {
            std::cout << "TestMessage send complete from Call context ID : " << call << std::endl;
            std::cout << "Quitting." << std::endl;
            break;
         }
         i++;
      }
   }

   /*void AsyncHelloRequestNextMessage(AsyncClientCall *call)
   {

      // The tag is the link between our thread (main thread) and the completion
      // queue thread. The tag allows the completion queue to fan off
      // notification handlers for the specified read/write requests as they
      // are being processed by gRPC.
      call->stream_->Read(&call->reply, reinterpret_cast<void *>(Type::READ));
   }*/

   // Loop while listening for completed responses.
   // Prints out the response from the server.
   void AsyncCompleteRpc()
   {
      void *got_tag;
      bool ok = false;

      // Block until the next result is available in the completion queue "cq".
      while (cq_.Next(&got_tag, &ok))
      {
         // The tag in this example is the memory location of the call object
         AsyncClientCall *call = static_cast<AsyncClientCall *>(got_tag);

         // Verify that the request was completed successfully. Note that "ok"
         // corresponds solely to the request for updates introduced by Finish().
         GPR_ASSERT(ok);

         // It's important to process all tags even if the ok is false. One might
         // want to deallocate memory that has be reinterpret_cast'ed to void*
         // when the tag got initialized. For our example, we cast an int to a
         // void*, so we don't have extra memory management to take care of.
         if (ok)
         {
            std::cout << "**** Processing completion queue tag " << got_tag << std::endl;
            switch (static_cast<Type>(reinterpret_cast<long>(got_tag)))
            {
            case Type::CONNECT:
               std::cout << "Server connected." << std::endl;
               call->serverConnected = true;
               break;
            case Type::WRITES_DONE:
               std::cout << "Streaming complete for that bidirectional stream:" << got_tag << std::endl;
               break;
            default:
               std::cout << "Received message :" << call->request.has_message() << ":" << call->reply.has_message() << ":" << std::endl;
               //Process requests from the server
               if (!call->request.has_message() && !call->reply.has_message())
               {
                  if (!call->serverConnected) {
                     std::cout << "Server connected." << std::endl;
                     call->serverConnected = true;
                     break;
                  }
               }
               if (call->reply.has_message())
               {
                  std::cout << "Read a new message:" << call->reply.message() << std::endl;
                  call->request.clear_message();
                  call->reply.clear_message();
               }
               if (call->request.has_message())
               {
                  call->stream_->Read(&call->reply, (void *)call);
               }
            }
         }

         /*if (call->status.ok())
            std::cout << "GrpcHandler received: " << call->reply.message() << std::endl;
         else
            std::cout << "RPC failed" << std::endl;*/

         // Once we're complete, deallocate the call object.
         // delete call;
      }
   }

private:
   // Similar to the async hello example in greeter_async_client but does not
   // wait for the response. Instead queues up a tag in the completion queue
   // that is notified when the server responds back (or when the stream is
   // closed). Returns false when the stream is requested to be closed.
   bool evaluateMessage(const std::string &user, AsyncClientCall *call)
   {
      std::cout << "Check write status:" << call->request.has_message() << std::endl;

      while (call->request.has_message() == 1 || !call->serverConnected)
      {
         // Wait until server is connected
         // Also wait until that client context request is processed successfully for future requests from that MAC
      }

      if (user == "quit")
      {
         std::cout << " ** Sending complete from: " << call << std::endl;
         call->stream_->WritesDone(reinterpret_cast<void *>(Type::WRITES_DONE));
         return false;
      }

      // This is important: You can have at most one write or at most one read
      // at any given time. The throttling is performed by gRPC completion
      // queue. If you queue more than one write/read, the stream will crash.
      // Because this stream is bidirectional, you *can* have a single read
      // and a single write request queued for the same stream. Writes and reads
      // are independent of each other in terms of ordering/delivery.

      // Data we are sending to the server.
      if (call->request.has_message() == 0)
      {
         call->request.set_message(user);
         std::cout << " ** Sending request from: " << call << std::endl;
         call->stream_->Write(call->request, (void *)call);
      }
      return true;
   }

   // Out of the passed in Channel comes the stub, stored here, our view of the
   // server's exposed services.
   std::unique_ptr<GrpcHandler::Stub> stub_;

   // The producer-consumer queue we use to communicate asynchronously with the
   // gRPC runtime.
   CompletionQueue cq_;

   // Container for the data we send to the server.
   NdrResponse request;

   // Container for the data we expect from the server.
   NdrResponse reply;
};

int main(int argc, char **argv)
{
   // Give the number of bidirectional pools as input
   int poolCount = 1;

   // Instantiate the client. It requires a channel, out of which the actual RPCs
   // are created. This channel models a connection to an endpoint (in this case,
   // localhost at port 50055). We indicate that the channel isn't authenticated
   // (use of InsecureChannelCredentials()).
   GrpcHandlerClient greeter(grpc::CreateChannel(
                                 "localhost:50055", grpc::InsecureChannelCredentials()),
                             poolCount);

   // Spawn reader thread that loops indefinitely
   std::thread thread_ = std::thread(&GrpcHandlerClient::AsyncCompleteRpc, &greeter);

   // int threadCount = 2;
   std::thread t[poolCount];

   int clientId = 0;
   time_t now = time(0);
   std::string macString = "00a0bc";
   for (int i = 0; i < poolCount; i++)
   {
      clientId++;
      // round robin the clientId for every MAC
      if (clientId > poolCount)
      {
         clientId = 1;
      }
      // Just check client context at the start of the test before sending messages
      if (greeter.hashMap[clientId] == NULL)
      {
         std::cout << "Client " << greeter.hashMap[clientId] << "no more" << std::endl;
      }

      while (!greeter.hashMap[clientId]->serverConnected) {
         //Wait until server is connected before we start sending NDRs
      }
      std::cout << "Server connected for " << greeter.hashMap[clientId] << " and we are good to process NDRs" << std::endl;
      t[i] = std::thread(&GrpcHandlerClient::sendMessage, &greeter, macString, i * 1000, greeter.hashMap[clientId]); // The actual RPC call!
                                                                                                                        // greeter.sendMessage(macString + to_string(j), greeter.hashMap[clientId]->call);
   }
   // Join the threads with the main thread
   for (int i = 0; i < poolCount; ++i)
   {
      std::cout << "Message processing for " << greeter.hashMap[clientId] << " complete" << std::endl;
      t[i].join();
   }
   time_t then = time(0);
   std::cout << "Time difference for processing is all messages is " << then-now << " seconds" << std::endl;

   // Verify client context is closed at the end of the run and there no bidirectional streams anymore
   for (auto i : greeter.hashMap)
   {
      std::cout << i.first << ":" << i.second << ":" << std::endl;
      if (i.second == NULL)
      {
         std::cout << "Client " << i.second << " no more" << std::endl;
      }
      else
      {
         i.second->stream_->Finish(&i.second->finish_status_, (void *)i.second);
         std::cout << "Client " << i.second << " no more after calling finish" << std::endl;
      }
   }

   std::cout << "All bidirectional streams closed successfully" << std::endl;

   // Below is the block for AsyncComplete queue which blocks for ever until we have a client restart..
   std::cout << "Press control-c to quit" << std::endl
             << std::endl;
   thread_.join(); // blocks forever

   return 0;
}
