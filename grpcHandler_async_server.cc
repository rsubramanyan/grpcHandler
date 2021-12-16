#include <unistd.h>
#include <memory>
#include <iostream>
#include <string>
#include <thread>
#include <vector>
#include <random>
#include <atomic>
#include <unordered_map>
#include <queue>

#include <grpc++/grpc++.h>
#include <grpc/support/log.h>
#include <grpcpp/alarm.h>

#include "grpcHandler.grpc.pb.h"

using grpc::Alarm;
using grpc::Server;
using grpc::ServerAsyncReaderWriter;
using grpc::ServerAsyncResponseWriter;
using grpc::ServerBuilder;
using grpc::ServerCompletionQueue;
using grpc::ServerContext;
using grpc::Status;
using grpc::StatusCode;
using grpcHandler::GrpcHandler;
using grpcHandler::NdrResponse;
using namespace std;

int g_thread_num = 1;
int g_cq_num = 1;
int g_pool = 1;
int g_port = 50051;
std::string g_varied = "false";
int g_readTimeout = 100;
int hoPrepCount = 0;
int randomCount = 1;

class BidiCallData;

std::atomic<void *> **g_instance_pool = nullptr;
// queue of ho preps to process and release <tag, message>
std::queue<pair<void *, std::string>> globalQueue;
// map of all streams
unordered_map<void *, BidiCallData *> hashMap;

class CallDataBase
{
public:
  CallDataBase(GrpcHandler::AsyncService *service, ServerCompletionQueue *cq) : service_(service), cq_(cq)
  {
  }

  virtual void Proceed(bool ok) = 0;

protected:
  // The means of communication with the gRPC runtime for an asynchronous
  // server.
  GrpcHandler::AsyncService *service_;
  
  // The producer-consumer queue where for asynchronous server notifications.
  ServerCompletionQueue *cq_;

  // Context for the rpc, allowing to tweak aspects of it such as the use
  // of compression, authentication, as well as to send metadata back to the
  // client.
  ServerContext ctx_;

  // What we get from the client.
  NdrResponse request_;

  // What we send back to the client.
  NdrResponse reply_;
};

class BidiCallData : CallDataBase
{

public:
   // queue of messages to be sent
   std::queue<std::string> localQueue;

   // Take in the "service" instance (in this case representing an asynchronous
   // server) and the completion queue "cq" used for asynchronous communication
   // with the gRPC runtime.
   BidiCallData(GrpcHandler::AsyncService *service, ServerCompletionQueue *cq) : CallDataBase(service, cq), rw_(&ctx_)
   {
      // Invoke the serving logic right away.
      status_ = StreamStatus::CONNECT;

      ctx_.AsyncNotifyWhenDone((void *)this);
      service_->RequestSayHelloNew(&ctx_, &rw_, cq_, cq_, (void *)this);
   }

   void Proceed(bool ok)
   {
      std::unique_lock<std::mutex> _wlock(this->m_mutex);

      switch (status_)
      {
         case StreamStatus::CONNECT:
            std::cout << "thread:" << std::this_thread::get_id() << " tag:" << this << " CONNECT: client connected" << std::endl;
            // add to the hashmap, so its localQueue can be accessed by the main thread
            hashMap[(void *)this] = new BidiCallData(service_, cq_);
            rw_.Read(&request_, (void *)this);
            status_ = StreamStatus::PROCESS;
            activeRead = true;
            break;

         case StreamStatus::PROCESS:
            // TODO - implement this shutdown behavior somewhere else
            // Meaning client said it wants to end the stream either by a 'writedone' or 'finish' call.
            // if (!ok)
            // {
            //   std::cout << "thread:" << std::this_thread::get_id() << " tag:" << this << " READ: CQ returned false." << std::endl;
            //   Status _st(StatusCode::OUT_OF_RANGE, "test error msg");
            //   status_ = StreamStatus::DONE;
            //   rw_.Finish(_st, (void *)this);
            //   std::cout << "thread:" << std::this_thread::get_id() << " tag:" << this << " READ: after call Finish(), cancelled:" << this->ctx_.IsCancelled() << std::endl;
            //   break;
            // }

            if (request_.has_message())
            {
               // Process the request from the client
               std::cout << "thread:" << std::this_thread::get_id() << " tag:" << this << " READ: Read a new message:" << request_.message() << std::endl;
               // simulate the generation of responses
               if (randomCount % 2 == 1 && g_varied == "true")
               {
                  globalQueue.push({(void *)this, "HO PREP"});
                  globalQueue.push({(void *)this, "HO PREP"});
                  globalQueue.push({(void *)this, "HO PREP"});
               }
               else
               {
                  globalQueue.push({(void *)this, "HO PREP"});
               }
               randomCount++;
               
               // Prepare to read another request, but don't start a READ (prioritize WRITE's)
               request_.clear_message();
               activeRead = false;
            } else {
               if (activeRead)
               {
                  std::cout << "READ timed out." << std::endl;
               }
            }

            if (!hashMap[(void *)this]->localQueue.empty())
            {
               // WRITE a message from the queue
               std::cout << "thread:" << std::this_thread::get_id() << " tag:" << this << " WRITE: Sending HO Prep to client" << std::endl;
               reply_.set_message(hashMap[(void *)this]->localQueue.front());
               hashMap[(void *)this]->localQueue.pop();
               rw_.Write(reply_, (void *)this);
            }
            else
            {
               // Start a READ if one isn't already active
               if (!activeRead)
               {
                  activeRead = true;
                  rw_.Read(&request_, (void *)this);
               }
            }
            // Push the call to the back of the completion queue
            status_ = StreamStatus::PUSH_TO_BACK;
            break;

         case StreamStatus::PUSH_TO_BACK:
            // Pushing the call to the back of the completion queue ensures
            // all streams are handled in a round-robin fashion
            status_ = StreamStatus::PROCESS;
            PutTaskBackToQueue();
            break;
         
         /* TODO - implement finish
         case StreamStatus::FINISH:
            std::cout << "thread:" << std::this_thread::get_id() << "tag:" << this << " FINISH: Server finish, cancelled:" << this->ctx_.IsCancelled() << std::endl;
            _wlock.unlock();
            delete this;
            break;
         */

         default:
            std::cerr << "ERROR: Unrecognized status: " << int(status_) << std::endl;
            assert(false);
      }
   }

private:
   // The means to get back to the client.
   ServerAsyncReaderWriter<NdrResponse, NdrResponse> rw_;

   // Let's implement a tiny state machine with the following states.
   enum class StreamStatus
   {
      CONNECT = 1,
      PROCESS = 2,
      PUSH_TO_BACK = 3,
      FINISH = 4
   };
   StreamStatus status_;

   // Whether the stream has an active READ on the completion queue
   bool activeRead = false;

   // alarm used to put tasks to the back of the completion queue
   std::unique_ptr<Alarm> alarm_;

   // puts tasks to the back of the completion queue
   void PutTaskBackToQueue()
   {
      alarm_.reset(new Alarm);
      alarm_->Set(cq_, gpr_now(gpr_clock_type::GPR_CLOCK_REALTIME), this);
   }

   std::mutex m_mutex;
};

class ServerImpl final
{
public:
  ~ServerImpl()
  {
    server_->Shutdown();

    // Always shutdown the completion queue after the server.
    for (const auto &_cq : m_cq)
      _cq->Shutdown();
  }

  // There is no shutdown handling in this code.
  void Run()
  {
    std::string server_address("0.0.0.0:" + std::to_string(g_port));

    ServerBuilder builder;
    // Listen on the given address without any authentication mechanism.
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    // Register "service_" as the instance through which we'll communicate with
    // clients. In this case it corresponds to an *asynchronous* service.
    builder.RegisterService(&service_);
    // Get hold of the completion queue used for the asynchronous communication
    // with the gRPC runtime.

    for (int i = 0; i < g_cq_num; ++i)
    {
      m_cq.emplace_back(builder.AddCompletionQueue());
    }

    // Finally assemble the server.
    server_ = builder.BuildAndStart();
    std::cout << "Server listening on " << server_address << std::endl;

    // Proceed to the server's main loop.
    std::vector<std::thread *> _vec_threads;

    for (int i = 0; i < g_thread_num; ++i)
    {
      int _cq_idx = i % g_cq_num;
      for (int j = 0; j < g_pool; ++j)
      {
        new BidiCallData(&service_, m_cq[_cq_idx].get());
      }

      _vec_threads.emplace_back(new std::thread(&ServerImpl::HandleRpcs, this, _cq_idx));
    }

    std::cout << g_thread_num << " working aysnc threads spawned" << std::endl;

    // listen to global queue for messages to appear
    while (true)
    {
      if (!globalQueue.empty())
      {
        std::cout << "   Incoming message: " << globalQueue.front().second << " from tag: " << globalQueue.front().first << std::endl;

        // add this message to this bidi's local queue
        hashMap[globalQueue.front().first]->localQueue.push(globalQueue.front().second);
        std::cout << "   Send to local queue: " << globalQueue.front().first << " Current Size : " << hashMap[globalQueue.front().first]->localQueue.size() << std::endl;
        globalQueue.pop();
      }
    }

    for (const auto &_t : _vec_threads)
      _t->join();
  }

private:
  // Class encompasing the state and logic needed to serve a request.

  // This can be run in multiple threads if needed.
  void HandleRpcs(int cq_idx)
  {
    // Spawn a new BidiCallData instance to serve new clients.
    void *tag; // uniquely identifies a request.
    bool ok;
    bool proceed = false;

    while (true)
    {
      // Block waiting to read the next event from the completion queue. The
      // event is uniquely identified by its tag, which in this case is the
      // memory address of a BidiCallData instance.
      // The return value of Next should always be checked. This return value
      // tells us whether there is any kind of event or cq_ is shutting down.
      std::chrono::time_point<std::chrono::system_clock> deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(g_readTimeout);
      switch (m_cq[cq_idx]->AsyncNext(&tag, &ok, deadline))
      {
         case grpc::CompletionQueue::TIMEOUT:
         //uncomment_to_debug std::cout << "TIMEOUT" << std::endl;
         break;
         case grpc::CompletionQueue::SHUTDOWN:
         std::cout << "SHUTDOWN" << std::endl;
         // TODO - implement shutdown procedure
         break;
         case grpc::CompletionQueue::GOT_EVENT:
         // uncomment_to_debug std::cout << "WORK_FOUND" << std::endl;
         proceed = true;
         break;
      }

      if (proceed)
      {
        CallDataBase *_p_ins = (CallDataBase *)tag;
        _p_ins->Proceed(ok);
      }
    }
  }

  std::vector<std::unique_ptr<ServerCompletionQueue>> m_cq;

  GrpcHandler::AsyncService service_;
  std::unique_ptr<Server> server_;
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
  if (argc != 6)
  {
    std::cout << "Usage:./program --thread=xx --cq=xx --port=xx --variedResponse=xx --readTimeout=xx";
    return 0;
  }

  g_thread_num = std::atoi(ParseCmdPara(argv[1], "--thread="));
  g_cq_num = std::atoi(ParseCmdPara(argv[2], "--cq="));
  g_pool = 1;
  g_port = std::atoi(ParseCmdPara(argv[3], "--port="));
  g_varied = ParseCmdPara(argv[4], "--variedResponse=");
  g_readTimeout = std::atoi(ParseCmdPara(argv[5], "--readTimeout="));

  ServerImpl server;
  server.Run();

  return 0;
}
