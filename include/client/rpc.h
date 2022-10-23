#pragma once
#include <string>

#include <brpc/channel.h>
#include <gflags/gflags.h>
#include <butil/time.h>
#include <bthread/mutex.h>

#include <TKV/store.pb.h>
#include <TKV/log.h>


namespace TKV {

struct AsyncSendMeta {
    StoreReq* request;
    StoreRes* response;
    brpc::Controller controller;

    int64_t region_id;
    int64_t start_time_us;


    AsyncSendMeta(StoreReq* request, StoreRes* res)
        : request(request), response(response) 
    {}
    
    ~AsyncSendMeta() {
        if (request) {
            delete request;
        }
        if (response) {
            delete response;
        }
    }
};

struct AsyncSendClosure: public braft::Closure {
   AsyncSendMeta* meta;
   google::Closure* done;
   
   AsyncSendClosure(AsyncSendMeta* meta, google::Closure* done)
       : meta(meta), done(done) 
   {}

   virtual void Run() override;
};

class RpcClient {
    RpcClient* get_instance() {
        static RpcClient instance;
        return &instance;
    }

    brpc::Channel* get_conn(const std::string& addr);
    brpc::Channel* create_conn(const std::string& addr);

    template<typename T>
    void send_request(const std::string& addr,  
                      AsyncSendMeta* meta,
                      google::Closure* done);
    
private:
    bthread_mutex_t _mutex;
    // address -> channel
    std::map<std::string, brpc::Channel*> _channels;
};
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
