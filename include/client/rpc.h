#pragma once
#include <string>

#include <braft/util.h>
#include <brpc/channel.h>
#include <gflags/gflags.h>
#include <butil/time.h>
#include <bthread/mutex.h>

#include "proto/store.pb.h"
#include "common/log.h"
#include "client/batch_data.h"


namespace TKV {
/* 请求超时、失败、重试等情况 */
struct AsyncSendMeta {
    int64_t region_id;
    std::shared_ptr<BatchData> batch_data;


    int64_t start_time_us;
    int64_t retry_time;
    brpc::Controller controller;


    AsyncSendMeta(int64_t region_id, std::shared_ptr<BatchData> batch_data)
        : region_id(region_id), batch_data(batch_data)
    {}

    /* request 请求成功 */
    void on_success();
    /* request 请求失败 */
    void on_failed();
    
};

struct AsyncSendClosure: public braft::Closure {
   AsyncSendMeta* meta;
   braft::Closure* done;
   
   AsyncSendClosure(AsyncSendMeta* meta)
       : meta(meta)
   {}
   
   ~AsyncSendClosure() {
       if (meta) {
           delete meta;
       }
   }

   virtual void Run() override;
};

class RpcClient {
public:
    RpcClient() {
        bthread_mutex_init(&_mutex, NULL);
    }

    ~RpcClient() {
        bthread_mutex_destroy(&_mutex);
    }

    brpc::Channel* get_conn(const std::string& addr);
    brpc::Channel* create_conn(const std::string& addr);

    // TKVStore
    void send_request(const std::string& addr,  
                      AsyncSendMeta* meta,
                      AsyncSendClosure* done);
    
private:
    bthread_mutex_t _mutex;
    // address -> channel
    std::map<std::string, brpc::Channel*> _channels;
};
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
