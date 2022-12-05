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

struct RawClosure: public braft::Closure {
    std::string*    result {nullptr};
    braft::Closure* done;
    
    RawClosure(std::string* result, braft::Closure* done)
        : result(result), done(done)
    {}

    void set_result(const std::string& res) {
        *result = res;
    }

    bool has_result() {
        return result != nullptr;
    }
    
    void set_status(butil::Status& s) {
        status() = s;
    }
    
    void Run() {
        if (!status().ok()) {
            DB_WARNING("KV request error, errmsg: %s", status().error_cstr());
        }
        if (done) {
            done->status() = status();
            done->Run();
        }
        delete this;
    }
};

struct OnRPCDone: public braft::Closure {
public:
   
   OnRPCDone(int64_t region_id, std::shared_ptr<BatchData> batch_data)
       : region_id(region_id), batch_data(batch_data)
   {}
   
   /* request 请求成功 */
   void on_success();
   /* request 请求失败 */
   void on_failed();
    
   virtual void Run() override;

public:
   int64_t region_id;
   std::shared_ptr<BatchData> batch_data;

   int64_t               start_ts;
   int64_t               reatry_times;
   brpc::Controller      controller; 
   braft::Closure*       done;
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
    void send_request(const std::string& addr,  OnRPCDone* done);
    
private:
    bthread_mutex_t _mutex;
    // address -> channel
    std::map<std::string, brpc::Channel*> _channels;
};
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
