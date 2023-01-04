#pragma once
#include <string>

#include <braft/util.h>
#include <brpc/channel.h>
#include <gflags/gflags.h>
#include <butil/time.h>
#include <bthread/mutex.h>

#include "proto/store.pb.h"
#include "common/log.h"


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
    // void send_request(const std::string& addr);  
    
private:
    bthread_mutex_t _mutex;
    // address -> channel
    std::map<std::string, brpc::Channel*> _channels;
};
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
