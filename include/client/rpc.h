#pragma once
#include <string>

#include <braft/util.h>
#include <brpc/channel.h>
#include <gflags/gflags.h>
#include <butil/time.h>
#include <bthread/mutex.h>
#include <brpc/channel.h>

#include "proto/store.pb.h"
#include "common/log.h"


namespace TKV {

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
    template<typename T1, typename T2>
    int send_request(const std::string& addr,
        brpc::Controller* cntl,
        const T1* request,
        T2* response, 
        google::protobuf::Closure* done) {

        auto channel = get_conn(addr);
        if (channel == nullptr) {
            DB_FATAL("Get channel for addr: %s failed", addr.c_str());
            return -1;
        }
        uint64_t log_id = butil::fast_rand();
        cntl->set_log_id(log_id);
        
        DB_DEBUG("[send] request %s", request->ShortDebugString().c_str());    
        pb::StoreService_Stub stub(channel);
        stub.query(cntl, request, response, done);

        if (done == NULL && cntl->Failed()) {
            DB_FATAL("[resp] request %s cntl failed", cntl->ErrorText().c_str()); 
            return -1;
        }
        return 0;
    }
    

private:
    bthread_mutex_t _mutex;
    // address -> channel
    std::map<std::string, brpc::Channel*> _channels;
};
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
