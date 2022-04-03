#pragma once
#include <brpc/channel.h>
#include <brpc/server.h>
#include <brpc/controller.h>
#include "proto/meta.pb.h"

namespace TKV {
class MetaServerInteract {
public:
    static const int RETRY_TIMES = 5;
    static MetaServerInteract* get_instance() {
        static MetaServerInteract instance;
        return &instance;
    }
    
    bool is_inited() {
        return _is_init;
    }

    int init();
    int init_internal(const std::string& meta_bns);
    
    // template impl send_request
    template<typename Request, typename Response>
    int send_request(const std::string& service_name, const Request& request, Response& response) {

    }

private:
    MetaServerInteract() {}
    
    brpc::Channel _bns_channel;
    int32_t _request_time_out = 30000;
    int32_t _connnect_time_out = 5000;
    bool    _is_init {false};
    std::mutex _master_leader_mutex;
    butil::EndPoint _master_leader_address;
};
} // namespace TKV 

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */

