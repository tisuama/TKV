#pragma once
#include <bthread/mutex.h>
#include <brpc/channel.h>
#include <brpc/server.h>
#include <brpc/controller.h>
#include <google/protobuf/descriptor.h>
#include "proto/meta.pb.h"
#include "common/log.h"


namespace TKV {
DECLARE_int64(time_between_meta_connect_error_ms);

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
        DB_DEBUG("send request for service name: %s", service_name.c_str());
        const ::google::protobuf::ServiceDescriptor* service_desc = pb::MetaService::descriptor();
        const ::google::protobuf::MethodDescriptor* method = service_desc->FindMethodByName(service_name);
        if (method == NULL) {
            DB_FATAL("service name not exist, exit now");
            return -1;
        }
        int retry_time = 0;
        uint64_t log_id = butil::fast_rand();
        do {
            if (retry_time > 0 && FLAGS_time_between_meta_connect_error_ms > 0) {
                bthread_usleep(1000 * FLAGS_time_between_meta_connect_error_ms);
            }
            brpc::Controller cntl;
            cntl.set_log_id(log_id);
            std::unique_lock<bthread_mutex_t> lock(_master_leader_mutex);
            butil::EndPoint leader_address = _master_leader_address;
            lock.unlock();
            if (leader_address.ip != butil::IP_ANY) {
                brpc::ChannelOptions channel_opt;
                channel_opt.timeout_ms = _request_timeout;
                channel_opt.connect_timeout_ms = _connect_timeout;
                brpc::Channel short_channle;
                if (short_channle.Init(leader_address, &channel_opt)) {
                    DB_WARNING("connect with meta server fail, channel init fail, leader addr: %s", 
                            butil::endpoint2str(leader_address).c_str());
                    ++retry_time;
                    continue;
                }
                DB_DEBUG("send request by short channel, request: %s", request.ShortDebugString().c_str());
                short_channle.CallMethod(method, &cntl, &request, &response, NULL);
            } else {
                _bns_channel.CallMethod(method, &cntl, &request, &response, NULL);
                if (!cntl.Failed() && response.errcode() == pb::SUCCESS) {
                    set_leader_address(cntl.remote_side());
                    DB_WARNING("connet with meta server by bns name, leader: %s",
                            butil::endpoint2str(cntl.remote_side()).c_str());
                    return 0;
                }
            }
            if (cntl.Failed()) {
                DB_WARNING("connect with server failed, send request failed, error: %s log_id: %lu",
                        cntl.ErrorText().c_str(), cntl.log_id());
                set_leader_address(butil::EndPoint());
                ++retry_time;
                continue;
            }
            if (response.errcode() == pb::HAVE_NOT_INIT) {
                DB_WARNING("connect with server failed, HAVE NOT INIT, log_id: %lu",
                        cntl.log_id());
                set_leader_address(butil::EndPoint());
                ++retry_time;
                continue;
            }
            if (response.errcode() == pb::NOT_LEADER) {
                DB_WARNING("connect with server: %s failed, not leader, redirect to: %s log_id: %lu",
                        butil::endpoint2str(cntl.remote_side()).c_str(),
                        response.leader().c_str(), cntl.log_id());
                butil::EndPoint leader_addr;
                butil::str2endpoint(response.leader().c_str(), &leader_addr);
                set_leader_address(leader_addr);
                ++retry_time;
                continue;
            }
            if (response.errcode() != pb::SUCCESS) {
                DB_WARNING("send meta server failed, response: %s, log_id: %lu", 
                        response.ShortDebugString().c_str(), cntl.log_id());
                return -1;
            } else {
                return 0;
            }
        } while (retry_time < RETRY_TIMES);
        return -1;
    }
    
    void set_leader_address(const butil::EndPoint& addr) {
        std::unique_lock<bthread_mutex_t> lock(_master_leader_mutex);
        _master_leader_address = addr;
    }

private:
    MetaServerInteract()  { bthread_mutex_init(&_master_leader_mutex, NULL); }
    ~MetaServerInteract() { bthread_mutex_destroy(&_master_leader_mutex); }  

    brpc::Channel _bns_channel;
    int32_t _request_timeout = 30000;
    int32_t _connect_timeout = 5000;
    bool    _is_init {false};
    bthread_mutex_t _master_leader_mutex;
    butil::EndPoint _master_leader_address;
};
} // namespace TKV 

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */

