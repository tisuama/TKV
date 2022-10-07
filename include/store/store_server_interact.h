#pragma once
#include <butil/endpoint.h>
#include <brpc/channel.h>
#include <brpc/server.h>
#include <brpc/controller.h>

#include <google/protobuf/descriptor.h>
#include "proto/store.pb.h"
#include "common/common.h"

namespace TKV {
DECLARE_int32(store_request_timeout);
DECLARE_int32(store_connect_timeout);

struct StoreReqOptions {
    int32_t request_timeout;
    int32_t connect_timeout;
    int32_t retry_times;

    StoreReqOptions()
        : request_timeout(FLAGS_store_request_timeout)
        , connect_timeout(FLAGS_store_connect_timeout)
        , retry_times(3)
    {}

    StoreReqOptions(const StoreReqOptions& options)
        : request_timeout(options.request_timeout)
        , connect_timeout(options.connect_timeout)
        , retry_times(options.retry_times) 
    {}
};

class StoreInteract {
public:
    StoreInteract(const std::string& store_address)
        : _store_address(store_address)
        , _req_options(StoreReqOptions())
    {}
    StoreInteract(const std::string& store_address, const StoreReqOptions& options)
        : _store_address(store_address)
        , _req_options(options)
    {}

    template<typename Request, typename Response>
    int send_request(uint64_t log_id, 
            const std::string& service_name,
            const Request& request,
            Response& response, 
            butil::IOBuf* attactment_data = nullptr) {
        // 初始化channel
        brpc::ChannelOptions opt;
        opt.timeout_ms = _req_options.request_timeout;
        opt.connect_timeout_ms = _req_options.connect_timeout;
        brpc::Channel store_channel;
        if (store_channel.Init(_store_address.c_str(), &opt) != 0) {
            DB_FATAL("Store init channel fail, store address: %s", _store_address.c_str());
            response.set_errcode(pb::CONNECT_FAIL);
            return -1;
        }
        const ::google::protobuf::ServiceDescriptor* service_desc = pb::StoreService::descriptor();
        const ::google::protobuf::MethodDescriptor* method = service_desc->FindMethodByName(service_name);
        if (method == NULL) {
            DB_FATAL("service name %s not exist", service_name.c_str());
            return -1;
        }
        brpc::Controller cntl;
        cntl.set_log_id(log_id);
        if (attactment_data != nullptr) {
            cntl.request_attachment().append(*attactment_data);
        }
        store_channel.CallMethod(method, &cntl, &request, &response, NULL);
        if (cntl.Failed()) {
            DB_FATAL("Send request fail, error: %s, log_id: %lu",
                    cntl.ErrorText().c_str(), cntl.log_id());
            response.set_errcode(pb::EXEC_FAIL);
            return -1;
        }
        if (response.errcode() != pb::SUCCESS) {
            DB_WARNING("Send store address fail, errcode: %d, log_id: %lu, instance: %s, response: %s, request: %s",
                    response.errcode(),
                    cntl.log_id(), 
                    _store_address.c_str(), 
                    response.ShortDebugString().c_str(),
                    request.ShortDebugString().c_str());
            return -1;
        }
        return 0;
    }

    template<typename Request, typename Response>
    int send_request(const std::string& service_name,
            const Request& request,
            Response& response) {
        uint64_t log_id = butil::fast_rand();
        return send_request(log_id, service_name, request, response);
    }
    
    template<typename Request, typename Response>
    int send_request_for_leader(uint64_t log_id, 
            const std::string& service_name,
            const Request& request,
            Response& response,
            butil::IOBuf* attactment_data = nullptr) {
        int retry_times = 0;
        do {
            auto ret = send_request(log_id, service_name, request, response, attactment_data);
            if (!ret) {
                return 0;
            }
            if (response.errcode() != pb::NOT_LEADER) {
                return -1;
            }
            DB_WARNING("connect with store %s fail, not leader, redirect to %s"
                    "log_id: %lu", _store_address.c_str(), response.leader().c_str(), log_id);
            butil::EndPoint leader_addr;
            butil::str2endpoint(response.leader().c_str(), &leader_addr);
            if (leader_addr.ip == butil::IP_ANY) {
                return -1;
            }
            _store_address = response.leader();
            ++retry_times;
        } while (retry_times < _req_options.retry_times);
        return -1;
    }

    template<typename Request, typename Response>
    int send_request_for_leader(const std::string& service_name,
            const Request& request,
            Response& response) {
        uint64_t log_id = butil::fast_rand();
        return send_request_for_leader(log_id, service_name, request, response);
    }

private:
    std::string  _store_address;
    StoreReqOptions _req_options;
};

} // namespace TKV 
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
