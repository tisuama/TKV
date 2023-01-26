#pragma once 
#include "store/store_server_interact.h"
#include "proto/store.pb.h"

namespace TKV {
class RpcSender {
public:
    static int send_no_op_request(const std::string& instance, 
            int64_t recevie_region_id,
            int64_t request_version);
    static int send_query_method(const pb::StoreReq& req, 
            const std::string& instance, int64_t recevie_region_id);
    static int send_init_region_method(const std::string& instance_address,
            const pb::InitRegion& init_region_request,
            pb::StoreRes& response);
};
} // namespace TKV 
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
