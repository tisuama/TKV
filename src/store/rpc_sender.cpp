#include "store/rpc_sender.h"
#include "proto/store.pb.h"

namespace TKV {
int RpcSender::send_no_op_request(const std::string& instance, 
        int64_t recevie_region_id,
        int64_t request_version) {
    int ret = 0;
    for (int i = 0; i < 5; i++) {
        pb::StoreReq req;
        req.set_op_type(pb::OP_NONE);
        req.set_region_id(recevie_region_id);
        req.set_region_version(request_version);
        ret = send_query_method(req, instance, recevie_region_id);
        if (ret < 0) {
            DB_WARNING("send no op fail, region_id: %ld, request: %s",
                    recevie_region_id, req.ShortDebugString().c_str());
            bthread_usleep(1000 * 1000LL);
        }
    }
    return ret;
}

int RpcSender::send_query_method(const pb::StoreReq& req, 
        const std::string& instance, int64_t recevie_region_id) {
    uint64_t log_id = butil::fast_rand();
    StoreInteract store_interact(instance);
    pb::StoreRes res;
    int ret = store_interact.send_request_for_leader(log_id, "query", req, res);
    return ret;
}

void RpcSender::get_peer_snapshot_size(const std::string& peer, int64_t region_id,
        uint64_t* data_size, uint64_t* meta_size, int64_t* snapshot_index) {
    pb::GetAppliedIndex request;
    request.set_region_id(region_id);
    pb::StoreRes response;
    
    StoreInteract store_interact(peer);
    int ret = store_interact.send_request("get_applied_index", request, response);
    if (ret == 0) {
        *data_size = response.region_raft_stat().snapshot_data_size();
        *meta_size = response.region_raft_stat().snapshot_meta_size();
        *snapshot_index = response.region_raft_stat().snapshot_index();
    }
}
} // namespace TKV 
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
