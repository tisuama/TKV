#include "meta/cluster_manager.h"
#include "common/common.h"
#include "proto/optype.pb.h"

namespace TKV {
using braft::Closure;
void ClusterManager::process_cluster_info(google::protobuf::RpcController* controller,
                        const meta_req* request,
                        meta_res* response, 
                        google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    uint64_t log_id = 0;
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
    if (cntl->has_log_id()) {
        log_id = cntl->log_id();
    }
    switch(request->op_type()) {
    case pb::OP_ADD_INSTANCE:
        if (!request->has_instance()) {
            ERROR_SET_RESPONSE(response, pb::INPUT_PARAM_ERROR, "no instance info", request->op_type(), log_id);
            return; 
        }
       // _meta_state_machine->process(controller, request, response, done_guard.release());
        return;
    default:
        ERROR_SET_RESPONSE(response, pb::INPUT_PARAM_ERROR, "op_type is not support", request->op_type(), log_id);
        return ;
    }
}

// called when on_apply
void ClusterManager::add_instance(const meta_req& request, Closure* done) {

} 
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
