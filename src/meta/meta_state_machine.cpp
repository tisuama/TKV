#include "meta/meta_state_machine.h"
#include "meta/cluster_manager.h"
#include <braft/util.h>

namespace TKV {

void MetaStateMachine::on_apply(braft::Iterator& iter) {
    for (; iter.valid(); iter.next()) {
        braft::Closure* done = iter.done();
        brpc::ClosureGuard done_guard(done);
        if (done) {
            auto meta_done = static_cast<MetaServerClosure*>(done);
            // try apply -> on_apply time cost
            meta_done->raft_time_cost = meta_done->time_cost.get_time();
        }
        butil::IOBufAsZeroCopyInputStream wrapper(iter.data());
        pb::MetaManagerRequest request;
        if (!request.ParseFromZeroCopyStream(&wrapper)) {
            DB_FATAL("parse from protobuf failed when on apply in MetaStateMachine");
            if (done) {
                auto meta_done = static_cast<MetaServerClosure*>(done);
                if (meta_done->response) {
                    meta_done->response->set_errcode(pb::PARSE_FROM_PB_FAIL);
                    meta_done->response->set_errmsg("parse from protobuf failed");
                }
                braft::run_closure_in_bthread(done_guard.release());
            }
            continue;
        }
        if (done && static_cast<MetaServerClosure*>(done)->response) {
            static_cast<MetaServerClosure*>(done)->response->set_op_type(request.op_type());
        }
        DB_DEBUG("on_apply, term: %ld, index: %ld, request op_type: %s", 
                iter.term(), iter.index(), pb::OpType_Name(request.op_type()).c_str());
        switch (request.op_type()) {
        case pb::OP_ADD_INSTANCE:
            ClusterManager::get_instance()->add_instance(request, done);
            break;
        default:
            DB_FATAL("unsupport request op_type, type: %s", request.op_type());
            IF_DONE_SET_RESPONSE(done, pb::UNSUPPORT_REQ_TYPE,  "unsupport request type");
        } 
        _applied_index = iter.index();
        if (done) {
            braft::run_closure_in_bthread(done_guard.release());
        }
    }
}

void MetaStateMachine::on_snapshot_save(braft::SnapshotWriter* writer,
                              braft::Closure* done) {
    // 
}

int MetaStateMachine::on_snapshot_load(braft::SnapshotReader* reader) {
    // 
    return 0;
}

void MetaStateMachine::store_heartbeat(::google::protobuf::RpcController* controller,
                       const ::TKV::pb::StoreHBRequest* request,
                       ::TKV::pb::StoreHBResponse* response,
                       ::google::protobuf::Closure* done) {
    TimeCost time_cost;
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
    uint64_t log_id = 0; 
    if (cntl->has_log_id()) {
        log_id = cntl->log_id();
    }
    if (!_is_leader.load()) {
        DB_WARNING("NOT LEADER, log_id: %lu", log_id);
        response->set_errcode(pb::NOT_LEADER);
        response->set_errmsg("not leader");
        response->set_leader(_node.leader_id().to_string());
        return;
    } 
    response->set_errcode(pb::SUCCESS);
    response->set_errmsg("sucess");
    
    DB_DEBUG("TKV store %s heart beat, time cost: %ld log_id: %lu",
            request->instance_info().address().c_str(), time_cost.get_time(), log_id);
    
    // 判断Instance信息
    // 判断Peer信息
    // 判断Table信息
    // 判断Peer所在Table
    // 更新Leader信息
}


void MetaStateMachine::on_leader_start() {
    //
}

void MetaStateMachine::on_leader_stop() {
    //
}

} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
