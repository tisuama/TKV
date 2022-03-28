#include "meta/meta_state_machine.h"

namespace TKV {
void MetaStateMachine::on_apply(braft::Iterator& iter) {
    for (; iter.valid(); iter.next()) {
        braft::Closure* done = iter.done();
        brpc::ClosureGuard done_guard(done);
        if (done) {
            auto meta_done = static_cast<MetaServerClosure*>(done);
            meta_done->raft_time_cost = meta_done->time_cost.get_time();
        }
        butil::IOBufAsZeroCopyInputStream wrapper(iter.data());

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
    brpc::ClosureGuard done_gurad(done);
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
    
    DB_NOTICE("TKV store %s heart beat, time cost: %ld log_id: %lu",
            request->instance_info().address().c_str(), time_cost.get_time(), log_id);

}
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
