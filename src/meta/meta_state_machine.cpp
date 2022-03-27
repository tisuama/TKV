#include "meta/meta_state_machine.h"

namespace TKV {
void MetaStateMachine::on_apply(braft::Iterator& iter) {
    // 
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
    brpc::ClosureGuard done_gurad(done);
    //
}
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
