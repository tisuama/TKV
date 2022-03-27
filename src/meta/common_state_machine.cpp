#include "meta/common_state_machine.h"

namespace TKV {
DECLAER_string(meta_server_bns);
DECLARE_int32(meta_replica_number);

DEFINE_int32(snapshot_interval_s, 600, "raft snapshot interval");
DEFINE_int32(election_timeout_ms, 1000, "raft election timeout(ms)");
// DEFINE_string(log_uri, "myraftlog://my_raft_log?id=", "raft log uri");
DEFINE_string(log_uri, "local://./raft_data/log", "raft log uri");
DEFINE_string(stable_uri, "local://./raft_data/stable", "raft stable uri");
DEFINE_string(snapshot_uri, "local://./raft_data/snapshot", "raft snapshot uri");
DEFINE_int64(check_migrate_interval_us, 60 * 1000 * 1000LL, "check meta server migrate interval(60)");
    
virtual int CommonStateMachine::init(const std::vector<braft::PeerId>& peers) {
    braft::NodeOptions options;
    options.election_timeout_ms = FLAGS_election_timeout_ms;
    options.fsm = this;
    options.initial = braft::Configuration(peers);
    options.snapshot_interval_s = FLAGS_snapshot_interval_s;
    options.log_uri = FLAGS_log_uri + std::to_string(_dummy_region_id);
    opitons.raft_meta_uri = FLAGS_stable_uri + _file_path;
    options.snapshot_uri = FLAGS_snapshot_uri + _file_path;
    int ret = _node.init(options);
    if (ret < 0) {
        DB_FATAL("raft node init fail");
        return ret;
    }
    DB_WARNING("raft init sucess, meta state machine init sucess");
    return 0;
}

virtual void raft_control(google::protobuf::RpcController* controller,
        const pb::RaftControlRequest* request,
        pb::RaftControlResponse* response,
        google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    //  
} 
virtual void process(google::protobuf::RpcController* controller,
        const pb::MetaManagerRequest* request, 
        pb::MetaManagerResponse* response,
        google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    // 
}
virtual void start_check_migrate() {
    
    // 
}
virtual void check_migrate() {
    
    //
}
virtual void on_apply(braft::Iterator& iter) {
    // 
} 

virtual void on_snapshot_save(braft::SnapshotWriter* writer, braft::Closure* done) {
    // 
}
virtual int on_snapshot_load(braft::SnapshotReader* reader) {
    // 
}
virtual void on_leader_start() {
    // 
}
virtual void on_leader_start(int64_t term) {
    // 
}
virtual void on_leader_stop() {
    // 
}
virtual void on_leader_stop(const butil::Status& status) {
    //
}
virtual void on_error(const butil::Error& e) {
    // 
}
virtual void on_configuration_committed(const braft::Configuration& conf) {
    // 
}
void start_check_bns() {
    // 
}
} // mamespace TKV

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
