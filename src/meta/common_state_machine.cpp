#include "meta/common_state_machine.h"

namespace TKV {
DECLARE_string(meta_server_bns);
DECLARE_int32(meta_replica_number);

DEFINE_int32(snapshot_interval_s, 600, "raft snapshot interval");
DEFINE_int32(election_timeout_ms, 1000, "raft election timeout(ms)");
// DEFINE_string(log_uri, "myraftlog://my_raft_log?id=", "raft log uri");
DEFINE_string(log_uri, "local://./raft_data/raft_log", "raft log uri");
DEFINE_string(stable_uri, "local://./raft_data/stable", "raft stable uri");
DEFINE_string(snapshot_uri, "local://./raft_data/snapshot", "raft snapshot uri");
DEFINE_int64(check_migrate_interval_us, 60 * 1000 * 1000LL, "check meta server migrate interval(60)");
    
int CommonStateMachine::init(const std::vector<braft::PeerId>& peers) {
    braft::NodeOptions options;
    options.election_timeout_ms = FLAGS_election_timeout_ms;
    options.fsm = this;
    options.initial_conf = braft::Configuration(peers);
    options.snapshot_interval_s = FLAGS_snapshot_interval_s;
    options.log_uri = FLAGS_log_uri;
    options.raft_meta_uri = FLAGS_stable_uri;
    options.snapshot_uri = FLAGS_snapshot_uri;
    int ret = _node.init(options);
    if (ret < 0) {
        DB_FATAL("raft node init fail");
        return ret;
    }
    DB_WARNING("raft init sucess, meta state machine init sucess");
    return 0;
}

void CommonStateMachine::start_check_migrate() {
    
    // 
}

void CommonStateMachine::check_migrate() {
    
    //
}

void CommonStateMachine::on_apply(braft::Iterator& iter) {
    // 
} 

void CommonStateMachine::on_snapshot_save(braft::SnapshotWriter* writer, braft::Closure* done) {
    // 
}

int CommonStateMachine::on_snapshot_load(braft::SnapshotReader* reader) {
    // 
    return 0;
}

void CommonStateMachine::on_leader_start() {
    // 
}

void CommonStateMachine::on_leader_start(int64_t term) {
    // 
}

void CommonStateMachine::on_leader_stop() {
    // 
}

void CommonStateMachine::on_leader_stop(const butil::Status& status) {
    //
}

void CommonStateMachine::on_error(const braft::Error& e) {
    // 
}

void CommonStateMachine::on_configuration_committed(const braft::Configuration& conf) {
    // 
}

void CommonStateMachine::start_check_bns() {
    // 
}

int CommonStateMachine::send_set_peer_request(bool remove_peer, 
                        const std::string& change_peer) {
    return 0;
}

} // mamespace TKV

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
