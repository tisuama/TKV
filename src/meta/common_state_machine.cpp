#include "meta/common_state_machine.h"

namespace TKV {
DECLARE_string(meta_server_bns);
DECLARE_int32(meta_replica_number);
DECLARE_int32(snapshot_interval_s);
DECLARE_int32(election_timeout_ms);

DEFINE_string(log_uri, "local://./raft_data/raft_log", "raft log uri");
DEFINE_string(stable_uri, "local://./raft_data/stable", "raft stable uri");
DEFINE_string(snapshot_uri, "local://./raft_data/snapshot", "raft snapshot uri");
DEFINE_int64(check_migrate_interval_us, 60 * 1000 * 1000LL, "check meta server migrate interval(60)");

void MetaServerClosure::Run() {
    // 如果apply不成功，会设置status并回调
    // on_apply 一定是成功commit的
    if (!status().ok()) {
        if (response) {
           response->set_errcode(pb::NOT_LEADER); 
           response->set_leader(butil::endpoint2str(com_fsm->get_leader()).c_str());
        }
        DB_FATAL("meta server closure failed, error_code: %d, error_msg: %s", 
                status().error_code(), status().error_cstr());
        // don't return here
    }
    total_time_cost =  time_cost.get_time();
    std::string remote_side;
    if (cntl != nullptr) {
        remote_side = butil::endpoint2str(cntl->remote_side()).c_str();
    }
    if (response != nullptr) {
        DB_NOTICE("response: %s raft_time_cost: %ld, total_time_cost: %ld, remote_side: %ld", 
                response->ShortDebugString().c_str(), raft_time_cost, total_time_cost);
    }  
    if (done) {
        done->Run();
    }
    delete this;
}
    
int CommonStateMachine::init(const std::vector<braft::PeerId>& peers) {
    braft::NodeOptions options;
    options.election_timeout_ms = FLAGS_election_timeout_ms;
    options.fsm = this;
    // Can get replica num from peers
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

void CommonStateMachine::process(::google::protobuf::RpcController* controller,
                   const ::TKV::pb::MetaManagerRequest* request,
                   ::TKV::pb::MetaManagerResponse* response,
                   ::google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (!_is_leader.load()) {
        if (response) {
            response->set_errcode(pb::NOT_LEADER);
            response->set_errmsg("not leader");
            response->set_leader(butil::endpoint2str(_node.leader_id().addr).c_str());
        }
        DB_WARNING("common state machine is not leader, request: %s", request->ShortDebugString().c_str());
        return ;// RAII closure is run
    }
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
    butil::IOBuf data;
    butil::IOBufAsZeroCopyOutputStream wrapper(&data);
    if (!request->SerializeToZeroCopyStream(&wrapper) && cntl) {
        cntl->SetFailed("Fail to serialized request");
        return ;
    }
    MetaServerClosure* closure = new MetaServerClosure();
    closure->cntl = cntl;
    closure->response = response;
    closure->done = done_guard.release();
    closure->com_fsm = this;
    braft::Task task;
    task.data = &data;
    task.done = closure;
    // 异步apply
    _node.apply(task);
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
