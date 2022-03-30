#include "meta/meta_server.h"
#include "engine/rocks_wrapper.h"

namespace TKV {
DECLARE_int32(meta_port); 
DECLARE_int32(meta_replica_num);
DECLARE_bool(meta_with_any_ip);
DECLARE_string(meta_ip);

void MetaServer::store_heartbeat(::google::protobuf::RpcController* controller,
     const ::TKV::pb::StoreHBRequest* request,
     ::TKV::pb::StoreHBResponse* response,
     ::google::protobuf::Closure* done) {
    // store heartbeat for MetaServer
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
    uint64_t log_id = 0;
    if (cntl->has_log_id()) {
        log_id = cntl->log_id();
    }
    RETURN_IF_NOT_INIT(_init_sucess, response, log_id); 
    if (_meta_state_machine) {
        _meta_state_machine->store_heartbeat(controller, request, response, done_guard.release());
    }
}
/*
* 将请求分发给各个manager类
* Manager 包含两类工作: 1) 做一下判断和处理，然后再common_state_machine里做on_apply
*                       2) on_apply后分别实现op_type里的各个函数，做commit后的应用到状态机
*/
void meta_manager(::google::protobuf::RpcController* controller,
                   const ::TKV::pb::MetaManagerRequest* request,
                   ::TKV::pb::MetaManagerResponse* response,
                   ::google::protobuf::Closure* done) {
    // 
}

int MetaServer::init(const std::vector<braft::PeerId>& peers) {
    
    butil::EndPoint addr;
    if (FLAGS_meta_with_any_ip) {
        addr.ip = butil::my_ip();
    } else {
        butil::str2ip(FLAGS_meta_ip.data(), &addr.ip);
    }
    addr.port = FLAGS_meta_port;
    braft::PeerId peer_id(addr, 0);
    _meta_state_machine = new (std::nothrow)MetaStateMachine(peer_id);
    if (_meta_state_machine == NULL) {
        DB_FATAL("new meta state machine failed");
        return -1;
    }
    auto ret = _meta_state_machine->init(peers);
    if (ret != 0) {
        DB_FATAL("meta state machine init failed");
        return -1;
    }
    DB_WARNING("meta state machine init sucess");
     
    return 0;
}
} //namespace of TKV

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */