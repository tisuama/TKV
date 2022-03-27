#include "meta/meta_server.h"
#include "engine/rocks_wrapper.h"

namespace TKV {
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

int MetaServer::init(const std::vector<braft::PeerId>& peers) {
    auto _db = RocksWrapper::get_instance(); 
    _db->init("raft");
    return 0;
}
} //namespace of TKV

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
