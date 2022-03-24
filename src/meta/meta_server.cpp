#include "meta/meta_server.h"
#include "engine/rocks_wrapper.h"

namespace TKV {
void MetaServer::store_heartbeat(::google::protobuf::RpcController* controller,
     const ::TKV::pb::StoreHBRequest* request,
     ::TKV::pb::StoreHBResponse* response,
     ::google::protobuf::Closure* done) {
    // store heartbeat for MetaServer
}

int MetaServer::init(const std::vector<braft::PeerId>& peers) {
    auto _db = RocksWrapper::get_instance(); 
    _db->init("raft");
    return 0;
}
} //namespace of TKV

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
