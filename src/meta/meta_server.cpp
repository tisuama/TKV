#include "meta/meta_server.h"

namespace TKV {
void MetaServer::store_heartbeat(::google::protobuf::RpcController* controller,
     const ::TKV::pb::StoreHBRequest* request,
     ::TKV::pb::StoreHBResponse* response,
     ::google::protobuf::Closure* done) {
    // store heartbeat for MetaServer
}
} //namespace of TKV

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
