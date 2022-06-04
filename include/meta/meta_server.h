#pragma once
#include <braft/raft.h>
#include "proto/meta.pb.h"
#include "meta/meta_state_machine.h"
#include "meta/meta_server_interact.h"
#include "common/common.h"

namespace TKV {

class MetaServer: public pb::MetaService {
public:
    // Layer 1
    static const std::string CLUSTER_IDENTIFY;
    static const std::string INSTANCE_CLUSTER_IDENTIFY;
    static const std::string SCHEMA_IDENTIFY;
    static const std::string PRIVILEGE_IDENTIFY;
    
    // Layer 2
    static const std::string NAMESPACE_SCHEMA_IDENTIFY;
    static const std::string MAX_ID_SCHEMA_IDENTIFY;
    static const std::string DATABASE_SCHEMA_IDENTIFY;
    

    virtual ~MetaServer() {}
    
    int init(const std::vector<braft::PeerId>& peers);

    virtual void store_heartbeat(::google::protobuf::RpcController* controller,
                         const ::TKV::pb::StoreHBRequest* request,
                         ::TKV::pb::StoreHBResponse* response,
                         ::google::protobuf::Closure* done) override;

    virtual void meta_manager(::google::protobuf::RpcController* controller,
                       const ::TKV::pb::MetaManagerRequest* request,
                       ::TKV::pb::MetaManagerResponse* response,
                       ::google::protobuf::Closure* done) override;

    static MetaServer* get_instance() {
        static MetaServer meta_server;
        return &meta_server;
    }
private:
    MetaServer() {}
    bthread::Mutex _meta_interact_mutex;
    // std::map<std::string, MetaServerInteract*> _meta_interact_map;
    MetaStateMachine* _meta_state_machine = nullptr;
    Bthread _flush_bth;
    Bthread _apply_region_bth;
    
    std::string _physical_room;
    bool _init_sucess = false;
    bool _shutdown = false;
};

} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
