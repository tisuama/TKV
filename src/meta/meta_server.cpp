#include "meta/meta_server.h"
#include "engine/rocks_wrapper.h"
#include "meta/cluster_manager.h"
#include "meta/meta_rocksdb.h"
#include "meta/privilege_manager.h"
#include "meta/schema_manager.h"
#include "meta/namespace_manager.h"
#include "meta/table_manager.h"
#include "meta/database_manager.h"
#include "meta/meta_state_machine.h"
#include "meta/tso_state_machine.h"

#include "meta/query_region_manager.h"

namespace TKV {
DECLARE_int32(meta_port); 
DECLARE_int32(meta_replica_num);
DECLARE_bool(meta_with_any_ip);
DECLARE_string(meta_ip);
DECLARE_string(default_physical_room);

const std::string MetaServer::CLUSTER_IDENTIFY(1, 0x1);
const std::string MetaServer::SCHEMA_IDENTIFY(1, 0x02);
const std::string MetaServer::PRIVILEGE_IDENTIFY(1, 0x03);
const std::string MetaServer::MAX_IDENTIFY(1, 0xFF);

const std::string MetaServer::MAX_ID_SCHEMA_IDENTIFY(1, 0x01);
const std::string MetaServer::NAMESPACE_SCHEMA_IDENTIFY(1, 0x02);
const std::string MetaServer::DATABASE_SCHEMA_IDENTIFY(1, 0x03);
const std::string MetaServer::TABLE_SCHEMA_IDENTIFY(1, 0x04);
const std::string MetaServer::REGION_SCHEMA_IDENTIFY(1, 0x05);

const std::string MetaServer::LOGICAL_CLUSTER_IDENTIFY(1, 0x1);
const std::string MetaServer::PHYSICAL_CLUSTER_IDENTIFY(1, 0x2);
const std::string MetaServer::INSTANCE_CLUSTER_IDENTIFY(1, 0x3);
const std::string MetaServer::INSTANCE_PARAM_CLUSTER_IDENTIFY(1, 0x4);

const std::string MetaServer::LOGICAL_KEY = "logical_room";

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
void MetaServer::meta_manager(::google::protobuf::RpcController* controller,
                   const ::TKV::pb::MetaManagerRequest* request,
                   ::TKV::pb::MetaManagerResponse* response,
                   ::google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
    uint64_t log_id = 0;
    if (cntl->has_log_id()) {
        log_id = cntl->log_id();
    }     
    RETURN_IF_NOT_INIT(_init_sucess, response, log_id);
    DB_DEBUG("meta manager request: %s, response: %p",  request->ShortDebugString().c_str(), response);
    if (request->op_type() == pb::OP_ADD_INSTANCE || 
        request->op_type() == pb::OP_ADD_PHYSICAL || 
        request->op_type() == pb::OP_ADD_LOGICAL) {
        ClusterManager::get_instance()->process_cluster_info(controller, request, response, done_guard.release());
        return ;
    } else if (request->op_type() == pb::OP_CREATE_USER) {
        PrivilegeManager::get_instance()->process_user_privilege(controller, request, response, done_guard.release()); 
        return;
    } else if (request->op_type() == pb::OP_CREATE_NAMESPACE) {
        SchemaManager::get_instance()->process_schema_info(controller, request, response, done_guard.release());
        return ;
    } else if (request->op_type() == pb::OP_CREATE_DATABASE) {
        SchemaManager::get_instance()->process_schema_info(controller, request, response, done_guard.release());
        return ;
    } else if (request->op_type() == pb::OP_CREATE_TABLE) {
        SchemaManager::get_instance()->process_schema_info(controller, request, response, done_guard.release());
        return ;
    } else {
        DB_WARNING("unknow op_type, request: %s", request->ShortDebugString().c_str());
    }
    response->set_errcode(pb::INPUT_PARAM_ERROR);
    response->set_errmsg("invalid op_type");
    response->set_op_type(request->op_type());
}

int MetaServer::init(const std::vector<braft::PeerId>& peers) {
    // init rocksdb
    auto ret = MetaRocksdb::get_instance()->init();
    if (ret < 0) {
        DB_FATAL("rocksdb init failed, exit now");
        return -1;
    }

    butil::EndPoint addr;
    if (FLAGS_meta_with_any_ip) {
        addr.ip = butil::my_ip();
    } else {
        butil::str2ip(FLAGS_meta_ip.data(), &addr.ip);
    }

    _physical_room = FLAGS_default_physical_room;

    addr.port = FLAGS_meta_port;
    braft::PeerId peer_id(addr, 0);
    _meta_state_machine = new (std::nothrow)MetaStateMachine(peer_id);
    DB_WARNING("meta server new MetaStateMachine, peer_id: %s", peer_id.to_string().c_str());
    if (_meta_state_machine == NULL) {
        DB_FATAL("new meta state machine failed");
        return -1;
    }
    ret = _meta_state_machine->init(peers);
    if (ret != 0) {
        DB_FATAL("meta state machine init failed");
        return -1;
    }
    DB_WARNING("meta state machine init sucess");

    _tso_state_machine = new (std::nothrow)TSOStateMachine(peer_id);
    if (_tso_state_machine == NULL) {
        DB_FATAL("new tso state machine failed");
        return -1;
    }
    ret = _tso_state_machine->init(peers);
    if (ret != 0) {
        DB_WARNING("tso state machine init failed");
        return -1;
    }
    DB_WARNING("tso state machine init sucess");

    _init_sucess = true;

    // set state machine for all kind of manager
    SchemaManager::get_instance()->set_meta_state_machine(_meta_state_machine);
    PrivilegeManager::get_instance()->set_meta_state_machine(_meta_state_machine);
    ClusterManager::get_instance()->set_meta_state_machine(_meta_state_machine);
    
    return 0;
}

void MetaServer::query(::google::protobuf::RpcController* controller,
                   const ::TKV::pb::MetaReq* request,
                   ::TKV::pb::MetaRes* response,
                   ::google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
    const auto& remote_side_tmp = butil::endpoint2str(cntl->remote_side());
    const char* remote_side = remote_side_tmp.c_str();
    uint64_t log_id = 0;
    if (cntl->has_log_id()) {
        log_id = cntl->log_id();
    }
    RETURN_IF_NOT_INIT(_init_sucess, response, log_id);

    TimeCost time_cost;
    response->set_errcode(pb::SUCCESS);
    response->set_errmsg("success");
    switch(request->op_type()) {
    case pb::QUERY_REGION:
        QueryRegionManager::get_instance()->get_region_info(request, response);
        break;
    default:
        DB_WARNING("query op_type:%s not valid, request: %s, log_id: %lu",
                request->op_type(), request->ShortDebugString().c_str(), log_id);
        response->set_errcode(pb::INPUT_PARAM_ERROR);
        response->set_errmsg("invalid op type");
    }
    DB_NOTICE("qeury op_type: %s, time cost: %ld, log_id: %lu, address: %s, request: %s",
            pb::QueryOpType_Name(request->op_type()).c_str(),
            time_cost.get_time(), log_id, remote_side, 
            request->ShortDebugString().c_str());
}

void MetaServer::tso_service(::google::protobuf::RpcController* controller,
           const ::TKV::pb::TSORequest* request,
           ::TKV::pb::TSOResponse* response,
           ::google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
    uint64_t log_id = 0;
    if (cntl->has_log_id()) {
        log_id = cntl->has_log_id();
    }
    RETURN_IF_NOT_INIT(_init_sucess, response, log_id);
    if (_tso_state_machine != nullptr) {
        _tso_state_machine->process(controller, request, response, done_guard.release());
    }
}
} //namespace of TKV

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
