#include "meta/cluster_manager.h"
#include "common/common.h"
#include "proto/optype.pb.h"
#include "meta/meta_rocksdb.h"

namespace TKV {
using braft::Closure;
void ClusterManager::process_cluster_info(google::protobuf::RpcController* controller,
                        const meta_req* request,
                        meta_res* response, 
                        google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    uint64_t log_id = 0;
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
    if (cntl->has_log_id()) {
        log_id = cntl->log_id();
    }
    switch(request->op_type()) {
    case pb::OP_ADD_INSTANCE:
        if (!request->has_instance()) {
            ERROR_SET_RESPONSE(response, pb::INPUT_PARAM_ERROR, "no instance info", request->op_type(), log_id);
            // done_guard is called when return ;
            return; 
        }
        _meta_state_machine->process(controller, request, response, done_guard.release());
        return;
    default:
        ERROR_SET_RESPONSE(response, pb::INPUT_PARAM_ERROR, "op_type is not support", request->op_type(), log_id);
        return ;
    }
}

// called when on_apply
void ClusterManager::add_instance(const meta_req& request, Closure* done) {
    auto& ins_info = const_cast<pb::InstanceInfo&>(request.instance());
    std::string address = ins_info.address();
    if (!ins_info.has_physical_room()) {
        ins_info.set_physical_room(FLAGS_default_physical_room);
    } 
    std::string physical_room = ins_info.physical_room();

    // 检查physical_room -> logical_room
    if (_phy_log_map.find(physical_room) == _phy_log_map.end()) {
        ins_info.set_logical_room(_phy_log_map[physical_room]);
    } else {
        DB_FATAL("get logical room for physical_room: %s failed", physical_room.c_str());
        // MetaServer done 
        IF_DONE_SET_RESPONSE(done, pb::INTERNAL_ERROR, "physical room to logical room failed");
        return;
    }

    // 实例已经存在
    if (_ins_info.find(address) != _ins_info.end()) {
        DB_WARNING("instance: %s is already exist", address.c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "instance already exist");
        return ;
    }

    // write in rocksdb
    std::string value;
    if (!ins_info.SerializeToString(&value)) {
        DB_WARNING("request serializeToString failed, request: %s", 
                request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::PARSE_TO_PB_FAIL, "serializedToString failed");
        return ;
    } 
    auto ret = MetaRocksdb::get_instance()->put_meta_info(construct_instance_key(address), value);
    if (ret < 0) {
        DB_WARNING("add instance: %s to rocksdb failed", request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INTERNAL_ERROR, "write db failed");
        return ;
    }
    
    // update mem datastruct
    Instance ins_mem(ins_info);
    {
        BAIDU_SCOPED_LOCK(_ins_mutex);
        _ins_phy_map[address] = physical_room;
        _phy_ins_map[physical_room].insert(address);
        _res_ins_map[ins_mem.resource_tag].insert(address);
        _ins_info[address] = ins_mem;
    } 
} 
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
