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
        // 暂时不考虑网段信息
    } 
    IF_DONE_SET_RESPONSE(done, pb::SUCCESS, "success");
    DB_NOTICE("add instance success, request: %s", request.ShortDebugString().c_str());
} 
/* 1. dead store下线之前选择补副本的instance
 *   1.1 peer：exclude_stores是peers的store address
 *   1.2 learner：exclude_stores是null
 * 2. store进行迁移
 *   2.1 peer：exclude_stores是peers的store address
 *   2.2 learner：exclude_stores是null
 * 3. 处理store心跳，补region peer，exclude_stores是peers store address
 * 4. 建表，创建每一个region的第一个peer，exclude_stores是null
 * 5. region分裂
 *   5.1 尾分裂选一个instance，exclude_stores是原region leader store address
 *   5.2 中间分裂选replica-1个instance，exclude_stores是peer store address
 * 6. 添加全局索引：exclude_stores是null
 */
 
// 暂时不实现网络相关的划分
int ClusterManager::select_instance_rolling(const std::string& resource_tag,  // 从resource_tag里选
        const std::set<std::string>& exclude_stores,
        const std::string& logical_room,
        std::string& select_instance) {
    CHECK(select_instance.empty());
    BAIDU_SCOPED_LOCK(_ins_mutex);
    if (_res_ins_map.find(resource_tag) == _res_ins_map.end() || 
            _res_ins_map[resource_tag].empty()) {
        DB_WARNING("resource tag: %s has no instance", resource_tag.c_str());
        return -1;
    }
    // Network Balance = False
    size_t rolling_times = 0;
    size_t ins_count = _res_ins_map[resource_tag].size();    
    auto& last_rolling_pos = _res_rolling_pos[resource_tag];
    auto& instance = _res_rolling_ins[resource_tag];
    for (; rolling_times < ins_count; ) {
        if (last_rolling_pos >= ins_count) {
            last_rolling_pos = 0;
            continue;
        }
        ++rolling_times;
        // 找到rolling_address
        select_instance = instance[last_rolling_pos];
    }
    if (select_instance.empty()) {
        DB_WARNING("select instance fail, has no legal store, resource_tag: %s", resource_tag.c_str());
        return -1;
    }
    DB_WARNING("select instance: %s for resouce tag: %s", select_instance.c_str(), resource_tag.c_str());
    return 0;
      
}
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
