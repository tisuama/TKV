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
    if (cntl && cntl->has_log_id()) {
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
    case pb::OP_ADD_LOGICAL:
    case pb::OP_DROP_LOGICAL:
        if (!request->has_logical_rooms()) {
            ERROR_SET_RESPONSE(response, pb::INPUT_PARAM_ERROR, "no logical room", request->op_type(), log_id);
            return ;
        }
        _meta_state_machine->process(controller, request, response, done_guard.release());
        return ;
    case pb::OP_ADD_PHYSICAL:
    case pb::OP_DROP_PHYSICAL:
        if (!request->has_physical_rooms()) {
            ERROR_SET_RESPONSE(response, pb::INPUT_PARAM_ERROR, "no physical room", request->op_type(), log_id);
            return ;
        }
        _meta_state_machine->process(controller, request, response, done_guard.release());
        return ;
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
    if (_phy_log_map.find(physical_room) != _phy_log_map.end()) {
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
    
    // 更新mem datastruct
    Instance ins_mem(ins_info);
    {
        BAIDU_SCOPED_LOCK(_ins_mutex);
        _ins_phy_map[address] = physical_room;
        _phy_ins_map[physical_room].insert(address);
        _res_ins_map[ins_mem.resource_tag].insert(address);
        _res_rolling_ins[ins_mem.resource_tag].push_back(address);
        _ins_info[address] = ins_mem;
        // 暂时不考虑网段信息
    } 

    // 更新scheduling相关
    {
        BAIDU_SCOPED_LOCK(_sche_mutex);
        auto& scheduling_info = _scheduling_info[ins_mem.address];
        scheduling_info.resource_tag = ins_mem.resource_tag;
        scheduling_info.logical_room = ins_mem.logical_room;
        scheduling_info.region_count_map = std::unordered_map<int64_t, int64_t>{};
        scheduling_info.region_map = std::unordered_map<int64_t, std::vector<int64_t>>{};
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

int ClusterManager::load_snapshot() {
    DB_WARNING("cluster manager start to load snapshot");
    // init map
    {
        BAIDU_SCOPED_LOCK(_phy_mutex);
        _phy_log_map[FLAGS_default_physical_room] = FLAGS_default_logical_room;
        _log_phy_map[FLAGS_default_logical_room] = std::set<std::string>{FLAGS_default_physical_room};
    }
    {
        BAIDU_SCOPED_LOCK(_ins_mutex);
        _phy_ins_map[FLAGS_default_logical_room] = std::set<std::string>();
    }

    rocksdb::ReadOptions read_options;
    read_options.prefix_same_as_start = true;
    read_options.total_order_seek = false;
    auto db = RocksWrapper::get_instance();
    std::unique_ptr<rocksdb::Iterator> iter(
            db->new_iterator(read_options, db->get_meta_info_handle()));
    iter->Seek(MetaServer::CLUSTER_IDENTIFY);
    std::string logical_prefix = MetaServer::CLUSTER_IDENTIFY;
    logical_prefix += MetaServer::LOGICAL_CLUSTER_IDENTIFY + MetaServer::LOGICAL_KEY;

    std::string physical_prefix = MetaServer::CLUSTER_IDENTIFY;
    physical_prefix += MetaServer::PHYSICAL_CLUSTER_IDENTIFY;

    std::string instance_prefix = MetaServer::CLUSTER_IDENTIFY;
    instance_prefix += MetaServer::INSTANCE_CLUSTER_IDENTIFY;

    std::string instance_param_prefix = MetaServer::CLUSTER_IDENTIFY;
    instance_param_prefix += MetaServer::INSTANCE_PARAM_CLUSTER_IDENTIFY;
   
    int ret = 0;
    for (; iter->Valid(); iter->Next()) {
        if (iter->key().starts_with(instance_prefix)) {
            ret = load_instance_snapshot(instance_prefix, iter->key().ToString(), iter->value().ToString());
        } else if (iter->key().starts_with(physical_prefix)) {
            ret = load_physical_snapshot(physical_prefix, iter->key().ToString(), iter->value().ToString());
        } else if (iter->key().starts_with(logical_prefix)) {
            ret = load_logical_snapshot(logical_prefix, iter->key().ToString(), iter->value().ToString());
        } else if (iter->key().starts_with(instance_param_prefix)) {
            ret = load_instance_param_snapshot(instance_param_prefix, iter->key().ToString(), iter->value().ToString());
        } else {
            DB_WARNING("unsupport cluster info  when load snapshot, key: %s",iter->key().data());
        }
        if (ret) {
            DB_FATAL("ClusterManager load snapshot fail, key: %s", iter->key().data());
            return -1;
        }
    }
    return 0;
}

// 机房映射关系
int ClusterManager::load_instance_snapshot(const std::string& instance_prefix, 
        const std::string& key, const std::string& value) {
    std::string address(key, instance_prefix.size());
    pb::InstanceInfo ins_info_pb;
    if (!ins_info_pb.ParseFromString(value)) {
        DB_FATAL("parse from pb fail when load instance snapshot, key: %s", key.c_str());
        return -1;
    }
    DB_WARNING("cluster load instance, instance info: %s", ins_info_pb.ShortDebugString().c_str());
    
    std::string physical_room = ins_info_pb.physical_room();
    if (physical_room.size() == 0) {
        ins_info_pb.set_physical_room(FLAGS_default_physical_room);
    }
    if (!ins_info_pb.has_logical_room()) {
        if (_phy_log_map.find(physical_room) != _phy_log_map.end()) {
            ins_info_pb.set_logical_room(_phy_log_map[physical_room]);
        } else {
            DB_FATAL("Fail to get logical room for physical_room: %s", physical_room.c_str());
        }
    }
    {
        BAIDU_SCOPED_LOCK(_ins_mutex);
        _ins_info[address] = Instance(ins_info_pb);
        _ins_phy_map[address] = ins_info_pb.physical_room();
        _phy_ins_map[ins_info_pb.physical_room()].insert(address);
        _res_ins_map[_ins_info[address].resource_tag].insert(address);
        _res_rolling_ins[_ins_info[address].resource_tag].push_back(address);
    }

    {
        BAIDU_SCOPED_LOCK(_sche_mutex);        
        auto& scheduling_info = _scheduling_info[ins_info_pb.address()];
        scheduling_info.logical_room = ins_info_pb.logical_room();
        scheduling_info.resource_tag = ins_info_pb.resource_tag();
        scheduling_info.region_count_map = std::unordered_map<int64_t, int64_t>{};
        scheduling_info.region_map = std::unordered_map<int64_t, std::vector<int64_t>>{};
    }
    
    return 0;
}

int ClusterManager::load_instance_param_snapshot(const std::string& instance_param_prefix, 
        const std::string& key, const std::string& value) {
    std::string resource_tag_or_address(key, instance_param_prefix.size());
    pb::InstanceParam ins_param_pb;
    if (!ins_param_pb.ParseFromString(value)) {
        DB_FATAL("parse from pb fail when load instance param snapshot, key: %s", key.c_str());
        return -1;
    }
    DB_WARNING("cluster load instance param, instance param pb: %s", ins_param_pb.ShortDebugString().c_str());
    if (resource_tag_or_address != ins_param_pb.resource_tag_or_address()) {
        DB_FATAL("diff reource tag: %s vs %s", resource_tag_or_address.c_str(), 
                ins_param_pb.resource_tag_or_address().c_str());
    }
    BAIDU_SCOPED_LOCK(_ins_param_mutex);
    _ins_param_map[resource_tag_or_address] = ins_param_pb;    
    return 0;
}

int ClusterManager::load_physical_snapshot(const std::string& physical_prefix,
        const std::string& key, const std::string& value) {
    pb::PhysicalRoom phy_log_pb;
    if (!phy_log_pb.ParseFromString(value)) {
        DB_FATAL("parse from pb fail when load physical snapshot", key.c_str());
        return -1;
    }
    DB_WARNING("cluster load physical, physical logical info: %s", phy_log_pb.ShortDebugString().c_str());
    BAIDU_SCOPED_LOCK(_phy_mutex);
    std::string logical_room = phy_log_pb.logical_room();
    std::set<std::string> physical_rooms;
    for (auto& physical_room : phy_log_pb.physical_rooms()) {
        physical_rooms.insert(physical_room);
        _phy_log_map[physical_room] = logical_room;
        _phy_ins_map[physical_room] = std::set<std::string>{};
    }
    _log_phy_map[logical_room] = physical_rooms;
    return 0;
}

int ClusterManager::load_logical_snapshot(const std::string& logical_prefix,
        const std::string& key, const std::string& value) {
    pb::LogicalRoom logical_info_pb;
    if (!logical_info_pb.ParseFromString(value)) {
        DB_FATAL("parse from pb fail when load logical snapshot, key: %s", key.c_str());
        return -1;
    }
    DB_WARNING("cluster load logical, logical info: %s", logical_info_pb.ShortDebugString().c_str());
    BAIDU_SCOPED_LOCK(_phy_mutex);
    for (auto logical_room : logical_info_pb.logical_rooms()) {
        _log_phy_map[logical_room] = std::set<std::string>();
    }
    return 0;
}

void ClusterManager::process_instance_heartbeat_for_store(const pb::InstanceInfo& instance_info) {
    int ret = update_instance_info(instance_info);
    if (ret == 0) {
        return ;
    }
    // 构造请求 1: add_instance 2: update_instance
    pb::MetaManagerRequest request;
    if (ret == 1) {
        request.set_op_type(pb::OP_ADD_INSTANCE);
    } else {
        request.set_op_type(pb::OP_UPDATE_INSTANCE);
    }
    pb::InstanceInfo* info = request.mutable_instance();
    *info = instance_info;
    DB_DEBUG("process instance heartbeat, request: %s", request.ShortDebugString().c_str());
    process_cluster_info(nullptr, &request, nullptr, nullptr);
}

void ClusterManager::process_instance_param_heartbeat_for_store(const pb::StoreHBRequest* request, 
        pb::StoreHBResponse* response) {
    std::string address = request->instance_info().address();
    std::string resource_tag = request->instance_info().resource_tag();
    
    BAIDU_SCOPED_LOCK(_ins_param_mutex);
    auto iter = _ins_param_map.find(resource_tag);
    if (iter != _ins_param_map.end()) {
        *(response->add_instance_param()) = iter->second;        
    }
    
    iter = _ins_param_map.find(address);
    if (iter != _ins_param_map.end()) {
        *(response->add_instance_param()) = iter->second;
    }
    DB_WARNING("process instance param heartbeat, response: %s", response->ShortDebugString().c_str());
}

/* return 1: add instance 2: update instance */
int ClusterManager::update_instance_info(const pb::InstanceInfo& info) {
    std::string address = info.address();
    BAIDU_SCOPED_LOCK(_ins_mutex);
    if (_ins_info.find(address) == _ins_info.end()) {
        return 1; // add instance
    }
    if (_ins_info[address].resource_tag != info.resource_tag()) {
        return 2; // need persist update
    }    
    // need mem update
    auto& instance = _ins_info[address];
    instance.capacity = info.capacity();
    instance.used_size = info.used_size();
    instance.resource_tag = info.resource_tag();
    instance.version = info.version();
    instance.instance_state.time_stamp = butil::gettimeofday_us();
    instance.dml_latency = info.dml_latency();
    instance.dml_qps = info.dml_qps();    
    instance.raft_total_latency = info.raft_total_latency();
    instance.raft_total_qps = info.raft_total_qps();
    instance.select_latency = info.select_latency();
    instance.select_qps = info.select_qps();
    
    // auto& status = instance.instance_state.state;
    // TODO: check and set SLOW
    return 0;
}

void ClusterManager::update_instance(const pb::MetaManagerRequest& request, braft::Closure* done) {
}

void ClusterManager::update_instance_param(const pb::MetaManagerRequest& request, braft::Closure* done) {
}

void ClusterManager::add_logical(const pb::MetaManagerRequest& request, braft::Closure* done) {
    pb::LogicalRoom logial_pb;
    for (auto add_room: request.logical_rooms().logical_rooms()) {
        if (_log_phy_map.count(add_room)) {
            DB_WARNING("Error, logical room: %s has exist", add_room.c_str());
            IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "logical room has exist");
            return ;
        }
        logial_pb.add_logical_rooms(add_room);
    }
    for (auto& old_logical_room: _log_phy_map) {
        logial_pb.add_logical_rooms(old_logical_room.first);
    }
    // update rocksdb 
    std::string value;
    if (!logial_pb.SerializeToString(&value)) {
        DB_WARNING("request serializeToString fail, request: %s", request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::PARSE_TO_PB_FAIL, "SerializeToString Fail");
        return ;
    }
    int ret = MetaRocksdb::get_instance()->put_meta_info(construct_logical_key(), value);
    if (ret < 0) {
        DB_FATAL("add logical room: %s to rocksdb fail", request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INTERNAL_ERROR, "write to rocksdb fail");
        return ;
    }
    // update mem
    BAIDU_SCOPED_LOCK(_phy_mutex);
    for (auto add_room: request.logical_rooms().logical_rooms()) {
        _log_phy_map[add_room] = std::set<std::string>();
    }
    IF_DONE_SET_RESPONSE(done, pb::SUCCESS, "success");
    DB_NOTICE("add logical room success, request: %s", request.ShortDebugString().c_str());
}

void ClusterManager::add_physical(const pb::MetaManagerRequest& request, braft::Closure* done) {
    auto& physical_room = request.physical_rooms();
    std::string logical_room = physical_room.logical_room();
    // 逻辑机房不存在的情况
    if (!_log_phy_map.count(logical_room)) {
        DB_WARNING("Error, logical room: %s not exist", logical_room.c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "logical not exist");
        return ;
    }
    pb::PhysicalRoom physical_pb;
    physical_pb.set_logical_room(logical_room);
    for (auto& add_room: physical_room.physical_rooms()) {
        if (_phy_log_map.find(add_room) != _phy_log_map.end()) {
            DB_WARNING("Error, physical room: %s already exist", add_room.c_str());
            IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "physical already exist");
            return ;
        }
        physical_pb.add_physical_rooms(add_room);
    }
    for (auto& old_physical_room: _log_phy_map[logical_room]) {
        physical_pb.add_physical_rooms(old_physical_room);
    }

    // update rocksdb
    std::string value;
    if (!physical_pb.SerializeToString(&value)) {
        DB_WARNING("request serializeToString fail, request: %s", request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::PARSE_TO_PB_FAIL, "SerializeToString Fail");
        return ;
    }
    int ret = MetaRocksdb::get_instance()->put_meta_info(construct_physical_key(logical_room), value);
    if (ret < 0) {
        DB_FATAL("add physical room: %s to rocksdb fail", request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INTERNAL_ERROR, "write to rocksdb fail");
        return ;
    }
    // update mem
    {
        BAIDU_SCOPED_LOCK(_phy_mutex);
        for (auto& add_room: physical_room.physical_rooms()) {
            _log_phy_map[logical_room].insert(add_room);
            _phy_log_map[add_room] = logical_room;
        }
    }
    {
        BAIDU_SCOPED_LOCK(_ins_mutex);
        for (auto& add_room: physical_room.physical_rooms()) {
            _phy_ins_map[add_room] = std::set<std::string>();
        }
    }
    IF_DONE_SET_RESPONSE(done, pb::SUCCESS, "success");
    DB_NOTICE("add physical room success, request: %s", request.ShortDebugString().c_str());
}
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
