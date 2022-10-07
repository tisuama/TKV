#pragma once
#include <bthread/mutex.h>
#include "meta/meta_server.h"
#include "meta/meta_state_machine.h"
#include "proto/meta.pb.h"

namespace TKV {
DECLARE_string(default_logical_room);
DECLARE_string(default_physical_room);

using braft::Closure;
struct InstanceStatusInfo {
    int64_t     time_stamp; // timestamp for prev heartbeat time
    pb::Status  state;
};

struct InstanceScheduingInfo {
    // 每个实例上保存每个表的哪些region, table => [region_ids]
    std::unordered_map<int64_t, std::vector<int64_t>> region_map;
    // 每个实例上保存每个表的region个数
    std::unordered_map<int64_t, int64_t>              region_count_map;

    // resource_tag
    std::string                                       logical_room;
    std::string                                       resource_tag;
};

struct Instance {
    std::string     address;
    int64_t         capacity;
    int64_t         used_size;
    std::string     resource_tag;
    std::string     physical_room;
    std::string     logical_room;
    std::string     version;
    InstanceStatusInfo instance_state; 
    
    int64_t         dml_latency;
    int64_t         dml_qps;
    int64_t         raft_total_latency;
    int64_t         raft_total_qps;
    int64_t         select_latency;
    int64_t         select_qps;

    Instance() {
        instance_state.time_stamp = butil::gettimeofday_us();
        instance_state.state = pb::NORMAL;
    }

    Instance(const pb::InstanceInfo& instance_info)
       : address(instance_info.address())
       , capacity(instance_info.capacity())
       , used_size(instance_info.capacity())
       , resource_tag(instance_info.resource_tag())
       , physical_room(instance_info.physical_room())
       , logical_room(instance_info.logical_room())
       , version(instance_info.version()) {
           if (instance_info.has_status() && instance_info.status() == pb::FAULTY) {
               instance_state.state = pb::FAULTY;
           } else {
               instance_state.state = pb::NORMAL;
           }
           instance_state.time_stamp = butil::gettimeofday_us();
       }

};

class ClusterManager {
public:

    typedef pb::MetaManagerRequest meta_req;
    typedef pb::MetaManagerResponse meta_res;
    static ClusterManager* get_instance() {
        static ClusterManager _instance;
        return &_instance;
    }
    
    ~ClusterManager() {
        bthread_mutex_destroy(&_phy_mutex);
        bthread_mutex_destroy(&_ins_mutex);
        bthread_mutex_destroy(&_ins_param_mutex);
        bthread_mutex_destroy(&_sche_mutex);
    }
    // called before apply for process
    void process_cluster_info(google::protobuf::RpcController* controller,
                            const meta_req* request,
                            meta_res* response, 
                            google::protobuf::Closure* done);

    // called when on_apply
    void add_instance(const meta_req& request, Closure* done);

    
    // comon fun
    std::string construct_instance_key(const std::string& instance) {
        return MetaServer::CLUSTER_IDENTIFY +
               MetaServer::INSTANCE_CLUSTER_IDENTIFY + 
               instance;
    }
    
    void set_meta_state_machine(MetaStateMachine* s) {
        _meta_state_machine = s;
    }
    
    bool logical_room_exist(const std::string& logical_room) {
        BAIDU_SCOPED_LOCK(_phy_mutex);
        if (_log_phy_map.find(logical_room) != _log_phy_map.end() &&
               _log_phy_map[logical_room].size() != 0)  {
            return true;
        }
        return false;
    } 

    bool get_resource_tag(const std::string& instance, std::string& resource_tag) {
        if (_scheduling_info.find(instance) == _scheduling_info.end()) {
            return false;
        }
        resource_tag = _scheduling_info[instance].resource_tag;
        return true;
    }

    std::string get_logical_room(const std::string& instance) {
        if (_scheduling_info.find(instance) != _scheduling_info.end()) {
            return _scheduling_info[instance].logical_room;
        }
        return "";
    }
    
    int select_instance_rolling(const std::string& resource_tag, 
            const std::set<std::string>& exclude_stores,
            const std::string& logical_room,
            std::string& select_instance);

    void process_instance_heartbeat_for_store(const pb::InstanceInfo& instance_info);

    void process_instance_param_heartbeat_for_store(const pb::StoreHBRequest* request, 
            pb::StoreHBResponse* response);

    int update_instance_info(const pb::InstanceInfo& info);

    void update_instance(const pb::MetaManagerRequest& request, braft::Closure* done);

    void update_instance_param(const pb::MetaManagerRequest& request, braft::Closure* done);

    void add_logical(const pb::MetaManagerRequest& request, braft::Closure* done);

    void add_physical(const pb::MetaManagerRequest& request, braft::Closure* done);

    int load_snapshot();
    int load_instance_snapshot(const std::string& instance_prefix, 
            const std::string& key, const std::string& value);
    int load_instance_param_snapshot(const std::string& instance_param_prefix, 
            const std::string& key, const std::string& value);
    int load_physical_snapshot(const std::string& physical_prefix,
            const std::string& key, const std::string& value);
    int load_logical_snapshot(const std::string& logical_prefix,
            const std::string& key, const std::string& value);

private:
    ClusterManager() {
        bthread_mutex_init(&_phy_mutex, NULL);
        bthread_mutex_init(&_ins_mutex, NULL);
        bthread_mutex_init(&_ins_param_mutex, NULL);
        bthread_mutex_init(&_sche_mutex, NULL);
        {
            BAIDU_SCOPED_LOCK(_phy_mutex);
            _phy_log_map[FLAGS_default_physical_room] = 
                FLAGS_default_logical_room;
            _log_phy_map[FLAGS_default_logical_room] = 
                std::set<std::string>{FLAGS_default_physical_room};
        }
        {
            BAIDU_SCOPED_LOCK(_ins_mutex);
            _phy_ins_map[FLAGS_default_physical_room] = std::set<std::string>{};
        }
    } 

    std::string construct_logical_key() {
        return MetaServer::CLUSTER_IDENTIFY + MetaServer::LOGICAL_CLUSTER_IDENTIFY
                + MetaServer::LOGICAL_KEY;
    }
    
    std::string construct_physical_key(const std::string& logical_key) {
        return MetaServer::CLUSTER_IDENTIFY + MetaServer::LOGICAL_CLUSTER_IDENTIFY
                + logical_key;
    }

    bool is_legal_for_select_instance(const std::string& candicate_instance,
            const std::string& resource_tag,
            const std::set<std::string>& exclude_stores,
            const std::string& logical_room);

private:
    // 各种对应关系
    // instance: 物理机房 => 1: 1
    // 物理位置: 逻辑机房 => 1: 1
    // 物理位置: instance => 1: N
    // 逻辑机房: 物理位置 => 1: M
    // 资    源: 物理位置 => 1: X
    bthread_mutex_t             _phy_mutex;
    // key: 物理机房 value: 逻辑机房
    std::unordered_map<std::string, std::string>           _phy_log_map;

    // key: 逻辑机房 value: 物理机房
    std::unordered_map<std::string, std::set<std::string>> _log_phy_map;

    bthread_mutex_t            _ins_mutex;
    // key: 实例 value: 物理机房
    std::unordered_map<std::string, std::string>           _ins_phy_map;
    // key: 物理机房 value: 实例 
    std::unordered_map<std::string, std::set<std::string>> _phy_ins_map;
    // key: resource_tag value: 实例 
    std::unordered_map<std::string, std::set<std::string>> _res_ins_map;
    // key: resource_tag value: tag下上一个rolling的instance
    std::unordered_map<std::string, size_t>                    _res_rolling_pos;
    std::unordered_map<std::string, std::vector<std::string>>  _res_rolling_ins;

    // 实例信息
    std::unordered_map<std::string, Instance>             _ins_info;
    std::unordered_map<std::string, std::string>          _container_id_to_ip;
    std::unordered_set<std::string>                       _slow_instance;
    
    bthread_mutex_t                                       _ins_param_mutex;
    std::unordered_map<std::string, pb::InstanceParam>    _ins_param_map;
    int                                                   _migreate_concurrency {2};

    MetaStateMachine*                                     _meta_state_machine {nullptr};
    
    bthread_mutex_t                                        _sche_mutex;
    std::unordered_map<std::string, InstanceScheduingInfo> _scheduling_info;
};

} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */

