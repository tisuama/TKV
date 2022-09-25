#pragma once

#include "meta/schema_manager.h"
#include "meta/meta_server.h"
#include "common/common.h"
#include <unordered_map>
#include <set>

namespace TKV {
struct RegionStateInfo {
    int64_t timestamp;
    pb::Status status;
};    

struct RegionPeerState {
    std::vector<pb::PeerStateInfo> legal_peers_state;   // peers in raft group
    std::vector<pb::PeerStateInfo> ilegal_peers_state;  // peers not in raft group
};

struct RegionLearnerState {
    // peer -> state
    std::map<std::string, pb::PeerStateInfo> learner_state_map;
    TimeCost                                 tc;
};

typedef std::shared_ptr<RegionStateInfo> SmartRegionStateInfo;
class RegionManager {
public:
    ~RegionManager() {
        bthread_mutex_destroy(&_region_mutex);
        bthread_mutex_destroy(&_ins_region_mutex);
        bthread_mutex_destroy(&_ins_learner_mutex);
        bthread_mutex_destroy(&_cond_mutex);
        bthread_mutex_destroy(&_doing_mutex);
    }
    static RegionManager* get_instance() {
        static RegionManager instance;
        return &instance;
    }
    
    int64_t get_max_region_id() {
        BAIDU_SCOPED_LOCK(_region_mutex);
        return _max_region_id;
    }

    void set_max_region_id(int64_t max_region_id) {
        BAIDU_SCOPED_LOCK(_region_mutex);
        _max_region_id = max_region_id;
    }
    
    std::string construct_max_region_id_key() {
        std::string max_region_id_key = MetaServer::SCHEMA_IDENTIFY +
            MetaServer::MAX_ID_SCHEMA_IDENTIFY + 
            SchemaManager::MAX_REGION_ID_KEY;
        return max_region_id_key;
    }

    std::string construct_region_key(int64_t region_id) {
        std::string region_key = MetaServer::SCHEMA_IDENTIFY + 
            MetaServer::REGION_SCHEMA_IDENTIFY;
        region_key.append((char*)region_id, sizeof(int64_t));
        return region_key;
    }

    void set_region_state(int64_t region_id, const RegionStateInfo& region_state) {
        _region_state_map.set(region_id, region_state);
    }  

    SmartRegionInfo get_region_info(int64_t region_id) {
        return _region_info_map.get(region_id);
    }
    
    void set_region_leader(int64_t region_id, const std::string& new_leader) {
        SmartRegionInfo region = _region_info_map.get(region_id);
        if (region) {
            auto new_region = std::make_shared<pb::RegionInfo>(*region);
            new_region->set_leader(new_leader);
            _region_info_map.set(region_id, new_region);
        }
    }

    void set_region_mem_info(int64_t region_id, int64_t log_index, 
            int64_t used_size, int64_t num_table_lines) {
        SmartRegionInfo region = _region_info_map.get(region_id);
        if (region) {
            region->set_log_index(log_index);
            region->set_used_size(used_size);
            region->set_num_table_lines(num_table_lines);
        }
    }

    void clear();
    
    int load_region_snapshot(const std::string& value);

    void set_region_info(const pb::RegionInfo& region_pb);

    void update_leader_status(const pb::StoreHBRequest* request, int64_t ts);
    
    void leader_heartbeat_for_region(const pb::StoreHBRequest* 
            request, pb::StoreHBResponse* response);
    
    void check_whether_update_region(int64_t region_id, bool has_peer_changed, 
            const pb::LeaderHB& leader_hb, const SmartRegionInfo& pre_region);

    void check_peer_count(int64_t region_id, 
            const pb::LeaderHB& leader_hb,
            std::unordered_map<int64_t, int64_t>& table_replica_nums,
            std::unordered_map<int64_t, std::string>& table_resource_tags,
            std::unordered_map<int64_t, std::unordered_map<std::string, int64_t>>& table_replica_dists,
            pb::StoreHBResponse* response);

    // Raft called
    void update_region(const pb::MetaManagerRequest& request, 
            const int64_t apply_index, 
            braft::Closure* done);


private:
    RegionManager(): _max_region_id(0) {
        _doing_recovery = false;
        _last_opt_times = butil::gettimeofday_us();
        bthread_mutex_init(&_region_mutex, NULL);
        bthread_mutex_init(&_ins_region_mutex, NULL);
        bthread_mutex_init(&_ins_learner_mutex, NULL);
        bthread_mutex_init(&_cond_mutex, NULL);
        bthread_mutex_init(&_doing_mutex, NULL);
    }
    bthread_mutex_t                         _region_mutex;
    int64_t                                 _max_region_id;
    int64_t                                 _last_opt_times;
    std::atomic<bool>                       _doing_recovery;
    ThreadSafeMap<int64_t, SmartRegionInfo> _region_info_map;
    bthread_mutex_t                         _ins_region_mutex;
    bthread_mutex_t                         _ins_learner_mutex;

    // 实例和region_id的映射关系，在需要主动发送迁移实例请求时需要
    std::unordered_map<std::string, std::unordered_map<int64_t, std::set<int64_t>>> _ins_region_map;
    std::unordered_map<std::string, std::unordered_map<int64_t, std::set<int64_t>>> _ins_learner_map;
    
    // region_id -> regionstate
    ThreadSafeMap<int64_t, RegionStateInfo> _region_state_map;
    ThreadSafeMap<int64_t, RegionPeerState> _region_peer_state_map;
    ThreadSafeMap<int64_t, RegionLearnerState> _region_learner_peer_state_map;
    
    bthread_mutex_t _cond_mutex;
    std::unordered_map<std::string, std::unordered_map<int64_t, int64_t>> _ins_leader_count; 

    // instance_table_id -> pk_prefix -> leader region count
    // std::unordered_map<std::string, std::unordered_map<std::string, int64_t>> _ins_pk_leader_count;
    
    // region_id -> logical room
    // 处理store心跳发现大部不均，标记需要迁移的region_id及候选store需要在logical_room
    // check_peer_count发现region_id在map里，直接按照大户的维度删除peer数量最多的candidate，
    // 否则按照table的维度删除peer
    std::unordered_map<int64_t, std::string> _remove_region_peer_on_pk_prefix;

    bthread_mutex_t       _doing_mutex;
    std::set<std::string> _doing_migrate;
};    
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
