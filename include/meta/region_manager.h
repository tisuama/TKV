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
    std::map<std::string, pb::PeerStateInfo> learner_state_map;
    TimeCost                                 tc;
};

typedef std::shared_ptr<RegionStateInfo> SmartRegionStateInfo;
class RegionManager {
public:
    ~RegionManager() {
        bthread_mutex_destroy(&_ins_region_mutex);
        bthread_mutex_destroy(&_ins_learner_mutex);
        bthread_mutex_destroy(&_cond_mutex);
        bthread_mutex_destroy(&_doing_mutex);
    }
    static RegionManager* get_instance() {
        static RegionManager instance;
        return &instance;
    }
    
    int64_t get_max_region_id() const {
        return _max_region_id;
    }

    void set_max_region_id(int64_t max_region_id) {
        _max_region_id = max_region_id;
    }
    
    std::string construct_max_region_id_key() {
        std::string max_region_id_key = MetaServer::SCHEMA_IDENTIFY +
            MetaServer::MAX_ID_SCHEMA_IDENTIFY + 
            SchemaManager::MAX_REGION_ID_KEY;
        return max_region_id_key;
    }

private:
    RegionManager(): _max_region_id(0) {
        _doing_recovery = false;
        _last_opt_times = butil::gettimeofday_us();
        bthread_mutex_init(&_ins_region_mutex, NULL);
        bthread_mutex_init(&_ins_learner_mutex, NULL);
        bthread_mutex_init(&_cond_mutex, NULL);
        bthread_mutex_init(&_doing_mutex, NULL);
    }
    int64_t                                 _max_region_id;
    int64_t                                 _last_opt_times;
    std::atomic<bool>                       _doing_recovery;
    ThreadSafeMap<int64_t, SmartRegionInfo> _region_info_map;
    bthread_mutex_t                         _ins_region_mutex;
    bthread_mutex_t                         _ins_learner_mutex;

    // ?????????region_id??????????????????????????????????????????????????????????????????
    std::unordered_map<std::string, std::unordered_map<int64_t, std::set<int64_t>>> _ins_region_map;
    std::unordered_map<std::string, std::unordered_map<int64_t, std::set<int64_t>>> _ins_learner_map;
    
    ThreadSafeMap<int64_t, RegionStateInfo> _region_state_map;
    ThreadSafeMap<int64_t, RegionPeerState> _region_peer_state_map;
    ThreadSafeMap<int64_t, RegionLearnerState> _region_leader_state_map;
    
    bthread_mutex_t _cond_mutex;
    std::unordered_map<std::string, std::unordered_map<int64_t, int64_t>> _ins_leader_count; 
    // instance_table_id -> pk_prefix -> leader region count
    std::unordered_map<std::string, std::unordered_map<std::string, int64_t>> _ins_pk_leader_count;
    // region_id -> logical room
    // ??????store????????????????????????????????????????????????region_id?????????store?????????logical_room
    // check_peer_count??????region_id???map???????????????????????????????????????peer???????????????candidate???
    // ????????????table???????????????peer
    std::unordered_map<int64_t, std::string> _remove_region_peer_on_pk_prefix;

    bthread_mutex_t       _doing_mutex;
    std::set<std::string> _doing_migrate;
};    
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
