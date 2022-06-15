#pragma once

#include <unordered_set>
#include <set>

namespace TKV {
class RegionManager {
public:
    ~RegionManager() {
    }
    static RegionManager* get_instance() {
        static RegionManager instance;
        return &instance;
    }
private:
    RegionManager(): _max_region_id(0) {
        _doing_recovery = false;
        _last_opt_times = butil::gettimeofday_us();
    }
    int64_t                                 _max_region_id;
    int64_t                                 _last_opt_times;
    std::atomic<bool>                       _doing_recovery;
    ThreadSafeMap<int64_t, SmartRegionInfo> _region_info_map;
    bthread_mutex_t                         _ins_region_mutex;
    bthread_mutex_t                         _ins_learner_mutex;

    // 实例和region_id的映射关系，在需要主动发送迁移实例请求时需要
    std::unordered_map<std::string, std::unordered_map<int64_t, std::set<int64_t>>> _ins_region_map;
    std::unordered_map<std::string, std::unordered_map<int64_t, std::set<int64_t>>> _ins_learner_map;
    
    ThreadSafeMap<int64_t, RegionStateInfo> _region_state_map;
    ThreadSafeMap<int64_t, RegionPeerState> _region_peer_state_map;
    ThreadSafeMap<int64_t, RegionLearnerState> _region_leader_state_map;
    

} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
