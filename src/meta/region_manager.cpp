#include "meta/region_manager.h"
#include "meta/table_manager.h"


namespace TKV {

void RegionManager::clear() {
    // reset region mem info
    _region_info_map.clear();
    _region_state_map.clear();
    _region_learner_peer_state_map.clear();
    _ins_region_map.clear();
    _ins_learner_map.clear();
    _ins_leader_count.clear();
}

void RegionManager::set_region_info(const pb::RegionInfo& region_pb) {
    int64_t table_id = region_pb.table_id();
    int64_t region_id = region_pb.region_id();    
    SmartRegionInfo  region_info = _region_info_map.get(region_id);
    {
        BAIDU_SCOPED_LOCK(_ins_region_mutex);
        if (region_info != nullptr) {
            // 删除旧的
            for (auto& peer: region_info->peers()) {
                _ins_region_map[peer][table_id].erase(region_id);
            }
        }
        // 插入新的
        for (auto& peer: region_pb.peers()) {
            DB_DEBUG("set region peer info, instance: %s, table_id: %ld, region_id: %ld",
                    peer.c_str(), table_id, region_id);
            _ins_region_map[peer][table_id].insert(region_id);
        }
        // 如果原peer不存在这个table的信息，则删除这个table的所有信息
        if (region_info != nullptr) {
            for (auto& peer: region_info->peers()) {
                if (_ins_region_map[peer][table_id].size() == 0) {
                    _ins_region_map[peer].erase(table_id);
                }
            }
        }
    }
    {
        BAIDU_SCOPED_LOCK(_ins_learner_mutex);
        if (region_info != nullptr) {
            for (auto& learner : region_info->learners()) {
                _ins_learner_map[learner][table_id].erase(region_id);
            }
        }
        for (auto& learner: region_pb.learners()) {
            DB_DEBUG("set region learner info, instance: %s, table_id: %ld, region_id: %ld",
                    learner.c_str(), table_id, region_id);
            _ins_learner_map[learner][table_id].insert(region_id);
        }
        if (region_info != nullptr) {
            for (auto& learner: region_info->learners()) {
                if (_ins_learner_map[learner][table_id].size() == 0) {
                    _ins_learner_map[learner].erase(table_id);
                }
            }
        }
    }
    auto smart_region_info = std::make_shared<pb::RegionInfo>(region_pb);
    _region_info_map.set(region_id, smart_region_info);
}

int RegionManager::load_region_snapshot(const std::string& value) {
    pb::RegionInfo region_pb;
    if (!region_pb.ParseFromString(value)) {
        DB_FATAL("parse from pb fail when load region snapshot, value: %s", value.c_str());
        return -1;
    }
    DB_WARNING("load region info: %s, size: %lu", region_pb.ShortDebugString().c_str(), value.size());
    if (region_pb.start_key() == region_pb.end_key() &&
            !region_pb.start_key().empty()) {
        // 空region
        return 0;
    } 
    set_region_info(region_pb);
    
    RegionStateInfo region_state;
    region_state.timestamp = butil::gettimeofday_us();
    region_state.status = pb::NORMAL;
    set_region_state(region_pb.region_id(), region_state);

    TableManager::get_instance()->add_region_id(region_pb.table_id(),
            region_pb.partition_id(),
            region_pb.region_id());

    TableManager::get_instance()->add_startkey_region_map(region_pb);

    // load peer snapshot
    RegionPeerState peer_state = _region_peer_state_map.get(region_pb.region_id());
    for (auto& peer: region_pb.peers()) {
        pb::PeerStateInfo new_peer_info;
        new_peer_info.set_peer_id(peer);
        new_peer_info.set_timestamp(region_state.timestamp);                
        new_peer_info.set_table_id(region_pb.table_id());
        new_peer_info.set_peer_status(pb::STATUS_NORMAL);
        peer_state.legal_peers_state.emplace_back(new_peer_info);
    }
    _region_peer_state_map.set(region_pb.region_id(), peer_state);

    // load learner snapshot
    RegionLearnerState learner_state;
    for (auto& learner: region_pb.learners()) {
        pb::PeerStateInfo tmp_learner_state;
        tmp_learner_state.set_timestamp(region_state.timestamp);
        tmp_learner_state.set_table_id(region_pb.table_id());
        tmp_learner_state.set_peer_status(pb::STATUS_NORMAL);
        learner_state.learner_state_map[learner] = tmp_learner_state;
    }
    _region_learner_peer_state_map.set(region_pb.region_id(), learner_state);
    
    return 0;
}
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
