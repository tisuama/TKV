#include "meta/region_manager.h"
#include "meta/table_manager.h"
#include "meta/meta_rocksdb.h"
#include "meta/cluster_manager.h"

#include <functional>

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

void RegionManager::update_leader_status(const pb::StoreHBRequest* request, int64_t ts) {
    for (auto& leader_region : request->leader_regions()) {
        // leader: pb::LeaderHB
        int64_t region_id = leader_region.region().region_id();
        int64_t table_id = leader_region.region().table_id();
        RegionStateInfo region_state;
        region_state.timestamp = ts;
        region_state.status = pb::NORMAL;
        _region_state_map.set(region_id, region_state);
        if (leader_region.peer_status_size() > 0) {
            auto& region_info = leader_region.region();
            if (!region_info.start_key().empty() && !region_info.end_key().empty()) {
                if (region_info.start_key() == region_info.end_key()) {
                    _region_peer_state_map.erase(region_id);
                    continue;
                }
            }
            _region_peer_state_map.init_if_not_exist_else_update(region_id, true, 
                    [&leader_region, ts, table_id](RegionPeerState& peer_state) {
                // 该region的RegionPeerState
                peer_state.legal_peers_state.clear();
                for (auto& peer_status: leader_region.peer_status()) {
                    peer_state.legal_peers_state.push_back(peer_status);
                    peer_state.legal_peers_state.back().set_table_id(table_id);
                    peer_state.legal_peers_state.back().set_timestamp(ts); 
                    for (auto it = peer_state.ilegal_peers_state.begin(); it != 
                            peer_state.ilegal_peers_state.end(); it++) {
                        if (it->peer_id() == peer_status.peer_id()) {
                            peer_state.ilegal_peers_state.erase(it);
                            break;
                        }
                    }
                }
            });
        }
    }
    // TODO: 将Learner节点更新进_region_learner_peer_state_map方便判断
}

void RegionManager::leader_heartbeat_for_region(const pb::StoreHBRequest* request, 
        pb::StoreHBResponse* response) {
    std::string instance = request->instance_info().address();
    std::vector<std::pair<std::string, pb::RaftControlRequest>> remove_peer_request;
    std::vector<std::pair<std::string, pb::InitRegion>>         add_learner_request; 
    std::vector<std::pair<std::string, pb::RemoveRegion>>       remove_learner_request;

    std::unordered_map<int64_t, int64_t> table_replica_nums;
    std::unordered_map<int64_t, std::string> table_resouce_tags;
    std::unordered_map<int64_t, std::unordered_map<std::string, int64_t>> table_replica_dists;
    std::unordered_map<int64_t, std::vector<std::string>> table_learner_resource_tags;

    std::set<int64_t> table_ids;
    for (auto& leader: request->leader_regions()) {
        const pb::RegionInfo& leader_info = leader.region();
        table_ids.insert(leader_info.table_id());
    }
    TableManager::get_instance()->get_table_info(table_ids, table_replica_nums, 
            table_resouce_tags, table_replica_dists, table_learner_resource_tags);
    DB_DEBUG("start process leader heartbeat, request: %s", request->ShortDebugString().c_str());
    // TODO: Learner
    for (auto& leader_hb: request->leader_regions()) {
        DB_DEBUG("Leader hb: %s", leader_hb.ShortDebugString().c_str());
        auto& leader_info = leader_hb.region(); 
        int64_t region_id = leader_info.region_id();
        auto pre_leader_info = get_region_info(region_id);
        // 新增region
        if (pre_leader_info == nullptr) {
            if (leader_info.start_key().empty() && 
                    leader_info.end_key().empty()) {
                // 该region为第一个region
                DB_WARNING("region_id: %ld is new, region info: %s", 
                        region_id, leader_info.ShortDebugString().c_str()); 
                pb::MetaManagerRequest req;
                req.set_op_type(pb::OP_UPDATE_REGION);
                *(req.add_region_infos()) = leader_info;
                SchemaManager::get_instance()->process_schema_info(NULL, &req, NULL, NULL);
            } else if (this->exist_region(leader_info.table_id(),
                                          leader_info.start_key(),
                                          leader_info.end_key(),
                                          leader_info.partition_id())) {
                DB_WARNING("region_id: %ld's region info: %s has exist", 
                        region_id, leader_info.ShortDebugString().c_str());
                // 当前添加的region已经存在，但是pre_leader_info为空
                pb::MetaManagerRequest req;
                req.set_op_type(pb::OP_UPDATE_REGION);
                // 删除该region
                req.set_add_delete_region(true);
                *(req.add_region_infos()) = leader_info; 
                SchemaManager::get_instance()->process_schema_info(NULL, &req, NULL, NULL);
            } else {
                DB_WARNING("region_id: %ld region info: %s is new",
                        region_id, leader_info.ShortDebugString().c_str());
                TableManager::get_instance()->add_new_region(leader_info);
            }
            continue;
        }
        size_t hash_heart = 0, hash_pre = 0;
        for (auto& state : leader_info.peers()) {
            hash_heart += std::hash<std::string>{}(state);
        } 
        for (auto& state : pre_leader_info->peers()) {
            hash_pre += std::hash<std::string>{}(state);
        } 
        bool peer_changed = (hash_heart != hash_pre);
        check_whether_update_region(region_id, 
                                    peer_changed, 
                                    leader_hb, 
                                    pre_leader_info);
        DB_DEBUG("Leader HB, region_id: %ld, peer_changed: %d", region_id, peer_changed);
        if (!peer_changed) {
            check_peer_count(region_id, 
                             leader_hb,
                             table_replica_nums,
                             table_resouce_tags,
                             table_replica_dists,
                             remove_peer_request, 
                             response);
        }
        // TODO: add_learner, remove_learner
        // TODO: remove_peer
    }
}

// 只有Leader才调用此接口
void RegionManager::update_region(const pb::MetaManagerRequest& request, 
        const int64_t apply_index, braft::Closure* done) {
    DB_DEBUG("update_region info: %s", request.ShortDebugString().c_str());
    TimeCost time_cost;
    std::vector<std::string> put_keys;
    std::vector<std::string> put_values;
    std::vector<bool> is_new;
    std::map<int64_t, std::string> min_start_key;
    std::map<int64_t, std::string> max_end_key;
    int64_t s_table_id = -1;

    std::vector<pb::RegionInfo> region_infos;
    if (request.region_infos().size() > 0) {
        for (auto& info: request.region_infos()) {
            region_infos.push_back(info);
        }
    } else {
        return;
    }
    
    // partition_id => (start_key => region_id)
    std::map<int64_t, std::map<std::string, int64_t>> key_id_map;
    for(auto& info: region_infos) {
        int64_t region_id = info.region_id();
        int64_t table_id = info.table_id();
        int64_t partition_id = info.partition_id();
        int ret = TableManager::get_instance()->whether_exist_table_id(table_id);
        if (ret < 0) {
            DB_WARNING("table_name: %s not exist, region_info: %s",
                    info.table_name().c_str(), 
                    info.ShortDebugString().c_str());
            IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "table not exist");
            return ;
        }
        bool new_add = true;
        SmartRegionInfo region_ptr = _region_info_map.get(region_id);
        if (region_ptr) {
            auto& mutable_info = static_cast<pb::RegionInfo&>(info);
            // 配置变更 conf_version + 1
            mutable_info.set_conf_version(region_ptr->conf_version() + 1);
            new_add = false;
        }
        std::string region_value;
        if (!info.SerializeToString(&region_value)) {
            DB_WARNING("request serialzed to string failed: %s", request.ShortDebugString().c_str());
            IF_DONE_SET_RESPONSE(done, pb::PARSE_TO_PB_FAIL, "serialzed to array failed");
            return ;
        }
        is_new.push_back(new_add);
        put_keys.push_back(construct_region_key(region_id));
        put_values.push_back(region_value);
        {
            auto it = min_start_key.find(partition_id);
            if (it == min_start_key.end()) {
                min_start_key[partition_id] = info.start_key();
            } else {
                if (it->second >= info.start_key()) {
                    it->second = info.start_key();
                }
            }

            it = max_end_key.find(partition_id);
            if (it == max_end_key.end()) {
                max_end_key[partition_id] = info.end_key();
            } else {
                if (end_key_compare(it->second, info.end_key()) <= 0) {
                    it->second = info.end_key();
                }
            }
        }  
        if (s_table_id == -1) {
            s_table_id = table_id;
        } else {
            if (s_table_id != table_id) {
                DB_WARNING("table_id: %ld found diff, s_table_id: %ld", 
                        table_id, s_table_id);
                return ;
            }
        }
        if (info.start_key() != info.end_key() || 
                (info.start_key().empty() && info.end_key().empty())) {
            key_id_map[partition_id][info.start_key()] = region_id;
        } 
    }
    bool add_delete_region = false;
    if (request.has_add_delete_region()) {
        add_delete_region = request.add_delete_region();
    }

    int ret = MetaRocksdb::get_instance()->put_meta_info(put_keys, put_values);
    if (ret < 0) {
        IF_DONE_SET_RESPONSE(done, pb::INTERNAL_ERROR, "write db fail");
        return ;
    }
    TableManager::get_instance()->update_start_key_region_id_map(s_table_id, 
        min_start_key, max_end_key, key_id_map);


    DB_DEBUG("update region mem, region_infos size: %ld", region_infos.size());
    // 更新内存
    for (size_t i = 0; i < region_infos.size(); i++) {
        auto& info = region_infos[i];
        DB_DEBUG("region_id: %ld udpate region info: %s", 
                info.region_id(), info.ShortDebugString().c_str());
        int64_t region_id = info.region_id();
        int64_t table_id = info.table_id();
        int64_t partition_id = info.partition_id();
        set_region_info(info);
        RegionStateInfo state;
        state.timestamp = butil::gettimeofday_us();
        state.status = pb::NORMAL;
        set_region_state(region_id, state);
        if (is_new[i]) {
            TableManager::get_instance()->add_region_id(table_id, partition_id, region_id);
        }
    }
    IF_DONE_SET_RESPONSE(done, pb::SUCCESS, "success");
    DB_NOTICE("update region request: %s", request.ShortDebugString().c_str());
}

void RegionManager::check_whether_update_region(int64_t region_id, bool has_peer_changed, 
        const pb::LeaderHB& leader_hb, const SmartRegionInfo& pre_region) {
    DB_DEBUG("region_id: %ld check update region, leader_hb: %s", 
            region_id, leader_hb.ShortDebugString().c_str());
    auto& leader_info = leader_hb.region();
    if (leader_info.log_index() < pre_region->log_index()) {
        DB_WARNING("region_id: %ld Leader log_index: %ld is less than pre region log_index: %ld",
                region_id, leader_info.log_index(), pre_region->log_index());
        return ;
    }
    // 如果version没有变，但是start_key和end_key变了，发生问题
    if (leader_info.version() == pre_region->version() && 
            (leader_info.start_key() != pre_region->start_key() || 
             leader_info.end_key() != pre_region->end_key())) {
        DB_FATAL("region_id: %ld version not changed, but start_key or end_key changed"
                "cur leader info: %s, pre info: %s", 
                region_id, 
                leader_info.ShortDebugString().c_str(),
                pre_region->ShortDebugString().c_str());
        return ;
    }
    bool version_changed = false, peer_changed = false;
    if (leader_info.version() > pre_region->version() || 
            leader_info.start_key() != pre_region->start_key() || 
            leader_info.end_key() != pre_region->end_key()) {
        version_changed = true;
    }
    if (leader_info.status() == pb::IDLE && has_peer_changed) {
        peer_changed = true;
    }
    if (version_changed) {
        DB_WARNING("Not implment version_changed");
    } else if (peer_changed) {
        DB_WARNING("Not implment peer_changed");
    } else {
        if (leader_info.status() == pb::IDLE && 
                leader_info.leader() != pre_region->leader()) {
            set_region_leader(region_id, leader_info.leader());
        }
        if (leader_info.status() == pb::IDLE && 
                (leader_info.log_index() != pre_region->log_index() || 
                 leader_info.used_size() != pre_region->used_size() || 
                 leader_info.num_table_lines() != pre_region->num_table_lines())) {
           set_region_mem_info(region_id, 
                   leader_info.log_index(), 
                   leader_info.used_size(), 
                   leader_info.num_table_lines()); 
        }
    }

}
void RegionManager::check_peer_count(int64_t region_id, 
        const pb::LeaderHB& leader_hb,
        std::unordered_map<int64_t, int64_t>& table_replica_nums,
        std::unordered_map<int64_t, std::string>& table_resource_tags,
        std::unordered_map<int64_t, std::unordered_map<std::string, int64_t>>& table_replica_dists,
        std::vector<std::pair<std::string, pb::RaftControlRequest>>& remove_peer_requests,
        pb::StoreHBResponse* response) {
    DB_DEBUG("region_id: %ld start check peer count, leader_hb: %d", region_id, leader_hb.status());
    if (leader_hb.status() != pb::IDLE) {
        return ;
    }
    const pb::RegionInfo& leader_info = leader_hb.region();
    int64_t table_id = leader_info.table_id();
    if (table_replica_nums.find(table_id) == table_replica_nums.end()) {
        DB_WARNING("region_id: %ld check peer failed, table_id: %ld not exist", region_id, table_id);
        return ;
    }
    int replica_num = table_replica_nums[table_id];

    // add peer
    bool need_add_peer = false;
    // 选resource_tag
    std::string table_resource_tag = table_resource_tags[table_id];
    DB_WARNING("region_id: %ld => [table_name: %s, table_id: %ld, replica_num: %d]", 
            region_id, leader_info.table_name().c_str(), table_id, replica_num);
    std::unordered_map<std::string, int> resource_tag_count;
    std::unordered_map<std::string, std::string> peer_resource_tags;
    for(auto& peer: leader_info.peers()) {
        std::string peer_resource_tag;
        if (!ClusterManager::get_instance()->get_resource_tag(peer, peer_resource_tag)) {
            DB_WARNING("region_id: %ld get resource_tag for peer: %s failed", region_id, peer.c_str());
            return ;
        }
        peer_resource_tags[peer] = peer_resource_tag;
        resource_tag_count[peer_resource_tag]++;
    }
    std::string candicate_logical_room;
    auto& logical_room_count_map = table_replica_dists[table_id];
    std::unordered_map<std::string, int64_t> cur_logical_room_count_map;
    // 如果该region所属table的逻辑存储池没有足够的副本
    if (resource_tag_count[table_resource_tag] < replica_num) {
        DB_WARNING("region_id: %ld resource_tag: %s count: %d less than replica num: %ld, [NEED ADD PEER]",
                region_id, table_resource_tag.c_str(), resource_tag_count[table_resource_tag], replica_num);
        need_add_peer = true;
    }
    // 如果没有指定机房分布表，只需按照replica_num计算, 此时该Region的Leader也没有logical_room
    if (logical_room_count_map.size() == 0 && leader_info.peers_size() < replica_num) {
        need_add_peer = true;
    }
    
    // 指定了机房分布表，要选择逻辑机房
    if (logical_room_count_map.size() > 0) {
        for (auto& peer: leader_info.peers()) {
            std::string logical_room = ClusterManager::get_instance()->get_logical_room(peer);
            if (peer_resource_tags[peer] == table_resource_tag && logical_room.size() > 0) {
                cur_logical_room_count_map[logical_room]++;
            }
        }
        
        for (auto& logical_count: logical_room_count_map) {
            auto& logical_room = logical_count.first;
            if (logical_room_count_map[logical_room] > cur_logical_room_count_map[logical_room]) {
                DB_WARNING("select candicate_logical_room: %s", logical_room.c_str());
                candicate_logical_room = logical_room;
                need_add_peer = true;
                break;
            }
        }
    }    


    // 一次增加一个peer
    if (need_add_peer) {
        std::set<std::string> peers_in_heart;
        for(auto& peer: leader_info.peers()) {
            peers_in_heart.insert(peer);
        }
        std::string new_instance;
        auto ret = ClusterManager::get_instance()->select_instance_rolling(
                table_resource_tag,
                peers_in_heart,
                candicate_logical_room,
                new_instance);
        if (ret < 0) {
            DB_FATAL("select store from cluster failed, region_id: %ld, "
                     "table_resource_tag: %s, peer_size: %ld",
                      region_id, table_resource_tag.c_str(), 
                      leader_info.peers_size());
            return ;
        } 
        pb::AddPeer* add_peer = response->add_add_peers();
        add_peer->set_region_id(region_id);
        for (auto& peer: leader_info.peers()) {
            add_peer->add_old_peers(peer);
            add_peer->add_new_peers(peer);
        }
        add_peer->add_new_peers(new_instance);
        DB_WARNING("region_id: %ld add new peer: %s", region_id, new_instance.c_str());
        return ;
    }

    // TODO: 选择一个peer remove

}
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
