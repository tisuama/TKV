#include "store/region.h"
namespace TKV {
DEFINE_int64(compact_delete_lines, 200000, "compact when num_deleted_lines > compact_delete_lines");

void Region::compact_data_in_queue() {
    _num_delete_lines = 0;
    // TODO: region_control compact_data_in_queue
}

void Region::construct_heart_beat_request(pb::StoreHBRequest& request, bool need_peer_balance) {
    if (_shutdown || !_can_heartbeat || _removed) {
        return ;
    }
    // TODO: multi-thread cond
    if (_num_delete_lines > FLAGS_compact_delete_lines) {
        DB_WARNING("region_id: %ld, delete %ld rows, do compact in queue",
                _region_id, _num_delete_lines.load());
        this->compact_data_in_queue();
    }
    
    // RegionInfo 
    pb::RegionInfo copy_region_info;
    this->copy_region(&copy_region_info);
    // Learner 在版本0时可以上报心跳
    if (copy_region_info.version() == 0 && !is_learner()) {
        DB_WARNING("region version is 0, region_id: %ld", _region_id);
        return ;
    }
    _region_info.set_num_table_lines(_num_delete_lines.load());
    
    // TODO: 增加peer心跳信息
    if (need_peer_balance || is_merged()) {
        // do something
    } 
    
    // Leader心跳信息
    std::vector<braft::PeerId> peers;
    if (this->is_leader() && _node.list_peers(&peers).ok()) {
        pb::LeaderHB* leader_heart = request.add_leader_regions();
        // TODO: region_control
        pb::RegionInfo* leader_region = leader_heart->mutable_region();
        // 将内存的_region_info -> leader_region中
        this->copy_region(leader_region);
        leader_region->set_leader(_address); 
        // region_info的log_index是之前持久化在磁盘的log_index，不太准
        leader_region->set_log_index(_applied_index);
        leader_region->clear_peers();
        for (auto& peer: peers) {
            leader_region->add_peers(butil::endpoint2str(peer.addr).c_str());
        }
        // TODO: construct peer status
    }
    
    // TODO: is_learner
    if (is_learner()) {
        // do something
    }
}

void Region::on_apply(braft::Iterator& iter) {
    // do something
} 

} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
