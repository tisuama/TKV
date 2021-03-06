#include "store/region.h"
#include "common/mut_table_key.h"
#include "raft/rocksdb_file_system_adaptor.h"
#include "common/concurrency.h"

#include <butil/file_util.h>
#include <butil/files/file_path.h>


namespace TKV {
DEFINE_int64(compact_delete_lines, 200000, "compact when num_deleted_lines > compact_delete_lines");
DECLARE_int32(election_timeout_ms);
DECLARE_int32(snapshot_interval_s);

DEFINE_string(raftlog_uri, "raft_log: //my_raft_log?id=", "raft_log uri");
DEFINE_string(stable_uri, "raft_meta://my_raft_meta?id=", "raft stable path");
DEFINE_string(snapshot_uri, "local://./raft_data/snapshot", "raft snapshot uri");

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
    
    if (need_peer_balance || is_merged()
            && _report_peer_info) {
        pb::PeerHB* peer_info = request.add_peer_info(); 
        peer_info->set_table_id(copy_region_info.table_id());
        peer_info->set_region_id(_region_id);
        peer_info->set_log_index(_applied_index);
        peer_info->set_start_key(copy_region_info.start_key());
        peer_info->set_end_key(copy_region_info.end_key());
        peer_info->set_is_learner(is_learner());
        if (get_leader().ip != butil::IP_ANY) {
            peer_info->set_exist_leader(true);
        } else {
            peer_info->set_exist_leader(false);
        }
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
}

void Region::on_apply(braft::Iterator& iter) {
} 

int Region::init(bool new_region, int32_t snapshot_times) {
    _shutdown = false;
    if (_init_success) {
        DB_WARNING("region id %ld has inited before", _region_id);
        return 0;
    }
    ON_SCOPED_EXIT([this]() {
            _can_heartbeat = true;
    });

    // 设置region成员信息
    MutableKey start;
    MutableKey end;
    start.append_i64(_region_id);
    end.append_i64(_region_id);
    end.append_u64(UINT64_MAX);
    _rocksdb_start = start.data();
    _rocksdb_end = end.data();
    _data_cf = _rocksdb->get_data_handle();
    _meta_cf = _rocksdb->get_meta_info_handle();
    _meta_writer = MetaWriter::get_instance();
    _resource.reset(new RegionResource);

    // 新建region
    if (new_region) {
        std::string snapshot_path(FLAGS_snapshot_uri, FLAGS_snapshot_uri.find("//") + 2);
        snapshot_path += "/region_" + std::to_string(_region_id);
        auto file_path = butil::FilePath(snapshot_path);
        // 数据没有删除完
        if (butil::DirectoryExists(file_path)) {
            DB_WARNING("new region_id: %ld exist snapshot path: %s",
                    _region_id, snapshot_path.c_str());
            // TODO: RegionControl
        }
        // 被add_peer的node不需要init meta
        // on_snapshot_load时会ingest meta column sst
        if (_region_info.peers_size() > 0) {
            TimeCost write_db_sst;
            if (_meta_writer->init_meta_info(_region_info) != 0) {
                DB_WARNING("write region to rocksdb fail when init region id: %ld", _region_id);
                return -1;
            }
            // Learner
            if (_is_learner && _meta_writer->write_learner_key(_region_info.region_id(), _is_learner) != 0) {\
                DB_FATAL("write learner to rocksdb fail when init region, region_id: %ld", _region_id);
                return -1;
            }
            DB_WARNING("region_id: %ld write init region info: %ld", _region_id, write_db_sst.get_time());            
        } else {
            _report_peer_info = true;
        }
    }

    // init raft node
    braft::NodeOptions options;
    std::vector<braft::PeerId> peers;
    for (int i = 0; i < _region_info.peers_size(); i++) {
        butil::EndPoint end_point;
        if (butil::str2endpoint(_region_info.peers(i).c_str(), &end_point) != 0) {
            DB_FATAL("str2endpoint fail, peers: %s, region_id: %ld", 
                    _region_info.peers(i).c_str(), _region_id);
            return -1;
        }
        peers.push_back(braft::PeerId(end_point));
    }
    options.election_timeout_ms = FLAGS_election_timeout_ms;
    options.fsm = this;
    options.initial_conf = braft::Configuration(peers);
    options.snapshot_interval_s = 0; // 自己设置？
    options.log_uri = FLAGS_raftlog_uri + std::to_string(_region_id);
    options.raft_meta_uri = FLAGS_stable_uri + std::to_string(_region_id);
    options.snapshot_uri = FLAGS_snapshot_uri + "/region_" + std::to_string(_region_id);
    options.snapshot_file_system_adaptor = &_snapshot_adaptor;
    
    bool is_restart = _restart;
    if (_is_learner) {
        // TODO: not impl in braft
        DB_WARNING("start init learner, region_id: %ld", _region_id);
    } else {
        DB_WARNING("start init node, region_id: %ld", _region_id);
        if (_node.init(options) != 0) {
            DB_FATAL("raft node init fail, region_id: %ld, region_info: %s",
                    _region_id, _region_info.ShortDebugString().c_str());
            return -1;
        }
        if (peers.size() == 1) {
            _node.reset_election_timeout_ms(FLAGS_election_timeout_ms); // 10ms
            DB_WARNING("region_id: %ld vote 0", _region_id);
        }
    }
    if (!is_restart && can_add_peer()) {
        _need_decrease = true;
    }
    
    // snapshot_times = 2
    while(snapshot_times > 0) {
        // init 的region会马上选主，等一会成为Leader
        bthread_usleep(1 * 1000 * 1000LL);
        _region_control.sync_do_snapshot();
        --snapshot_times;
    }
    this->copy_region(&_resource->region_info);
    // TODO: compact时候删除多余数据
    DB_WARNING("=== region_id: %ld init success, region_info: %s", _region_id, 
            _resource->region_info.ShortDebugString().c_str());
    _init_success = true;
    
    return 0;
}

void Region::on_snapshot_save(braft::SnapshotWriter* writer, braft::Closure* done) {
    TimeCost cost;
    brpc::ClosureGuard done_gurad(done);
    if (this->get_version() == 0) {
        // 等待异步队列为空
        this->wait_async_apply_log_queue_empty();
    }
    // META_CF和DATA_CF的数据要做快照
    if (writer->add_file(SNAPSHOT_META_FILE) != 0 ||
        writer->add_file(SNAPSHOT_DATA_FILE) != 0) {
        done->status().set_error(EINVAL, "Fail to add snapshot");
        DB_WARNING("Error while add extra_fs to writer, region_id: %ld", _region_id);
        return ;
    }
    DB_WARNING("region_id: %ld snapshot save complete, time cost: %ld",
            _region_id, cost.get_time());
    this->reset_snapshot_status();
}

void Region::reset_snapshot_status() {
    // 重置后续判断是否需要快照的标准
    if (_snapshot_time_cost.get_time() > FLAGS_snapshot_interval_s * 1000 * 1000) {
        _snapshot_num_table_lines = _num_table_lines.load();
        _snapshot_index = _applied_index;
        _snapshot_time_cost.reset();
    }
}

int Region::on_snapshot_load(braft::SnapshotReader* reader) {
    this->reset_timecost();
    TimeCost time_cost;
    DB_WARNING("region_id: %ld start to load snapshot, path: %s", _region_id, reader->get_path());
    ON_SCOPED_EXIT([this]() {
        _meta_writer->clear_doing_snapshot(_region_id);
        DB_WARNING("region_id: %ld end  to laod snapshot", _region_id);
    });

    std::string data_sst_file = reader->get_path() + SNAPSHOT_DATA_FILE_WITH_SLASH;
    std::string meta_sst_file = reader->get_path() + SNAPSHOT_META_FILE_WITH_SLASH;
    std::map<int64_t, std::string> prepared_log_entrys;
    // butil::FilePath  
    butil::FilePath snapshot_meta_file(meta_sst_file);
    if (_restart && !Store::get_instance()->doing_snapshot_when_stop(_region_id)) {
        // 本地重启，不需要做过多的事情
        DB_WARNING("region_id: %ld restart with no snapshot load", _region_id);
        on_snapshot_load_for_restart(reader, prepared_log_entrys);
     } else if (!butil::PathExists(snapshot_meta_file)) { // 文件是否存在
         //数据不完整
        DB_WARNING("region_id: %ld no meta sst file", _region_id); 
        return -1;
     } else {
         // 正常流程的snapshot没有加载完，重启需要重新ingest sst
         // 或者是Follower正常InstallSnapshot流程
        _meta_writer->write_doing_snapshot(_region_id);
        DB_WARNING("region_id: %d doing on snapshot load when closed", _region_id);
        int ret = 0;
        if (is_addpeer()) {
            ret = Concurrency::get_instance()->snapshot_load_concurrency.increase_wait();
            DB_WARNING("region_id: %ld snapshot load, wait_time: %ld, ret: %d",
                    _region_id, time_cost.get_time(), ret);
        }
        ON_SCOPED_EXIT([this]() {
            if (is_addpeer()) {
                Concurrency::get_instance()->snapshot_load_concurrency.decrease_broadcast();
                if (_need_decrease) {
                    _need_decrease = false;
                    Concurrency::get_instance()->received_add_peer_concurrency.decrease_broadcast();
                }
            }    
        });

        if (_region_info.version() != 0) {
            int64_t old_data_index = _data_index;
            DB_WARNING("region_id: %ld clear data on_snapshot_load", _region_id);
            _meta_writer->clear_meta_info(_region_id);
            ret = RegionControl::ingest_meta_sst(meta_sst_file, _region_id);
            if (ret < 0) {
                DB_FATAL("ingest sst fail, region_id: %ld", _region_id);
                return -1;
            }
            // Read _applied_index and _data_index from rocksdb
            _meta_writer->read_applied_index(_region_id, &_applied_index, &_data_index);
            // 生成的data_cf的SST文件是从idx = 0开始
            butil::FilePath file_path(data_sst_file + "0");                  
            // 两种情况：1) 此次快照的data_index更大，包含数据更多
            // 2) restart时有快照没有安装完成，因为data_sst_file还存在
            if (_data_index > old_data_index || 
                    (_restart && butil::PathExists(file_path))) {
                RegionControl::remove_data(_region_id);
                // reingest sst
                // TODO: on_snapshot_load
            }
        }
     }
}

void Region::on_snapshot_load_for_restart(braft::SnapshotReader* reader, 
        std::map<int64_t, std::string>& prepared_log_entrys) {
    // TODO: 考虑没有committed的日志
    // Read applied index from meta_cf 
    _meta_writer->read_applied_index(_region_id, &_applied_index, &_data_index);
    _num_table_lines = _meta_writer->read_num_table_lines(_region_id);
    DB_WARNING("load snaphshot for restart, region_id: %ld, applied_index: %ld, data_index: %ld",
            _region_id, _applied_index, _data_index);
}

} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
