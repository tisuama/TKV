#include "store/region.h"
#include "common/mut_table_key.h"
#include "raft/rocksdb_file_system_adaptor.h"
#include "common/concurrency.h"
#include "store/store.h"
#include "store/rpc_sender.h"
#include "proto/optype.pb.h"
#include "store/closure.h"

#include <butil/file_util.h>
#include <butil/files/file_path.h>
#include <butil/strings/string_split.h>

namespace TKV {
DECLARE_int32(election_timeout_ms);
DECLARE_int32(snapshot_interval_s);

DEFINE_int64(compact_delete_lines, 200000, "compact when num_deleted_lines > compact_delete_lines");
DEFINE_int64(split_duration_us, 3600 * 1000 * 1000LL, "split duration, default: 3600s");

DECLARE_string(raftlog_uri);
DECLARE_string(stable_uri);
DECLARE_string(snapshot_uri);

void Region::compact_data_in_queue() {
    // 删除数据太多，开始compact
    _num_delete_lines = 0;
    RegionControl::compact_data_in_queue(_region_id);
}

void Region::construct_heart_beat_request(pb::StoreHBRequest& request, bool need_peer_balance) {
    if (_shutdown || !_can_heartbeat || _removed) {
        return ;
    }
    // TODO: multi-thread cond
    if (_num_delete_lines > FLAGS_compact_delete_lines) {
        DB_WARNING("region_id: %ld, delete %ld rows, do compact in queue",
                _region_id, _num_delete_lines.load());
        compact_data_in_queue();
    }
    
    // RegionInfo 
    pb::RegionInfo copy_region_info;
    copy_region(&copy_region_info);
    // Learner 在版本0时可以上报心跳
    if (copy_region_info.version() == 0 && !is_learner()) {
        DB_WARNING("region version is 0, region_id: %ld", _region_id);
        return ;
    }
    _region_info.set_num_table_lines(_num_delete_lines.load());
    
    if (need_peer_balance || is_merged()
            && _report_peer_info) {
        pb::PeerHB* peer_info = request.add_peer_infos(); 
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
    if (is_leader() && _node.list_peers(&peers).ok()) {
        pb::LeaderHB* leader_heart = request.add_leader_regions();
        // TODO: region_control
        pb::RegionInfo* leader_region = leader_heart->mutable_region();
        // 将内存的_region_info -> leader_region中
        copy_region(leader_region);
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
    for (; iter.valid(); iter.next()) {
        braft::Closure* done =  iter.done();
        butil::IOBuf data = iter.data();
        butil::IOBufAsZeroCopyInputStream wrapper(data);
        auto request = std::make_shared<pb::StoreReq>();
        if (!request->ParseFromZeroCopyStream(&wrapper)) {
            DB_FATAL("Parse failed, region_id: %ld", _region_id);
            if (done != nullptr) {
                ((DMLClosure*)done)->response->set_errcode(pb::PARSE_FROM_PB_FAIL);
                ((DMLClosure*)done)->response->set_errmsg("parse failed");
                braft::run_closure_in_bthread(done);
            }
            continue;
        }

        // 重置计时器
        reset_timecost(); 
        int64_t term = iter.term();
        int64_t index = iter.index();
        // 分裂情况下， region version为0 => 异步逻辑 
        // OP_ADD_VERSION_FOR_SPLIT_REGION以及分裂结束 => 同步逻辑
        // verion为0时，on_leader_start一定会提交一条OP_CLEAR_APPLYING_TXN => 同步逻辑
        
        do_apply(term, index, *request, done);
        if (done) {
            done->Run();
        } 
    }
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
            RegionControl::remove_data(_region_id);
            RegionControl::remove_meta(_region_id);
            RegionControl::remove_log_entry(_region_id);
            RegionControl::remove_snapshot_path(_region_id);
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
    std::string region_str = "id=" + std::to_string(_region_id);
    options.election_timeout_ms = FLAGS_election_timeout_ms;
    options.fsm = this;
    options.initial_conf = braft::Configuration(peers);
    options.snapshot_interval_s = 0; // 自己设置？
    options.log_uri = FLAGS_raftlog_uri + region_str;
    options.raft_meta_uri = FLAGS_stable_uri + region_str;
    options.snapshot_uri = FLAGS_snapshot_uri + region_str;
    options.snapshot_file_system_adaptor = &_snapshot_adaptor;
    
    bool is_restart = _restart;
    if (_is_learner) {
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
    copy_region(&_resource->region_info);
    // TODO: compact时候删除多余数据
    DB_WARNING("region_id: %ld init success, region_info: %s", _region_id, 
            _resource->region_info.ShortDebugString().c_str());
    _init_success = true;
    
    return 0;
}

void Region::on_snapshot_save(braft::SnapshotWriter* writer, braft::Closure* done) {
    TimeCost time_cost;
    brpc::ClosureGuard done_guard(done);
    if (get_version() == 0) {
        // 等待异步队列为空
        wait_async_apply_log_queue_empty();
    }
    // META_CF和DATA_CF的数据要做快照
    if (writer->add_file(SNAPSHOT_META_FILE) != 0 ||
        writer->add_file(SNAPSHOT_DATA_FILE) != 0) {
        done->status().set_error(EINVAL, "Fail to add snapshot");
        DB_WARNING("Error while add extra_fs to writer, region_id: %ld", _region_id);
        return ;
    }
    DB_WARNING("region_id: %ld snapshot save complete, time cost: %ld",
            _region_id, time_cost.get_time());
    reset_snapshot_status();
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
    reset_timecost();
    TimeCost time_cost;
    DB_WARNING("region_id: %ld start to load snapshot, path: %s", _region_id, reader->get_path().c_str());
    ON_SCOPED_EXIT([this]() {
        _meta_writer->clear_doing_snapshot(_region_id);
        DB_WARNING("region_id: %ld end  to laod snapshot", _region_id);
    });

    std::string data_sst_file = reader->get_path() + SNAPSHOT_DATA_FILE_WITH_SLASH;
    std::string meta_sst_file = reader->get_path() + SNAPSHOT_META_FILE_WITH_SLASH;
    std::map<int64_t, std::string> prepared_log_entrys;
    // butil::FilePath  
    butil::FilePath snapshot_meta_file(meta_sst_file);
    // 非restart情况下，不回Install不完整的SST
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
                    Concurrency::get_instance()->receive_add_peer_concurrency.decrease_broadcast();
                }
            }    
        });

        if (_region_info.version() != 0) {
            int64_t old_data_index = _data_index;
            DB_WARNING("region_id: %ld clear data on_snapshot_load", _region_id);
            // 为on_snapshot_load的meta_info不能清除
            _meta_writer->clear_meta_info(_region_id);
            ret = RegionControl::ingest_meta_sst(meta_sst_file, _region_id);
            if (ret < 0) {
                DB_FATAL("ingest sst fail, region_id: %ld", _region_id);
                return -1;
            }
            // Read _applied_index and _data_index from rocksdb
            _meta_writer->read_applied_index(_region_id, &_applied_index, &_data_index);
            // SST生成的data_cf的SST文件是从idx = 0开始，meta_cf没有idx
            butil::FilePath file_path(data_sst_file + "0");                  
            // 两种情况：1) 此次快照的data_index更大，包含数据更多
            // 2) restart时有快照没有安装完成，因为data_sst_file还存在
            if (_data_index > old_data_index || 
                    (_restart && butil::PathExists(file_path))) {
                // remove old data
                RegionControl::remove_data(_region_id);
                // ingest snapshot sst
                ret = ingest_snapshot_sst(reader->get_path());
                if (ret) {
                    DB_FATAL("ingest sst fail when snapshot load, region_id: %ld", _region_id);
                    return -1;
                }
            } else {
                DB_WARNING("region_id: %ld no need clear data, data_index: %ld, old_data_index: %ld", 
                        _region_id, _data_index, old_data_index);
            }
        } else { // region_info.version() == 0，没有数据
            if (is_learner()) {
                // TODO: check learner snapshot
            } else {
                if (check_follower_snapshot(butil::endpoint2str(get_leader()).c_str()) != 0) {
                    DB_FATAL("region_id: %ld check follower snapshot fail", _region_id);
                    return -1;
                }
            }
            // 先ingest data在ingest meta, meta_sst_file可能有新的version
            // 这样add_peer遇到重启时，直接靠version=0可以清理掉残留的region
            // Store::init_before_listen
            ret = ingest_snapshot_sst(reader->get_path());
            if (ret) {
                DB_FATAL("ingest sst fail when on snapshot load, region_id: %ld", _region_id);
                return -1;
            }
            ret = RegionControl::ingest_meta_sst(meta_sst_file, _region_id);
            if (ret < 0) {
                DB_FATAL("ingest sst fail, region_id: %ld", _region_id);
                return -1;
            }
        }
        DB_WARNING("region_id: %ld scuccess load snapshot, ingest sst file", _region_id);
    }
    _meta_writer->read_applied_index(_region_id, &_applied_index, &_data_index);
    _num_table_lines = _meta_writer->read_num_table_lines(_region_id);
    pb::RegionInfo region_info;
    int ret = _meta_writer->read_region_info(_region_id, region_info);
    if (ret < 0) {
        DB_FATAL("read region info fail on snapshot load, region_id: %ld", _region_id);
        return -1;
    } 
    if (_applied_index <= 0) {
        DB_FATAL("region_id: %ld applied_index <= 0", _region_id);
        return -1;
    }
    if (_num_table_lines < 0) {
        DB_WARNING("region_id: %ld num_table_lines: %ld", _region_id, _num_table_lines.load());
        _meta_writer->update_num_table_lines(_region_id, 0);
        _num_table_lines = 0;
    }
    region_info.set_can_add_peer(true);
    set_region_with_update_range(region_info);
    if (!compare_and_set_legal()) {
        DB_FATAL("region_id: %ld is not illegal, should be removed", _region_id);
        return -1;
    }
    _new_region_infos.clear();
    _snapshot_num_table_lines = _num_table_lines.load();
    _snapshot_index = _applied_index;
    _snapshot_time_cost.reset();
    copy_region(&_resource->region_info);
    _restart = false;
    // Learner read for read
    _learner_ready_for_read = true;
    return 0;
}

void Region::on_snapshot_load_for_restart(braft::SnapshotReader* reader, 
        std::map<int64_t, std::string>& prepared_log_entrys) {
    // TODO: 考虑没有committed的日志
    // Read applied index from meta_cf 
    _meta_writer->read_applied_index(_region_id, &_applied_index, &_data_index);
    _num_table_lines = _meta_writer->read_num_table_lines(_region_id);
    DB_WARNING("load snapshot for restart, region_id: %ld, applied_index: %ld, data_index: %ld",
            _region_id, _applied_index, _data_index);
}

int Region::ingest_snapshot_sst(const std::string& dir) {
    // ingest采用move的方式，先创建硬链接，再ingest
    butil::DirReaderPosix dir_reader(dir.c_str());
    if (!dir_reader.IsValid()) {
        return -1;
    }
    int cnt = 0;
    while (dir_reader.Next()) {
        std::string sub_path = dir_reader.name();
        std::vector<std::string> split_vec;
        butil::SplitString(sub_path, '/', &split_vec);
        std::string data_path = split_vec.back();
        // stars_with
        if (data_path.find(SNAPSHOT_DATA_FILE) == 0) {
            std::string link_path = dir + "/link." + data_path;
            ::link(sub_path.c_str(), link_path.c_str());
            DB_WARNING("region_id: %ld ingest source path: %s", _region_id, link_path.c_str());
            if (is_addpeer() && !_restart) {
                // TODO: wait rocksdb not stall
            }
            int ret = RegionControl::ingest_data_sst(link_path, _region_id, true);
            if (ret < 0) {
                DB_FATAL("region_id: %ld ingest sst fail", _region_id);
                return -1;
            }
            cnt++;
        }
    }
    if (!cnt) {
        DB_WARNING("region_id: %ld is empty when on snapshot load", _region_id);
    }
    return 0; 
}

int Region::check_follower_snapshot(const std::string& peer) {
    uint64_t peer_data_size = 0;
    uint64_t peer_meta_size = 0;
    int64_t snapshot_index = 0;
    RpcSender::get_peer_snapshot_size(peer, _region_id, 
            &peer_data_size, &peer_meta_size, &snapshot_index);
    DB_WARNING("region_id: %ld is new, no need clear data, "
            "remote_data_size: %lu, cur_data_size: %lu, "
            "remote_meta_size: %lu, cur_meta_size: %lu, "
            "region_info: %s",
            _region_id, peer_data_size, snapshot_data_size(),
            peer_meta_size, snapshot_meta_size(),
            _region_info.ShortDebugString().c_str());

    if (peer_data_size != 0 && snapshot_data_size() != 0 && 
            peer_data_size != snapshot_data_size()) {
        DB_FATAL("region_id: %ld cur_data_size: %lu, remote_data_size: %lu", 
                _region_id, snapshot_data_size(), peer_data_size);
        return -1;
    }
    if (peer_meta_size != 0 && snapshot_meta_size() != 0 && 
            peer_meta_size != snapshot_meta_size()) {
        DB_FATAL("region_id: %ld cur_meta_size: %lu, remote_meta_size: %lu", 
                _region_id, snapshot_meta_size(), peer_meta_size);
        return -1;
    }
    return 0;
}

void Region::set_region_with_update_range(const pb::RegionInfo& region_info) {
    BAIDU_SCOPED_LOCK(_region_lock);
    _region_info.CopyFrom(region_info);
    _version = _region_info.version();
    std::shared_ptr<RegionResource> new_resource(new RegionResource);
    new_resource->region_info = region_info;
    {
        BAIDU_SCOPED_LOCK(_resource_lock);
        _resource = new_resource;
    }
    // TODO: compaction的时候删除多余的数据
    DB_WARNING("region_id: %ld, start_key: %s, end_key: %s", _region_id, 
            rocksdb::Slice(region_info.start_key()).ToString().c_str(),
            rocksdb::Slice(region_info.end_key()).ToString().c_str());
}

bool Region::check_region_legal_complete() {
    do {
        // store init_region时调用此函数循环检查, 如果_legal_region = false，store调用drop_region删除
        bthread_usleep(10 * 1000 * 1000LL);
        // 3600s没有收到请求，且version也没有更新的话，则分裂失败
        if (_removed) {
            DB_WARNING("region_id: %ld has been removed", _region_id);
            return true;
        }
        if (get_timecost() > FLAGS_split_duration_us) {
            if (compare_and_set_illegal()) {
                DB_WARNING("region_id: %ld split or add peer fail, set illegal", _region_id);
                return false;
            } else {
                DB_WARNING("region_id: %ld split or add peer success", _region_id);
                return true;
            }
        } else if (get_version() > 0) {
            DB_WARNING("region_id: %ld split or add peer success", _region_id);
            return true;
        } else {
            DB_WARNING("region_id: %ld split or add peer not complete, need wait, time cost: %ld",
                    _region_id, get_timecost());
        }
    } while (1);
}

void Region::query(::google::protobuf::RpcController* controller,
                   const ::TKV::pb::StoreReq* request, 
                   ::TKV::pb::StoreRes* response,
                   ::google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl = (brpc::Controller*)controller;
    uint64_t log_id = 0;
    if (cntl->has_log_id()) {
        log_id = cntl->log_id();
    }
    const auto& remote_side_tmp = butil::endpoint2str(cntl->remote_side());
    const char* remote_side = remote_side_tmp.c_str();
    if (!is_leader()) {
        // 支持非一致性读
        if (!request->select_without_leader() || _shutdown || !_init_success
                || (is_learner() && !learner_ready_for_read())) {
            response->set_errcode(pb::NOT_LEADER);
            response->set_errmsg("Not Leader");
            DB_WARNING("region_id: %ld not leader, leader: %s, log_id: %lu, remote_side: %s",
                    _region_id, butil::endpoint2str(get_leader()).c_str(), log_id, remote_side);
            return ;
        }
    }
    // valid region version
    if (!valid_version(request, response)) {
        DB_WARNING("region_id: %ld version too old, log_id: %lu, request version: %ld, region version: %ld"  
                " op_type: %s, remote_side: %s", _region_id, log_id, request->region_version(), get_version(), 
                pb::OpType_Name(request->op_type()).c_str(), remote_side);
        return ;
    }
    // 启动时，或者Foller落后太多，需要读Leader
    if (request->op_type() == pb::OP_SELECT && request->region_version() > get_version()) {
        response->set_errcode(pb::NOT_LEADER); 
        response->set_leader(butil::endpoint2str(get_leader()).c_str());
        response->set_errmsg("Not Leader");
        DB_WARNING("region_id: %ld not leader, request version: %ld, region version: %ld, log_id: %lu, remote_side: %s",
                _region_id, request->region_version(), get_version(), log_id, butil::endpoint2str(get_leader()).c_str());
        return ;
    }
    
    switch(request->op_type()) {
        case pb::OP_NONE: 
            apply(request, response, cntl, done_guard.release());
            break;
        default:
            break;
    }
    return ; 
}

bool Region::valid_version(const pb::StoreReq* request, pb::StoreRes* response) {
    if (request->region_version() < get_version()) {
        response->Clear();
        response->set_errcode(pb::VERSION_OLD);
        response->set_errmsg("Region version too old");

        std::string leader_str = butil::endpoint2str(get_leader()).c_str();
        response->set_leader(leader_str);
        auto region = response->add_regions();
        copy_region(region);
        region->set_leader(leader_str);
        // Case1: mrege
        if (!region->start_key().empty() && region->start_key() == region->end_key()) {
            // start_key == end_key, region发生merge
            response->set_is_merge(true);
            if (_merge_region_info.start_key() != region->start_key()) {
                DB_FATAL("region_id: %ld merge region: %ld start key not equal", 
                        _region_id, _merge_region_info.region_id());
            } else {
                response->add_regions()->CopyFrom(_merge_region_info);
                DB_WARNING("region_id: %d merge region info: %s",
                        _region_id, _merge_region_info.ShortDebugString().c_str());
            }
        } else {  // Case2: split
            response->set_is_merge(false);
            for (auto& r: _new_region_infos) {
                if (r.region_id() != 0 && r.version() != 0) {
                    response->add_regions()->CopyFrom(r);
                    DB_WARNING("region_id: %ld new region: %ld", _region_id, r.region_id());
                } else {
                    DB_FATAL("region_id: %ld, new region info: %s", _region_id, r.ShortDebugString().c_str());
                }
            }
        }
        return false;
    }
    return true;
}

void Region::apply(const pb::StoreReq* request, pb::StoreRes* response, 
        brpc::Controller* cntl, google::protobuf::Closure* done) {
    uint64_t log_id = 0;
    if (cntl->has_log_id()) {
        log_id = cntl->log_id();
    }
    butil::IOBuf data;
    butil::IOBufAsZeroCopyOutputStream wrapper(&data);
    if (!request->SerializeToZeroCopyStream(&wrapper)) {
        cntl->SetFailed(brpc::EREQUEST, "Fail to serialize request");
        return ;
    }
    DMLClosure* c = new DMLClosure;
    c->cost.reset();
    c->op_type = request->op_type();
    c->log_id = log_id;
    c->response = response;
    c->region = this;

    braft::Task task;
    task.data = &data;
    task.done = c;
    _node.apply(task);
} 

void Region::do_apply(int64_t term, int64_t index, const pb::StoreReq& request, braft::Closure* done) {
    if (index <= _applied_index) {
        DB_WARNING("region_id: %ld, log_index: %ld, applied_index: %ld has been exexuted", 
                _region_id, index, _applied_index);
        return ;
    }
    _region_info.set_log_index(index);
    _applied_index = index;
    pb::StoreRes res;
    switch(request.op_type()) {
        case pb::OP_NONE:
            _meta_writer->update_apply_index(_region_id, _applied_index, _data_index);
            if (done) {
                ((DMLClosure*)done)->response->set_errcode(pb::SUCCESS);
            }
            DB_NOTICE("region_id: %ld OP_NONE sucess, applied_index: %ld, term: %ld",
                    _region_id, _applied_index,  term);
            break;
        default:
            _meta_writer->update_apply_index(_region_id, _applied_index, _data_index);
            DB_NOTICE("region_id: %ld OP_TYPE: %d ERROR, not support", _region_id, request.op_type());
            if (done) {
                ((DMLClosure*)done)->response->set_errcode(pb::UNSUPPORT_REQ_TYPE);
                ((DMLClosure*)done)->response->set_errmsg("unsupport request type");
            }
            break;
    }
}

void Region::on_shutdown() {
    DB_WARNING("region_id: %ld shutdown", _region_id);
}

void Region::on_leader_start(int64_t term) {
    DB_WARNING("region_id: %ld Leader start at term: %ld", _region_id, term);
    _region_info.set_leader(butil::endpoint2str(get_leader()).c_str());
    // TODO: APPLYING_TXN 指定
    leader_start(term);
}

void Region::leader_start(int64_t term) {
    _is_leader.store(true);
    _expect_term = term;
    DB_WARNING("region_id: %ld Leader start at term: %ld", _region_id, term);
}

void Region::on_leader_stop() {
    DB_WARNING("region_id: %ld Leader stop", _region_id);
    _is_leader.store(false);
    // TODO: 只读事务清理
}

void Region::on_leader_stop(const butil::Status& status) {
    DB_WARNING("region_id: %ld Leader stop, err_code: %d, err_msg: %s",
            _region_id, status.error_code(), status.error_cstr());
    _is_leader.store(false);
    // 只读事务清理
}
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
