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
// raft
DECLARE_int32(election_timeout_ms);
DECLARE_int32(snapshot_interval_s);
DECLARE_string(raftlog_uri);
DECLARE_string(stable_uri);
DECLARE_string(snapshot_uri);
// txn
DECLARE_int64(exec_1pc_out_fsm_timeout_ms);

DEFINE_int64(compact_delete_lines, 200000, "compact when num_deleted_lines > compact_delete_lines");
DEFINE_int64(split_duration_us, 3600 * 1000 * 1000LL, "split duration, default: 3600s");

static bool is_dml_op_type(const pb::OpType& op_type) {
    // 当前还不支持INSERT/DELETE/UPDATE/SELETE_FOR_UPDATE等DML语句
    if (op_type == pb::OP_TXN_KV_PUT || 
        op_type == pb::OP_TXN_KV_GET ||
        op_type == pb::OP_TXN_KV_DELETE || 
        op_type == pb::OP_RAW_KV_PUT || 
        op_type == pb::OP_RAW_KV_GET ||
        op_type == pb::OP_RAW_KV_DELETE) {
        return true;
    }
    return false;
}

void Region::compact_data_in_queue() {
    _num_delete_lines = 0;
    RegionControl::compact_data_in_queue(_region_id);
}

void Region::construct_heart_beat_request(pb::StoreHBRequest& request, bool need_peer_balance) {
    if (_shutdown || !_can_heartbeat || _removed) {
        return ;
    }
    if (_num_delete_lines > FLAGS_compact_delete_lines) {
        DB_WARNING("region_id: %ld, delete %ld rows, do compact in queue",
                _region_id, _num_delete_lines.load());
        compact_data_in_queue();
    }
    
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
}

void Region::on_apply(braft::Iterator& iter) {
    for (; iter.valid(); iter.next()) {
        braft::Closure* done =  iter.done();
        brpc::ClosureGuard done_guard(done);

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
            braft::run_closure_in_bthread(done_guard.release());
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
    options.snapshot_interval_s = 0;
    options.log_uri = FLAGS_raftlog_uri + region_str;
    options.raft_meta_uri = FLAGS_stable_uri + region_str;
    options.snapshot_uri = FLAGS_snapshot_uri + region_str;
    options.snapshot_file_system_adaptor = &_snapshot_adaptor;

    _txn_pool.init(_region_id, _use_ttl, _online_ttl_us);
    
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
    
    // init 的region会马上选主，等一会成为Leader
    bthread_usleep(1 * 1000 * 1000LL);
    // snapshot_times = 2
    while(snapshot_times > 0) {
        _region_control.sync_do_snapshot();

        DB_DEBUG("region_id: %ld do %d-th snapshot", _region_id, snapshot_times);

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
            // 两种情况：
            // 1) 此次快照的data_index更大，包含数据更多
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
            std::string child_path = dir + "/" + data_path;
            ::link(child_path.c_str(), link_path.c_str());
            DB_WARNING("region_id: %ld ingest source path: %s, child_path: %s", _region_id, link_path.c_str(), child_path.c_str());
            if (is_addpeer() && !_restart) {
                // TODO: wait rocksdb not stall
                DB_WARNING("region_id: %ld ingest data failed", _region_id);
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
    DB_DEBUG("region_id: %ld process query, request: %s, done: %p", 
            _region_id, request->ShortDebugString().c_str(), done);

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
    
    switch(request->op_type()) {
        case pb::OP_NONE: {
            apply(request, response, cntl, done_guard.release());
            break;
        }
        case pb::OP_KV_BATCH: 
        case pb::OP_PREPARE:
        case pb::OP_COMMIT:
        case pb::OP_ROLLBACK:
        case pb::OP_RAW_KV_PUT:
        case pb::OP_RAW_KV_GET:
        case pb::OP_RAW_KV_DELETE: {
            uint64_t txn_id = 0;
            if (request->txn_infos_size() > 0) {
                txn_id = request->txn_infos(0).txn_id();
            }
            if (txn_id == 0) {
                exec_out_txn_query(controller, request, response, done_guard.release());
            } else {
                exec_in_txn_query(controller, request, response, done_guard.release());
            }
            break;
        }
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
        if (!region->start_key().empty() && region->start_key() == region->end_key()) {
            /* start_key == end_key, region发生merge */
            response->set_is_merge(true);
            if (_merge_region_info.start_key() != region->start_key()) {
                DB_FATAL("region_id: %ld merge region: %ld start key not equal", 
                        _region_id, _merge_region_info.region_id());
            } else {
                response->add_regions()->CopyFrom(_merge_region_info);
                DB_WARNING("region_id: %d merge region info: %s",
                        _region_id, _merge_region_info.ShortDebugString().c_str());
            }
        } else {
            /* region发生split */
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

void Region::apply(const pb::StoreReq* request, 
        pb::StoreRes* response, 
        brpc::Controller* cntl, 
        google::protobuf::Closure* done) {

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
    c->done = done;
    
    DB_DEBUG("region_id: %ld appy, done: %p", _region_id, done);

    braft::Task task;
    task.data = &data;
    task.done = c;
    _node.apply(task);
} 

void Region::do_apply(int64_t term, 
        int64_t index, 
        const pb::StoreReq& request, 
        braft::Closure* done) {

    if (index <= _applied_index) {
        DB_WARNING("region_id: %ld, log_index: %ld, applied_index: %ld has been exexuted", 
                _region_id, index, _applied_index);
        return ;
    }
    _region_info.set_log_index(index);
    _applied_index = index;
    pb::StoreRes res;
    switch(request.op_type()) {
        case pb::OP_PREPARE:
        case pb::OP_COMMIT:
        case pb::OP_ROLLBACK: {
            _data_index = _applied_index;
            apply_txn_request(request, done, _applied_index, term);
            break;
        }
        case pb::OP_KV_BATCH: {
            // 存储计算分离时使用，走separate模式
            _data_index = _applied_index;
            uint64_t txn_id = request.txn_infos_size() > 0 ? request.txn_infos(0).txn_id(): 0;
            if (txn_id == 0) {
                apply_kv_out_txn(request, done, _applied_index, term);
            } else {
                apply_kv_in_txn(request, done, _applied_index, term);
            }
            break;
        }
        case pb::OP_RAW_KV_PUT:
        case pb::OP_RAW_KV_GET:
        case pb::OP_RAW_KV_DELETE: {
            // 无事务或者single-transaction没有走separate模式
            break;
        }
        case pb::OP_NONE: {
            _meta_writer->update_apply_index(_region_id, _applied_index, _data_index);
            if (done) {
                ((DMLClosure*)done)->response->set_errcode(pb::SUCCESS);
            }
            DB_NOTICE("region_id: %ld OP_NONE sucess, applied_index: %ld, term: %ld",
                    _region_id, _applied_index,  term);
            break;
        }
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
    // TODO: 只读事务清理
}

void Region::exec_in_txn_query(google::protobuf::RpcController* controller, 
        const pb::StoreReq* request, 
        pb::StoreRes*       response,
        google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    brpc::Controller*  cntl = (brpc::Controller*)controller;
    uint64_t log_id = 0;
    if (cntl->has_log_id()) {
        log_id = cntl->log_id();
    }
    int ret = 0;
    const auto& remote_side_tmp = butil::endpoint2str(cntl->remote_side());
    const char* remote_side = remote_side_tmp.c_str();

    pb::OpType op_type = request->op_type();
    const pb::TransactionInfo& txn_info = request->txn_infos(0);
    uint64_t txn_id = txn_info.txn_id();
    int seq_id = txn_info.seq_id();
    SmartTransaction txn = _txn_pool.get_txn(txn_id);
    int last_seq = (txn == nullptr ? 0: txn->seq_id());
    
    if (txn_info.need_update_primary_ts()) {
        // TODO: update primary ts
        // _txn_pool.update_primary_ts(txn_info);
    }
    if (txn == nullptr) { // 没有该事务的相关信息
        ret = _meta_writer->read_transaction_rollbacked_tag(_region_id, txn_id); 
        if (ret == 0) { // 已经rollback，直接返回
            DB_FATAL("Transaction Error: txn has been rollback due to timeout, remote_side: %s, "
                    "region_id: %ld, txn_id: %lu, log_id: %lu, op_type: %s",
                    remote_side, _region_id, txn_id, log_id, pb::OpType_Name(op_type).c_str());
            response->set_errcode(pb::TXN_IS_ROLLBACK);
            response->set_affected_rows(0);
            return ;
        }
        // 幂等处理: 由于core、切主、超时等原因重发请求
        int finish_affected_rows = _txn_pool.get_finished_txn_affected_rows(txn_id);
        if (finish_affected_rows != -1) {
            DB_FATAL("Transaction Error: txn has been exec before, remote side: %s, region_id: %ld, "
                    "txn_id: %lu, log_id: %lu, op_type: %s",
                    remote_side, _region_id, txn_id, log_id, pb::OpType_Name(op_type).c_str());
            response->set_affected_rows(finish_affected_rows);
            response->set_errcode(pb::SUCCESS);
            return ;
        }
        if (op_type == pb::OP_ROLLBACK) {
            // Out fsm 执行失败后切主
            DB_WARNING("Transaction Warn: txn not exist, remote_side: %s, "
                    "txn_id: %lu, log_id: %lu, op_type: %s",
                    remote_side, _region_id, txn_id, log_id, pb::OpType_Name(op_type).c_str());
            response->set_affected_rows(0);
            response->set_errcode(pb::SUCCESS);
            return ;
        }
    } else if (txn_info.start_seq_id() != 1 && last_seq >= seq_id) {
        // 超时等原因重发，该事务已经执行过了
        DB_WARNING("Transaction Warn: txn has exec before, remote_side: %s"
                "region_id: %ld, txn_id: %lu, op_type: %s, last_seq: %d, seq_id: %d, log_id: %lu",
                remote_side, _region_id, txn_id, pb::OpType_Name(op_type).c_str(), last_seq, seq_id, log_id);
        response->set_affected_rows(txn->dml_num_affected_rows);
        response->set_errcode(txn->err_code);
        return ;
    }

    if (txn != nullptr) {
        // 正在处理该事务的相关请求
        if (!txn->is_finished() && txn->in_process()) {
            DB_WARNING("Transaction Note: txn in process, remote_side: %s"
                    "region_id: %ld, txn_id: %lu, op_type: %s, log_id: %lu",
                    remote_side, _region_id, txn_id, pb::OpType_Name(op_type).c_str(), log_id);
            response->set_affected_rows(0);
            response->set_errcode(pb::IN_PROCESS);
            return ;
        }
        // 这个事务语句正在处理，线性
        txn->set_in_process(true);
    }
    // Read only事务不提交raft log，直接prepare/commit/rollback
    // Region第一次执行事务时，start_seq_id = 1 (seq_id = 1 => BEGIN)
    // Client请求时需要将seq_id >= start_seq_id的plan从缓存中发送过来
    if (txn_info.start_seq_id() != 1 && 
            (op_type == pb::OP_PREPARE || 
             op_type == pb::OP_COMMIT  || 
             op_type == pb::OP_ROLLBACK)) {
        if (txn != nullptr && !txn->has_dml_executed()) {
            // read only事务
            bool optimize_1pc = txn_info.optimize_1pc();
            _txn_pool.read_only_txn_process(_region_id, txn, op_type, optimize_1pc);
            // 在[prepare/commit/rollback]是没有dml语句需要执行
            txn->set_in_process(false);
            response->set_affected_rows(0);
            response->set_errcode(pb::SUCCESS);
            return ;
        }
    }

    bool apply_success = true;
    ON_SCOPED_EXIT(([this, &txn, &apply_success, txn_id]() {
        if (txn != nullptr && !apply_success) {
            txn->rollback_current_request();
            txn->set_in_process(false);
            DB_DEBUG("region_id: %ld, txn_id: %lu, need rollback current seq_id: %d", 
                    _region_id, txn->txn_id(), txn->seq_id());
        }
    }));

    // 有些op在cache中没有execute
    if (last_seq < seq_id - 1) {
        ret = execute_cached_cmd(*request, *response, txn_id, txn, 0, 0, log_id);
    }

    switch(op_type) {
        case pb::OP_TXN_KV_PUT:
        case pb::OP_TXN_KV_GET:
        case pb::OP_TXN_KV_DELETE: {
            if (_split_param.split_slow_down) {
                /* split 处于slow down阶段，需要sleep */
                DB_WARNING("region_id: %ld is spliting, slow down time: %ld, remote_side: %s",
                        _region_id, _split_param.split_slow_down_cost, remote_side);
                bthread_usleep(_split_param.split_slow_down_cost);
            }
            /* 最长30s */
            int64_t disable_write_wait = get_split_wait_time();
            int ret = _disable_write_cond.timed_wait(disable_write_wait);
            if (ret != 0) {
                DB_WARNING("region_id: %ld disable write timeout: %ld", _region_id, disable_write_wait);
                ERROR_SET_RESPONSE_FAST(response, pb::DISABLE_WRITE_TIMEOUT, "disable write timeout", log_id);
                return ;
            }

            _real_writing_cond.increase();
            ON_SCOPED_EXIT([this]() {
                _real_writing_cond.decrease_broadcast();
            });

            int64_t expected_term = _expect_term;
            pb::StoreReq* req = nullptr;
            if (is_dml_op_type(op_type)) {
                // 事务里执行这次OP
                dml(*request, *response, 0LL, 0LL);
                if (response->errcode() != pb::SUCCESS) {
                    apply_success = false;
                    DB_WARNING("[DML FAILED] regin_id: %ld, txn_id: %lu, log_id: %lu, remote_side: %s",
                            _region_id, txn_id, log_id, remote_side.c_str());
                    return ;
                }
                if (txn != nullptr && txn->is_separate()) {
                    req = txn->get_raft_req();
                    req->clear_txn_infos();
                    auto req_txn_info = req->add_txn_infos();
                    req_txn_info->CopyFrom(txn_info);
                    req_txn_info->set_op_type(pb::OP_KV_BATCH);
                    req_txn_info->set_region_id(_region_id);
                    req_txn_info->set_region_version(_version);
                    req_txn_info->set_num_increase_rows(txn->num_increase_rows);
                }
            } else if (op_type == pb::OP_PREPARE && txn != nullptr && !txn->has_dml_executed()) {
                bool optimize_1pc = txn_info.optimize_1pc();
                _txn_pool.read_only_txn_process(_region_id, txn, op_type, optimize_1pc);
                txn->set_in_process(false);
                response->set_affected_rows(0);
                response->set_errcode(pb::SUCCESS);
                DB_WARNING("TransactionNote, NO write dml when commit/rollback, remote_side: %s"
                        "region_id: %ld, txn_id: %lu, op_type: %s, log_id: %lu, optimize_1pc: %d",
                        _region_id, txn_id, pb::OpType_Name(op_type).c_str(), log_id, optimize_1pc);
                return ;
            } 
        }
        case default: {
            response->set_errcode(pb::UNSUPPORT_REQ_TYPE);
            response->set_errmsg("Not support");
            DB_FATAL("region_id: %ld, log_id: %lu, txn_id: %lu, op_type: %d not support",
                    _region_id, log_id, txn_id, op_type);
        }
    }
}

void Region::exec_out_txn_query(google::protobuf::RpcController* controller, 
        const pb::StoreReq* request, 
        pb::StoreRes*       response,
        google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl = (brpc::Controller*)controller;
    uint64_t log_id = 0; 
    if (cntl->has_log_id()) { 
        log_id = cntl->log_id();
    }
    const auto& remote_side_tmp = butil::endpoint2str(cntl->remote_side());
    const char* remote_side = remote_side_tmp.c_str();

    pb::OpType op_type = request->op_type();
    switch(op_type) {
    case pb::RAW_KV_PUT: {
        if (_split_param.split_slow_down) {
            /* split 处于slow down阶段，需要sleep */
            DB_WARNING("region_id: %ld is spliting, slow down time: %ld, remote_side: %s",
                    _region_id, _split_param.split_slow_down_cost, remote_side);
            bthread_usleep(_split_param.split_slow_down_cost);
        }
        /* 最长30s */
        int64_t disable_write_wait = get_split_wait_time();
        int ret = _disable_write_cond.timed_wait(disable_write_wait);
        if (ret != 0) {
            DB_WARNING("region_id: %ld disable write timeout: %ld", _region_id, disable_write_wait);
            ERROR_SET_RESPONSE_FAST(response, pb::DISABLE_WRITE_TIMEOUT, "disable write timeout", log_id);
            return ;
        }

        _real_writing_cond.increase();
        ON_SCOPED_EXIT([this]() {
            _real_writing_cond.decrease_broadcast();
        });
        
        // 默认计算存储分离 _storage_compute_separate = true;
        if (is_dml_op_type(op_type) && _storage_compute_separate) {
            exec_kv_out_txn(request, response, remote_side, done_guard.release());
        } else {
            // 非存算分离模式
            butil::IOBuf data;
            butil::IOBufAsZeroCopyOutputStream wrapper(&data);
            if (!request->SerializeToZeroCopyStream(&wrapper)) {
                cntl->SetFailed(brpc::EREQUEST, "Fail to serialze request");
                return ;
            }

            DMLClosure* c = new DMLClosure;
            c->op_type = op_type;
            c->log_id = log_id;
            c->response = response;
            c->region = this;
            c->remote_side = remote_side;
            int64_t expected_term = _expect_term;

            // 默认使用1pc，成功后c->transaction = txn
            if (is_dml_op_type(op_type) && _txn_pool.exec_1pc_out_fsm()) {
                dml_1pc(*request, response->op_type(), request->plan(), *response, 0, 0, done_guard.release());
                if (response.errcode() != pb::SUCCESS) {
                    DB_FATAL("region_id: %ld, log_id: %lu exec dml_1pc failed", _region_id, log_id);
                    return ;
                }
            }
            c->cost.reset();
            c->done = done_guard.release();

            braft::Task task;
            task.data = &data;
            task.done = c;
            if (is_dml_op_type(op_type)) {
                task.expected_term = expected_term;
            }
            _node.apply(task);
        }
        break;
    }
    default:
        DB_WARNING("region_id: %ld op_type: %d not support", _region_id, op_type);
        break;
    }
}

void Region::commit_raft_msg(const pb::StoreReq& request) {
    static rocksdb::WriteOptions write_option;
    rocksdb::WriteBatch batch;
    for (auto& op: request.kv_ops()) {
        if (!op.has_value()) {
            batch.Delete(op.key());
        } else {
            batch.Put(op.key(), op.value());    
        }
    }
    _rocksdb->write(write_option, update);
} 

void Region::apply_txn_request(const pb::StoreReq& request, 
        braft::Closure* done, 
        int64_t index, 
        int64_t term) {
}

// 执行在客户端缓存的命令
int Region::execute_cached_cmd(const pb::StoreReq& request, 
        pb::StoreRes& response,
        uint64_t txn_id,
        SmartTransaction& txn,
        int64_t applied_index,
        int64_t term,
        uint64_t log_id) {

    if (request.txn_infos_size() == 0) {
        return 0;
    }
    const pb::TransactionInfo& txn_info = request.txn_infos(0);
    int last_seq = (txn == nullptr ? 0 : txn->seq_id());

    // 从last_seq + 1开始执行到seq_id - 1
    for (auto& cache_item: txn_info.cache_plans()) {
        const pb::OpType op_type = cache_item.op_type();
        int seq_id = cache_item.seq_id();
        if (seq_id <= last_seq) {
            DB_WARNING("Transaction Error, region_id: %ld, txn_id: %lu has been executed", _region_id, txn_id);
            continue;
        }
        // 应该执行成功，可能在其他peer已经执行过了
        pb::StoreRes res;
        dml_2pc(request, op_type, cache_item, res, applied_index, term, seq_id); 
        
        if (res.has_errcode() && res.errcode() != pb::SUCCESS) {
            response.set_errcode(res.errcode());
            response.set_errmsg(res.errmsg());
            return -1;
        }

        if (op_type == pb::OP_BEGIN && ((txn = _txn_pool.get_txn(txn_id)) == nullptr)) {
            DB_FATAL("Transaction Error, region_id: %ld, txn_id: %lu, get txn failed after pb::OP_BEGIN", _region_id, txn_id);
            response.set_errcode(pb::EXEC_FAIL);
            response.set_errmsg("Txn not found");
            return -1;
        }
    }
    return 0;
}

void Region::dml_1pc(const pb::StoreReq& request,
        const pb::OpType op_type,
        const pb::CachePlan& plan,
        pb::StoreRes& response,
        int64_t applied_index, 
        int64_t term,
        braft::Closure* done) {
    TimeCost cost; 
    // for out txn dml query, create new txn
    // for single-region 2pc query, simply fetch the txn create before
    
    // 兼容无事务功能的log entry，以及强制1pc DML query(用来灌数据)
    // bool is_new_txn = !((request.op_type() == pb::OP_PREPARE) && request.txn_infos(0).optimize_1pc());
    // bool single_region_txn = (request.op_type() == pb::OP_BEGIN) && request.txn_infos(0).optimize_1pc();
    
    // 简化模式，optimize_1pc 暂时不启用 
    bool is_new_txn = true;
    bool single_region_txn = false;
    CHECK(!(is_new_txn && single_region_txn));

    // 初始化一个Transaction
    SmartTransaction txn = nullptr;
    uint64_t txn_id = 0;
    if (is_new_txn) {
        Transaction::TxnOptions txn_opt;
        txn_opt.dml_1pc = is_dml_op_type(op_type);
        txn_opt.in_fsm = (done == nullptr);
        if (!txn_opt.in_fsm) {
            txn_opt.lock_timeout = FLAGS_exec_1pc_out_fsm_timeout_ms;
        }
        txn = SmartTransaction(new Transaction(0, &_txn_pool));
        txn->set_resource(_resource);
        txn->set_separate(false /* 非seperate模式 */);
        txn->begin(txn_opt);
    }
    
    bool commit_success = false;
    ON_SCOPED_EXIT([&]() {
        if (!txn) {
            return ;
        }
        // rollback if not commit success
        if (!commit_success) {
            txn->rollback();
        }
    });

    if (txn == nullptr) {
        response.set_errcode(pb::EXEC_FAIL);
        response.set_errmsg("Txn is null");
        DB_FATAL("region_id: %ld exec 1pc failed, is_new_txn: %d, single_region_txn: %d, applied_index: %ld, log_id: %lu",
                _region_id, is_new_txn, single_region_txn, applied_index, log_id);
        return ;
    }

    // 先在Leader锁住相关资源
    if ret = process_txn(txn, txn_id, plan, op_type);
    if (ret < 0) {
        txn->err_code = pb::EXEC_FAIL;
        response->set_errcode(pb::EXEC_FAIL);
        response->set_errmsg("exec failed");
        DB_FATAL("[dml_1pc] region_id: %ld, txn_id: %lu exec op: %s failed, exit",
                _region_id, txn_id, pb::OpType_Name(op_type).c_str());
        return ;
    }

    // OP_TRUNCATE_TABLE
    if (op_type == pb::OP_TRUNCATE_TABLE) {
        _num_table_lines = 0;
    } 
     
    // follower记录applied_index
    int64_t txn_num_table_lines = txn->num_increase_rows + _num_table_lines;
    if (txn_id == 0 && done == nullptr) {
        _meta_writer->write_meta_index_and_num_table_lines(
                _region_id, applied_index, _data_index, txn_num_table_lines, txn);
    } 

    // follower
    if (done == nullptr) {
        int ret = txn->commit();
        if (!ret) {
            commit_success = true;
        }
        if (commit_success) {
            if (txn->num_increase_rows < 0) {
                _num_delete_lines -= txn->num_increase_rows;
            }
            _num_table_lines = txn_num_table_lines;
        }
    } else {
        // Leader同样需要在on_apply时回调
        // 在out fsm的请求会在Leader设置done，此时txn_id = 0
        commit_success = true;
        (DMLClosure*)done->transaction = txn;
        (DMLClosure*)done->txn_num_increase_rows = txn->num_increase_rows;
    }

    if (commit_success) {
        response.set_affected_rows(txn->dml_num_affected_rows);
        response.set_errcode(pb::SUCCESS);
    } else {
        response.set_errcode(pb::EXEC_FAIL);
        response.set_errmsg("Txn commit failed");
        DB_FATAL("region_id: %ld, txn_id: %lu, applied_index: %ld commit failed",
                _region_id, txn_id, applied_index);
    }
    
    int64_t dml_cost = cost.get_time();
    DB_DEBUG("[FINISH dml_1pc] region_id: %ld, txn_id: %lu, op_type: %s, time cost: %ld", 
            _region_id, txn_id, pb::OpType_Name(op_type).c_str(), dml_cost);
}

void Region::dml_2pc(const pb::StoreReq& request, 
        const pb::OpType op_type,
        const pb::CachePlan& plan,
        pb::StoreRes& response,
        int64_t applied_index,
        int64_t term, 
        int32_t seq_id) {
    // Leader事务才能在Raft外执行(Out fsm)
    if (applied_index == 0 || term == 0 || !is_leader()) {
        response.set_leader(butil::endpoint2str(get_leader()).c_str());
        response.set_errcode(pb::NOT_LEADER);
        response.set_errmsg("Not Leader");
        DB_WARNING("region_id: %ld, term: %ld, not leader and not in raft", _region_id, term);
        return ;
    }    

    TimeCost cost;
    std::set<int> need_rollback_seq;
    if (request.txn_infos_size() == 0) {
        response.set_errcode(pb::EXEC_FAIL);
        response.set_errmsg("request txn info empty");
        DB_FATAL("region_id: %ld txn info empty", _region_id);
        return ;
    }
    const pb::TransactionInfo& txn_info = request.txn_infos(0); 
    for (int seq: txn_info.need_rollback_seq()) {
        need_rollback_seq.insert(seq);
    }
    int64_t txn_num_increase_rows = 0;

    uint64_t txn_id = txn_info.txn_id();
    uint64_t rocksdb_txn_id = 0;
    auto txn = _txn_pool.get_txn(txn_id);
    // 事务在Leader切换后已经rollback了
    if (op_type != pb::OP_BEGIN && (txn != nullptr || txn->is_rollbacked())) {
        response.set_leader(butil::endpoint2str(get_leader()).c_str());
        response.set_errcode(pb::NOT_LEADER);
        response.set_errmsg("Not Leader, Maybe transfer leader");
        DB_WARNING("region_id: %ld txn_id: %lu, seq_id: %d not found", _region_id, txn_id, seq_id);
        return ;
    }
    bool need_write_rollback = false;
    if (op_type != pb::OP_BEGIN && txn != nullptr) {
        // 回滚已经执行过的语句
        for (auto it = need_rollback_seq.rbegin(); it != need_rollback_seq.rend(); it++) {
            int seq = *it;
            txn->rollback_to_point(seq);
            DB_WARNING("region_id: %ld, txn_id: %lu, cur_seq_id: %d, txn_seq_id: %d, rollback back to seq: %d", 
                    _region_id, txn_id, seq_id, txn->seq_id(), seq);
        }
        // 当前事务语句需要回滚
        if (need_rollback_seq.count(seq_id) != 0) {
            DB_WARNING("region_id: %ld, txn_id: %lu, seq_id: %d, req sed_id: %d",
                    _region_id, txn_id, txn->seq_id(), seq_id);
        }
        // 提前更新事务seq_id
        txn->set_seq_id(seq_id);
        // 设置事务checkpoint
        if (op_type != pb::OP_PREPARE && op_type != pb::OP_COMMIT && op_type != pb::OP_ROLLBACK) {
            txn->set_save_point();
        }
        // 更新primary_region_id
        if (txn_info.has_primary_region_id()) {
            txn->set_primary_region_id(txn_info.primary_region_id());
        }
        // 是否需要rollback？
        need_write_rollback = txn->need_write_rollback();
        rocksdb_txn_id = txn->rocksdb_txn_id();
    }
    
    // commit/rollback step1: 第一步
    if (op_type == pb::OP_ROLLBACK || op_type == pb::OP_COMMIT) {
        int64_t num_table_lines = _num_table_lines;
        if (op_type == pb::OP_COMMIT) {
            num_table_lines += txn->num_increase_rows;
        }
        // _commit_meta_mutex.lock();
        // 事务commit前持久化，加锁的目的：防止pre_commit和commit之间open_snapshot
        _meta_writer->write_pre_commit(_region_id, txn_id, num_table_lines, applied_index);
    }

    ON_SCOPED_EXIT(([this, op_type, applied_index, txn_id, txn_info, need_write_rollback]() {
        // commit/rollback step2: 第二步
        if (op_type == pb::OP_ROLLBACK || op_type == pb::OP_COMMIT) {
            int ret = _meta_writer->write_meta_after_commit(_region_id, _num_table_lines, 
                    applied_index, _data_index, txn_id, need_write_rollback);
            if (ret < 0) {
                DB_FATAL("region_id: %ld, txn_id: %lu, log_index: %ld write meta after commit failed",
                        _region_id, txn_id, applied_index);
            }
            // _commit_meta_mutex.unlock();
        }
    }));

    txn = _txn_pool.get_txn(txn_id);
    int ret = process_txn(txn, txn_id, plan, op_type);
    if (ret < 0) {
        txn->err_code = pb::EXEC_FAIL;
        response->set_errcode(pb::EXEC_FAIL);
        response->set_errmsg("exec failed");
        DB_FATAL("[dml_2pc] region_id: %ld, txn_id: %lu exec op: %s failed, exit",
                _region_id, txn_id, pb::OpType_Name(op_type).c_str());
        return ;
    }
    
    if (txn != nullptr) {
        txn->err_code = pb::SUCCESS;
        txn->set_seq_id(seq_id);

        // commit/rollback命令不缓存
        if (op_type != pb::OP_ROLLBACK && op_type != pb::OP_COMMIT) {
            pb::CachePlan plan_item;
            plan_item.set_op_type(op_type);
            plan_item.set_seq_id(seq_id);
            // CASE1: separate模式
            if (is_dml_op_type(op_type) && txn->is_separate()) {
                // separate模式模式下仅添加kv
                pb::StoreReq* raft_seq = txn->get_raft_req();
                plan_item.set_op_type(pb::OP_KV_BATCH);
                for (auto& kv_op: raft_seq->kv_ops()) {
                    plan_item.add_kv_ops()->CopyFrom(kv_op);
                }
            } else {
                // TODO: CASE2: 非separate模式
                abort();
            }
            txn->push_cmd_to_cache(seq_id, plan_item);
        }
    } else if (op_type != pb::OP_COMMIT || op_type != pb::OP_ROLLBACK) { 
        // Txn not found when which txn_id != 0
        response.set_leader(butil::endpoint2str(get_leader()).c_str());
        response.set_errcode(pb::EXEC_FAIL);
        response.set_errmsg("TXN NOT FOUND");
        DB_WARNING("region_id: %ld txn_id: %lu, seq_id: %d not found", _region_id, txn_id, seq_id);
        return ;
    }

    response.set_errcode(pb::SUCCESS);
    if (op_type == pb::OP_TRUNCATE_TABLE) {
        _num_table_lines = 0;
        // compact_data_in_queue();
    } else if (op_type != pb::OP_COMMIT && op_type != pb::OP_ROLLBACK) {
        // dml_2pc暂时通过txn接口自己计算
        // txn->num_increase_rows += state.num_increase_rows();
    } else {
        // 事务commit/rollback更新num_table_lines
        int64_t txn_num_increase_rows = txn->num_table_lines;
        _num_table_lines += txn_num_increase_rows;
        if (txn_num_increase_rows < 0) {
            _num_delete_lines -= txn_num_increase_rows;
        }
    }
    
    int64_t dml_cost = cost.get_time();

    DB_DEBUG("[FINISH DML_2PC] region_id: %ld, txn_id: %lu, op_type: %s, time cost: %ld", 
            _region_id, txn_id, pb::OpType_Name(op_type).c_str(), dml_cost);
}

void Region::apply_kv_out_txn(const pb::StoreReq& request, 
        braft::Closure* done,
        int64_t index, 
        int64_t term) {
    TimeCost cost;
    SmartTransaction txn = nullptr;
    bool commit_success = false;
    int64_t num_table_lines = 0;
    int64_t num_increase_rows = 0;    
    auto resource = get_resource();
    ON_SCOPED_EXIT(([this, &txn, &commit_success, done]() {
        if (!commit_success) {
            if (txn != nullptr) {
                txn->rollback();
            }
            if (done != nullptr) {
                ((DMLClosure*)done)->response->set_errcode(pb::EXEC_FAIL);
                ((DMLClosure*)done)->response->set_errmsg("[COMMIT FAILED] in fsm");
            }
        } else {
            if (done != nullptr) {
                ((DMLClosure*)done)->response->set_errmsg(pb::SUCCESS);
                ((DMLClosure*)done)->response->set_errmsg("SUCCESS");
            }
        }
    }));
    
    if (done == nullptr) {
        // follower
        txn = SmartTransaction(new Transaction(0, &_txn_pool));
        txn->set_resource(resource);
        txn->begin(Transaction::TxnOptions());
        int64_t table_id = get_table_id();
        for (auto& kv_op: request.kv_ops()) {
            pb::OpType op_type = kv_op.op_type();
            int ret = 0;
            if (get_version() != 0) {
                if (op_type == pb::OP_TXN_KV_PUT) {
                    ret = txn->put_kv(kv_op.key(), kv_op.value());
                }
            }
        }
    }
}

void Region::apply_kv_in_txn(const pb::StoreReq& request,
        braft::Closure* done,
        int64_t index,
        int64_t term) {
    // 
}

void Region::dml(const pb::StoreReq& request, 
        pb::StoreRes& response,
        int64_t applied_index, 
        int64_t term) {
    bool optimize_1pc = false;
    bool seq_id = 0;
    if (request.txn_infos_size() > 0) {
        seq_id = request.txn_infos(0).seq_id();
    }
    if (request.op_type() == pb::OP_PREPARE && optimize_1pc) {
        dml_1pc(request, request.op_type(), request.plan(), 
                response, applied_index, term, nullptr /* done */);
    } else {
        dml_2pc(request, response.op_type(), request.plan(), 
                applied_index, term, seq_id);
    }
}

int Region::process_txn(SmartTransaction txn, uint64_t txn_id, pb::CachePlan& plan, pb::OpType op_type) {
    if (op_type == OP_BEGIN && txn == nullptr) {
        // 开始一个事务，可以是separate模式，也可以是非separate模式
        int ret = _txn_pool.begin_txn(txn_id, txn, txn_info.primary_region_id(), 300 * 1000 /* 事务超时时间(ms) */);
        if (ret != 0) {
            DB_WARNING("region_id: %ld, txn_id: %lu begin_txn failed", _region_id, txn_id);
            return -1;
        }
    } else if (txn != nullptr) {
        if (op_type == pb::OP_PREPARE) {
            auto s = txn->prepare();
            if (s.IsExpired()) {
                DB_WARNING("region_id: %ld, txn_id: %lu, seq_id: %d txn expired", 
                        _region_id, txn->txn_id(), txn->seq_id);
                return -1;
            } else if (!s.ok()) {
                DB_WARNING("region_id: %ld, txn_id: %lu, seq_id: %d unknow error",
                        _region_id, txn->txn_id(), txn->seq_id);
                return -1;
            }
        } else if (op_type == pb::OP_COMMIT) {
            txn->commit(); 
            if (s.IsExpired()) {
                DB_WARNING("region_id: %ld, txn_id: %lu, seq_id: %d txn expired", 
                        _region_id, txn->txn_id(), txn->seq_id);
                return -1;
            } else if (!s.ok()) {
                DB_WARNING("region_id: %ld, txn_id: %lu, seq_id: %d unknow error",
                        _region_id, txn->txn_id(), txn->seq_id);
                return -1;
            }
            _txn_pool.remove_txn(txn->txn_id(), true /* mark finished */);
            return 0;
        } else if (op_type == pb::OP_ROLLBACK) {
            txn->rollback();
            if (s.IsExpired()) {
                DB_WARNING("region_id: %ld, txn_id: %lu, seq_id: %d txn expired", 
                        _region_id, txn->txn_id(), txn->seq_id);
                return -1;
            } else if (!s.ok()) {
                DB_WARNING("region_id: %ld, txn_id: %lu, seq_id: %d unknow error",
                        _region_id, txn->txn_id(), txn->seq_id);
                return -1;
            }
            _txn_pool.remove_txn(txn->txn_id(), true /* mark finished */);
            return 0;
        } else {
            // dml_op_type
            if (plan.kv_ops_size() > 0 && txn->is_separate()) {
                // 暂时只实现了key-value的操作，没有其他op
                for (auto kv_op: plan.kv_ops()) {
                    txn->add_kv_op(kv_op);
                }    
            }
        }
    }
    return 0;
}
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
