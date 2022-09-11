#include "store/region_control.h"
#include "store/rpc_sender.h"
#include "store/store.h"
#include "store/closure.h"
#include "raft/raft_helper.h"

#include <rocksdb/options.h>

namespace TKV {
DEFINE_bool(allow_blocking_flush, true, "allow_blocking_flush");
DEFINE_int32(compact_interval_s, 1, "compact_interval(s)");
DEFINE_bool(allow_compact_range, true, "allow_comapct_range");
DECLARE_string(snapshot_uri);
DECLARE_string(stable_uri);

class RaftControlDone: public braft::Closure {
public:
    RaftControlDone(google::protobuf::RpcController* controller,
            const pb::RaftControlRequest* request,
            pb::RaftControlResponse* response,
            google::protobuf::Closure* done,
            braft::Node* node,
            std::shared_ptr<UpdateRegionStatus> auto_reset) 
        : _controller(controller)
        , _request(request)
        , _response(response)
        , _done(done)
        , _node(node)
        , _auto_reset(auto_reset) 
    {}

    virtual ~RaftControlDone() {}

    virtual void Run() {
        brpc::Controller* cntl = static_cast<brpc::Controller*>(_controller);
        uint64_t log_id = 0;
        if (cntl->has_log_id()) {
            log_id = cntl->log_id();
        }
        if (status().ok()) {
            DB_NOTICE("region_id: %ld raft control rpc success, type: %d, remote_side: %s, log_id: %ld",
                    _request->region_id(), _request->op_type(), 
                    butil::endpoint2str(cntl->remote_side()).c_str(), log_id);
            _response->set_errcode(pb::SUCCESS);
        } else {
            DB_WARNING("region_id: %ld raft control rpc failed, type: %d, remote_side: %s, log_id: %ld",
                    _request->region_id(), _request->op_type(),
                    butil::endpoint2str(cntl->remote_side()).c_str(), log_id);
            _response->set_errcode(pb::INTERNAL_ERROR);
            _response->set_errmsg(status().error_cstr());
            _response->set_leader(butil::endpoint2str(_node->leader_id().addr).c_str());
        }
        _done->Run();
        delete this;
    }
private:
    google::protobuf::RpcController* _controller;
    const pb::RaftControlRequest* _request;
    pb::RaftControlResponse*      _response;
    google::protobuf::Closure*    _done;
    braft::Node*                  _node;
    std::shared_ptr<UpdateRegionStatus> _auto_reset;
};

void RegionControl::sync_do_snapshot() {
    DB_WARNING("region_id: %ld sync do snapshot start", _region_id);
    std::string address = Store::get_instance()->address();
    butil::EndPoint leader = _region->get_leader();
    if (leader.ip != butil::IP_ANY) {
        address = butil::endpoint2str(leader).c_str();
    }
    auto ret = RpcSender::send_no_op_request(address, _region_id, _region->_region_info.version());
    if (ret < 0) {
        DB_WARNING("send no op fail, region_id: %ld", _region_id);
    }
    BthreadCond sync_sign;
    sync_sign.increase();
    CovertToSyncClosure* done = new CovertToSyncClosure(sync_sign, _region_id);    
    if (!_region->is_learner()) {
        _region->_node.snapshot(done);
    } else {
        // TODO: snapshot for learner node
        CHECK(false && "learner snapshot not impl");
    }
    sync_sign.wait();
    DB_WARNING("region_id: %ld sync do snapshot success", _region_id);
}

int RegionControl::remove_data(int64_t drop_region_id) {
    rocksdb::WriteOptions options;
    MutableKey start_key;
    MutableKey end_key;
    start_key.append_i64(drop_region_id);

    end_key.append_i64(drop_region_id);
    end_key.append_u64(UINT64_MAX);
    
    auto rocksdb = RocksWrapper::get_instance();
    auto data_cf = rocksdb->get_data_handle();
    if (!data_cf) {
        DB_WARNING("rocksdb data cf is not exist, region_id: %ld", drop_region_id);
        return -1;
    }
    TimeCost time_cost;
    auto s = rocksdb->remove_range(options, data_cf, start_key.data(), end_key.data(), true);
    if (!s.ok()) {
        DB_WARNING("rocksdb remove range error, code: %d, msg: %s, region_id: %ld",
                s.code(), s.ToString().c_str(), drop_region_id);
        return -1;
    }
    DB_WARNING("region_id: %ld remove range success, cost: %ld", drop_region_id, time_cost.get_time()); 
    return 0;
}


int RegionControl::remove_meta(int64_t drop_region_id) {
    return MetaWriter::get_instance()->clear_all_meta_info(drop_region_id);
}

int RegionControl::remove_log_entry(int64_t drop_region_id) {
    TimeCost time_cost;
    rocksdb::WriteOptions options;
    MutableKey start_key;
    MutableKey end_key;
    start_key.append_i64(drop_region_id);
    end_key.append_i64(drop_region_id);
    end_key.append_u64(drop_region_id);
    auto rocksdb = RocksWrapper::get_instance();
    auto s = rocksdb->remove_range(options, rocksdb->get_raft_log_handle(), 
            start_key.data(), end_key.data(), true);
    if (!s.ok()) {
        DB_WARNING("remove range error, code: %d, msg: %s, region_id: %ld",
                s.code(), s.ToString().c_str(), drop_region_id);
        return -1;
    }
    DB_WARNING("remove raft log entry, region_id: %ld, cost: %ld", drop_region_id, time_cost.get_time());

    // TODO: remove log entry
    
    return 0;
} 

int RegionControl::remove_snapshot_path(int64_t drop_region_id) {
    std::string snapshot_path_str(FLAGS_snapshot_uri, FLAGS_snapshot_uri.find("//") + 2);    
    snapshot_path_str += "/region_" + std::to_string(drop_region_id);
    // raft_meta_uri
    std::string stable_path_str(FLAGS_stable_uri, FLAGS_stable_uri.find("//") + 2);
    stable_path_str += "region_" + std::to_string(drop_region_id);
    // Delete whether directory or file
    butil::FilePath snapshot_path(snapshot_path_str);
    butil::DeleteFile(snapshot_path, true); 
    butil::FilePath stable_path(stable_path_str);
    butil::DeleteFile(stable_path, true);
    DB_WARNING("drop snapshot path directory, region_id: %ld", drop_region_id);
    return 0;
}

int RegionControl::ingest_meta_sst(const std::string& meta_sst_file, int64_t region_id) {
    return MetaWriter::get_instance()->ingest_meta_sst(meta_sst_file, region_id);
}

// move_files: true
int RegionControl::ingest_data_sst(const std::string& data_sst_file, int64_t region_id, bool move_files) {
    auto rocksdb = RocksWrapper::get_instance();
    rocksdb::IngestExternalFileOptions ingest_options;
    ingest_options.move_files = move_files; 
    ingest_options.write_global_seqno = false;
    ingest_options.allow_blocking_flush = FLAGS_allow_blocking_flush; 

    // start ingest
    auto data_cf = rocksdb->get_data_handle(); 
    auto s = rocksdb->ingest_external_file(data_cf, { data_sst_file }, ingest_options);
    if (!s.ok()) {
        DB_WARNING("region_id: %ld ingest file: %s fail, error: %s", region_id, 
                data_sst_file.c_str(), s.ToString().c_str());
        if (!FLAGS_allow_blocking_flush) {
            // check whether ingest failed because not flush
            rocksdb::FlushOptions flush_options;
            s = rocksdb->flush(flush_options, data_cf);
            if (!s.ok()) {
                DB_WARNING("region_id: %ld flush data to rocksdb failed, err: %s",
                        region_id, s.ToString().c_str());
                return -1;
            }
            s = rocksdb->ingest_external_file(data_cf, { data_sst_file }, ingest_options);
            if (!s.ok()) {
                DB_FATAL("Error when adding file: %s, region_id: %ld, error: %s", 
                        data_sst_file.c_str(), region_id, s.ToString().c_str());
                return -1;
            }
            return 0;
        }
        return -1;
    }
    return 0;
}

int RegionControl::clear_all_info_for_region(int64_t drop_region_id) {
    DB_WARNING("region_id: %ld clear all info for region, do compact in queue", drop_region_id);
    remove_data(drop_region_id);
    remove_meta(drop_region_id);
    remove_snapshot_path(drop_region_id);
    remove_log_entry(drop_region_id);
    return 0;
}

void RegionControl::compact_data_in_queue(int64_t region_id) {
    // static 变量不用捕获
    static ThreadSafeMap<int64_t, bool> in_compact_regions;
    if (in_compact_regions.count(region_id) == 1) {
        DB_WARNING("region_id: %ld has been put in queue before", region_id);
        return ;
    }
    in_compact_regions[region_id] = true;
    Store::get_instance()->compact_queue().run([region_id] {
        if (in_compact_regions.count(region_id) == 1) {
            if (!Store::get_instance()->is_shutdown() && FLAGS_allow_compact_range) {
                RegionControl::compact_data(region_id);
                in_compact_regions.erase(region_id);
                bthread_usleep(FLAGS_compact_interval_s * 1000 * 1000LL);
            }
        }    
    });
}

void RegionControl::compact_data(int64_t region_id) {
    MutableKey start_key;
    MutableKey end_key;
    start_key.append_i64(region_id);
    
    end_key.append_i64(region_id);
    end_key.append_u64(UINT64_MAX);

    auto rocksdb = RocksWrapper::get_instance();
    auto data_cf = rocksdb->get_data_handle();
    if (!data_cf) {
        DB_WARNING("region_id: %ld no data cf", region_id);
        return ;
    }
    TimeCost time_cost;
    rocksdb::Slice start(start_key.data());
    rocksdb::Slice end(end_key.data());
    rocksdb::CompactRangeOptions compact_options;
    auto s = rocksdb->compact_range(compact_options, data_cf, &start, &end);
    if (!s.ok()) {
        DB_WARNING("region_id: %ld compact range error, msg: %s",
                region_id, s.ToString().c_str());
    }
    DB_WARNING("region_id: %ld compact range success", region_id);
}


void common_raft_control(google::protobuf::RpcController* controller,
    const pb::RaftControlRequest* request,
    pb::RaftControlResponse* response,
    google::protobuf::Closure* done,
    braft::Node* node) {

    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
    uint64_t log_id = 0;
    if (cntl->has_log_id()) {
        log_id = cntl->log_id();
    }
    int64_t region_id = request->region_id();
    response->set_region_id(region_id);     
    auto reset_fn = [region_id](UpdateRegionStatus* update_region) {
        update_region->reset_region_status(region_id);
    }; 

    std::shared_ptr<UpdateRegionStatus> auto_reset(UpdateRegionStatus::get_instance(), reset_fn);

    switch (request->op_type()) {
    case pb::Snapshot: {
        RaftControlDone* raft_done = new RaftControlDone(cntl, request, response, done_guard.release(), node, auto_reset);
        node->snapshot(raft_done);
        break;
    }
    default:
        DB_FATAL("Fail, node: %s upsupport request type: %s, log_id: %lu",
                node->node_id().group_id.c_str(),
                request->ShortDebugString().c_str(),
                log_id);
        return ;
    }
}
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
