#include "common/common.h"
#include "meta/meta_state_machine.h"
#include "meta/cluster_manager.h"
#include "meta/privilege_manager.h"
#include "meta/namespace_manager.h"
#include "meta/database_manager.h"
#include "meta/table_manager.h"
#include "meta/region_manager.h"
#include <braft/util.h>
#include <errno.h>

namespace TKV {
const std::string META_INFO_SST =  "meta_info.sst";

void MetaStateMachine::on_apply(braft::Iterator& iter) {
    for (; iter.valid(); iter.next()) {
        braft::Closure* done = iter.done();
        brpc::ClosureGuard done_guard(done);
        if (done) {
            auto meta_done = static_cast<MetaServerClosure*>(done);
            // try apply -> on_apply time cost
            meta_done->raft_time_cost = meta_done->time_cost.get_time();
        }
        butil::IOBufAsZeroCopyInputStream wrapper(iter.data());
        pb::MetaManagerRequest request;
        if (!request.ParseFromZeroCopyStream(&wrapper)) {
            DB_FATAL("parse from protobuf failed when on apply in MetaStateMachine");
            if (done) {
                auto meta_done = static_cast<MetaServerClosure*>(done);
                if (meta_done->response) {
                    meta_done->response->set_errcode(pb::PARSE_FROM_PB_FAIL);
                    meta_done->response->set_errmsg("parse from protobuf failed");
                }
                braft::run_closure_in_bthread(done_guard.release());
            }
            continue;
        }
        if (done && static_cast<MetaServerClosure*>(done)->response) {
            static_cast<MetaServerClosure*>(done)->response->set_op_type(request.op_type());
        }
        DB_DEBUG("on_apply, term: %ld, index: %ld, request op_type: %s", 
                iter.term(), iter.index(), pb::OpType_Name(request.op_type()).c_str());
        switch (request.op_type()) {
        case pb::OP_ADD_INSTANCE:
            ClusterManager::get_instance()->add_instance(request, done);
            break;
        case pb::OP_CREATE_USER:
            PrivilegeManager::get_instance()->create_user(request, done);
            break;
        case pb::OP_CREATE_NAMESPACE:
            NamespaceManager::get_instance()->create_namespace(request, done);
            break;
        case pb::OP_CREATE_DATABASE:
            DatabaseManager::get_instance()->create_database(request, done);
            break;
        case pb::OP_CREATE_TABLE:
            TableManager::get_instance()->create_table(request, iter.index(), done);
            break;
        default:
            DB_FATAL("unsupport request op_type, type: %s", request.op_type());
            IF_DONE_SET_RESPONSE(done, pb::UNSUPPORT_REQ_TYPE,  "unsupport request type");
        } 
        _applied_index = iter.index();
        if (done) {
            // cntl的回调
            braft::run_closure_in_bthread(done_guard.release());
        }
    }
}

void MetaStateMachine::on_snapshot_save(braft::SnapshotWriter* writer,
                              braft::Closure* done) {
    DB_WARNING("snapshot save info, max_namespace_id: %ld, max_database_id: %ld, "
            "max_table_id: %ld, max_region_id: %ld", 
            NamespaceManager::get_instance()->get_max_namespace_id(),
            DatabaseManager::get_instance()->get_max_database_id(),
            TableManager::get_instance()->get_max_table_id(),
            RegionManager::get_instance()->get_max_region_id());
    // 创建snapshot
    rocksdb::ReadOptions read_options;
    // 相同prefix的数据 = false
    read_options.prefix_same_as_start = false;
    read_options.total_order_seek = true;
    auto iter = RocksWrapper::get_instance()->new_iterator(read_options, 
            RocksWrapper::get_instance()->get_meta_info_handle());
    iter->SeekToFirst();
    Bthread bth;
    std::function<void()> save_snapshot_fun = [this, done, iter, writer]() {
        this->save_snapshot(done, iter, writer);
    };
    bth.run(save_snapshot_fun);
}

void MetaStateMachine::save_snapshot(braft::Closure* done, 
                   rocksdb::Iterator* it, 
                   braft::SnapshotWriter* writer) {
    brpc::ClosureGuard done_guard(done);
    std::unique_ptr<rocksdb::Iterator> iter(it);

    std::string snapshot_path = writer->get_path();
    std::string sst_file_path = snapshot_path + "/" + META_INFO_SST;

    rocksdb::Options option = RocksWrapper::get_instance()->get_options(
            RocksWrapper::get_instance()->get_meta_info_handle());
    SstFileWriter sst_writer(option);
    DB_WARNING("meta snapshot path: %s", snapshot_path.c_str());
    auto s = sst_writer.open(sst_file_path);
    if (!s.ok()) {
        DB_WARNING("Error when opening file: %s, err_msg: %s", 
                sst_file_path.c_str(), s.ToString().c_str());
        done->status().set_error(EINVAL, "File to open sst file");
        return ;
    }
    for (; iter->Valid(); iter->Next()) {
        auto res = sst_writer.put(iter->key(), iter->value());
        if (!res.ok()) {
            DB_WARNING("Error while adding key: %s, err_msg: %s",
                    iter->key(), 
                    s.ToString().c_str());
            done->status().set_error(EINVAL, "Fail to put sst file");
            return ;
        }
    }
    s = sst_writer.finish();
    if (!s.ok()) {
        DB_WARNING("Error while finish sst file: %s, err_msg: %s", 
                sst_file_path.c_str(), s.ToString().c_str());
        done->status().set_error(EINVAL, "Fail to finish sst file");
        return ;
    }
    if ((writer->add_file(META_INFO_SST)) != 0) {
        done->status().set_error(EINVAL, "File to add file");
        DB_WARNING("Error while add file");
        return ;
    }
}    

int MetaStateMachine::on_snapshot_load(braft::SnapshotReader* reader) {
    DB_WARNING("meta start machine start on snapshot load");
    // delete old data first
    std::string start_key(MetaServer::CLUSTER_IDENTIFY);
    std::string end_key(MetaServer::MAX_IDENTIFY);
    rocksdb::WriteOptions write_options;
    auto s = RocksWrapper::get_instance()->remove_range(write_options,
            RocksWrapper::get_instance()->get_meta_info_handle(),
            start_key, end_key, false);
    if (!s.ok()) {
        DB_FATAL("remove range fail when on snapshot load, err_msg: %s",
                s.ToString().c_str());
        return -1;
    }
    DB_WARNING("remove range success when on snapshot load, msg: %s",
            s.ToString().c_str());
    
    std::vector<std::string> files;
    reader->list_files(&files);
    for (auto& file: files) {
        DB_WARNING("snapshot file: %s", file.c_str());
        if (file == META_INFO_SST) {
            std::string snapshot_path = reader->get_path();
            _applied_index = parse_snapshot_index_from_path(snapshot_path, false);
            DB_WARNING("meta snapshot_path: %s, applied_index: %ld", snapshot_path.c_str(), _applied_index);
            snapshot_path.append(META_INFO_SST);

            // ingest 
            rocksdb::IngestExternalFileOptions ingest_options;
            s = RocksWrapper::get_instance()->ingest_external_file(
                    RocksWrapper::get_instance()->get_meta_info_handle(),
                    {snapshot_path}, ingest_options);
            if (!s.ok()) {
                DB_WARNING("Error while ingest file: %s, err_msg: %s",
                        snapshot_path.c_str(), s.ToString().c_str());
                return -1;
            }
            // 恢复内存
            int ret = 0;
            ret = ClusterManager::get_instance()->load_snapshot();
            if (ret) {
                DB_FATAL("ClusterManager load snapshot fail");
                return -1;
            }
            ret = PrivilegeManager::get_instance()->load_snapshot();
            if (ret) {
                DB_FATAL("PrivilegeManager load snapshot fail");
                return -1;
            }
            ret = SchemaManager::get_instance()->load_snapshot();
            if (ret) {
                DB_FATAL("SchemaManager load snapshot fail");
                return -1;
            }
        }
    }
    set_have_data(true);    
    return 0;
}

void MetaStateMachine::store_heartbeat(::google::protobuf::RpcController* controller,
                       const ::TKV::pb::StoreHBRequest* request,
                       ::TKV::pb::StoreHBResponse* response,
                       ::google::protobuf::Closure* done) {
    TimeCost time_cost;
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
    uint64_t log_id = 0; 
    if (cntl->has_log_id()) {
        log_id = cntl->log_id();
    }
    if (!_is_leader.load()) {
        DB_WARNING("NOT LEADER, log_id: %lu", log_id);
        response->set_errcode(pb::NOT_LEADER);
        response->set_errmsg("not leader");
        response->set_leader(_node.leader_id().to_string());
        return;
    } 
    response->set_errcode(pb::SUCCESS);
    response->set_errmsg("sucess");
    
    DB_DEBUG("TKV store %s heart beat, time cost: %ld log_id: %lu",
            request->instance_info().address().c_str(), time_cost.get_time(), log_id);
    
    // 判断Instance信息
    // 判断Peer信息
    // 判断Table信息
    // 判断Peer所在Table
    // 更新Leader信息
}


void MetaStateMachine::on_leader_start() {
    DB_WARNING("on leader start at new term");
    _leader_start_timestamp = butil::gettimeofday_us();
    if (!_health_check_start) {
        std::function<void()> f = [this]() {
            health_check_function();
        };
        _bth.run(f);
        _health_check_start = true;
    } else {
        DB_FATAL("start health check thread has altredy started");
    }
    CommonStateMachine::on_leader_start();
    _is_leader.store(true);
}

void MetaStateMachine::on_leader_stop() {
    _is_leader.store(false);
    if (_health_check_start) {
        _bth.join();
        _health_check_start = false;
        DB_WARNING("health check thread stop");
    }
    DB_WARNING("on leader stop");
    CommonStateMachine::on_leader_stop();
}

void MetaStateMachine::health_check_function() {
    DB_WARNING("start health check function");
} 
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
