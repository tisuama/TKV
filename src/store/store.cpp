#include "store/store.h"
#include "store/meta_writer.h"
#include "common/mut_table_key.h"
#include "store/region_control.h"
#include "common/concurrency.h"
#include <sys/statfs.h>

namespace TKV {
DECLARE_string(default_physical_room);
DECLARE_int32(store_port);
DECLARE_string(resource_tag);
DECLARE_int32(balance_periodicity);
DECLARE_string(db_path);
DECLARE_int32(store_id);

DEFINE_int64(store_heart_beat_interval_us, 30 * 1000 * 1000, "store heartbeat interval, default: 30s"); 

int Store::init_before_listen(std::vector<std::int64_t>& init_region_ids) {
    butil::EndPoint addr;
    addr.ip = butil::my_ip();
    addr.port = FLAGS_store_port;
    _address = butil::endpoint2str(addr).c_str();
    _meta_server_interact = MetaServerInteract::get_instance();
    if (_meta_server_interact->init()) {
        DB_FATAL("meta serer init failed");
        return -1;
    }
    _physical_room = FLAGS_default_physical_room;     
    _resource_tag = FLAGS_resource_tag;
    _rocksdb = RocksWrapper::get_instance();
    if (!_rocksdb) {
        DB_FATAL("create rocksdb handle failed, exit now");
        return -1;
    }
    int res = _rocksdb->init(FLAGS_db_path);
    
    // schema_factory
    _factory = SchemaFactory::get_instance();
    if (res != 0) {
        DB_FATAL("rocksdb init failed");
        return -1;
    }
    // MetaWriter
    _meta_writer = MetaWriter::get_instance();
    _meta_writer->init(_rocksdb, _rocksdb->get_meta_info_handle());
    
    // TODO: LogEntryReader
    
    // First heartbeat
    pb::StoreHBRequest  request;
    pb::StoreHBResponse response;

    // 构造store 心跳信息
    construct_heart_beat_request(request);

    DB_WARNING("heartbeat request: %s is construct when init store", request.ShortDebugString().data());
    TimeCost time_cost;
    if (_meta_server_interact->send_request("store_heartbeat", request, response) == 0){
        _factory->update_tables_double_buffer_sync(response.schema_change_info());
        DB_WARNING("send heartbeat request to meta server success");
    } else {
        DB_FATAL("send heartbeat request to meta server failed");
        return -1;
    }
    time_cost.reset();
    DB_WARNING("get schema info from meta server sucess");

    // Set region info has been exist before
    std::vector<pb::RegionInfo> region_infos;
    int ret = _meta_writer->parse_region_infos(region_infos);
    if (ret < 0) {
        DB_FATAL("read region infos from rocksdb failed");
        return ret;
    }
    // restart region 
    for (auto& r : region_infos) {
        DB_WARNING("region info: %s when init store", r.ShortDebugString().data());
        int64_t region_id = r.region_id();
        // version info
        if (r.version() == 0) {
            DB_WARNING("region_id: %ld version: %ld is 0, dropped. region_info: %s",
                    region_id, r.version(), r.ShortDebugString().data());
            RegionControl::clear_all_info_for_region(region_id);
            continue;
        }
        braft::GroupId groupid(std::string("region_") + std::to_string(region_id)); 
        butil::EndPoint addr;
        str2endpoint(_address.c_str(), &addr);
        braft::PeerId peerid(addr, 0);
        bool is_learner = _meta_writer->read_learner_key(region_id) == 1? true: false;
        DB_DEBUG("region_id: %ld, is_learner: %d", region_id, is_learner);
        
        // clear peers info
        if (!is_learner) {
            r.clear_peers();
        }
        // new region info
        SmartRegion region(new(std::nothrow) Region(_rocksdb, _factory, _address, groupid, 
                    peerid, r, region_id, is_learner));
        if (!region) {
            DB_FATAL("new region fail, mme allocate fail, region_info: %s",
                    r.ShortDebugString().data());
            return -1;
        }
        /*
            重启的region和新创建的region区别：
            1) 重启的region在on_snaphsot_load时不加载SST
            2) 重启的region在on_snaphsot_load不受并发限制
        */
        region->set_restart(true);
        // Inset SmartRegion into _region_mapping
        this->set_region(region);
        init_region_ids.push_back(region_id);
    }
    
    // process doing snapshot region 
    ret = _meta_writer->parse_doing_snapshot(_doing_snapshot_regions);
    if (ret < 0) {
        DB_FATAL("read doing snapshot regions from rocksdb failed");
        return -1;
    } else {
        for (auto r : _doing_snapshot_regions) {
            DB_WARNING("region_id: %ld is doing snapshot load when store stop", r);
        }
    }
    
    // TODO: start db statitics
    DB_WARNING("store init before listen success, region size: %ld, doing snapshot region size: %ld", 
            init_region_ids.size(), _doing_snapshot_regions.size());
    
    return 0;
}

int Store::init_after_listen(const std::vector<std::int64_t>& init_region_ids) {
    // 开始上报心跳
    _heart_beat_bth.run([this]() { heart_beat_thread(); });

    TimeCost time_cost;
    ConcurrencyBthread init_region_bthread(5);
    // 从本地rocksdb恢复哪些region
    for (auto& region_id: init_region_ids) {
        auto init_fn = [this, region_id]() {
            SmartRegion region = get_region(region_id);
            if (region == nullptr) {
                DB_WARNING("region_id: %ld no region info", region_id);
                return ;
            }
            int ret = region->init(false /* new_region = false */, 0);
            if (ret < 0) {
                DB_WARNING("region_id: %ld init fail when store init", region_id);
                return ;
            }
        };
        init_region_bthread.run(init_fn);
    }
    init_region_bthread.join();
    DB_WARNING("Store %d init region sucess, time_cost: %ld", FLAGS_store_id, time_cost.get_time());

    return 0;
}

void Store::init_region(::google::protobuf::RpcController* controller,
                     const ::TKV::pb::InitRegion* request,
                     ::TKV::pb::StoreRes* response,
                     ::google::protobuf::Closure* done) {
    // Called by create_table
    TimeCost time_cost;
    brpc::ClosureGuard done_gurad(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
    if (!_factory) {
        cntl->SetFailed(EINVAL, "record encoder not set");
        return ;
    }
    if (_shutdown) {
        DB_WARNING("Store has been shutdown");
        response->set_errcode(pb::INTERNAL_ERROR);
        response->set_errmsg("store has shutdown");
        return ;
    }
    uint64_t log_id = 0;
    if (cntl->has_log_id()) {
        log_id = cntl->log_id();
    }
    const pb::RegionInfo& region_info = request->region_info();
    int64_t table_id = region_info.table_id();
    int64_t region_id = region_info.region_id();
    const auto& remote_side_tmp = butil::endpoint2str(cntl->remote_side());
    const char* remote_side = remote_side_tmp.c_str();
    
    // 只有add_peer操作，can_add_peer设置为true
    bool is_addpeer = request->region_info().can_add_peer();

    // rocksdb stall -> cannot add peer
    // receive_add_peer_concurrency后，直到on_snapshot_load后才释放。
    ON_SCOPED_EXIT([is_addpeer](){
        if (is_addpeer) {
            Concurrency::get_instance()->receive_add_peer_concurrency.decrease_broadcast();
        } 
    });
    if (is_addpeer) {
        // Wait region::on_snapshot_load here
        int ret = Concurrency::get_instance()->receive_add_peer_concurrency.increase_timed_wait(1000 * 1000 * 10);
        if (ret != 0) {
            DB_WARNING("receive_add_peer_concurrency timeout, count: %d, log_id: %lu, remote_side: %s",
                    Concurrency::get_instance()->receive_add_peer_concurrency.count(), log_id, remote_side);
            response->set_errcode(pb::CANNOT_ADD_PEER);
            response->set_errmsg("receive_add_peer_concurrency timeout");
            return ;
        }
    }
    
    // 新增Table信息
    if (!_factory->exist_table_id(table_id)) {
        if (request->has_schema_info()) {
            this->update_schema_info(request->schema_info(), nullptr);
        } else {
            ERROR_SET_RESPONSE_FAST(response, pb::INPUT_PARAM_ERROR, "table info is missing when add region", log_id);
            return ;
        }
    }
    auto pre_region = this->get_region(region_id);
    // 已经删除，遇到新建，此时需要马上删除
    if (pre_region != nullptr && pre_region->removed()) {
        drop_region_from_store(region_id, false);
    }
    pre_region = get_region(region_id);
    if (pre_region) {
        ERROR_SET_RESPONSE_FAST(response, pb::REGION_ALREADY_EXIST, "region id has exist and drop fail when init region", log_id);
        return ;
    }

    // construct region
    braft::GroupId groupid(std::string("region_") + std::to_string(region_id));
    butil::EndPoint addr;
    if (str2endpoint(_address.c_str(), &addr)) {
        ERROR_SET_RESPONSE_FAST(response, pb::INTERNAL_ERROR, "address is illegal", log_id);
        return ;
    }
    braft::PeerId peerid(addr, 0);
    // new Region
    SmartRegion region(new(std::nothrow) Region(_rocksdb, _factory, _address, groupid, 
                peerid, request->region_info(), region_id, request->region_info().is_learner()));
    if (!region) {
        ERROR_SET_RESPONSE_FAST(response, pb::INTERNAL_ERROR, "new region failed", log_id);
        return ;
    }
    // binglog region = false;
    DB_WARNING("new region info: %s, log_id: %ld, remote_side: %s",
            request->ShortDebugString().c_str(), log_id, remote_side);
    // 更新内存信息
    this->set_region(region);
    // region init
    int ret = region->init(true, request->snapshot_times());
    if (ret < 0) {
        // 删除该region所有的相关信息
        RegionControl::clear_all_info_for_region(region_id);
        this->erase_region(region_id);
        DB_FATAL("region_id: %ld init fail when add region, log_id: %lu",
                region_id, log_id);
        response->set_errcode(pb::INTERNAL_ERROR);
        response->set_errmsg("region init failed when add region");
        return ;
    }
    response->set_errcode(pb::SUCCESS);
    response->set_errmsg("add region success");
    if (request->region_info().version() == 0) {
        Bthread bth;
        std::function<void()> check = [this, region_id]() {
            this->check_region_legal_complete(region_id);
        };
        bth.run(check);
        DB_WARNING("region_id: %ld init region version is 0, should check region legal",
                region_id, log_id);
    }
    DB_WARNING("region_id: %ld init region success, log_id: %lu, time_cost: %lu, remote_side: %s",
           region_id, log_id, time_cost.get_time(), remote_side); 
}

void Store::construct_heart_beat_request(pb::StoreHBRequest& request) {
    static int64_t count = 0;
    request.set_need_leader_balance(false);
    ++count;
    if (count % FLAGS_balance_periodicity == 0) {
        request.set_need_leader_balance(true);
    }

    bool need_peer_balance = false;
    // init before listen 会第一次上报心跳
    if (count == 2 || 
            _last_heart_time.get_time() > FLAGS_store_heart_beat_interval_us * 4) {
        need_peer_balance = true;
    }
    if (count % FLAGS_balance_periodicity == (FLAGS_balance_periodicity / 2)) {
        need_peer_balance = true;
        request.set_need_peer_balance(true);
    }
    // 构造instance信息
    pb::InstanceInfo* info = request.mutable_instance_info();
    info->set_address(_address);
    info->set_physical_room(_physical_room);
    info->set_resource_tag(_resource_tag);
    info->set_raft_total_latency(_raft_total_cost.latency(60));
    info->set_raft_total_qps(_raft_total_cost.qps(60));
    info->set_select_latency(_select_time_cost.latency(60));
    info->set_select_qps(_select_time_cost.qps(60));
    
    // 读取硬盘参数
    struct statfs sfs;
    statfs(FLAGS_db_path.c_str(), &sfs);
    int64_t capacity = sfs.f_blocks * sfs.f_bsize;
    int64_t left_size = sfs.f_bavail * sfs.f_bsize;
    // Set bvar info
    _disk_total.set_value(capacity);
    _disk_used.set_value(capacity - left_size);
    
    info->set_capacity(capacity);
    info->set_used_size(capacity - left_size);

    // 构造schema version信息
    std::unordered_map<int64_t, int64_t> table_id_version_map;
    _factory->get_all_table_version(table_id_version_map);
    for (auto table_info : table_id_version_map) {
        pb::SchemaHB* schema = request.add_schema_infos();
        schema->set_table_id(table_info.first);
        schema->set_version(table_info.second);
    }

    // 构造所有region的version信息
    traverse_copy_region_map([&request, need_peer_balance](const SmartRegion& region) {
        region->construct_heart_beat_request(request, need_peer_balance);       
    });
}

void Store::update_schema_info(const pb::SchemaInfo& table, std::map<int64_t, int64_t>* reverser_index_map) {
    _factory->update_table(table);
    if (table.has_deleted() && table.deleted()) {
        return ;
    }
    // indexs not implement
    (void)reverser_index_map;
}

int Store::drop_region_from_store(int64_t drop_region_id, bool need_delay_drop) {
    DB_WARNING("region need remove, region_id: %ld, need_delay_drop: %d", drop_region_id, need_delay_drop);
    SmartRegion region = get_region(drop_region_id);
    if (!region) {
        DB_WARNING("region_id: %ld not exist, maybe removed", drop_region_id);
        return -1;
    }
    if (!region->removed()) {
        region->shutdown();
        region->join();
        region->set_removed(true);
        DB_WARNING("region_id: %ld closed for removed", drop_region_id);
    }
    if (!need_delay_drop) {
        region->set_removed(true);
        RegionControl::clear_all_info_for_region(drop_region_id);
        this->erase_region(drop_region_id); 
    }
    return 0;
}

void Store::get_applied_index(::google::protobuf::RpcController* controller,
             const ::TKV::pb::GetAppliedIndex* request,
             ::TKV::pb::StoreRes* response,
             ::google::protobuf::Closure* done) {
    brpc::ClosureGuard done_gurad(done);
    response->set_errcode(pb::SUCCESS);
    response->set_errmsg("success");
    SmartRegion region = get_region(request->region_id());
    if (!region) {
        DB_FATAL("region_id: %ld not exist, maybe removed", request->region_id());
        response->set_errcode(pb::REGION_NOT_EXIST);
        response->set_errmsg("region not exist");
        return ;
    }
    response->set_region_status(region->region_status());
    response->set_applied_index(region->get_log_index());
    response->mutable_region_raft_stat()->set_applied_index(region->get_log_index());
    response->mutable_region_raft_stat()->set_snapshot_meta_size(region->snapshot_meta_size());
    response->mutable_region_raft_stat()->set_snapshot_data_size(region->snapshot_data_size());
    response->mutable_region_raft_stat()->set_snapshot_index(region->snapshot_index());
    response->set_leader(butil::endpoint2str(region->get_leader()).c_str());
}

/* 检查region是否分裂或者add peer成功 */
/* region->version() == 0时检查 */
void Store::check_region_legal_complete(int64_t region_id) {
    DB_WARNING("region_id: %ld start to check whether split or add peer complete", region_id);
    auto region = get_region(region_id);
    if (!region) {
        DB_WARNING("region_id: %ld not exist", region_id);
        return ;
    }
    // check
    if (region->check_region_legal_complete()) {
        DB_WARNING("region_id: %ld split or add peer success", region_id);
    } else {
        DB_WARNING("region_id: %ld split or add peer failed", region_id);
        this->drop_region_from_store(region_id, false);
    }
}

void Store::heart_beat_thread() {
    while(!_shutdown) {
        send_heart_beat();
        bthread_usleep_fast_shutdown(FLAGS_store_heart_beat_interval_us, _shutdown);
    }
}

void Store::send_heart_beat() {
    pb::StoreHBRequest request;
    pb::StoreHBResponse response;
    // 1. 构造心跳
    construct_heart_beat_request(request);
    // 2. 发送请求
    if (_meta_server_interact->send_request("store_heartbeat", request, response)) {
        DB_WARNING("Error, send heart beat request to meta fail, request: %s",
                request.ShortDebugString().c_str());
    } else {
        process_heart_beat_response(response);
    }
    DB_DEBUG("store heart beat request: %s", request.ShortDebugString().c_str());
    DB_DEBUG("store heart beat response: %s", response.ShortDebugString().c_str());
    _last_heart_time.reset();
}

void Store::process_heart_beat_response(const pb::StoreHBResponse& response) {
    // TODO:
    DB_WARNING("process_heart_beat_response not implement now");
}

void Store::query(::google::protobuf::RpcController* controller,
                  const ::TKV::pb::StoreReq* request, 
                  ::TKV::pb::StoreRes* response,
                  ::google::protobuf::Closure* done) {
    brpc::ClosureGuard doen_gurad(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
    uint64_t log_id = 0;
    const auto& remote_side_tmp = butil::endpoint2str(cntl->remote_side());
    const char* remote_side = remote_side_tmp.c_str();
    if (cntl->has_log_id()) {
        log_id = cntl->log_id();
    }
    SmartRegion region = get_region(request->region_id());
    if (region == nullptr || region->removed()) {
        response->set_errcode(pb::REGION_NOT_EXIST);
        response->set_errmsg("region not exist in store");
        DB_WARNING("region_id: %ld not exist in store, log_id: %lu, remote_side: %s",
                request->region_id(), log_id, remote_side);
        return;
    }
    region->query(controller, request, response, doen_gurad.release());
}
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
