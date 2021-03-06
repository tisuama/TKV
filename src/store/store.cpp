#include "store/store.h"
#include "store/meta_writer.h"
#include "common/mut_table_key.h"
#include <sys/statfs.h>

namespace TKV {
DECLARE_string(default_physical_room);
DECLARE_int32(store_port);
DECLARE_string(resource_tag);
DECLARE_int32(balance_periodicity);

DEFINE_int64(store_heart_beat_interval_us, 30 * 1000 * 1000, "store heartbeat interval, default: 30s"); 
DEFINE_string(db_path, "./rocks_db", "rocksdb db path of store data");

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
    // construct_heart_beat_request(request);
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
            // TODO: RegionControl
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
    
    DB_WARNING("store init before listen success, region size: %ld, doing snapshot region size: %ld", init_region_ids.size(), _doing_snapshot_regions.size());
    
    return 0;
}

int Store::init_after_listen(const std::vector<std::int64_t>& init_region_ids) {
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
    if (!_shutdown) {
        DB_WARNING("Store has been shutdown");
        response->set_errcode(pb::INPUT_PARAM_ERROR);
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
    
    // ??????add_peer?????????can_add_peer?????????true
    bool is_addpeer = request->region_info().can_add_peer();

    // rocksdb stall -> cannot add peer
    // received_add_peer_concurrency????????????on_snapshot_load???????????????
    ON_SCOPED_EXIT([is_addpeer](){
        if (is_addpeer) {
            Concurrency::get_instance()->received_add_peer_concurrency.decrease_broadcase();
        } 
    });
    if (is_addpeer) {
        // Wait region::on_snapshot_load here
        int ret = Concurrency::get_instance()->received_add_peer_concurrency.increase_time_wait(1000 * 1000 * 10);
        if (ret != 0) {
            DB_WARNING("received_add_peer_concurrency timeout, count: %d, log_id: %lu, remote_side: %s",
                    Concurrency::get_instance()->received_add_peer_concurrency.count(), log_id, remote_side);
            response->set_errcode(pb::CANNOT_ADD_PEER);
            response->set_errmsg("received_add_peer_concurrency timeout");
            return ;
        }
    }
    
    // ??????Table??????
    if (!_factory->exist_table_id(table_id)) {
        if (request->has_schema_info()) {
            this->update_schema_info(request->schema_info(), nullptr);
        } else {
            ERROR_SET_RESPONSE_FAST(response, pb::INPUT_PARAM_ERROR, "table info is missing when add region", log_id);
            return ;
        }
    }
    auto pre_region = this->get_region(region_id);
    // ??????????????????????????????????????????????????????
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
    // ??????????????????
    this->set_region(region);
    // region init
    // int ret = region->init(true, request->snapshot_times());
    // TODO:
}

void Store::construct_heart_beat_request(pb::StoreHBRequest& request) {
    static int64_t count = 0;
    request.set_need_leader_balance(false);
    ++count;
    if (count % FLAGS_balance_periodicity == 0) {
        request.set_need_leader_balance(true);
    }

    bool need_peer_balance = false;
    // init before listen ????????????????????????
    if (count == 2 || 
            _last_heart_time.get_time() > FLAGS_store_heart_beat_interval_us * 4) {
        need_peer_balance = true;
    }
    if (count % FLAGS_balance_periodicity == (FLAGS_balance_periodicity / 2)) {
        need_peer_balance = true;
        request.set_need_peer_balance(true);
    }
    // ??????instance??????
    pb::InstanceInfo* info = request.mutable_instance_info();
    info->set_address(_address);
    info->set_physical_room(_physical_room);
    info->set_resource_tag(_resource_tag);
    info->set_raft_total_latency(_raft_total_cost.latency(60));
    info->set_raft_total_qps(_raft_total_cost.qps(60));
    info->set_select_latency(_select_time_cost.latency(60));
    info->set_select_qps(_select_time_cost.qps(60));
    
    // ??????????????????
    struct statfs sfs;
    statfs(FLAGS_db_path.c_str(), &sfs);
    int64_t capacity = sfs.f_blocks * sfs.f_bsize;
    int64_t left_size = sfs.f_bavail * sfs.f_bsize;
    // Set bvar info
    _disk_total.set_value(capacity);
    _disk_used.set_value(capacity - left_size);
    
    info->set_capacity(capacity);
    info->set_used_size(capacity - left_size);

    // ??????schema version??????
    std::unordered_map<int64_t, int64_t> table_id_version_map;
    _factory->get_all_table_version(table_id_version_map);
    for (auto table_info : table_id_version_map) {
        pb::SchemaHB* schema = request.add_schema_info();
        schema->set_table_id(table_info.first);
        schema->set_version(table_info.second);
    }

    // ????????????region???version??????
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
    (void*)reverser_index_map;
}

int Store::drop_region_from_store(int64_t drop_region_id, bool need_delay_drop) {
    // TODO: need to impl drop_region_from_store
    DB_WARNING("region need remove, region_id: %ld, need_delay_drop: %d", drop_region_id, need_delay_drop);
    return 0;
}

} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
