#include "store/store.h"
#include "store/meta_writer.h"
#include "common/mut_table_key.h"
#include <sys/statfs.h>

namespace TKV {
DECLARE_string(default_physical_room);
DECLARE_int32(store_port);
DECLARE_string(resource_tag);
DECLARE_string(db_path);
DECLARE_int32(balance_periodicity);

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
    
    // LogEntryReader
    
    // First heartbeat
    pb::StoreHBRequest  request;
    pb::StoreHBResponse response;
    // construct_heart_beat_request(request);
    DB_WARNING("heartbeat request: %s is construct when init store", request.ShortDebugString().data());
    TimeCost time_cost;
    if (_meta_server_interact->send_request("store_heartbeat", request, response) == 0){
        _factory->update_tables_double_buffer_sync(response.schema_change_info());
        DB_WARNING("send heartbeat request to meta server success");
        // do something
    } else {
        DB_FATAL("send heartbeat request to meta server failed");
        return -1;
    }
    int heartbeat_cost = time_cost.get_time();
    time_cost.reset();
    DB_WARNING("get schema info from meta server sucess");

    // Set region info has been exist before
    
    return 0;
}

int Store::init_after_listen(const std::vector<std::int64_t>& init_region_ids) {
    return 0;
}

void Store::init_region(::google::protobuf::RpcController* controller,
                     const ::TKV::pb::InitRegion* request,
                     ::TKV::pb::StoreRes* response,
                     ::google::protobuf::Closure* done) {
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
        pb::SchemaHB* schema = request.add_schema_info();
        schema->set_table_id(table_info.first);
        schema->set_version(table_info.second);
    }

    // 构造所有region的version信息
    traverse_copy_region_map([&request, need_peer_balance](const SmartRegion& region) {
        region->construct_heart_beat_request(request, need_peer_balance);       
    });
}

Store::~Store() {
}

} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
