#include "store/store.h"

namespace TKV {
DECLARE_string(default_physical_room);
DECLARE_int32(store_port);
DECLARE_string(resource_tag);
DECLARE_string(db_path);
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
    if (res != 0) {
        DB_FATAL("rocksdb init failed");
        return -1;
    }
    // MetaWriter
    
    // LogEntryReader
    
    // First heartbeat
    pb::StoreHBRequest  request;
    pb::StoreHBResponse response;
    // construct_heart_beat_request(request);
    DB_WARNING("heartbeat request: %s is construct when init store", request.ShortDebugString().data());
    TimeCost time_cost;
    if (_meta_server_interact->send_request("store_heartbeat", request, response) == 0){
        DB_WARNING("send heartbeat request to meta server success");
        // do something
    } else {
        DB_FATAL("send heartbeat request to meta server failed");
        return -1;
    }
    int heartbeat_cost = time_cost.get_time();
    time_cost.reset();

    // do something
    
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

Store::~Store() {
}

} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
