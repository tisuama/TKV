#include "common/log.h"
#include "client/batch_data.h"
#include "client/region_cache.h"

namespace TKV {
static inline void set_region_ver(pb::StoreReq* request, KeyLocation& key_location) {
    auto region_ver = key_location.region_ver;
    request->set_region_id(region_ver.region_id);
    request->set_region_version(region_ver.ver);
    request->set_start_key(key_location.start_key);
    request->set_end_key(key_location.end_key);
}
	
void BatchData::put(const std::string& key, 
        const std::string& value, 
        KeyLocation& key_location,
        braft::Closure* done) {
    auto region_ver = key_location.region_ver;
    int64_t region_id = region_ver.region_id;

    auto request = get_request(region_id);

    auto closure = get_closure(region_id);
    closure->push_back(done);

    auto version = get_version(region_id);
    *version = region_ver;

    set_region_ver(request, key_location);
    
    request->set_op_type(pb::OP_PUT_KV);
    
    auto op = request->add_kv_ops();
    op->set_key(key);
    op->set_value(value);
    
    DB_DEBUG("region_id: %ld put data, request: %s, version: %s, version point: %p", 
            region_id, 
            request->ShortDebugString().c_str(), 
            version->to_string().c_str(),
            version);
} 

void BatchData::get(const std::string& key, 
        std::string* value, 
        KeyLocation& key_location,
        braft::Closure* done) {
    auto region_ver = key_location.region_ver;
    int64_t region_id =region_ver.region_id;

    auto request = get_request(region_id);

    auto closure = get_closure(region_id);
    closure->push_back(done);

    auto version = get_version(region_id);
    *version = region_ver;

    set_region_ver(request, key_location);

    request->set_op_type(pb::OP_GET_KV);
    auto op = request->add_kv_ops();
    op->set_key(key);

    DB_DEBUG("region_id: %ld get data, request: %s, version: %s, version point: %p", 
            region_id, 
            request->ShortDebugString().c_str(), 
            version->to_string().c_str(),
            version);
}
} // namespace TKV 
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
