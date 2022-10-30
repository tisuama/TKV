#include "client/batch_data.h"
#include "client/region_cache.h"

namespace TKV {
static inline set_region_ver(pb::StoreReq* request, KeyLocation& key_location) {
    auto region_ver = key_location.region_ver;
    request->set_region_id(region_ver.region_id);
    request->set_conf_version(region_ver.conf_ver);
    request->set_version(region_ver.ver);
    request->set_start_key(key.start_key());
    request->set_end_key(key.end_key());
}
	
void BatchData::put(const std::string& key, 
        const std::string& value, 
        KeyLocation& key_location,
        braft::Closure* done) {
    auto request = get_request(region_id);
    auto closures = get_closure();
    closures->push_back(done);

    set_region_ver(request, key_location);
    
    auto batch_data = request.add_batch_data();
    batch_data->set_key(key);
    batch_data->set_value(value);
    batch_data->set_op_type(pb::OP_RAW_PUT);
} 

void BatchData::get(const std::string& key, 
        std::string* value, 
        KeyLocation& key_location,
        braft::Closure* done) {
    auto request = get_request(region_id);
    auto closures = get_closure();
    closures->push_back(done);

    set_region_ver(request, key_location);

    auto batch_data = request.add_batch_data();
    batch_data->set_key(key);
    batch_data->set_op_type(pb::OP_RAW_GET);
}
} // namespace TKV 
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
