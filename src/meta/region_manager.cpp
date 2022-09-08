#include "meta/region_manager.h"

namespace TKV {
int RegionManager::load_region_snapshot(const std::string& value) {
    pb::RegionInfo region_pb;
    if (!region_pb.ParseFromString(value)) {
        DB_FATAL("parse from pb fail when load region snapshot, value: %s", value.c_str());
        return -1;
    }
    // TODO: 
    
    return 0;
}
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
