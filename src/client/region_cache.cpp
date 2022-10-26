#include "client/region_cache.h"

namespace TKV {
SmartRegion RegionCache::search_cache_region(const std::string& key) {
    BAIDU_SCOPED_LOCK(_region_mutex);
    auto it = _regions_map.upper(key);
    if (it != region_cache.end() && it->second->contains(key)) {
        return it->second;        
    }
    // 空key考虑为最大值
    if (_regions_map.begin() != _regions_map.end() && _regions_map.begin()->second->contains(key)) {
        return _regions_map.begin()->second;
    }
    return nullptr;
}

KeyLocation  RegionCache::locate_key(const std::string& key) {
    SmartRegion region = search_cache_region(key);
    if (region != nullptr) {
        return KeyLocation(region->ver_id(), region->start_key(), region->end_key());
    }

    region = load_region_by_key(key);
    insert_to_cache(region);
    
    return KeyLocation(region->ver_id(), region->start_key(), region->end_key());
}

SmartRegion RegionCache::load_region_by_key(const std::string& key) {
    
} 
} //namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */

