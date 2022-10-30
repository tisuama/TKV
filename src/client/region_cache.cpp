#include "client/region_cache.h"

namespace TKV {
SmartRegion RegionCache::search_cache_region(const std::string& key) {
    BAIDU_SCOPED_LOCK(_region_mutex);
    auto it = _regions_map.upper_bound(key);
    if (it != _regions_map.end() && it->second->contains(key)) {
        return it->second;        
    }
    // 空key考虑为最大值
    if (_regions_map.begin() != _regions_map.end() && _regions_map.begin()->second->contains(key)) {
        return _regions_map.begin()->second;
    }
    return nullptr;
}

KeyLocation RegionCache::locate_key(const std::string& key) {
    SmartRegion region = search_cache_region(key);
    if (region != nullptr) {
        return KeyLocation(region->ver_id(), region->start_key(), region->end_key());
    }

    reload_region();
    
    return KeyLocation(region->ver_id(), region->start_key(), region->end_key());
}

void RegionCache::reload_region() {
    std::vector<pb::RegionInfo> region_infos;
    // span and load region info
    int ret = 0;
    do {
        ret = _meta_client->reload_region(region_infos);
    } while (ret != 0);
    
    // update region info
    for (auto& region_info: region_infos) {
        update_region(std::make_shared<Region>(region_info, region_info.leader()));
    }
} 

void RegionCache::update_region(SmartRegion region) {
    BAIDU_SCOPED_LOCK(_region_mutex);
    _regions_map[region->end_key()] = region;
    _regions[region->ver_id()] = region;
}

SmartRegion RegionCache::get_region(const RegionVerId& id) {
    BAIDU_SCOPED_LOCK(_region_mutex);
    auto it = _regions.find(id);
    if (it == _regions.end()) {
        return nullptr;
    }
    return it->second;
}
} //namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */

