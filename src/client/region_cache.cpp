#include "common/log.h"
#include "client/region_cache.h"

namespace TKV {
SmartRegion RegionCache::search_cache_region(const std::string& key) {
    BAIDU_SCOPED_LOCK(_region_mutex);
    auto it = _regions_map.upper_bound(key);
    if (it != _regions_map.end() && it->second->contains(key)) {
        return it->second;        
    }
    // 空key考虑为最大值
    if (_regions_map.begin() != _regions_map.end() && 
        _regions_map.begin()->second->contains(key)) {
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
    
    region = search_cache_region(key);
    if (!region) {
        CHECK("locate key failed" == 0);
    }
    
    DB_DEBUG("Locate key: %s to region_id: %ld", key.c_str(), region->region_id());
    return KeyLocation(region->ver_id(), region->start_key(), region->end_key());
}

void RegionCache::reload_region() {
    DB_DEBUG("start reload region info");
    std::vector<pb::RegionInfo> region_infos;
    // span and load region info
    int ret = 0;
    do {
        ret = _meta_client->reload_region(region_infos);
        bthread_usleep(1LL * 1 * 1000 * 1000);
    } while (ret != 0);
    
    // update region info
    DB_DEBUG("reload region info, size: %ld", region_infos.size());
    for (auto& region_info: region_infos) {
        DB_DEBUG("reload region info: %s", region_info.ShortDebugString().c_str());
        update_region(std::make_shared<Region>(region_info, region_info.leader()));
    }
} 

void RegionCache::update_region(SmartRegion region) {
    BAIDU_SCOPED_LOCK(_region_mutex);
    DB_DEBUG("update region: %p, addr: %s", region.get(), region->leader.c_str());
    _regions_map[region->end_key()] = region;
    _regions[region->ver_id()] = region;
}

SmartRegion RegionCache::get_region(const RegionVerId& id) {
    BAIDU_SCOPED_LOCK(_region_mutex);
    auto it = _regions.find(id);
    if (it == _regions.end()) {
        CHECK("region not found");
    }
    return it->second;
}

void RegionCache::drop_region(const RegionVerId& id) {
    BAIDU_SCOPED_LOCK(_region_mutex);
    auto it = _regions.find(id);
    if (it != _regions.end()) {
        auto iter_by_key = _regions_map.find(it->second->end_key());
        if (iter_by_key != _regions_map.end()) {
            _regions_map.erase(iter_by_key);
        }
        _regions.erase(it);
        DB_DEBUG("drop region: %ld", id.region_id);
    }
}

bool RegionCache::update_leader(const RegionVerId& id, const std::string& leader) {
    {
        BAIDU_SCOPED_LOCK(_region_mutex);
        auto  it = _regions.find(id);
        if (it == _regions.end()) {
            return false;
        }
        if (it->second->switch_peer(leader)) {
            return true;        
        }
    }
    drop_region(id);
    return false;

}

std::unordered_map<RegionVerId, std::vector<std::string>>
RegionCache::group_keys_by_region(const std::vector<std::string>& keys) {
    std::unordered_map<RegionVerId, std::vector<std::string>> result;
    KeyLocation location;
    for (auto& key: keys) {
        if (!location.contains(key)) {
            location = locate_key(key);
        }
        DB_DEBUG("locate key: %s, locate: %s", key.c_str(), location.region_ver.to_string().c_str());
        result[location.region_ver].push_back(key);
    }
    return result;
} 
} //namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */

