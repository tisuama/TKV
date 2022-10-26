#pragma once
#include <string>

#include <bthread/mutex.h>

class TKV::RegionVerId;
template<>
struct hash<TKV::RegionVerId>
{
    using argument_type = TKV::RegionVerId;
    using result_type = size_t;
    size_t operator()(const TKV::RegionVerId& key) const { return key.id };
}


namespace TKV {
struct Store {
    const uint64_t id;
    const std::string addr;
    const std::string peer_addr;
};

struct RegionVerId {
    uint64_t id;
    uint64_t conf_ver;
    uint64_t ver;

    RegionVerId()
        : RegionVerId(0, 0, 0)
    {}
    
    RegionVerId(uint64_t id, uint64_t conf_ver, uint64_t ver)
       : id(id), conf_ver(conf_ver), ver(ver)
    {} 

    bool operator=(const RegionVerId& rhs) const {
        return id == rhs.id  && conf_ver == rhs.conf_ver && ver == rhs.ver;
    }
    
    std::string to_string() {
        return "{" + std::to_string(id) + "," + std::to_string(conf_ver) + "," + std::to_string(ver) + ")";
    }
};

struct Region {
    pb::RegionInfo meta;
    std::string    leader;

    Region(const pb::RegionInfo& meta, const std::string& leader)
        : meta(meta), leader(leader)
    {}

    const std::string& start_key() const { return meta.start_key(); }

    const std::string& end_key()   const { return meta.end_key();   }

    bool contains(const std::string& key) const { 
        return key >= start_key() && (key < end_key() || meta.end_key().empty()) 
    }
    
    RegionVerId ver_id() const {
        return RegionVerId {
            meta.id(),
            meta.conf_version(),
            meta.version()
        };
    }

    bool switch_peer(const std::string& peer) {
        leader = peer;
    }
};

using SmartRegion = std::shared_ptr<Region>;

struct KeyLocation {
    RegionVerId region_ver;
    std::string start_key;
    std::string end_key;

    KeyLocation() = default;
    KeyLocation(const RegionVerId& region_ver, const std::string& start_key, const std::string& end_key)
        : region_ver(region_ver), start_key(start_key), end_key(end_key)
    {}

    bool contains(const std::string& key) const {
        return key >= start_key && (key < end_key || end_key.empty());
    }
};

class RegionCache {
public:
    RegionCache() {
        bthread_mutex_init(&_store_mutex, NULL);
        bthread_mutex_init(&_region_mutex, NULL);
    }
    

    SmartRegion  search_cache_region(const std::string& key);
    KeyLocation  locate_key(const std::string& key);
    SmartRegion  load_region_by_key(const std::string& key); 

    ~RegionCache() {
        bthread_mutex_destroy(&_store_mutex);
        bthread_mutex_destroy(&_region_mutex);
    }

private:
    // end_key -> SmartRegion
    std::map<std::string, SmartRegion>           _regions_map;
    // region_ver -> SmartRegion
    std::unordered_map<RegionVerId, SmartRegion> _regions;
    std::map<uint64_t, Store>   _stores;
    bthread_mutex_t             _store_mutex;
    bthread_mutex_t             _region_mutex;
};

using SmartRegionCache = std::unique_ptr<RegionCache>;
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
