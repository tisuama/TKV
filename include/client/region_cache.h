#pragma once
#include <string>
#include <functional>
#include <bthread/mutex.h>

#include "proto/meta.pb.h"
#include "client/meta_client.h"

namespace TKV {
struct Store {
    const uint64_t id;
    const std::string addr;
    const std::string peer_addr;
};

struct RegionVerId {
    int64_t  region_id;
    int64_t  conf_ver;
    int64_t  ver;

    RegionVerId()
        : RegionVerId(0, 0, 0)
    {}
    
    RegionVerId(int64_t region_id, int64_t conf_ver, int64_t ver)
       : region_id(region_id), conf_ver(conf_ver), ver(ver)
    {} 

    bool operator==(const RegionVerId& rhs) const {
        return region_id == rhs.region_id  && conf_ver == rhs.conf_ver && ver == rhs.ver;
    }
    
    std::string to_string() const {
        /* (region_id, conf_ver, region_ver) */
        return "(" + std::to_string(region_id) + "," + 
            std::to_string(conf_ver) + "," + std::to_string(ver) + ")";
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

    int64_t region_id()      const { return meta.region_id(); }

    bool contains(const std::string& key) const { 
        return key >= start_key() && (key < end_key() || meta.end_key().empty());
    }

    RegionVerId ver_id() const {
        return RegionVerId {
            meta.region_id(),
            meta.conf_version(),
            meta.version()
        };
    }

    void switch_peer(const std::string& peer) {
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

} // namespace TKV

namespace std {
template<>
struct hash<TKV::RegionVerId>
{
    using argument_type = TKV::RegionVerId;
    using result_type = size_t;
    size_t operator()(const TKV::RegionVerId& key) const { return key.region_id; }
};
}

namespace TKV {
class RegionCache {
public:
    RegionCache(std::shared_ptr<MetaClient> meta_client) 
        : _meta_client(meta_client)
    {
        bthread_mutex_init(&_store_mutex, NULL);
        bthread_mutex_init(&_region_mutex, NULL);
    }
    
    ~RegionCache() {
        bthread_mutex_destroy(&_store_mutex);
        bthread_mutex_destroy(&_region_mutex);
    }

    SmartRegion  search_cache_region(const std::string& key);

    KeyLocation  locate_key(const std::string& key);

    void reload_region(); 

    void update_region(SmartRegion region);

    SmartRegion get_region(const RegionVerId& id);


private:
    // end_key -> SmartRegion
    // 普通请求是确定是哪个region
    std::map<std::string, SmartRegion>           _regions_map;
    // region_ver -> SmartRegion
    // 客户端调用split_region时使用
    std::unordered_map<RegionVerId, SmartRegion> _regions;

    std::map<uint64_t, Store>                    _stores;
    
    std::shared_ptr<MetaClient>                  _meta_client;

    bthread_mutex_t             _store_mutex;
    bthread_mutex_t             _region_mutex;
};

} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
