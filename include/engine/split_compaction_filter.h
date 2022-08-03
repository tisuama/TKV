#pragma once

#include <rocksdb/compaction_filter.h>
#include <bthread/mutex.h>

#include "common/key_encoder.h"
#include "common/schema_factory.h"
#include "common/common.h"


namespace TKV {
// Filter Region Info
class SplitCompactionFilter: public rocksdb::CompactionFilter {
std::map<int64_t, FilterRegionInfo*> KeyMap;
typedef DoubleBuffer<KeyMap> DoubleBufferKey;
public:
    static SplitCompactionFilter* get_instance() {
        static SplitCompactionFilter instance;
        return &instance;
    }
    
    ~SplitCompactionFilter() {}
    
    const char* Name() const override {
        return "SplitCompactionFilter";
    }

    /* false: key-value需要保留; true: key-value不需要保留 */
    bool Filter(int level, const rocksdb::Slice& key, const rocksdb::Slice& value,
            std::string* /* new_value */, bool* / value_changed */) const override {
        if (level < 5) {
            return false;
        }
        // prefix: 8 bytes
        static int prefix_len = sizeof(int64_t);
        if (key.size() < prefix_len) {
            return false;
        }
        TableKey table_key(key);
        int64_t region_id = table_key.extract_i64(0);
        FilterRegionInfo* filter = get_filter_region_info(region_id);
        if (filter == nullptr || filter->end_key.empty()) {
            return false;
        }
        // TODO: TTL binglog存在时间进行Filter
        return false;
    }
    
    void set_filter_region_info(int64_t region_id, const std::string& end_key,
            bool use_ttl, int64_t online_ttl_expire_time_us) {
        auto old = get_filter_region_info(region_id);
        if (old != nullptr && old->end_key == end_key) {
            return ;
        }
        auto fn = [region_id, end_key, use_ttl, online_ttl_expire_time_us](KeyMap& key_map) {
            auto new_info = new FilterRegionInfo(use_ttl, end_key, online_ttl_expire_time_us);
            key_map[region_id] = new_info;
        };
        _range_key_map.modify(fn);
    }

    FilterRegionInfo* get_filter_region_info(int64_t region_id) const {
        auto m = _range_key_map.read();
        if (m->find(region_id) == m.end()) {
            return nullptr;
        }
        return (*m)[region_id];
    }
    
private:
    struct FilterRegionInfo {
        FilterRegionInfo(bool use_ttl, const std::string& end_key, 
                int64_t online_ttl_expire_time_us) 
            : use_ttl(use_ttl)
            , end_key(end_key)
            , online_ttl_expire_time_us(online_ttl_expire_time_us)
        {}

        bool use_ttl {false};
        std::string end_key;
        int64_t online_ttl_expire_time_us;
    };

    SplitCompactionFilter() {
        _factory = SchemaFactory::get_instance();
    }
    
    mutable DoubleBufferKey _range_key_map;
    SchemaFactory* _factory;

};
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */

