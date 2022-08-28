#pragma once
#include <rocksdb/compaction_filter.h>
#include <bthread/mutex.h>
#include "raft/my_raft_log_storage.h"
#include "common/key_encoder.h"


namespace TKV {
class RaftLogCompactionFilter: public rocksdb::CompactionFilter {
public:
    static RaftLogCompactionFilter* get_instance() {
        static RaftLogCompactionFilter instance;
        return &instance;
    }
    ~RaftLogCompactionFilter() {
        bthread_mutex_destroy(&_mutex);
    }

    const char* Name() const override {
        return "RaftLogCompactionFilter";
    }

    bool Filter(int /*level*/,
                const rocksdb::Slice& key,
                const rocksdb::Slice& /*existing_value*/,
                std::string* /*new value*/,
                bool* /*value changed*/) {
        if (key.size() != MyRaftLogStorage::LOG_DATA_KEY_SIZE) {
            return false;
        }
        uint64_t tmp_region_id = *(uint64_t*)key.data();
        int64_t region_id = KeyEncoder::decode_i64(KeyEncoder::to_endian_u64(tmp_region_id));
        uint64_t tmp_index_id = *(uint64_t*)(key.data() + sizeof(int64_t) + 1);
        int64_t index = KeyEncoder::decode_i64(KeyEncoder::to_endian_u64(tmp_index_id));
        BAIDU_SCOPED_LOCK(_mutex);
        auto it = _first_index_map.find(region_id);
        if (it == _first_index_map.end()) {
            return false;
        }
        // TODO: 分裂时禁止删除log
        // log_index < first_log_index -> compaction时删除
        return index < iter->second;     
    }

    int update_first_index_map(int64_t region_id, int64_t index) {
        DB_WARNING("update compaction filter, region_id: %ld, index: %ld",
                region_id, index);
        BAIDU_SCOPED_LOCK(_mutex);
        _first_index_map[region_id] = index;
        return 0;
    }

    int remove_region_id(int64_t region_id) {
        DB_WARNING("remove compaction filter, region_id: %ld", region_id);
        BAIDU_SCOPED_LOCK(_mutex);
        _first_index_map.erase(region_id);
        return 0;
    }


private:
    RaftLogCompactionFilter() {
        bthread_mutex_init(&_mutex, NULL);
    }
    std::unordered_map<int64_t, int64_t> _first_index_map;
    mutable bthread_mutex_t _mutex;
};
}
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
