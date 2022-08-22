#pragma once

#include <string>
#include <atomic>

#include <rocksdb/slice.h>

namespace TKV {
struct LogHead {
    explicit LogHead(const rocksdb::Slice& raw) {
        butil::RawUnpacker(raw.data())
            .unpack64((uint64_t&)term)
            .unpack32((uint32_t&)type);
    }
    
    LogHead(int64_t term, int type): term(term), type(type) { }
    void serialize_to(void* data) {
        butil::RawPacker(data).pack64(term).pack32(type);
    }
    int64_t term;
    int type;
};

class MyRaftLogStorage: public braft::LogStorage {
public:
private:
    std::atomic<int64_t> _first_log_index;
    std::atomic<int64_t> _last_log_index;
    int64_t _region_id;

    RocksWrapper* _db;
    rocksdb::ColumnFamilyHandle* _raft_log_handle;
    bool is_binlog_region {false};

    IndexTermMap _term_map;
    bthread_mutex_t _mutex;
};
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
