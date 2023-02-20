#pragma once
#include "engine/rocks_wrapper.h"

namespace TKV {
using pb::LockInfo;
using pb::Write;

class RocksSnapshot {
public:
    RocksSnapshot(rocksdb::Snapshot* snapshot, 
            RocksWrapper* db)
        : _snapshot(snapshot)
        , _db(db)
    {}

    rocksdb::Status get_cf(KV_CF cf, const std::string& key, std::string* val);
    
private:
    rocksdb::Snapshot* _snapshot {NULL};
    RocksWrapper*      _db       {NULL};
};

class MvccReader {
public:
    MvccReader(RocksSnapshot* snapshot)
        : _snapshot(snapshot)
    {}
    
    bool load_lock(const std::string& key, LockInfo& lock);

    bool seek_write(const std::string& key, uint64_t seek_ts, 
            uint64_t& commit_ts, Write& write_record);

private:
    RocksSnapshot*      _snapshot    {NULL}; 
    rocksdb::Iterator*  _data_cursor {NULL};
    rocksdb::Iterator*  _lock_cursor {NULL};
    rocksdb::Iterator*  _write_cursor{NULL};
    std::string         _lower_bound;
    std::string         _upper_bound;
    std::string         _current_key;  
};

class SnapshotReader {
public:
    SnapshotReader(uint64_t start_ts, rocksdb::Snapshot* snapshot)
        : _start_ts(start_ts)
        , _reader(snapshot)
    {}

    bool load_lock(const std::string& key, LockInfo& lock) {
        return _reader.load_lock(key, lock);
    }
    
    bool seek_write(const std::string& key, uint64_t seek_ts, 
            uint64_t& commit_ts, Write& write_record) {
        return _reader.seek_write(key, seek_ts, commit_ts, write_record);
    }

private:
    uint64_t    _start_ts;
    MvccReader  _reader;
};
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
