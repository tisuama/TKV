#pragma once
#include "engine/rocks_wrapper.h"

namespace TKV {
class MvccReader {
public:
    MvccReader(rocksdb::Snapshot* snapshot)
        : _snapshot(snapshot)
    {}

private:
    rocksdb::Snapshot*  _snapshot    {NULL};
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

private:
    uint64_t    _start_ts;
    MvccReader  _reader;
};
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
