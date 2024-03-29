#pragma once
#include "engine/rocks_wrapper.h"
#include "txn/concurrency.h"

namespace TKV {

class MvccTxn {
public:
    MvccTxn(uint64_t start_ts, ConcurrencyManager* concurrency)
        : _start_ts(start_ts)
        , _concurrency(concurrency)
    {}


private:
    uint64_t        _start_ts;
    uint64_t        _write_size;
    rocksdb::WriteBatch   _modifies;
    ConcurrencyManager*   _concurrency;
    std::vector<MemLock*> _guards;
    std::vector<LockInfo> _locks_for_1pc;
    std::vector<LockInfo> _new_locks;
};
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
