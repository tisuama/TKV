#pragma once
#include "txn/latch.h"

namespace TKV {
    
class Scheduler {
public:
    Scheduler()
        : _stopped(false)
        , _latches(256)
    {}
    
    // TxnLock注意释放内存
    TxnLock* acquire_lock(uint64_t start_ts, std::vector<std::string>& keys);
    
    void release_lock(TxnLock* lock);

    void execute();

private:

    bool        _stopped;
    Latches     _latches;
};
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
