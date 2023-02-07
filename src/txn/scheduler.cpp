#include "txn/scheduler.h"

namespace TKV {
TxnLock* Scheduler::Lock(uint64_t start_ts, std::vector<std::string>& keys) {
    auto txn_lock = _latches.gen_lock(start_ts, keys);
    if (_latches.acquire(lock) == false) {
        txn_lock->cond.incrase_wait();
    }
    if (txn_lock->is_locked()) {
        CHECK("TxnLock locked failed" == 0);
    }
    return txn_lock;
}

void Scheduler::UnLock(TxnLock* lock) {
    auto wakeup_list = _latches.release(lock);
    
    // wake_fn
    auto wake_fn = [wakeup_list]() {
        for (auto lock: wakeup_list) {
            if (_latches.acquire(lock)) {
                lock->decrease_signal();
            }
        }
    };
    Bthread bth;
    bth.run(wake_fn);
}
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
