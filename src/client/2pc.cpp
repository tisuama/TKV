#include "client/2pc.h"

namespace TKV {

int64_t txn_lock_ttl(std::chrono::milliseconds start, uint64_t txn_size) {
    return 0;
}

void TTLManager::keep_alive(std::shared_ptr<TwoPhaseCommitter> committer) {
    if (state.load(std::memory_order_relaxed) == StateClosed) {
        return ;
    }

    bthread_usleep(std::chrono::milliseconds(LockTTL / 2).count() * 1000LL);
    uint64_t now = committer->cluster->oracle->get_low_resolution_ts();        
    uint64_t uptime = extract_physical(now) - extract_physical(committer->start_ts);
    uint64_t new_ttl = uptime + LockTTL;

    // TODO: send txn heartbeat
}
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
