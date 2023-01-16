#include "client/2pc.h"

namespace TKV {

int64_t txn_lock_ttl(std::chrono::milliseconds start, uint64_t txn_size) {
    return 0;
}

void TTLManager::keep_alive(std::shared_ptr<TwoPhaseCommitter> committer) {
}


void do_action_on_keys(backoffer& bo, const std::vector<std::string>& keys, action action) {
    auto groups = cluster->region_cache->group_keys_by_region(keys);    
    // TODO:
}
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
