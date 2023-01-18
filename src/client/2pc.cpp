#include "client/2pc.h"

namespace TKV {

int64_t txn_lock_ttl(std::chrono::milliseconds start, uint64_t txn_size) {
    return 0;
}

void TTLManager::keep_alive(std::shared_ptr<TwoPhaseCommitter> committer) {
}


void do_action_on_keys(backoffer& bo, const std::vector<std::string>& keys, action action) {
    auto groups = cluster->region_cache->group_keys_by_region(keys);    
    
    std::vector<BatchKeys> batch;
    int primary_idx = -1;
    for (auto group: groups) {
        uint32_t j = 0;
        for (uint32_t i = 0; i < group.second.size(); i = j) {
            uint32_t size = 0;
            std::vector<std::string> sub_keys;
            for (j = i; j < group.second.size() && size < TxnCommitBatchSize; j++) {
                auto& key = group.second[j];
                size += key.size();
                if (action == TxnPwrite) {
                    size += mutations[key].size();
                } 
                if (key == primary_lock) {
                    primary_idx = batch.size();
                }
                sub_keys.push_back(key);
            }
            batch.emplace_back(BatchKeys(group.first, sub_keys));
        }
    }
    if (primary_idx != -1 && primary_idx != 0) {
        std::swap(batch[0], batch[primary_idx]);
        batch[0].is_primary = true;
    }

    if constexpr (action == TxnCommit || action == TxnCleanUp) {
        do_action_on_batchs(bo, std::vector<BatchKeys>(batch.begin(), batch.begin() + 1));
        batch = std::vector<BatchKeys>(batch.begin() + 1, batch.end());
    }
    if (action != TxnCommit) {
        do_action_on_batchs(bo, batch);
    }
}

void TwoPhaseCommitter::do_action_on_batch(BackOffer& bo, const std::vector<BatchKeys>& batch) {
    // batch data
    
}
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
