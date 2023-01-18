#include "client/2pc.h"
#include "client/async_send_meat.h"

namespace TKV {

int64_t txn_lock_ttl(std::chrono::milliseconds start, uint64_t txn_size) {
    return 0;
}

void TTLManager::keep_alive(std::shared_ptr<TwoPhaseCommitter> committer) {
}


void do_action_on_keys(backoffer& bo, const std::vector<std::string>& keys, Action action) {
    // RegionVerId -> keys
    auto groups = cluster->region_cache->group_keys_by_region(keys);    
    
    std::vector<BatchKeys> batchs;
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
                    primary_idx = batchs.size();
                }
                sub_keys.push_back(key);
            }
            batchs.emplace_back(BatchKeys(group.first, sub_keys));
        }
    }
    if (primary_idx != -1 && primary_idx != 0) {
        std::swap(batchs[0], batchs[primary_idx]);
        batchs[0].is_primary = true;
    }

    if constexpr (action == TxnCommit || action == TxnCleanUp) {
        // primary key
        do_action_on_batchs(bo, std::vector<BatchKeys>(batchs.begin(), batchs.begin() + 1, action));
        batchs = std::vector<BatchKeys>(batchs.begin() + 1, batchs.end());
    }
    if (action != TxnCommit) {
        // secondary key
        do_action_on_batchs(bo, batchs, action);
    }
}

void TwoPhaseCommitter::do_action_on_batch(BackOffer& bo, const std::vector<BatchKeys>& batchs, Action action) {
    for (const auto& batch : batchs) {
        if (action == TxnPwrite) {
            region_txn_size[batch.region_id] = batch.keys.size();
            pwrite_singl_batch(bo, batch);
        } else if (action == TxnCommit) {
            commit_single_batch(bo, batch);
        }
    }

}

// 同步调用
void pwrite_single_batch(BackOffer& bo, const BatchKeys& batch, Action action) {
    uint64_t batch_txn_size = region_txn_size[batch.region_ver.region_id];

    for (;;) {
        AsyncSendMeta* meta = new AsyncSendMeta(cluster, batch.region_ver);
        pb::StoreReq*  request = &meta->request;
        
        request->set_primary_lock(primary_lock);
        request->set_start_version(start_ts);
        request->set_lock_ttl(lock_ttl);
        request->set_txn_size(batch_txn_size);
        request->set_max_commit_ts(max_commit_ts);

        if (use_async_commit) {
            // TODO: 异步commit的逻辑
        } else {
            // TODO: pessimistic lock怎么处理
            // start_ts + 1 读到最新值原则
            request->set_min_commit_ts(start_ts + 1);
        }
        
        
    }
}

void commit_single_batch(BackOffer& bo, const BatchKeys& batch, Action action) {

}

} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
