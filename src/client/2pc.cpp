#include "client/2pc.h"
#include "client/async_send_meat.h"


namespace TKV {
constexpr uint64_t ManagedLockTTL = 20000; // 20s

uint64_t txn_lock_ttl(std::chrono::milliseconds start, uint64_t txn_size) {
    return 0;
}

uint64_t send_txn_heart_beat(BackOffer& bo, std::shared_ptr<Cluster> cluster, 
        std::string& primary_lock, uint64_t start_ts, uint64_t new_ttl) {
    for (;;) {
        auto loate = cluster->region_cache->locate_key(primary_lock);
        
        AsyncSendMeta* meta = new AsyncSendMeta(cluster, loate.region_ver);
        auto request = meta->request.mutable_txn_hb_req(); 
        
        request->set_primary_lock(primary_lock);
        request->set_start_version(start_ts);
        request->set_advise_lock_ttl(new_ttl);


        auto region = cluster->region_cache->get_region(meta->region_ver);
        int r = cluster->rpc_client->send_request(region->leader, 
                                                  &meta->cntl, 
                                                  &meta->request, 
                                                  &meta->response, 
                                                  NULL);   
        if (r < 0) {
            // rpc错误
            bo.backoff(BoRegionMiss);
            continue;
        }
        auto response = meta->response->mutable_txn_hb_res();
        if (response->has_error()) {
            DB_WARNING("txn heart beat error, request: %s, response: %s", 
                    request->ShortDebugString().c_str(),
                    response->ShortDebugString().c_str());
            return -1;
        } 
        return response->lock_ttl();
    }
}

void TTLManager::keep_alive(std::shared_ptr<TwoPhaseCommitter> committer) {
    for (;;) {
        if (state.load(std::memory_order_acquire) == StateClosed) {
            return ;
        }
        
        bthread_usleep(ManagedLockTTL / 2);

        // 事务保活机制，延长事务执行时间
        BackOffer bo(PessimisticLockMaxBackOff);
        uint64_t now = committer->cluster->oracle->get_low_resolution_ts();
        uint64_t uptime = extract_physical(now) - extract_physical(committer->start_ts);
        uint64_t new_ttl = uptime + ManagedLockTTL;
        
        auto ret = send_txn_heart_beat(bo, 
                committer->cluster, 
                committer->primary_lock, 
                committer->start_ts, 
                new_ttl);

        if (ret < 0) {
            return ;
        }
    }
}

TwoPhaseCommitter::TwoPhaseCommitter(Txn* txn, bool use_async_commit)
    : start_time(txn->start_time)
    , use_async_commit(use_async_commit) {

    committed = false;
    txn->walk_buffer([&](const std::string& key, const std::string& value) {
            keys.push_back(key);
            mutations.emplace(key, value);
    });    

    cluster = txn->cluster;
    start_ts = txn->start_ts;
    primary_lock = keys[0];
    txn_size = mutations.size();

    // 默认lock_ttl为3s
    lock_ttl = DefaultLockTTL;    
    // 事务大于32M时，lock_ttl变成20s
    if (txn_size > TTLRunThreshold) {
        lock_ttl = ManagedLockTTL;
    }
}

void TwoPhaseCommitter::execute() {
    // 2pc执行流程
    if (use_async_commit) {
        // TODO: 异步commit的逻辑
    }

    // step1: pwrite keys
    BackOffer pwrite_bo(PwriteMaxBackOff);
    pwrite_keys(pwrite_bo, keys);

    if (use_async_commit) {
        // TODO: 异步commit的逻辑
    }

    // step2: commit keys
    commit_ts = cluster->meta_client->gen_tso();
    BackOffer commit_bo(CommitMaxBackOff);
    commit_keys(commit_bo, keys);

    ttl_manager.close();
}

int TwoPhaseCommitter::do_action_on_keys(backoffer& bo, const std::vector<std::string>& keys, Action action) {
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
        // commit
        do_action_on_batchs(bo, std::vector<BatchKeys>(batchs.begin(), batchs.begin() + 1, action));
        batchs = std::vector<BatchKeys>(batchs.begin() + 1, batchs.end());
    }
    if constexpr (action != TxnCommit) {
        // pwrite/rollback
        do_action_on_batchs(bo, batchs, action);
    }
}

int TwoPhaseCommitter::do_action_on_batch(BackOffer& bo, const std::vector<BatchKeys>& batchs, Action action) {
    // TODO: Pwrite和Commit是否可以并发？
    for (const auto& batch : batchs) {
        // 循环, primary_lock在batchs[0];
        if (action == TxnPwrite) {
            region_txn_size[batch.region_id] = batch.keys.size();
            pwrite_singl_batch(bo, batch);
        } else if (action == TxnCommit) {
            commit_single_batch(bo, batch);
        }
    }

}

// 同步调用
int pwrite_single_batch(BackOffer& bo, const BatchKeys& batch, Action action) {
    uint64_t batch_txn_size = region_txn_size[batch.region_ver.region_id];

    for (;;) {
        AsyncSendMeta* meta = new AsyncSendMeta(cluster, batch.region_ver);
        pb::PwriteRequest* request = meta->request.mutable_pwrite_req();         
        
        request->set_primary_lock(primary_lock);
        request->set_start_version(start_ts);
        request->set_lock_ttl(lock_ttl);
        request->set_txn_size(batch_txn_size);
        request->set_max_commit_ts(max_commit_ts);

        if (use_async_commit) {
            // TODO: 异步commit的逻辑
        } else {
            // TODO: pessimistic lock怎么处理
            // 同步commit，start_ts + 1 读到最新值原则
            request->set_min_commit_ts(start_ts + 1);
        }

        // pwrite同步发送
        auto region = cluster->region_cache->get_region(meta->region_ver);
        int r = cluster->rpc_client->send_request(region->leader, 
                                                  &meta->cntl, 
                                                  &meta->request, 
                                                  &meta->response, 
                                                  NULL);   
        if (r < 0) {
            // rpc错误
            return -1;
        }

        // 处理pwrite结果：有key没有pwrite成功
        if (response->key_errors_size() != 0) {
            std::vector<std::shared_ptr<Lock>> locks;
            int size = response->key_errors_size() 
            for (int i = 0; i < size; i++) {
                const auto& err = response->key_error(i);
                if (err.has_already_exist()) {
                    return -1;
                }
                auto lock = build_lock_from_key_error(err);
                locks.push_back(lock);
            }
            auto ms_before_expired = cluster->lock_resolver->resolve_lock_for_write(bo, start_ts, locks);
            if (ms_before_expired > 0) {
                bo.backoff_with_max_sleep(BoTxnLock, ms_before_expired);
            }
            continue;
        } else {
            if (batch.keys[0] == primary_lock) {
                // 在primary lock写入成功后， 如果事务大小大于32M
                // 开启TTLManager，TTLManager在commit时关闭
                if (txn_size > TTLRunThreshold) {
                    ttl_manager.run(std::shared_from_this());
                }
            }
            if (use_async_commit) {
                // TODO: 异步commit的逻辑
            }
        }
    }
}

int commit_single_batch(BackOffer& bo, const BatchKeys& batch, Action action) {

}

} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
