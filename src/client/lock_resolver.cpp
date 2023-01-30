#include "client/lock_resolver.h"
#include "client/cluster.h"
#include "client/async_send.h"

namespace TKV {
int64_t LockResolver::resolve_locks(BackOffer& bo, uint64_t caller_start_ts, 
            std::vector<std::shared_ptr<Lock>>& locks, 
            std::vector<uint64_t>& pushed,
            bool for_write) {
    TxnExpireTime before_txn_expired;   
    if (locks.empty()) {
        return before_txn_expired.value();
    }

    std::unordered_map<uint64_t, std::unordered_set<RegionVerId>> clean_txns;
    bool push_failed = false;
    if (!for_write) {
        pushed.reserve(locks.size());
    }
    for (auto& lock: locks) {
        bool force_sync_commit = false;
        TxnStatus s;
        s = get_txn_status_from_lock(bo, lock, caller_start_ts, force_sync_commit);
        if (s.code != ECode::Success) {
            before_txn_expired.update(0);
            pushed.clear();
            return before_txn_expired.value();
        }
        if (s.ttl == 0) { // Case1: commit或者rollback情况
            bool exist = true;
            if (clean_txns.find(lock->txn_id) == clean_txns.end()) {
                // 事务状态已经存在，无需查询
                exist = false;
            }
            auto& txn_set = clean_txns[lock->txn_id];
            int r = 0;
            if (!force_sync_commit 
                    && s.primary_lock.has_use_async_commit() 
                    && s.primary_lock.use_async_commit() 
                    && !exist) {
                // TODO: resolve_lock_async
                if (s.code == ECode::NoAsyncCommit) {
                    force_sync_commit = false;
                    continue;
                } else {
                    return -1;
                }
            } else if (lock->lock_type == pb::PESSIMISTICLOCK) {
                // TODO: resolve pessimistic lock
            }  else {
                r = resolve_lock(bo, lock, s, txn_set);
            }
            if (r < 0) {
                DB_FATAL("Resolve txn failed: %lu", lock->txn_id);
                before_txn_expired.update(0);
                pushed.clear();
                return before_txn_expired.value();
            }
        } else { // Case2: pwrite
            auto expired_time = cluster->oracle->util_expired(lock->txn_id, s.ttl);
            before_txn_expired.update(expired_time);
            if (for_write) {
                // 写冲突
                // 1) 如果是乐观事务且当前事务 < Lock的拥有者，abort当前事务
                // 在大事务场景下可能避免死锁
                if (lock->lock_type != pb::PESSIMISTICLOCK && 
                        lock->txn_id > caller_start_ts) {
                    DB_FATAL("Write conflict detected, txn_id: %lu", lock->txn_id);
                    pushed.clear();
                    return -1;
                }
            } else {
                if (s.action != pb::MIN_COMMIT_TS_PUSHED) {
                    push_failed = true;
                    break;
                }
                pushed.push_back(lock->txn_id);
            }
        }
    }
    if (push_failed) {
        pushed.clear();
    }
    return before_txn_expired.value();
}

TxnStatus LockResolver::get_txn_status_from_lock(BackOffer& bo, std::shared_ptr<Lock> lock, 
            uint64_t caller_start_ts, bool force_sync_commit) {
    uint64_t current_ts;
    if (lock->ttl == 0) {
        current_ts = UINT64_MAX;
    } else {
        current_ts = cluster->oracle->get_low_resolution_ts();
    }

    bool rollback_if_not_exist = false;
    
    for (;;) {
        auto s = get_txn_status(bo, lock->txn_id, lock->primary, 
                caller_start_ts, current_ts, rollback_if_not_exist, force_sync_commit);
        
        if (s.code == ECode::TxnNotFound) {
            bo.backoff(BoTxnNotFound);
            continue;
        }

        auto expired_time = cluster->oracle->util_expired(lock->txn_id, lock->ttl);
        if (expired_time <= 0) {
            DB_WARNING("Lock: %lu has been expired", lock->txn_id);
            if (lock->lock_type == pb::PESSIMISTICLOCK) {
                return TxnStatus{};
            }
            rollback_if_not_exist = true;
        } else {
            if (lock->lock_type == pb::PESSIMISTICLOCK) {
                TxnStatus s;
                s.ttl = lock->ttl;
                return s;
            }
        }
    }
}

TxnStatus LockResolver::get_txn_status(BackOffer& bo, uint64_t txn_id,
            const std::string& primary,
            uint64_t caller_start_ts, 
            uint64_t current_ts,
            bool rollback_if_not_exist,
            bool force_sync_commit) {
    TxnStatus* cached_status = get_resolved(txn_id);
    if (cached_status != nullptr) {
        return *cached_status;
    }

    auto location = cluster->region_cache->locate_key(primary);
    AsyncSendMeta meta(cluster,  location.region_ver);
    meta.request.set_op_type(pb::OP_GET_TXN_STATUS);
    pb::CheckTxnStatusRequest* request = meta.request.mutable_check_txn_req();
    request->set_primary_key(primary);
    request->set_lock_ts(txn_id);
    request->set_caller_start_ts(caller_start_ts);
    request->set_current_ts(current_ts);
    request->set_rollback_if_not_exist(rollback_if_not_exist);
    request->set_force_sync_commit(force_sync_commit);

    TxnStatus s;
    auto region = cluster->region_cache->get_region(meta.region_ver);
    int r = cluster->rpc_client->send_request(region->leader, 
                                              &meta.cntl, 
                                              &meta.request, 
                                              &meta.response, 
                                              NULL);   
    
    if (r < 0) {
        CHECK("rpc error" == 0);
    }

    auto response = meta.response.mutable_check_txn_res();
    if (response->has_key_error()) {
        const auto& key_error = response->key_error();
        if (key_error.has_txn_not_found()) {
            s.code = ECode::TxnNotFound;
        } else {
            s.code = ECode::UnKnow;
        }
        return s;
    }

    s.action = response->action(); 
    s.primary_lock = response->lock_info();
    if (s.primary_lock.has_use_async_commit() && s.primary_lock.use_async_commit()) {
        // TODO: 异步commit逻辑
    } else if (response->lock_ttl() != 0) {
        s.ttl = response->lock_ttl();
    } else {
        s.ttl = 0;
        s.commit_ts = response->commit_version();
        if (s.is_cleanable()) {
            put_resolved(txn_id, s);
        }
    }
    return s;
}

int LockResolver::resolve_lock(BackOffer& bo, std::shared_ptr<Lock> lock,
            TxnStatus& status, std::unordered_set<RegionVerId>& txn_set) {

    auto locate = cluster->region_cache->locate_key(lock->key);
    if (txn_set.count(locate.region_ver) > 0) {
        return 0;
    }
    
    AsyncSendMeta meta(cluster, locate.region_ver);
    meta.request.set_op_type(pb::OP_RESOLVE_LOCK);
    pb::ResolveLockRequest* request = meta.request.mutable_resolve_req();
    request->set_start_version(lock->txn_id);
    if (status.is_committed()) {
        request->set_commit_version(status.commit_ts);
    }
    if (lock->txn_size < BigTxnThreshold) {
        request->add_keys(lock->key);
        if (!status.is_committed()) {
            DB_WARNING("Resolved rollback lock: %lu", lock->txn_id);
        }
    }
    
    auto region = cluster->region_cache->get_region(meta.region_ver);
    int r = cluster->rpc_client->send_request(region->leader, 
                                              &meta.cntl, 
                                              &meta.request, 
                                              &meta.response, 
                                              NULL);   
    if (r < 0) {
        CHECK("rpc failed" == 0);
    }
   
    auto response = meta.response.mutable_resolve_res();
    if (response->has_key_error()) {
        return -1;
    }
    if (lock->txn_size >= BigTxnThreshold) {
        txn_set.insert(locate.region_ver);
    }
    return 0;
}
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
