#pragma once
#include "common/common.h"
#include "client/backoff.h"
#include "proto/store.pb.h"

#include <queue>

namespace TKV {
constexpr size_t ResolvedCacheSize = 2048;
constexpr int  BigTxnThreshold    = 16;
const uint64_t DefaultLockTTL      = 3000;
const uint64_t MaxLockTTL          = 1200000;

struct Cluster;
struct RegionVerId;

struct TxnStatus {
    uint64_t ttl       {0};
    uint64_t commit_ts {0};
    TKV::pb::Action action;
    TKV::pb::LockInfo primary_lock;
    ECode    code      {ECode::Success};

    bool is_committed() const { 
        return ttl == 0 && commit_ts > 0; 
    }

    // 判断是否可以清除
    bool is_cleanable() const {
        if (is_committed()) {
            // 已经commit
            return true;
        }
        if (ttl == 0) {
            // 已经rollback
            if (action == pb::Action::NO_ACTION 
             || action == pb::Action::LOCK_NOT_EXIST_ROLLBACK
             || action == pb::Action::TTL_EXPIRE_ROLLBACK) {
                return true;
            }
        }
        return false;
    }
};

struct Lock {
    std::string     key;
    std::string     primary;
    uint64_t        txn_id;
    uint64_t        ttl;
    uint64_t        txn_size;
    TKV::pb::TxnOp  lock_type;    
    bool            use_async_commit;
    uint64_t        lock_for_update_ts;
    uint64_t        min_commit_ts;

    explicit Lock(const TKV::pb::LockInfo& lock) 
        : key(lock.key())
        , primary(lock.primary_lock())
        , txn_id(lock.lock_version())
        , ttl(lock.lock_ttl())
        , txn_size(lock.txn_size())
        , lock_type(lock.lock_type())
        , use_async_commit(lock.use_async_commit())
        , min_commit_ts(lock.min_commit_ts())
    {}
};

inline std::shared_ptr<TKV::Lock> build_lock_from_key_error(const TKV::pb::KeyError& key_error) {
    CHECK(key_error.has_locked());
    return std::make_shared<Lock>(key_error.locked());
}

struct TxnExpireTime {
    int64_t txn_expire;
    
    TxnExpireTime()
        : txn_expire(0)
    {}

    void update(int64_t lock_expire) {
        txn_expire = std::min(txn_expire, std::max((int64_t)0, lock_expire));
    }

    int64_t value() const {
        return txn_expire;
    }
};

// LockResolver resolve lock and cache txn status
class LockResolver {
public:
    explicit LockResolver(std::shared_ptr<Cluster> cluster)
        : cluster(cluster)
    {}

    void update(std::shared_ptr<Cluster> cluster) {
        cluster = cluster;
    }

    int64_t resolve_lock_for_write(BackOffer& bo, uint64_t caller_start_ts,
            std::vector<std::shared_ptr<Lock>>& locks) {
        std::vector<uint64_t> ignored;
        return resolve_locks(bo, caller_start_ts, locks, ignored, true /* for write */);
    }

    int64_t resolve_locks(BackOffer& bo, 
            uint64_t caller_start_ts, 
            std::vector<std::shared_ptr<Lock>>& locks, 
            std::vector<uint64_t>& pushed) {
        return resolve_locks(bo, caller_start_ts, locks, pushed, false/* for write */); 
    }

    // LockResolver 需要经历三个步骤：
    // 1) 使用 LockTTL选择过期的Lock
    // 2) 对于每一个key，通过primary_key确认事务提交状态
    // 3) 发送`ResolveLock`命令给对应的region
    // return: ms_before_expired
    int64_t resolve_locks(BackOffer& bo, uint64_t caller_start_ts, 
            std::vector<std::shared_ptr<Lock>>& locks, 
            std::vector<uint64_t>& pushed,
            bool for_write);

private:
    TxnStatus get_txn_status_from_lock(BackOffer& bo, std::shared_ptr<Lock> lock, 
            uint64_t caller_start_ts, bool force_sync_commit);

    TxnStatus get_txn_status(BackOffer& bo, uint64_t txn_id,
            const std::string& primary,
            uint64_t caller_start_ts, 
            uint64_t current_ts,
            bool rollback_if_not_exist,
            bool force_sync_commit);

    // 同步resolve lock 方法
    int resolve_lock(BackOffer& bo, std::shared_ptr<Lock> lock,
            TxnStatus& status, std::unordered_set<RegionVerId>& txn_set);
    
    // 异步resolve lock 方法
    int resolve_lock_async(BackOffer& bo, std::shared_ptr<Lock> lock, TxnStatus& status);

    // 悲观事务resolve lock 方法
    int resolve_pessimistic_lock(BackOffer& bo, std::shared_ptr<Lock> lock, 
            std::unordered_set<RegionVerId>& txn_set);
    
    TxnStatus* get_resolved(uint64_t txn_id) {
        BAIDU_SCOPED_LOCK(mu);
        auto it = resolved.find(txn_id);
        if (it == resolved.end()) {
            return nullptr;
        }
        return &(it->second);
    }
    
    void put_resolved(uint64_t txn_id, const TxnStatus& status) {
        BAIDU_SCOPED_LOCK(mu);
        if (resolved.find(txn_id) != resolved.end()) {
            return ;
        }
        cached.push(txn_id);
        resolved.emplace(txn_id, status);
        if (cached.size() > ResolvedCacheSize) {
            auto to_moved = cached.front();
            cached.pop();
            resolved.erase(to_moved);
        }
    }

    std::shared_ptr<Cluster> cluster;
    bthread_mutex_t          mu;
    std::unordered_map<int64_t, TxnStatus> resolved;
    std::queue<int64_t>                    cached;
};
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
