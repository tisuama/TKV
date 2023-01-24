#pragma once
#include <string>

namespace TKV {
constexpr size_t ResolvedCacheSize = 2048;
constexpr int  BigTxnThreashold = 16;
const uint64_t DefaultLockTTL = 3000;
const uint64_t MaxLockTTL     = 1200000;

struct Cluster;

struct TxnStatus {
    uint64_t ttl       {0};
    uint64_t commit_ts {0};
    TKV::pb::Action action;
    TKV::pb::LockInfo primary_lock;

    bool is_committed() const { 
        return ttl == 0 && commit_ts > 0; 
    }

    bool is_cleanable() const {
        if (is_committed()) {
            return true;
        }
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

inline std::shared_ptr<TxnStatus> build_lock_from_key_error(const TKV::pb::KeyError& key_error) {
    CHECK(key_error.has_locked());
    return std::make_shared<Lock>(key_error.locked());
}

struct TxnExpiredTime {
    bool    initialized;
    int64_t txn_expire;
    
    TxnExpiredTime()
        : initialized(false)
        , txn_expire(0)
    {}

    void update(int64_t lock_expire) {
        if (lock_expire <= 0) {
            lock_expire = 0;
        }
        if (!initialized) {
            txn_expire = lock_expire;
            initialized = true;
        } else if (lock_expire < txn_expire) {
            txn_expire = lock_expire;
        }
    }

    int64_t value() const {
        return initialized ? txn_expire: 0;
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
            std::vector<std::shared_ptr<Lock>& locks) {
        std::vector<uint64_t> ignored;
        return resolve_locks(bo, caller_start_ts, locks, ignored, true);
    }

    // LockResolver 需要经历三个步骤：
    // 1) 使用 LockTTL选择过期的key
    // 2) 对于每一个key，通过primary_key确认事务提交状态
    // 3) 发送`ResolveLock`命令给对应的region
    // return: ms_before_expired
    int64_t resolve_locks(BackOffer& bo, uint64_t caller_start_ts, 
            std::vector<std::shared_ptr<Lock>>& locks, 
            std::vector<uint64_t>& pushed);
    
    int64_t resolve_locks(BackOffer& bo, uint64_t caller_start_ts, 
            std::vector<std::shared_ptr<Lock>>& locks);


private:


    std::shared_ptr<Cluster> cluster;
    bthread_mutex_t          mu;
    std::unordered_map<int64_t, TxnStatus> resolved;
    std::deque<int64_t>                    cached;
};
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
