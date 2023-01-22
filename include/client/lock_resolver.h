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

inline std::shared_ptr<TxnStatus> extract_lock_from_key_error(const TKV::pb::KeyError& key_error) {
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

class LockResolver {
    // TODO: LockResolver
};
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
