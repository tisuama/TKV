#pragma once
#include <string>
#include <memory>
#include <unordered_map>
#include <cmath>
#include <chrono>

#include "client/cluster.h"
#include "client/backoff.h"

namespace TKV {
constexpr uint64_t LockTTL = 20000; // 20s
constexpr uint64_t BytesPerMB = 1024 * 1024;
constexpr uint64_t TTLRunThreshold = 32 * 1024 * 1024; // 32M
constexpr uint64_t PessimisticLockBackoff = 20000;     // 20s
constexpr uint32_t TxnCommitBatchSize = 16 * 1024;

int64_t send_txn_heart_beat(BackOffer& bo, std::shared_ptr<Cluster> cluster, 
        std::string& primary_lock, uint64_t start_ts, uint64_t new_ttl);

class Txn;
class TwoPhaseCommitter;
class TTLManager {
public:
    TTLManager()
        : state(StateUninitizlized)
        , work_running(false)
    {}
    
    void run(std::shared_ptr<TwoPhaseCommitter> committer) {
        uint32_t expected = StateUninitizlized;
        if (!state.compare_exchange_strong(expected, 
                    StateRunning, 
                    std::memory_order_acquire, /* 后面访存指令 */
                    std::memory_order_relaxed)) {
            return ;
        }
        work_running = true;
        auto keep_alive_fn = [&]() {
            keep_alive(committer);
        };
        
        worker.run(keep_alive_fn); 
    }

    void close() {
        uint32_t expected = StateRunning;
        state.compare_exchange_strong(expected, StateClosed, std::memory_order_acq_rel);
        if (work_running) {
            worker.join();
            work_running = false;
        }
    }

    void keep_alive(std::shared_ptr<TwoPhaseCommitter> committer);

private:
    enum TTLManagerState {
        StateUninitizlized = 0,
        StateRunning,
        StateClosed
    };

    std::atomic<uint32_t> state;
    bool        work_running;
    Bthread     worker;
};

class TwoPhaseCommitter: public std::enable_shared_from_this<TwoPhaseCommitter> {
public:
    explicit TwoPhaseCommitter(Txn* txn, bool use_async_commit = false);

    int execute();

private:
    enum Action {
        TxnPwrite = 0,
        TxnCommit,
        TxnCleanUp
    };

    struct BatchKeys {
        RegionVerId              region_ver;
        std::vector<std::string> keys;
        bool                     is_primary;
        BatchKeys(const RegionVerId& region_ver, std::vector<std::string> keys, bool is_primary = false)
            : region_ver(region_ver)
            , keys(std::move(keys))
            , is_primary(is_primary)
        {}
    };

    void calculate_max_commit_ts();

    int pwrite_keys(BackOffer& bo, const std::vector<std::string>& keys) {
        return do_action_on_keys(bo, keys, TxnPwrite);
    }

    int commit_keys(BackOffer& bo, const std::vector<std::string>& keys) {
        return do_action_on_keys(bo, keys, TxnCommit);
    }

    int do_action_on_keys(BackOffer& bo, const std::vector<std::string>& keys, Action action);

    int do_action_on_batchs(BackOffer& bo, const std::vector<BatchKeys>& batchs, Action action);

    int pwrite_single_batch(BackOffer& bo, const BatchKeys& batch, Action action);

    int commit_single_batch(BackOffer& bo, const BatchKeys& batch, Action action);

private:
    friend class TTLManager;

    std::unordered_map<std::string, std::string> mutations;
    std::vector<std::string> keys;
    
    uint64_t start_ts  {0};

    bthread_mutex_t commit_mutex;
    uint64_t commit_ts {0};
    uint64_t min_commit_ts {0};
    uint64_t max_commit_ts {0};

    std::chrono::milliseconds start_time;

    std::shared_ptr<Cluster> cluster;

    std::unordered_map<uint64_t, int> region_txn_size;
    uint64_t txn_size;

    int lock_ttl = 0;

    std::string primary_lock;
    // primary_key写到kv store
    bool committed;
    
    bool use_async_commit {false};

    TTLManager  ttl_manager;

    Bthread     worker;
};

} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
