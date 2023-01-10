#pragma once
#include <string>
#include <memory>
#include <unordered_map>
#include <time>
#include <cmath>

namespace TKV {
class TTLManager {
public:
    TTLManager()
        : state(StateUninitizlized)
        , work_running(false)
    {}
    
    void run(std::shared_ptr<TwoPhaseCommitter> committer) {
        uint32_t expected = StateUninitizlized;
        if (!state.compare_exchanged_strong(expected, 
                    StateRunning, 
                    std::memory_order_acquire, 
                    std::memory_order_relaxed)) {
            return ;
        }
        work_running = true;
    }

    void close() {
        // close
    }


private:
    enum TTLManagerState {
        StateUninitizlized = 0,
        StateRunning,
        StateClosed
    };

    std::atomic<uint32_t> state;
    bool work_running;

};

class TwoPhaseCommitter: public std::enable_shared_from_this<TwoPhaseCommitter> {
public:
    explicit TwoPhaseCommitter(Txn* txn, bool use_async_commit = false);

    void execute();

private:

private:
    friend class TTLManager;

    std::unordered_map<std::string, std::string> mutations;
    std::vector<std::string> keys;
    
    uint64_t start_ts  {0};

    bthread_mutex_t commit_mutex;
    uint64_t commit_ts {0};
    uint64_t min_commit_ts {0};
    uint64_t max_commit_ts {0};

    uint64_t start_ts;

    std::shared<Cluster> cluster;

    std::unordered_map<uint64_t, int> region_txn_size;
    uint64_t txn_size;

    int lock_ttl = 0;

    std::string primary_lock;
    // primary_key写到kv store
    bool committed;
    
    bool use_async_commit {false};

    TTLManager  ttl_manager;
};
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
