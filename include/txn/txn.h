#pragma once
#include <vector>
#include <unordered_map>
#include <memory>
#include "txn/latch.h"
#include "proto/store.pb.h"
#include "engine/rocks_wrapper.h"

namespace TKV {

// 事务的内存数据结构
enum Action {
    Pwrite = 0, 
    PwritePessimistic,
    Rollback,
    CleanUp,
    Commit,
    ActionNone
};

enum PessimisticLockKind {
    PessimisticLockInmemory = 0,
    PessimisticLockPipeline,
    PessimisticLockSync,
};

enum TransactionKind {
    // OptimisticTxn(bool) => bool is skip_constraint_check
    OptimisticTxn = 0,
    // PessimisticTxn(ts)  => ts: for_update_ts
    PessimisticTxn
};

enum CommitKind {
    CommitOnePc = 0,
    CommitAsync,
    CommitTwoPc
};

enum class LockStatus {
    LOCKED = 0,
    PESSIMISTIC_LOCKED,
    // Error
    NONE_LOCKED,
    LOCKED_ERROR,
};

struct TxnContext {
    Action              action  {ActionNone};
    ErrorInner          errcode {InnerSuccess};

    // All kinds
    TransactionKind     txn_kind    {OptimisticTxn};
    PessimisticLockKind pessi_mode  {PessimisticLockSync};
    CommitKind          commit_kind {CommitTwoPc};

    // Region
    int64_t            region_id;
    int64_t            term;

    // DeadLine        deadline
    TxnLock*            lock        {NULL};
    ConcurrencyManager* concurrency {NULL};
    MvccTxn*            txn         {NULL};
    SnapshotReader*     reader      {NULL};

    // DB
    RocksSnapshot*      snapshot    {NULL};

    // Client request/response
    pb::StoreReq*       req      {NULL};
    pb::StoreRes*       res      {NULL};

    // RPC Closure
    google::protobuf::Closure*    done {NULL};

    ~TxnContext() {
        // TODO: Release resource
        if (lock) {
            delete lock;
        }
    }
};

class Pwriter {
public:

    // Pwriter
    Pwriter(uint64_t start_ts, uint64_t lock_ttl, uint64_t txn_size, 
        uint64_t min_commit_ts, uint64_t max_commit_ts)
        : _start_ts(start_ts)
        , _lock_ttl(lock_ttl)
        , _txn_size(txn_size)
        , _min_commit_ts(min_commit_ts)
        , _max_commit_ts(max_commit_ts)\
    {}

    ~Pwriter() {}

    void set_primary_lock(const std::string& primary) {
        _primary_lock = primary;
    }

    void add_mutation(const pb::Mutation& m);

    // Async commit for primary row
    void add_secondary(const std::string& key);

    void process_write(TxnContext* ctx);

    void process_read(TxnContext* ctx);

    // single pwrite
    uint64_t pwrite(TxnContext* ctx, const pb::Mutation& m);

private:
    LockStatus check_lock(LockInfo& lock, 
            uint64_t& min_commit_ts, TransactionKind txn_kind);

    bool skip_constraint_check(TransactionKind txn_kind) const ;

    bool check_for_newer_version(TxnContext* ctx, uint64_t& commit_ts, Write& write_record);
    
    bool need_min_commit_ts(TxnContext* ctx);
    
    bool try_one_pc(TxnContext* ctx);
    
    uint64_t write_lock(LockStatus lock_status, const Mutation& m, TxnContext* ctx, bool is_new_lock);

    uint64_t async_commit_ts(const std::string& key, TxnContext* ctx);

    std::vector<std::string>  _keys;
    std::vector<pb::Mutation> _mutations;
    std::vector<std::string>  _secondaries;

    // start_ts可以作为txn_id使用
    uint64_t                  _start_ts;
    uint64_t                  _for_update_ts;
    uint64_t                  _lock_ttl;
    std::string               _primary_lock;
    bool                      _committed;
    uint64_t                  _commit_ts        {0};
    bool                      _use_async_commit {false};
    uint64_t                  _txn_size         {0};
    uint64_t                  _min_commit_ts;
    uint64_t                  _max_commit_ts;
};

} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
