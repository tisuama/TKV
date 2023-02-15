#include <vector>
#include <unordered_map>
#include <memory>

#include "txn/latch.h"
#include "proto/store.pb.h"
#include "engine/rocks_wrapper.h"
#pragma once

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

enum PessimisticLockMode {
    PessimisticLockInmemory = 0,
    PessimisticLockPipeline,
    PessimisticLockSync,
};

enum TxnKind {
    OptimisticTxn = 0,
    PessimisticTxn
};

struct TxnContext {
    Action              action    {ActionNone};
    ErrorInner          errcode   {InnerSuccess};
    PessimisticLockMode mode      {PessimisticLockSync};

    // Region
    int64_t            region_id;
    int64_t            term;

    // DeadLine        deadline
    //
    TxnLock*            lock      {NULL};

    // Client request/response
    pb::StoreReq*       req       {NULL};
    pb::StoreRes*       res       {NULL};

    // RPC Closure
    google::protobuf::Closure*    done {NULL};

    ~TxnContext() {
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
    {
    }

    ~Pwriter() {
    }

    void add_mutation(const pb::Mutation& m);

    void add_secondary(const std::string& key);

    void process_write(TxnContext* ctx);

    void process_read(TxnContext* ctx);

private:

    std::vector<std::string>                     _keys;
    std::vector<pb::Mutation>                    _mutations;
    std::vector<std::string>                     _secondaries;

    // start_ts可以作为txn_id使用
    TxnKind                  _txn_kind         {OptimisticTxn};
    uint64_t                 _start_ts;
    uint64_t                 _lock_ttl;
    std::string              _primary_lock;
    bool                     _committed;
    uint64_t                 _commit_ts        {0};
    bool                     _use_async_commit {false};
    uint64_t                 _txn_size         {0};
    uint64_t                 _min_commit_ts;
    uint64_t                 _max_commit_ts;
};
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
