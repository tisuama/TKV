#include <vector>
#include <unordered_map>
#include <memory>

#include "common/latch.h"
#pragma once

namespace TKV {
/* 
a) 事务操作   
1. src/service/kv.rs:1869
macro_rules! txn_command_future
sched_txn_command

2. The entry point of the storage scheduler.
src/storage/mod.rs:1416
pub fn sched_txn_command<T: StorageCallbackType>

3. scheduler.rs:531
schedule_command
  execute(task)
    process(snapshot, task)
        process_write(snapshot, task, &mut statistics)

4. process_write:490
command/prewrite.rs
    pwrite(&mut txn, &mut reader, context.extra_op)

5. process_write:32
actions/prewrite.rs
    pwrite(txn, reader, txn_props, mutation, secondary_keys..)

b) 快照读
1. storage/mod.rs:588
get(ctx, key, start_ts)

2. storage/mod.rs:2858
prepare_snapshot_ctx
*/

// 事务的内存数据结构

enum Action {
    Pwrite = 0, 
    PwritePessimistic,
    Rollback,
    CleanUp,
    Commit,
    ActionNone
};

struct TxnContext {
    Action          action  {ActionNone};
    // DeadLine        deadline;
    TxnLock*        lock    {NULL};
    pb::StoreReq*   request {NULL};
    pb::StoreRes*   response{NULL};
    google::protobuf::Closure* done{NULL};
};

class Txn {
public:
    Txn(TxnContext*  txn_ctx, 
            uint64_t start_ts, 
            uint64_t lock_ttl, 
            std::string primary)
        : _txn_ctx(txn_ctx)
        , _start_ts(start_ts)
        , _lock_ttl(lock_ttl)
        , _primary_lock(primary)
    {}

    void pwrite(const pb::Mutation& m, const std::string& primary);

    void commit(const std::string& key, uint64_t commit_ts);

private:
    TxnContext*              _txn_ctx           {NULL};
    std::vector<std::string>                    _keys;
    std::unordered_map<std::string, std::string> _mutations;
    // start_ts可以作为txn_id使用
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
