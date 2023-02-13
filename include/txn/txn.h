#include <vector>
#include <unordered_map>
#include <memory>

#include "common/latch.h"
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

class Txn;
struct TxnContext {
    Action              action    {ActionNone};
    ErrorInner          errcode   {Success};   


    // Region
    uint64_t            region_id;
    uint64_t            term;

    RocksWrapper*       db        {NULL};
    rocksdb::Snapshot*  snapshot  {NULL}; 
    
    // DeadLine        deadline;
    TxnLock*            lock      {NULL};

    // Client request/response
    pb::StoreReq*       req       {NULL};
    pb::StoreRes*       res       {NULL};

    // RPC Closure
    google::protobuf::Closure*    done {NULL};

    ~TxnContext() {
        if (snapshot) {
            db->release_snapshot(snapshot); 
            snapshot = NULL;
        }
        if (lock) {
            delete lock;
        }
        if (txn) {
            delete txn;
        }
    }
};

class Txn {
public:
    Txn(TxnContext*  txn_ctx)
        : _txn_ctx(txn_ctx)
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
