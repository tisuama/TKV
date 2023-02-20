#include "txn/txn.h"
#include "txn/mvcc.h"
#include "txn/reader.h"

namespace TKV {
void Pwriter::add_mutation(const pb::Mutation& m) {
    _mutations.push_back(m);
    _keys.push_back(m.key());
}

void Pwriter::add_secondary(const std::string& key) {
    _secondaries.push_back(key);
}

void Pwriter::process_write(TxnContext* ctx) {
    // TODO: check_max_ts_synced

    auto txn = MvccTxn(_start_ts, ctx->concurrency);
    auto reader = SnapshotReader(_start_ts, ctx->snapshot);
    ctx->txn = txn;
    ctx->reader = reader;

    // 返回值
    uint64_t final_commit_ts = 0;
    std::vector<MemLock*> locks;

    bool async_commit_pk = false;
    if (_secondaries.size() > 0) {
        async_commit_pk = true;
    }

    // start pwrite
    for (auto m: _mutations) {
    }
}

void Pwriter::process_read(TxnContext* ctx) {
}

/* return min_commit_ts */
uint64_t Pwriter::pwrite(TxnContext* ctx, const pb::Mutation& m) {
    bool should_not_write = (m.op() == pb::OP_CHECK_NOT_EXIST);
    bool should_not_exist = (m.op() == pb:: OP_INSERT || m.op() == pb::OP_CHECK_NOT_EXIST);
    if (ctx->txn_id == PessimisticTxn && should_not_write) {
        // ERROR
    }
    if (should_not_exist) {
        // update max ts
        ctx->concurrency->update_max_ts(_start_ts);
    }

    LockInfo lock;
    auto r = ctx->reader->load_lock(&m.key(), lock);
    if (r) {
        uint64_t min_commit_ts = 0;
        auto lock_status = check_lock(lock_info, min_commit_ts, ctx->txn_kind);
        // 乐观锁，且已经被上锁
        if (lock_status == LOCKED) {
            return min_commit_ts;
        }
    }
}

// Check the key is locked at any timestamp
LockStatus Pwriter::check_lock(LockInfo& lock, uint64_t& min_commit_ts, TransactionKind txn_kind) {
    if (lock.lock_version() != _start_ts) {
        if (txn_kind == PessimisticTxn) {
            // 悲观事务模式下，Lock的start_ts对不上，应该abort 
            return LOCKED_ERROR;
        }
        // key已经上锁
        return LOCKED;
    }
    if (lock.lock_type() == pb::PESSIMISTIC_LOCK) {
        // Lock属于悲观锁，且被当前事务拥有，更新信息
        _lock_ttl = std::max(_lock_ttl, lock.ttl());
        _min_commit_ts = std::max(_min_commit_ts, lock.min_commit_ts());
        return PESSIMISTIC_LOCKED;
    }
    // 乐观锁，重复Command，已经上锁
    uint64_t min_commit_ts = 0;
    if (lock.use_async_commit()) {
        min_commit_ts = lock.min_commit_ts();
    }
    return LOCKED;
}

bool Pwriter::skip_constraint_check(TransactionKind txn_kind) {
    if (txn_kind == OptimisticTxn) {
        return true;
    }
    // 悲观事务， 检查pessimistic_action，默认设置为DoPessimicticCheck
    return true;
} 

bool Pwriter::check_for_newer_version(TxnContext* ctx, uint64_t& commit_ts, Write& write_record) {
    uint64_t seek_ts = UINT64_MAX;
    while (r = ctx->reader->seek_write(key, seek_ts, commit_ts, write_record)) {
        if (commit_ts == _start_ts && write_record.write_type() == pb::ROLLBACK) {
            // Error: SelfRolledBack
            return false;
        }
        if (ctx->txn_kind == OptimisticTxn && commit_ts > _start_ts) {
            // Error: Optimistic
            return false;
        }
        // 悲观事务
        if (ctx->txn_kind == PessimisticTxn) {
            // DoConstraintCheck：不需要悲观锁但是需要constraint_check
            if (commit_ts > _start_ts /* && _pessimistic_action == DoConstraintCheck */) {
                // Error: LazyUniquenessCheck
                return false;
            }
            if (commit_ts > _for_udpate_ts) {
                if (write_record.write_type() == pb::ROLLBACK) {
                    seek_ts = commit_ts - 1;  
                    continue;
                }
                // write conflict for pessimistic txn, pessimistic lock 
                // must be lost for corresponding row key
                // Error: Pessimistic Lock Not Found
                return false;
            }
        }
        // TODO: check_data_constraint
        return true;
    }
    if (seek_ts == UINT64_MAX) {
        // 没有该Key的相关write record
    }
    return true;
}

} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
