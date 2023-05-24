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

uint64_t Pwriter::pwrite(TxnContext* ctx, const pb::Mutation& m) {
    // 等同于Get操作
    bool should_not_write = (m.op() == pb::OP_CHECK_NOT_EXIST);

    bool should_not_exist = (m.op() == pb:: OP_INSERT || m.op() == pb::OP_CHECK_NOT_EXIST);
    if (ctx->txn_id == PessimisticTxn && should_not_write) {
        // ERROR
    }
    if (should_not_exist) {
        // 在需要保证Linearizability和Si隔离级别时 Update max_ts
        ctx->concurrency->update_max_ts(_start_ts);
    }

    LockInfo lock;
    auto r = ctx->reader->load_lock(&m.key(), lock);
    if (r) {
        uint64_t min_commit_ts = 0;
        auto lock_status = check_lock(lock_info, min_commit_ts, ctx->txn_kind);
        if (lock_status == LockStatus::LOCKED) {
            // OK, Key is locked
            return min_commit_ts;
        }
    }

    if (should_not_write) {
        // CheckNotExist
        ctx->concurrency->update_max_ts(_start_ts);
        uint64_t min_commit_ts = 0;
        if (need_min_commit_ts(ctx)) {
            min_commit_ts = max(_min_commit_ts, _start_ts + 1);
        }
        return min_commit_ts;
    }

    bool is_new_lock = true; 
    uint64_t final_commit_ts = write_lock(lock_status, m, ctx, is_new_lock);
    return final_commit_ts;
}

// Check the key is locked at any timestamp
LockStatus Pwriter::check_lock(LockInfo& lock, uint64_t& min_commit_ts, TransactionKind txn_kind) {
    if (lock.lock_version() != _start_ts) {
        if (txn_kind == PessimisticTxn) {
            // 悲观事务模式默认是DoPessimiticCheck
            return LockStatus::LOCKED_ERROR;
        }
        // key已经上锁
        return LockStatus::LOCKED;
    }
    if (lock.lock_type() == pb::PESSIMISTIC_LOCK) {
        // Lock属于悲观锁，且被当前事务拥有，更新信息
        _lock_ttl = std::max(_lock_ttl, lock.ttl());
        _min_commit_ts = std::max(_min_commit_ts, lock.min_commit_ts());
        return LockStatus::PESSIMISTIC_LOCKED;
    }
    // 乐观锁，重复Command，已经上锁
    uint64_t min_commit_ts = 0;
    if (lock.use_async_commit()) {
        min_commit_ts = lock.min_commit_ts();
    }
    return LockStatus::LOCKED;
}

bool Pwriter::skip_constraint_check(TransactionKind txn_kind) {
    if (txn_kind == OptimisticTxn) {
        return true;
    }
    // 悲观事务，检查pessimistic_action，默认设置为DoPessimicticCheck
    return true;
} 

bool Pwriter::check_for_newer_version(TxnContext* ctx, uint64_t& commit_ts, Write& write_record) {
    uint64_t seek_ts = UINT64_MAX;
    while (r = ctx->reader->seek_write(key, seek_ts, commit_ts, write_record)) {
        // commit_ts = _start_ts => OK
        // 当前key存在一个commit_ts = _start_ts的rollback记录
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
        return true;
    }
    if (seek_ts == UINT64_MAX) {
        // TODO: last_change_ts, versions_to_last_change
    }
    return true;
}

bool Pwriter::need_min_commit_ts(TxnContext* ctx) {
    return ctx->commit_kind == CommitAsync ||
        ctx->commit_kind == CommitOnePc;
}

bool Pwriter::try_one_pc(TxnContext* ctx) {
    return ctx->commit_kind == CommitOnePc;
}

uint64_t Pwriter::write_lock(LockStatus lock_status, const Mutation& m, TxnContext* ctx, bool is_new_lock) {
    // TODO: 短value优化
    bool try_one_pc = try_one_pc();

    LockInfo write_lock;
    write_lock.set_primary_lock(_primary_lock);
    write_lock.set_lock_version(_start_ts);
    write_lock.set_key(m.key());
    write_lock.set_lock_ttl(_lock_ttl);
    write_lock.set_lock_type(m.op());
    write_lock.set_for_update_ts(_for_udpate_ts);
    write_lock.set_txn_size(_txn_size);
    write_lock.set_min_commit_ts(_min_commit_ts);

    if (_secondaries.size() > 0) {
        write_lock.set_use_async_commit(true);
        for (auto& s: _secondaries) {
            write_lock.add_secondary(s);
        }
    }

    uint64_t final_commit_ts = 0;
    if (write_lock.use_async_commit() || try_one_pc) {
        final_commit_ts = async_commit_ts(_primary_lock, write_lock, ctx);
    }

    if (try_one_pc) {
        ctx->txn->put_lock_for_1pc(key, write_lock, lock_status == LockStatus::PESSIMISTIC_LOCKED);
    } else {
        ctx->txn->put_lock(key, write_lock, is_new_lock);
    }
}

uint64_t Pwriter::async_commit_ts(const std::string& key, TxnContext* ctx) {
    // TODO: 计算min_commit_ts
    return 0;
}
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
