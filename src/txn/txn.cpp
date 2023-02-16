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

    MvccTxn txn(_start_ts, ctx->concurrency);
    SnapshotReader reader(_start_ts, ctx->snapshot);

    // 返回值
    uint64_t final_commit_ts = 0;
    std::vector<LockRef*> locks;
    // start pwrite
    for (auto m: _mutations) {
    }
}

void Pwriter::process_read(TxnContext* ctx) {
}

} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
