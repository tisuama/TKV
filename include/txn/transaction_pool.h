#pragma once
#include "common/common.h"

#include <atomic>

namespace TKV {
class TransactionPool {
public:
    TransactionPool(): _num_prepared_txn(0), _txn_count(0) {}

private:
    int64_t     _region_id;
    int64_t     _latest_active_txn_ts {0};
    bool        _use_ttl {false};

    // txn_id => txn hander
    ThreadSafeMap<uint64_t, SmartTransaction>  _txn_map;
    // txn_id => affected_rows
    DoubleBuffer<ThreadSafeMap<uint64_t, int>> _finished_txn_map; 

    TimeCost             _clean_finished_txn_cost;

    BthreadCond          _num_prepared_txn;
    std::atomic<int32_t> _txn_count;
    MetaWriter*          _meta_writer {nullptr};
};
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
