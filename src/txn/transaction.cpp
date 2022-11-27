#include "txn/transaction.h"
#include "store/meta_writer.h"

namespace TKV {
DECLARE_int64(exec_1pc_in_fsm_timeout_ms);
DECLARE_int32(rocks_transaction_lock_timeout_ms);

int Transaction::begin(const Transaction::TxnOptions& txn_opt) {
    rocksdb::TransactionOptions rocks_txn_opt;
    if (txn_opt.dml_1pc && txn_opt.in_fsm) {
        rocks_txn_opt.lock_timeout = FLAGS_exec_1pc_in_fsm_timeout_ms;
    } else {
        rocks_txn_opt.lock_timeout = txn_opt.lock_timeout;
    }
    return begin(rocks_txn_opt);
}

int Transaction::begin(const rocksdb::TransactionOptions& txn_opt) {
    if ((_txn_db = RocksWrapper::get_instance()) == nullptr) {
        return -1;
    }
    _data_cf = _txn_db->get_data_handle();
    _meta_cf = _txn_db->get_meta_handle();
    if (!_data_cf || !_meta_cf) {
        DB_WARNING("Transaction begin failed, data_cf: %p, meta_cf: %p", _data_cf, _meta_cf);
        return -1;
    }
    _txn_opt = txn_opt;
    if (_txn_opt.lock_timeout == -1) {
        _txn_opt.lock_timeout = FLAGS_rocks_transaction_lock_timout_ms + 
            butil::fast_rand_less_than(FLAGS_rocks_transaction_lock_timout_ms);
    }
    auto txn = _txn_db->begin_transaction(_write_opt, _txn_opt);
    if (txn == nullptr) {
        DB_WARNING("start transaction failed");
        return -1;
    }
    if (_pool != nullptr) {
        _use_ttl = _pool->use_ttl();
        _online_ttl_base_expire_time_us = _pool->online_ttl_base_expire_time_us();
    }
    _last_active_time = butil::gettimeofday_us();
    _txn = txn;
    begin_time = _last_active_time;
    if (_use_ttl) {
        _read_ttl_timestamp_us = _last_active_time;
    }
    _in_process = true;
    _current_req_point_req.insert(1);
    _snapshot = _txn_db->get_snapshot();
    return 0;
}

rocksdb::status Transaction::rollback() {
    BAIDU_SCOPED_LOCK(_txn_mutex);
    last_active_time = butil::gettimeofday_us();
    if (_is_finished) {
        DB_WARNING("txn_id: %lu is finished, rollback now", _txn_id);
        return rocksdb::Status();
    }
    auto s = _txn->Rollback();
    if (s.ok()) {
        _is_finished = true;
        _is_rollbacked = true;
    }
    return s;
}
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
