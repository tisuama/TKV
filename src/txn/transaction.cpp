#include "txn/transaction.h"
#include "txn/transaction_pool.h"
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
    _meta_cf = _txn_db->get_meta_info_handle();
    if (!_data_cf || !_meta_cf) {
        DB_WARNING("Transaction begin failed, data_cf: %p, meta_cf: %p", _data_cf, _meta_cf);
        return -1;
    }
    _txn_opt = txn_opt;
    if (_txn_opt.lock_timeout == -1) {
        _txn_opt.lock_timeout = FLAGS_rocks_transaction_lock_timeout_ms + 
            butil::fast_rand_less_than(FLAGS_rocks_transaction_lock_timeout_ms);
    }
    auto txn = _txn_db->begin_transaction(_write_opt, _txn_opt);
    if (txn == nullptr) {
        DB_WARNING("start transaction failed");
        return -1;
    }
    if (_pool != nullptr) {
        _use_ttl = _pool->use_ttl();
        _online_ttl_us = _pool->online_ttl_us();
    }
    last_active_time = butil::gettimeofday_us();
    _txn = txn;
    begin_time = last_active_time;
    if (_use_ttl) {
        _read_ttl_timestamp_us = last_active_time;
    }
    _in_process = true;
    _current_req_point_seq.insert(1);
    _snapshot = _txn_db->get_snapshot();
    return 0;
}

int Transaction::rollback() {
    BAIDU_SCOPED_LOCK(_txn_mutex);
    last_active_time = butil::gettimeofday_us();
    if (_is_finished) {
        DB_WARNING("txn_id: %lu is finished, rollback now", _txn_id);
        return 0;
    }
    auto s = _txn->Rollback();
    if (s.ok()) {
        // 设置rollbacked并结束
        _is_finished = true;
        _is_rollbacked = true;
        return 0;
    }
    DB_FATAL("txn_id: %lu, rollback failed, errmsg: %s", _txn_id, s.ToString().c_str());
    return -1;
}

int Transaction::commit() {
    BAIDU_SCOPED_LOCK(_txn_mutex);
    last_active_time = butil::gettimeofday_us();
    if (_is_rollbacked) {
        DB_WARNING("TransactionWarn: commit a rollbacked txn, txn_id: %lu", _txn_id);
        return -1;
    }
    if (_is_finished) {
        DB_WARNING("TransactionWarn: commit a finished txn, txn_id: %lu", _txn_id);
        return -1;
    }
    if (_txn->GetName().size() != 0 && !_is_prepared) {
        DB_FATAL("TransactionError: commit a un-prepared txn, txn_id: %lu", _txn_id);
        return -1;
    }
    auto s = _txn->Commit();
    if (s.ok()) {
        _is_finished = true;
        return 0;
    }
    return -1;
}

void Transaction::rollback_current_request() {
    BAIDU_SCOPED_LOCK(_txn_mutex);
    last_active_time = butil::gettimeofday_us();
    if (_current_req_point_seq.size() == 1) {
        DB_WARNING("txn_id: %lu, seq_id: %d no need rollback", _txn_id, _seq_id);
        return ;
    }
    int first_seq_id = *_current_req_point_seq.begin(); 
    for (auto it  = _current_req_point_seq.rbegin(); 
               it != _current_req_point_seq.rend(); it++) {
        int seq_id = *it;
        if (first_seq_id == seq_id) {
            break;
        }
        {
            BAIDU_SCOPED_LOCK(_cache_kv_mutex);
            _cache_kv_map.erase(seq_id);
        }
        if (!_save_point_seq.empty() && _save_point_seq.top() == seq_id) {
            num_increase_rows = _save_point_increase_rows.top();
            _save_point_seq.pop();
            _txn->RollbackToSavePoint();
            DB_WARNING("txn_name: %s txn_id: %lu, first_seq_id: %d rollback cmd seq_id: %d, num_increase_rows: %ld",
                    _txn->GetName().c_str(), first_seq_id, seq_id, num_increase_rows);
        }
    }
    _seq_id = first_seq_id;
    _store_req.Clear();
    _is_applying = false;
    _current_req_point_seq.clear();
    _current_req_point_seq.insert(_seq_id);
}

int Transaction::prepare() {
    BAIDU_SCOPED_LOCK(_txn_mutex);
    if (_is_prepared) {
        return 0;
    }
    if (_is_rollbacked) {
        DB_WARNING("txn_name: %s txn_id: %lu prepared a rollbacked txn", _txn->GetName().c_str(), _txn_id);
        return -1;
    }
    last_active_time = butil::gettimeofday_us();
    auto s = _txn->Prepare();
    if (s.ok()) {
        _is_prepared = true;
        _prepare_time_us = butil::gettimeofday_us();
        return 0;
    }
    DB_FATAL("txn_id: %lu, rollback failed, errmsg: %s", _txn_id, s.ToString().c_str());
    return -1;
}

void Transaction::rollback_to_point(int seq_id) {
    BAIDU_SCOPED_LOCK(_txn_mutex);
    last_active_time = butil::gettimeofday_us();
    // cached_plan中的东西需要清除，因为cache_plan需要发送给follow
    if (_save_point_seq.empty()) {
        DB_WARNING("txn_id: %s, seq_id: %d, top_seq empty", _txn->GetName().c_str(), seq_id);
    }
    _need_rollback_seq.insert(seq_id);
    {
        BAIDU_SCOPED_LOCK(_cache_kv_mutex);
        _cache_kv_map.erase(seq_id);
    }
    if (!_save_point_seq.empty() && _save_point_seq.top() == seq_id) {
        num_increase_rows = _save_point_increase_rows.top();
        _save_point_seq.pop();
        _save_point_increase_rows.pop();
        _txn->RollbackToSavePoint();
    }
}

int  Transaction::set_save_point() {
    BAIDU_SCOPED_LOCK(_txn_mutex);
    last_active_time = butil::gettimeofday_us();
    if (_save_point_seq.empty() || _save_point_seq.top() < _seq_id) {
        _txn->SetSavePoint();
        _save_point_seq.push(_seq_id);
        _save_point_increase_rows.push(num_increase_rows);
        _current_req_point_seq.insert(_seq_id);
    }
    return _seq_id;
}

void Transaction::push_cmd_to_cache(int seq_id, pb::CachePlan plan_item) {
    BAIDU_SCOPED_LOCK(_txn_mutex);
    _seq_id = seq_id;
    if (_cache_kv_map.count(seq_id) > 0) {
        return ;
    }
    if (plan_item.op_type() != pb::OP_BEGIN) {
        _has_dml_executed = true;
    }
    _cache_kv_map.insert({seq_id, plan_item});
}

void Transaction::add_kv_op(const pb::KvOp& cached_kv_op) {
    pb::KvOp* kv_op = _store_req.add_kv_ops();
    kv_op->CopyFrom(cached_kv_op);
}

int Transaction::put_meta_info(const std::string& key, const std::string& value) {
    auto s = _txn->Put(_meta_cf, rocksdb::Slice(key), rocksdb::Slice(value));
    if (!s.ok()) {
        DB_FATAL("txn_id: %lu, put meta info failed, errmsg: %s", _txn_id, s.ToString().c_str());
        return -1;
    }
    return 0;
}

int Transaction::remove_meta_info(const std::string& key) {
    auto s = _txn->Delete(_meta_cf, rocksdb::Slice(key));
    if (!s.ok()) {
        DB_FATAL("txn_id: %lu, put meta info failed, errmsg: %s", _txn_id, s.ToString().c_str());
        return -1;
    }
    return 0;
}
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
