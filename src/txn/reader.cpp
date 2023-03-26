#include "txn/reader.h"

namespace TKV {
rocksdb::Status RocksSnapshot::get_cf(KV_CF cf, const std::string& key, std::string* val) {
    auto handle = _db->get_handle(cf);
    rocksdb::ReadOption read_opt;
    // Read from snapshot
    read_opt.snapshot = _snapshot;
    read_opt.fill_cache = true;
    auto s = _db->get(read_opt, handle, rocksdb::Slice(key.data(), key.length()), val);
    return s;
}

bool MvccReader::load_lock(const std::string& key, LockInfo& lock) {
    std::string val;
    auto s = _snapshot->get_cf(CF_LOCK, key, &val);
    if (s == status()::NotFound()) {
        return false;
    }
    if (!lock.ParseFromString(val)) {
        CHECK("parse failed" == 0);
    }
    return true;
}

// (comit_ts, write_record) => seek_ts前的一条commit记录
bool MvccReader::seek_write(const std::string& key, uint64_t seek_ts, 
            uint64_t& commit_ts, Write& write_record) {
    return true;
}
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
