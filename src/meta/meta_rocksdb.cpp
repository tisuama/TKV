#include "meta/meta_rocksdb.h"
#include "common/log.h"

namespace TKV {
DECLARE_string(db_path);

int MetaRocksdb::init() {
    _rocksdb = RocksWrapper::get_instance();
    if (!_rocksdb) {
        DB_FATAL("create rocksdb handle failed, exit now");
        return -1;
    }     
    int ret = _rocksdb->init(FLAGS_db_path);
    if (ret) {
        DB_FATAL("rocksdb init failed, exit now");
        return -1;
    }
    _handle = _rocksdb->get_meta_info_handle();
    DB_WARNING("rocksdb init sucess, db_path: %s", FLAGS_db_path.c_str());
    return 0;
}

int MetaRocksdb::put_meta_info(const std::string& key, const std::string& value) {
    rocksdb::WriteOptions write_option;
    write_option.disableWAL = true;
    auto s = _rocksdb->put(write_option, _handle, rocksdb::Slice(key), rocksdb::Slice(value));
    if (!s.ok()) {
        DB_WARNING("put rocksdb failed, err_msg: %s, key: %s, value: %s", 
                s.ToString().data(), key.data(), value.data());
        return -1;
    }
    return 0;
}

int MetaRocksdb::put_meta_info(const std::vector<std::string>& keys,
                  const std::vector<std::string>& values) {
    if (keys.size() != values.size()) {
        DB_WARNING("key-value size doesn't match, key size: %ld, value size: %ld", 
                keys.size(), values.size());
        return -1;
    }
    rocksdb::WriteOptions write_option;
    write_option.disableWAL = true;
    rocksdb::WriteBatch batch;
    for (size_t i = 0; i < keys.size(); i++) {
        batch.Put(_handle, keys[i], values[i]);
    }
    auto s = _rocksdb->write(write_option, &batch);
    if (!s.ok()) {
        DB_WARNING("put rocksdb failed, err_msg: %s", s.ToString().data());
        return -1;
    }
    return 0;
}

int MetaRocksdb::get_meta_info(const std::string& key, std::string* value) {
    rocksdb::ReadOptions read_option;
    auto s = _rocksdb->get(read_option, _handle, rocksdb::Slice(key), value);
    if (!s.ok()) {
        return -1;
    }
    return 0;
}

int MetaRocksdb::delete_meta_info(const std::vector<std::string>& keys) {
    rocksdb::WriteOptions write_option;
    write_option.disableWAL = true;
    rocksdb::WriteBatch batch;
    for (auto& key: keys) {
        batch.Delete(_handle, key);
    }
    auto s = _rocksdb->write(write_option, &batch);
    if (!s.ok()) {
        DB_WARNING("delete write batch to rocksdb failed, err_msg: %s", 
                s.ToString().data());
        return -1;
    }
    return 0;
}

int MetaRocksdb::write_meta_info(const std::vector<std::string>& put_keys,
                    const std::vector<std::string>& put_values, 
                    const std::vector<std::string>& delete_keys) {
    rocksdb::WriteOptions write_option;
    write_option.disableWAL = true;
    if (put_keys.size() != put_values.size()) {
        DB_FATAL("put_keys size doesn't match put_values, put_keys size: %ld, put_values size: %ld",
                put_keys.size(), put_values.size());
        return -1;
    }
    rocksdb::WriteBatch batch;
    for (size_t i = 0; i < put_keys.size(); i++) {
        batch.Put(_handle, put_keys[i], put_values[i]);
    }
    for (auto& key : delete_keys) {
        batch.Delete(_handle, key);
    }
    auto s = _rocksdb->write(write_option, &batch);
    if (!s.ok()) {
        DB_WARNING("write batch to rocksdb failed, err_msg: %s",
                s.ToString().data());
        return -1;
    }
    return 0;
}
} // namespace TKV 
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
