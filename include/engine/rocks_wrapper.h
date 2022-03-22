#pragma once
#include <string>
#include <rocksdb/db.h>
#include <rocksdb/slice.h>
#include <rocksdb/convenience.h>
#include <rocksdb/cache.h>
#include <rocksdb/listener.h>
#include <rocksdb/options.h>
#include <rocksdb/status.h>
#include <rocksdb/slice_transform.h>
#include <rocksdb/utilities/transaction.h>
#include <rocksdb/utilities/transaction_db.h>



namespace TKV {
using rocksdb::Status;
class RocksWrapper {
public:
    static const std::string RAFT_LOG_CF {"raft_log"};
    static const std::string DATA_CF {"data"};
    static const std::string METAINFO_CF {"meta_info"};
    static std::atomic<int64_t> raft_cf_remove_range_count;
    static std::atomic<int64_t> data_cf_remove_range_count;
    static std::atomic<int64_t> meta_cf_remove_range_count;
    
    virtual ~RocksWrapper();
    static RocksWrapper* get_instance() {
        static RocksWrapper _instance;
        return &_instance;
    }
    int32_t init(const std::string& path);
    Status write(const rocksdb::WriteOptions& options, rocksdb::WriteBatch* updates) {
        return _db->Write(options, updates);
    }
    Status write(const rocksdb::WriteOptions& options, 
                    rocksdb::ColumnFamilyHandle* column_family,
                    const std::vector<std::string>& keys,
                    const std::vector<std::string>& values) {
        rocksdb::WriteBatch batch;
        for (size_t i = 0; i < keys.size(); i++) {
            batch.Put(column_family, keys[i], values[i]);
        }
        return _db->Write(options, &batch);
    }
    Status get(const rocksdb::ReadOptions& options, 
                rocksdb::ColumnFamilyHandle* column_family,
                const rocksdb::Slice& key,
                std::string* value) {
        return _db->Get(options, column_family, key, value);
    }
    Status put(const rocksdb::WriteOptions& options, 
                rocksdb::ColumnFamilyHandle* column_family, 
                const rocksdb::Slice& key,
                const rocksdb::Slice& value) {
        return _db->Put(options, column_family, key, value);
    }
    Status compact_range(const rocksdb::CompactRangeOptions& options,
                            rocksdb::ColumnFamilyHandle* column_family,
                            const rocksdb::Slice* begin,
                            const rocksdb::Slice* end) {
        return _db->CompactRange(options, column_family, begin, end);
    }
    Status flush(const rocksdb::FlushOptions& options, 
                    rocksdb::ColumnFamilyHandle* column_family) {
        return _db->Flush(options, column_family);
    }
    Status remove(const rocksdb::WriteOptions& options,
                    rocksdb::ColumnFamilyHandle* column_family,
                    const rocksdb::Slice& key)  {
        return _db->Delete(options, column_family, key);
    }
    Status remove_range(const rocksdb::WriteOptions& options,
                        rocksdb::ColumnFamilyHandle* column_family,
                        const rocksdb::Slice& begin, 
                        const rocksdb::Slice& end,
                        bool delete_files_in_range);
    rocksdb::Iterator* new_iterator(const rocksdb::ReadOptions& options,
                                    rocksdb::ColumnFamilyHandle* column_family) {
        return _db->NewIterator(options, column_family);
    }
    rocksdb::Iterator* new_iterator(const rocksdb::ReadOptions& options,
                                    const std::string cf) {
        if (_column_families.count(cf) == 0) {
            return nullptr;
        }
        return _db->NewIterator(options, _column_families[cf]);
    }
    Status ingest_external_file(rocksdb::ColumnFamilyHandle* column_family,
                                const std::vector<std::string>& external_file,
                                const rocksdb::IngestExternalFileOptions& options) {
        return _db->IngestExternalFile(column_family, external_file, options);
    }

    rocksdb::ColumnFamilyHandle* get_raft_log_hande();
    rocksdb::ColumnFamilyHandle* get_data_handle();
    rocksdb::ColumnFamilyHandle* get_meta_info_handle();
    rocksdb::DB* get_db() {
        return _db;
    }

    int32_t create_column_family(const std::string& cf_name);
    int32_t delete_column_family(const std::string& cf_name);

    rocksdb::Options get_options(rocksdb::ColumnFamilyHandle* column_family) {
        return _db->GetOptions(column_family);
    }
    rocksdb::DBOptions get_db_options() {
        return _db->GetDBOptions();
    }
    rocksdb::Cache* get_cache() {
        return _cache;
    }
    const rocksdb::Snapshot* get_snapshot() {
        return _db->GetSnapshot();
    }
    void releas_snapshot(const rocksdb::Snapshot* snapshot) {
        _db->ReleaseSnapshot(snapshot);
    }
    void close() {
        delete _db;
    }
    void set_flush_file_number(const std::string& cf_name, uint64_t file_num) {
        if (cf_name == DATA_CF) {
            _flush_file_num = file_num;
        }
    }
    uint64_t flush_file_number() const {
        return _flush_file_num;
    }
private:
    RocksWrapper(): _is_init(false), _db{nullptr} {}
    std::string _db_path;
    bool _is_init {false};

    rocksdb::DB* _db {nullptr};
    rocksdb::Cache* _cache {nullptr};

    std::map<std::string, rocksdb::ColumnFamilyHandle*> _column_families;
    
    rocksdb::ColumnFamilyOptions _log_cf_option;
    rocksdb::ColumnFamilyOptions _data_cf_option;
    rocksdb::ColumnFamilyOptions _meta_cf_option;
    uint64_t _flush_file_num {0};
};
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */

