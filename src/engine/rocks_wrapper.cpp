#include <rocksdb/filter_policy.h>
#include "engine/rocks_wrapper.h"
#include "common/common.h"

namespace TKV {
DEFINE_bool(rocks_use_partitioned_index_filters, false, "rocks_use_partitioned_index_filters");
DEFINE_bool(rocks_skip_stats_update_on_db_open, false, "rocks_skip_stats_update_on_db_open");
DEFINE_int32(rocks_block_size, 64 * 1024, "default: 64K");
DEFINE_int32(rocks_block_cache_size_mb, 8 * 1024, "block cache size, default: 8G");
DEFINE_uint64(rocks_hard_pending_compaction_g, 256, "rocks_hard_pending_compaction_g, default: 256G");
DEFINE_uint64(rocks_soft_pending_compaction_g, 64, "rocks_soft_pending_compaction_g, default: 64G");
DEFINE_uint64(rocks_compaction_readahead_size, 0, "rocks_compaction_readahead_size, default: 0");
DEFINE_int32(rocks_data_compaction_pri, 3, "rocks_data_compaction_pri, default: 3(kMinOverlappingRatio)");
DEFINE_double(rocks_level_multiplier, 10, "data_cf rocks max_byte_for_level_multiplier, default: 10");
DEFINE_double(rocks_high_pri_pool_ratio, 0.5, "rocks cache high pri pool ration, default: 0.5");
DEFINE_int32(rocks_max_open_files, 1024, "rocks max_open_files, default: 1024");
DEFINE_int32(rocks_max_subcompactions, 4, "rocks_max_subcompactions, default: 4");
DEFINE_int32(rocks_max_background_compactions, 20, "rocks_max_background_compactions, default: 20");
DEFINE_bool(rocks_optimize_filter_for_hits, false, "rocks_optimize_filter_for_hits");
DEFINE_int32(rocks_slowdown_write_sst_cnt, 10, "level0_slowdown_write_trigger, default: 10");
DEFINE_int32(rocks_stop_write_sst_cnt, 40, "level0 stop write trigger");
DEFINE_bool(rocks_skip_any_corrupt_record, false, "rocks_skip_any_corrupt_record");
DEFINE_bool(rocks_data_dynamic_level_bytes, true, "rocks_data_dynamic_level_bytes");
DEFINE_int64(rocks_flush_memtable_interval_us, 10 * 60 * 1000 * 1000LL,  "rocks_flush_memtable_interval_us");
DEFINE_int32(rocks_max_background_jobs, 24, "rocks_max_background_jobs, default: 24");
DEFINE_int32(rocks_max_write_buffer_number, 6, "rocks_max_write_buffer_number, default: 6");
DEFINE_int32(rocks_write_buffer_size, 128 * 1024 * 1024, "rocks_write_buffer_size");
DEFINE_int32(rocks_min_write_buffer_number_to_merge, 2, "rocks_min_write_buffer_num_to_merge");
DEFINE_int32(rocks_level0_file_num_compaction_trigger, 5, "Number of files to trigger level0 compaction");
DEFINE_int32(rocks_max_bytes_for_level_base, 1024 * 1024 * 1024, "level1's total size");
DEFINE_int32(rocks_target_file_size_base, 128 * 1024 * 1024, "rocks_target_file_size_base");
DEFINE_int32(addpeer_rate_limit_level, 1, "0: no limit, 1: limit when stall, 2: limit when compaction pending, default: 1");
DEFINE_bool(rocks_delete_files_in_range, true, "delete files in range");
DEFINE_bool(rocks_enable_bottommost_compression, false, "rocks_enable_bottommost_compression");
DEFINE_bool(rocks_kSkipAnyCorruptedRecords, false, "rocks_kSkipAnyCorruptedRecords");

const std::string RocksWrapper::RAFT_LOG_CF = "raft_log";
const std::string RocksWrapper::DATA_CF = "data_cf";
const std::string RocksWrapper::METAINFO_CF = "meta_info";
// define here
std::atomic<int64_t> RocksWrapper::raft_cf_remove_range_count {0};
std::atomic<int64_t> RocksWrapper::data_cf_remove_range_count {0};
std::atomic<int64_t> RocksWrapper::meta_cf_remove_range_count {0};

int32_t RocksWrapper::init(const std::string& path) {
    if (_is_init) return 0;
    rocksdb::BlockBasedTableOptions table_options;
    if (FLAGS_rocks_use_partitioned_index_filters) {
        table_options.index_type = rocksdb::BlockBasedTableOptions::kTwoLevelIndexSearch;
        table_options.partition_filters = true;
        table_options.metadata_block_size = 4096;
        table_options.cache_index_and_filter_blocks = true;
        table_options.pin_top_level_index_and_filter = true;
        table_options.cache_index_and_filter_blocks_with_high_priority = true;
        table_options.pin_l0_filter_and_index_blocks_in_cache = true;
        table_options.block_cache = rocksdb::NewLRUCache(FLAGS_rocks_block_cache_size_mb * 1024 * 1024LL, 8, false, FLAGS_rocks_high_pri_pool_ratio);
        FLAGS_rocks_max_open_files = -1;
    } else {
        table_options.data_block_index_type = rocksdb::BlockBasedTableOptions::kDataBlockBinaryAndHash;
        table_options.block_cache = rocksdb::NewLRUCache(FLAGS_rocks_block_cache_size_mb * 1024 * 1024LL, 8);
    }
    // rocksdb version
    table_options.format_version = 4;
    table_options.block_size = FLAGS_rocks_block_size;
    _cache = table_options.block_cache.get();
    table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10));

    // rocksdb option
    rocksdb::Options db_options;
    db_options.IncreaseParallelism(FLAGS_rocks_max_background_jobs);
    db_options.create_if_missing = true;
    db_options.max_open_files = FLAGS_rocks_max_open_files;
    db_options.skip_stats_update_on_db_open = FLAGS_rocks_skip_stats_update_on_db_open;
    db_options.compaction_readahead_size = FLAGS_rocks_compaction_readahead_size;
    db_options.WAL_ttl_seconds = 10 * 60;
    db_options.WAL_size_limit_MB = 0;
    db_options.max_background_compactions = FLAGS_rocks_max_background_compactions;
    if (FLAGS_rocks_kSkipAnyCorruptedRecords) {
        db_options.wal_recovery_mode = rocksdb::WALRecoveryMode::kSkipAnyCorruptedRecords; 
    }
    db_options.statistics = rocksdb::CreateDBStatistics();
    db_options.max_subcompactions = FLAGS_rocks_max_subcompactions;
    db_options.max_background_flushes = 2;
    db_options.env->SetBackgroundThreads(2, rocksdb::Env::HIGH);

    // log cf
    // prefix_extractor need to cal again
    _log_cf_option.prefix_extractor.reset(rocksdb::NewFixedPrefixTransform(sizeof(uint64_t) + 1));
    _log_cf_option.OptimizeLevelStyleCompaction();
    _log_cf_option.compaction_pri = rocksdb::kOldestLargestSeqFirst;
    _log_cf_option.level0_file_num_compaction_trigger = 5;
    _log_cf_option.level0_slowdown_writes_trigger = FLAGS_rocks_slowdown_write_sst_cnt;
    _log_cf_option.level0_stop_writes_trigger = FLAGS_rocks_stop_write_sst_cnt;
    _log_cf_option.target_file_size_base = FLAGS_rocks_target_file_size_base;
    _log_cf_option.max_bytes_for_level_base = 1024 * 1024 * 1024;
    _log_cf_option.level_compaction_dynamic_level_bytes = FLAGS_rocks_data_dynamic_level_bytes;
    _log_cf_option.max_write_buffer_number = FLAGS_rocks_max_write_buffer_number;
    _log_cf_option.max_write_buffer_number_to_maintain = _log_cf_option.max_write_buffer_number;
    _log_cf_option.write_buffer_size = FLAGS_rocks_write_buffer_size;
    _log_cf_option.min_write_buffer_number_to_merge = FLAGS_rocks_min_write_buffer_number_to_merge;

    // data cf 
    _data_cf_option.prefix_extractor.reset(rocksdb::NewFixedPrefixTransform(sizeof(uint64_t) * 2));
    _data_cf_option.OptimizeLevelStyleCompaction();
    _data_cf_option.compaction_pri = static_cast<rocksdb::CompactionPri>(FLAGS_rocks_data_compaction_pri);
    // need to impl compaction filter first to set
    // _data_cf_option.compaction_filter = SplitCompactionFilter::get_instance();
    _data_cf_option.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));
    _data_cf_option.compaction_style = rocksdb::kCompactionStyleLevel;
    _data_cf_option.optimize_filters_for_hits = FLAGS_rocks_optimize_filter_for_hits;
    _data_cf_option.level0_file_num_compaction_trigger = FLAGS_rocks_level0_file_num_compaction_trigger;
    _data_cf_option.level0_slowdown_writes_trigger = FLAGS_rocks_slowdown_write_sst_cnt;
    _data_cf_option.level0_stop_writes_trigger = FLAGS_rocks_stop_write_sst_cnt;
    _data_cf_option.target_file_size_base = FLAGS_rocks_target_file_size_base;
    _data_cf_option.level_compaction_dynamic_level_bytes = FLAGS_rocks_data_dynamic_level_bytes;
    _data_cf_option.max_bytes_for_level_multiplier = FLAGS_rocks_level_multiplier;
    _data_cf_option.hard_pending_compaction_bytes_limit = FLAGS_rocks_hard_pending_compaction_g * 1073741824ull;
    _data_cf_option.soft_pending_compaction_bytes_limit = FLAGS_rocks_soft_pending_compaction_g * 1073741824ull;

    _data_cf_option.max_write_buffer_number = FLAGS_rocks_max_write_buffer_number;
    _data_cf_option.max_write_buffer_number_to_maintain = _data_cf_option.max_write_buffer_number;
    _data_cf_option.write_buffer_size = FLAGS_rocks_write_buffer_size;
    _data_cf_option.min_write_buffer_number_to_merge = FLAGS_rocks_min_write_buffer_number_to_merge;
    _data_cf_option.max_bytes_for_level_base = FLAGS_rocks_max_bytes_for_level_base; 
    // compression 
    if (FLAGS_rocks_enable_bottommost_compression) {
        _data_cf_option.bottommost_compression_opts.enabled = true;
        _data_cf_option.bottommost_compression = rocksdb::kZSTD;
        _data_cf_option.bottommost_compression_opts.max_dict_bytes = 1 << 14;
        _data_cf_option.bottommost_compression_opts.zstd_max_train_bytes = 1 << 18;
    }


    // meta cf
    _meta_cf_option.prefix_extractor.reset(rocksdb::NewFixedPrefixTransform(1));
    _meta_cf_option.OptimizeLevelStyleCompaction();
    _meta_cf_option.compaction_pri = rocksdb::kOldestLargestSeqFirst;
    _meta_cf_option.level_compaction_dynamic_level_bytes = FLAGS_rocks_data_dynamic_level_bytes;
    _meta_cf_option.max_write_buffer_number_to_maintain = _meta_cf_option.max_write_buffer_number;

    // open db
    _db_path = path;

    std::vector<std::string> column_family_names;
    rocksdb::Status s;
    s = rocksdb::DB::ListColumnFamilies(db_options, path, &column_family_names);
    if (!s.ok()) {
        // new db
        s = rocksdb::DB::Open(db_options, path, &_db);
        if (s.ok()) {
            DB_WARNING("open db: %s sucess", path.data());
        } else {
            DB_FATAL("open db: %s failed, err_msg: %s", path.data(), s.ToString().c_str());
            return -1;
        }
    } else {
        std::vector<rocksdb::ColumnFamilyDescriptor> column_family_desc;
        std::vector<rocksdb::ColumnFamilyHandle*> handles;
        for (auto& c : column_family_names) {
            if (c == RAFT_LOG_CF) {
                column_family_desc.push_back(rocksdb::ColumnFamilyDescriptor(RAFT_LOG_CF, _log_cf_option));
            } else if (c == DATA_CF) {
                column_family_desc.push_back(rocksdb::ColumnFamilyDescriptor(DATA_CF, _data_cf_option));
            } else if (c == METAINFO_CF) {
                column_family_desc.push_back(rocksdb::ColumnFamilyDescriptor(METAINFO_CF, _meta_cf_option));
            } else {
                column_family_desc.push_back(rocksdb::ColumnFamilyDescriptor(c, rocksdb::ColumnFamilyOptions()));
                DB_WARNING("column_family_desc push column_family_name: %s for default", c.data());
            }
        }
        s = rocksdb::DB::Open(db_options, path, column_family_desc, &handles, &_db);
        if (s.ok()) {
            DB_WARNING("reopen db: %s sucess", path.data());
            for (auto& h : handles) {
                _column_families[h->GetName()] = h;
                DB_WARNING("open column family: %s", h->GetName().data());
            }
        } else {
            DB_FATAL("reopen db: %s, error message: %s", path.data(), s.ToString().data());
            return -1;
        }
    } 
    // check column_family
    if (_column_families.count(RAFT_LOG_CF) == 0) {
        rocksdb::ColumnFamilyHandle* raft_log_handle;
        s = _db->CreateColumnFamily(_log_cf_option, RAFT_LOG_CF, &raft_log_handle);
        if (s.ok()) {
            DB_WARNING("create column_family %s sucess", RAFT_LOG_CF.data());
            _column_families[RAFT_LOG_CF] = raft_log_handle;
        } else {
            DB_WARNING("create column_family %s failed", RAFT_LOG_CF.data());
            return -1;
        }
    }
    if (_column_families.count(DATA_CF) == 0) {
        rocksdb::ColumnFamilyHandle* data_handle;
        s = _db->CreateColumnFamily(_data_cf_option, DATA_CF, &data_handle);
        if (s.ok()) {
            DB_WARNING("create column_family %s sucess", DATA_CF.data());
            _column_families[DATA_CF] = data_handle;
        } else {
            DB_WARNING("create column_family %s failed", DATA_CF.data());
            return -1;
        }
    }
    if (_column_families.count(METAINFO_CF) == 0) {
        rocksdb::ColumnFamilyHandle* meta_info_handle;
        s = _db->CreateColumnFamily(_meta_cf_option, METAINFO_CF, &meta_info_handle);
        if (s.ok()) {
            DB_WARNING("create column_family %s sucess", METAINFO_CF.data());
            _column_families[METAINFO_CF] = meta_info_handle;
        } else {
            DB_WARNING("create column_family %s failed", METAINFO_CF.data());
            return -1;
        }
    }
    _is_init = true;
    DB_WARNING("rocksdb init sucess");
    return 0;
}


Status RocksWrapper::remove_range(const rocksdb::WriteOptions& options,
                    rocksdb::ColumnFamilyHandle* column_family,
                    const rocksdb::Slice& begin, 
                    const rocksdb::Slice& end,
                    bool delete_files_in_range) {
    auto raft_cf = get_raft_log_handle();
    auto data_cf = get_data_handle();
    auto meta_cf = get_meta_info_handle();
    if (raft_cf != nullptr && column_family->GetID() == raft_cf->GetID()) {
        raft_cf_remove_range_count++;
    } else if (data_cf != nullptr && column_family->GetID() == data_cf->GetID()) {
        data_cf_remove_range_count++;
    } else if (meta_cf != nullptr && column_family->GetID() == meta_cf->GetID()) {
        meta_cf_remove_range_count++;
    }
    if (delete_files_in_range && FLAGS_rocks_delete_files_in_range) {
        auto s = rocksdb::DeleteFilesInRange(_db, column_family, &begin, &end, false);
        if (!s.ok()) {
            return s;
        }
    }
    rocksdb::WriteBatch batch;
    batch.DeleteRange(column_family, begin, end);
    return _db->Write(options, &batch);
}

int32_t RocksWrapper::delete_column_family(const std::string& cf_name) {
    if (_column_families.count(cf_name) == 0) {
        DB_FATAL("delete column family %s failed", cf_name.data());
        return -1;
    } 
    auto cf_hander = _column_families[cf_name];
    auto s = _db->DropColumnFamily(cf_hander);
    if (!s.ok()) {
        DB_WARNING("drop column family %s failed, error msg: %s",
                cf_name.data(), s.ToString().data());
        return -1;
    }
    s = _db->DestroyColumnFamilyHandle(cf_hander);
    if (!s.ok()) {
        DB_WARNING("drop column family %s failed, error mgs: %s", 
                cf_name.data(), s.ToString().data());
        return -1;
    }
    _column_families.erase(cf_name);
    return 0;
}

int32_t RocksWrapper::create_column_family(const std::string& cf_name) {
    if (_column_families.count(cf_name) != 0) {
        DB_FATAL("column family: %s is already exist", cf_name.data());
        return -1;
    }
    rocksdb::ColumnFamilyHandle* cf_handle = nullptr;
    auto s = _db->CreateColumnFamily(_data_cf_option, cf_name, &cf_handle);
    if (s.ok()) {
        DB_WARNING("create column family %s sucess", cf_name.data());
        _column_families[cf_name] = cf_handle;
        return 0;
    } else {
        DB_FATAL("create column family %s failed", cf_name.data());
        return -1;
    }

}

rocksdb::ColumnFamilyHandle* RocksWrapper::get_raft_log_handle() {
    if (!_is_init || _column_families.count(RAFT_LOG_CF) == 0) {
        DB_FATAL("db has not been init or raft log not init");
        return nullptr;
    }
    return _column_families[RAFT_LOG_CF];
}

rocksdb::ColumnFamilyHandle* RocksWrapper::get_data_handle() {
    if (!_is_init || _column_families.count(DATA_CF) == 0) {
        DB_FATAL("db has not been init or raft log not init");
        return nullptr;
    }
    return _column_families[RAFT_LOG_CF];
}

rocksdb::ColumnFamilyHandle* RocksWrapper::get_meta_info_handle() {
    if (!_is_init || _column_families.count(METAINFO_CF) == 0) {
        DB_FATAL("db has not been init or raft log not init");
        return nullptr;
    }
    return _column_families[METAINFO_CF];
}

rocksdb::ColumnFamilyHandle* RocksWrapper::get_handle(KV_CF CF) {
    switch(CF) {
        case CF_LOCK:
        case CF_WRITE:
        case CF_DATA:
            return _column_families[DATA_CF];
        case CF_META:
            return _column_families[METAINFO_CF];
        case CF_RAFT_LOG:
            return _column_families[RAFT_LOG_CF];
        default:
            abort();
    }
    return _column_families[DATA_CF];
}
} // namespace TKV

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
