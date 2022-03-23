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
DEFINE_int32(rocks_min_write_buffer_num_to_merge, 2, "rocks_min_write_buffer_num_to_merge");
DEFINE_int32(rocks_level0_file_num_compaction_trigger, 5, "Number of files to trigger level0 compaction");
DEFINE_int32(rocks_max_bytes_for_level_base, 1024 * 1024 * 1024, "level1's total size");
DEFINE_int32(rocks_target_file_size_base, 128 * 1024 * 1024, "rocks_target_file_size_base");
DEFINE_int32(addpeer_rate_limit_level, 1, "0: no limit, 1: limit when stall, 2: limit when compaction pending, default: 1");
DEFINE_bool(rocks_delete_files_in_range, true, "delete files in range");
DEFINE_bool(rocks_enable_bottommost_compression, false, "rocks_enable_bottommost_compression");

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
        table_options.pin_l0_filter_and_index_block_in_cache = true;
        table_options.block_cache = rocksdb::NewLRUCache(FLAGS_block_cache_size_mb * 1024 * 1024LL, 8, false, FLAGS_rocks_high_pri_pool_ratio);
        FLAGS_rocks_max_open_files = -1;'
    } else {
        table_options.data_block_index_type = rocksdb::BlockBasedTableOptions::kDataBlockBinaryAndHash;
        table_options.block_cache = rocksdb::NewLRUCache(FLAGS_rocks_block_cache_size_mb * 1024 * 1024LL, 8);
    }
    table_options.format_version = 4;
    table_options.block_size = FLAGS_rocks_block_size;
    table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10));
    _cache = table_options.block_cache.get();

    // rocksdb option
    rocksdb::Options db_options;
    db_options.IncreaseParalleism(FLAGS_max_background_jobs);
    db_options.create_if_missing = true;
    db_options.max_open_files = FLAGS_rocks_max_open_files;
    db_options.skip_stats_update_on_db_open = FLAGS_rocks_skip_stats_update_on_db_open;
    db_options.compaction_readahead_size = FLAGS_rocks_compaction_readahead_size;
    db_options.WAL_ttl_seconds = 10 * 60;
    db_options.WAL_size_limit_mb = 0;
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
    _log_cf_option.OptimizedLevelStyleCompaction();
    _log_cf_option.compaction_pri = rocksdb::kOldestLargestSeqFirst;
    _log_cf_option.level0_file_num_compaction_trigger = 5;
    _log_cf_option.level0_slowdown_writes_trigger = FLAGS_rocks_slowdown_write_sst_cnt;
    _log_cf_option.level0_stop_writes_trigger = FLAGS_rocks_stop_rite_sst_cnt;
    _log_cf_option.target_file_size_base = FLAGS_rocks_target_file_size_base;
    _log_cf_option.max_byte_for_level_base = 1024 * 1024 * 1024;
    _log_cf_option.level_compaction_dynamic_level_bytes = FLAGS_rocks_data_dynamic_level_bytes;
    _log_cf_option.max_write_buffer_number = FLAGS_rocks_max_write_buffer_number;
    _log_cf_option.max_write_buffer_nummber_to_maintain = FLAGS_rocks_max_write_buffer_number;
    _log_cf_option.write_buffer_size = FLAGS_rocks_write_buffer_size;
    _log_cf_option.min_write_buffer_number_to_merge = FLAGS_rocks_min_write_buffer_number_to_merge;

    // data cf 
    _data_cf_option.prefix_extractor.reset(rocksdb::NewFixedPrefixTransform(sizeof(uint64_t) * 2));
    _data_cf_option.OptimizedLevelStyleCompaction();
    _data_cf_option.compaction_pri = static_cast<rocksdb::ComapctionPri>(FLAGS_rocks_data_compation_pri);
    // need to impl compaction filter first to set
    // _data_cf_option.compaction_filter = SplitCompactionFilter::get_instance();
    _data_cf_option.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));
    _data_cf_option.compaction_style = rocksdb::kCompactionStyleLevel;
    _data_cf_option.optimize_filters_for_hit = FLAGS_rocks_optimize_filters_for_hit;
    _data_cf_option.level0_file_num_compaction_trigger = FLAGS_rocks_level0_file_num_compaction_trigger;
    _data_cf_option.level0_slowdown_writes_trigger = FLAGS_rocks_slowdown_write_sst_cnt;
    _data_cf_option.level0_stop_writes_trigger = FLAGS_rocks_stop_rite_sst_cnt;
    _data_cf_option.target_file_size_base = FLAGS_rocks_target_file_size_base;
    _data_cf_option.level_compaction_dynamic_level_bytes = FLAGS_rocks_data_dynamic_level_bytes;
    _data_cf_option.max_bytes_for_level_multiplier = FLAGS_rocks_max_bytes_for_level_multiplier;
    _data_cf_option.hard_pending_compaction_bytes_limit = FLAGS_rocks_hard_pending_compaction_g * 1073741824ull;
    _data_cf_option.soft_pending_compaction_bytes_limit = FLAGS_rocks_soft_pending_compaction_g * 1073741824ull;

    _data_cf_option.max_write_buffer_number = FLAGS_rocks_max_write_buffer_number;
    _data_cf_option.max_write_buffer_nummber_to_maintain = FLAGS_rocks_max_write_buffer_number;
    _data_cf_option.write_buffer_size = FLAGS_rocks_write_buffer_size;
    _data_cf_option.min_write_buffer_number_to_merge = FLAGS_rocks_min_write_buffer_number_to_merge;
    _data_cf_option.max_bytes_for_level_base = FLAGS_rocks_max_bytes_for_level_base; 
    // compression 
    if (FLAGS_enable_bottommost_compression) {
        _data_cf_option.bottommost_compression_opts.enabled = true;
        _data_cf_option.bottommost_compression = rocksdb::kZSTD;
        _data_cf_option.bottommost_compression_opts.max_dict_bytes = 1 << 14;
        _data_cf_option.bottommost_compression_opts.zstd_max_train_bytes = 1 << 18;
    }


    // meta cf

    return 0;
}
} // namespace TKV

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
