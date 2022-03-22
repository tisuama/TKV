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
DEFINE_double(rocks_high_pri_pool_ration, 0.5, "rocks cache high pri pool ration, default: 0.5");
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
DEFINE_int32(rocks_max_write_buffer_num, 6, "rocks_max_write_buffer_num, default: 6");
DEFINE_int32(rocks_write_buffer_size, 128 * 1024 * 1024, "rocks_write_buffer_size");
DEFINE_int32(rocks_min_write_buffer_num_to_merge, 2, "rocks_min_write_buffer_num_to_merge");
DEFINE_int32(rocks_level0_file_num_compaction_trigger, 5, "Number of files to trigger level0 compaction");
DEFINE_int32(rocks_max_bytes_for_level_base, 1024 * 1024 * 1024, "level1's total size");
DEFINE_bool(rocks_enable_compress, false, "enable zstd compress");
DEFINE_int32(rocks_target_file_size_base, 128 * 1024 * 1024, "rocks_target_file_size_base");
DEFINE_int32(addpeer_rate_limit_level, 1, "0: no limit, 1: limit when stall, 2: limit when compaction pending, default: 1");
DEFINE_bool(rocks_delete_files_in_range, true, "delete files in range");

int32_t RocksWrapper::init(const std::string& path) {
    if (_is_init) return 0;
    rocksdb::BlockBasedTableOptions table_options;
    return 0;
}
} // namespace TKV

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
