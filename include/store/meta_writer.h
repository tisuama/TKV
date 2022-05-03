#pragma once
#include "common/common.h"
#include "engine/rocks_wrapper.h"
#include "proto/store.pb.h"

namespace TKV {
class MetaWriter {
public:
    static const rocksdb::WriteOptions write_options;

    static const std::string META_IDENTIFY;
    static const std::string APPLIED_INDEX_IDENTIFY;
    static const std::string NUM_TABLE_LINE_IDENTIFY;
    static const std::string REGION_INFO_IDENTIFY;
    static const std::string DOING_SNAPSHOT_IDENTIFY; 
    static const std::string LEARNER_IDENTIFY;
    static const std::string LOCAL_STORAGE_IDENTIFY;
    
    virtual ~MetaWriter() {}
    static MetaWriter* get_instance() {
        static MetaWriter instance;
        return &instance;
    }
    
    void init(RocksWrapper* rocksdb, rocksdb::ColumnFamilyHandle* meta_cf) {
        _rocksdb = rocksdb;
        _meta_cf = meta_cf;
    }
    
    rocksdb::ColumnFamilyHandle* get_handle() {
        return _meta_cf;
    }

    int init_meta_info(const pb::RegionInfo& region_info);
    int update_region_info(const pb::RegionInfo& region_info);
    int update_num_table_lines(int64_t region_id, int64_t num_table_lines);
    int update_apply_index(int64_t region_id, int64_t applied_index, int64_t data_index);
    int write_doing_snapshot(int64_t region_id);
    int write_batch(rocksdb::WriteBatch* updates, int64_t region_id);
    
    // SST FILE
    int ingest_sst_file(const std::string& meta_sst_file, int64_t region_id); 

    // Clear Region info
    int clear_meta_info(int64_t drop_region_id);
    int clear_all_meta_info(int64_t drop_region_id);
    int clear_region_info(int64_t drop_region_id);
    int clear_doing_snapshot(int64_t region_id);

    int parse_region_infos(std::vector<pb::RegionInfo>& region_infos);
    int parse_doing_snapshot(std::set<int64_t>& region_ids);
    void read_applied_index(int64_t region_id, int64_t* applied_index, int64_t* data_index);
    void read_applied_index(int64_t region_id, 
            const rocksdb::ReadOptions& options, int64_t* applied_index, int64_t* data_index);
    int64_t read_num_table_lines(int64_t region_id);
    int read_region_info(int64_t region_id, pb::RegionInfo& region_info);
    int read_learner_key(int64_t region_id);
    int write_learner_key(int64_t region_id);
    int read_doing_snapshot(int64_t region_id);

public:
    std::string region_info_key(int64_t regin_id) const;
    std::string region_for_store_key(int64_t region_id) const;
    std::string applied_index_key(int64_t region_id) const;
    std::string num_table_lines_key(int64_t region_id) const;
    std::string log_index_key_prefix(int64_t reigon_id) const;
    std::string doing_snapshot_key(int64_t region_id) const;
    std::string learner_key(int64_t region_id) const;

    // encode
    std::string encode_applied_index(int64_t applied_index, int64_t data_index) const;
    std::string encode_num_table_lines(int64_t line) const;
    std::string encode_region_info(const pb::RegionInfo& region_info) const;
    std::string encode_learner_flag(int64_t ts) const;

    // decode
    uint64_t decode_log_index_key(const rocksdb::Slice& key);
    
    std::string meta_info_prefix(int64_t region_id);

private:
    MetaWriter() {}
private:
    RocksWrapper* _rocksdb {nullptr};
    rocksdb::ColumnFamilyHandle* _meta_cf {nullptr};
};
}// namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */

