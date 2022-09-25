#include "common/table_key.h"
#include "store/meta_writer.h"
#include "common/mut_table_key.h"
#include <rocksdb/sst_file_reader.h>

namespace TKV {
const rocksdb::WriteOptions MetaWriter::write_options;
// META_INFO_HANDLE
const std::string MetaWriter::META_IDENTIFY(1, 0x01);
// key: META_IDENTIFY + region_id + identify
const std::string MetaWriter::APPLIED_INDEX_IDENTIFY(1, 0x01);
const std::string MetaWriter::NUM_TABLE_LINE_IDENTIFY(1, 0x02);
// ...
const std::string MetaWriter::REGION_INFO_IDENTIFY(1, 0x05);
const std::string MetaWriter::DOING_SNAPSHOT_IDENTIFY(1, 0x07); 
const std::string MetaWriter::LEARNER_IDENTIFY(1, 0x0C);
const std::string MetaWriter::LOCAL_STORAGE_IDENTIFY(1, 0x0D);
const std::string MetaWriter::PREPARE_TXN_LOG_INDEX_IDENTIFY(1, 0x03);

int MetaWriter::parse_region_infos(std::vector<pb::RegionInfo>& region_infos) {
    rocksdb::ReadOptions read_options;
    read_options.prefix_same_as_start = true;    
    // Seek based prefix
    read_options.total_order_seek = false;
    read_options.fill_cache = false;
    std::unique_ptr<rocksdb::Iterator> iter(_rocksdb->new_iterator(read_options, _meta_cf));
    std::string  prefix = MetaWriter::META_IDENTIFY;
    for (iter->Seek(prefix); iter->Valid(); iter->Next()) {
        std::string identify;
        TableKey(iter->key()).extract_char(1 + sizeof(int64_t), 1, identify);
        // Seek region info here
        if (identify != MetaWriter::REGION_INFO_IDENTIFY) {
            continue;
        }
        pb::RegionInfo region_info;
        if (!region_info.ParseFromString(iter->value().ToString())) {
            DB_FATAL("Parse from pb failed when load region info, key: %s",
                    iter->value().ToString().data());
            continue;
        }
        region_infos.push_back(region_info);
    }
    return 0;
}

std::string MetaWriter::learner_key(int64_t region_id) const {
    MutableKey key;
    key.append_char(MetaWriter::META_IDENTIFY.c_str(), 1);
    key.append_i64(region_id);
    key.append_char(MetaWriter::LEARNER_IDENTIFY.c_str(), 1);
    return key.data();
}

std::string MetaWriter::encode_learner_flag(int64_t learner_flag) const {
    MutableKey value;
    value.append_i64(learner_flag);
    return value.data();
}

int MetaWriter::read_learner_key(int64_t region_id) {
    std::string value;
    rocksdb::ReadOptions options;
    auto status = _rocksdb->get(options, _meta_cf, rocksdb::Slice(learner_key(region_id)), &value);    
    if (!status.ok()) {
        DB_WARNING("Error when read learner_key, Error: %s, region_id: %ld", 
                status.ToString().data(), region_id);
        return -1;
    }
    DB_DEBUG("region_id: %ld read learner: %s", region_id, value.data());
    // decode learner flag
    return TableKey(rocksdb::Slice(value)).extract_i64(0);
}

int MetaWriter::write_learner_key(int64_t region_id, bool is_learner) {
    DB_DEBUG("write learner key, region: %ld, is_learner: %d write: %s",
            region_id, is_learner, encode_learner_flag(is_learner).c_str());
    auto status = _rocksdb->put(MetaWriter::write_options, _meta_cf, 
            rocksdb::Slice(learner_key(region_id)),
            rocksdb::Slice(encode_learner_flag(is_learner)));            
    if (!status.ok()) {
        DB_FATAL("write learner key failed, err_msg: %s, region_id: %ld, learner: %d",
              status.ToString().data(), region_id, is_learner);  
        return -1;
    }
    return 0;
}

std::string MetaWriter::doing_snapshot_key(int64_t region_id) const {
    MutableKey key;
    key.append_char(MetaWriter::META_IDENTIFY.c_str(), 1);
    key.append_i64(region_id);
    key.append_char(MetaWriter::DOING_SNAPSHOT_IDENTIFY.c_str(), 1);
    return key.data();
}

int MetaWriter::parse_doing_snapshot(std::set<int64_t>& region_ids) {
    rocksdb::ReadOptions read_options;
    read_options.prefix_same_as_start = true;    
    // Seek based prefix
    read_options.total_order_seek = false;
    read_options.fill_cache = false;
    std::unique_ptr<rocksdb::Iterator> iter(_rocksdb->new_iterator(read_options, _meta_cf));
    std::string  prefix = MetaWriter::META_IDENTIFY;
    for (iter->Seek(prefix); iter->Valid(); iter->Next()) {
        std::string identify;
        TableKey(iter->key()).extract_char(1 + sizeof(int64_t), 1, identify);
        // Seek region info here
        if (identify != MetaWriter::DOING_SNAPSHOT_IDENTIFY) {
            continue;
        }
        region_ids.insert(TableKey(rocksdb::Slice(iter->key())).extract_i64(1));
    }
    return 0;
}

int MetaWriter::write_doing_snapshot(int64_t region_id) {
    auto status = _rocksdb->put(MetaWriter::write_options, _meta_cf, 
            rocksdb::Slice(doing_snapshot_key(region_id)),
            rocksdb::Slice(""));
    if (!status.ok()) {
        DB_FATAL("write doing snapshot failed, err_msg: %s, region_id: %ld",
                status.ToString().c_str(), region_id);
        return -1;
    }
    return 0;
}

std::string MetaWriter::encode_region_info(const pb::RegionInfo& region_info) const {
    std::string str;
    if (!region_info.SerializeToString(&str)) {
        DB_FATAL("region info: %s serialze to string fail", region_info.ShortDebugString().c_str());
        str.clear();
    }
    return str;
}

std::string MetaWriter::encode_applied_index(int64_t applied_index, int64_t data_index) const {
    MutableKey key;
    key.append_i64(applied_index);
    key.append_i64(data_index);
    return key.data();
}

std::string MetaWriter::encode_num_table_lines(int64_t line) const {
    MutableKey key;
    key.append_i64(line);
    return key.data();
}

std::string MetaWriter::region_info_key(int64_t region_id) const {
    MutableKey key;
    key.append_char(MetaWriter::META_IDENTIFY.data(), 1);
    key.append_i64(region_id);
    key.append_char(MetaWriter::REGION_INFO_IDENTIFY.data(), 1);
    return key.data();
}

std::string MetaWriter::applied_index_key(int64_t region_id) const {
    MutableKey key;
    key.append_char(MetaWriter::META_IDENTIFY.data(), 1);
    key.append_i64(region_id);
    key.append_char(MetaWriter::APPLIED_INDEX_IDENTIFY.data(), 1);
    return key.data();
}
std::string MetaWriter::num_table_lines_key(int64_t region_id) const {
    MutableKey key;
    key.append_char(MetaWriter::META_IDENTIFY.data(), 1);
    key.append_i64(region_id);
    key.append_char(MetaWriter::NUM_TABLE_LINE_IDENTIFY.data(), 1);
    return key.data();
}

/* init a new region's meta info */
int MetaWriter::init_meta_info(const pb::RegionInfo& region_info) {
    std::vector<std::string> keys;
    std::vector<std::string> values;
    std::string region_info_str = encode_region_info(region_info); 
    if (region_info_str.empty()) {
        return -1;
    }

    // persistent regioin info
    int64_t region_id = region_info.region_id();
    keys.push_back(region_info_key(region_id));
    values.push_back(region_info_str);

    // persistent applied index
    keys.push_back(applied_index_key(region_id));
    values.push_back(encode_applied_index(0, 0));

    // persistent num table line
    keys.push_back(num_table_lines_key(region_id));
    values.push_back(encode_num_table_lines(0));

    auto status = _rocksdb->write(MetaWriter::write_options, _meta_cf, keys, values);
    if (!status.ok()) {
        DB_FATAL("persistent init meta info failed, region_id: %ld, err_msg: %s",
                region_id, status.ToString().c_str());
        return -1;
    }
    return 0;
}

std::string MetaWriter::meta_info_prefix(int64_t region_id) {
    MutableKey key;
    key.append_char(MetaWriter::META_IDENTIFY.c_str(), 1);
    key.append_i64(region_id);
    return key.data();
}

std::string MetaWriter::log_index_key_prefix(int64_t region_id) const {
    MutableKey key;
    key.append_char(MetaWriter::META_IDENTIFY.c_str(), 1);
    key.append_i64(region_id);
    key.append_char(MetaWriter::PREPARE_TXN_LOG_INDEX_IDENTIFY.c_str(), 1);
    return key.data();
}

void MetaWriter::read_applied_index(int64_t region_id, 
      int64_t* applied_index, int64_t* data_index) {
    std::string value;
    rocksdb::ReadOptions options;
    read_applied_index(region_id, options, applied_index, data_index);
}

void MetaWriter::read_applied_index(int64_t region_id, 
        const rocksdb::ReadOptions& options, int64_t* applied_index, int64_t* data_index) {
    std::string value;
    auto s = _rocksdb->get(options, _meta_cf, rocksdb::Slice(applied_index_key(region_id)), &value);
    if (!s.ok()) {
        DB_WARNING("read applied index failed, Err: %s, region_id: %ld",
                s.ToString().c_str(), region_id);
        *applied_index = -1;
        *data_index = -1;
        return ;
    }
    TableKey tkey(value);
    *applied_index = tkey.extract_i64(0);
    // 兼容
    if (value.size() == 16) {
        *data_index = tkey.extract_i64(8);
    } else {
        *data_index = *applied_index;
    }
}

int64_t MetaWriter::read_num_table_lines(int64_t region_id) {
    std::string value;
    rocksdb::ReadOptions options;
    auto s = _rocksdb->get(options, _meta_cf, rocksdb::Slice(num_table_lines_key(region_id)), &value);
    if (!s.ok()) {
        DB_WARNING("read num table lines failed, Err: %s, region_id: %ld",
                s.ToString().c_str(), region_id);
        return -1;
    }
    return TableKey(rocksdb::Slice(value)).extract_i64(0);
}

int MetaWriter::update_num_table_lines(int64_t region_id, int64_t num_table_lines) {
    auto s = _rocksdb->put(MetaWriter::write_options, _meta_cf, 
            rocksdb::Slice(num_table_lines_key(region_id)),
            rocksdb::Slice(encode_num_table_lines(num_table_lines)));
    if (!s.ok()) {
        DB_FATAL("write update num table lines, Err: %s, region_id: %ld", 
                s.ToString().c_str(), region_id);
    }
    return 0;
}

/* Install快照的时候 */
int MetaWriter::clear_meta_info(int64_t drop_region_id) {
    TimeCost time_cost;
    rocksdb::WriteBatch batch;
    rocksdb::WriteOptions options;
    batch.Delete(_meta_cf, applied_index_key(drop_region_id));
    batch.Delete(_meta_cf, num_table_lines_key(drop_region_id));
    auto s = _rocksdb->write(options, &batch);
    if (!s.ok()) {
        DB_FATAL("drop region fail, err_msg: %s, code: %d, region_id: %ld",
                s.code(), s.ToString().c_str(), drop_region_id);
        return -1;
    }
    DB_WARNING("clear meta info success, cost: %ld, region_id: %ld",
            drop_region_id, time_cost.get_time());
    return 0;
}

int MetaWriter::clear_all_meta_info(int64_t drop_region_id) {
    TimeCost time_cost;
    rocksdb::WriteBatch batch;
    rocksdb::WriteOptions options;
    batch.Delete(_meta_cf, applied_index_key(drop_region_id));
    batch.Delete(_meta_cf, num_table_lines_key(drop_region_id));
    // more meta info is deleted
    batch.Delete(_meta_cf, region_info_key(drop_region_id));
    batch.Delete(_meta_cf, doing_snapshot_key(drop_region_id));
    batch.Delete(_meta_cf, learner_key(drop_region_id));

    auto s = _rocksdb->write(options, &batch);
    if (!s.ok()) {
        DB_FATAL("drop region fail, err_msg: %s, code: %d, region_id: %ld",
                s.code(), s.ToString().c_str(), drop_region_id);
        return -1;
    }
    DB_WARNING("clear meta info success, cost: %ld, region_id: %ld",
            drop_region_id, time_cost.get_time());
    return 0;
}

/* meta 数据量很少，直接写到memtable减少ingest导致的Flush */
int MetaWriter::ingest_meta_sst(const std::string& meta_sst_file, int64_t region_id) {
    auto options = _rocksdb->get_options(_rocksdb->get_meta_info_handle());
    rocksdb::SstFileReader reader(options);
    auto s = reader.Open(meta_sst_file);
    if (!s.ok()) {
        DB_WARNING("SstFileReader open failed, region_id: %ld", 
                s.ToString().c_str(), region_id);
        return -1;
    }
    rocksdb::ReadOptions read_options;
    std::unique_ptr<rocksdb::Iterator> iter(reader.NewIterator(read_options));
    // Seek(""): 从第一条开始
    for (iter->Seek(""); iter->Valid(); iter->Next()) {
        s = _rocksdb->put(MetaWriter::write_options, _meta_cf, iter->key(), iter->value());
        if (!s.ok()) {
            DB_FATAL("region_id: %ld put meta fail, err_msg: %s", 
                    region_id, s.ToString().c_str());
            return -1;
        }
    }
    return 0;
}     

int MetaWriter::read_region_info(int64_t region_id, pb::RegionInfo& region_info) {
    std::string value;
    rocksdb::ReadOptions options;
    auto s = _rocksdb->get(options, _meta_cf, rocksdb::Slice(region_info_key(region_id)), &value);
    if (!s.ok()) {
        DB_FATAL("region_id: %ld read region info fail, err_msg: %s", 
                region_id, s.ToString().c_str());
        return -1;
    }
    if (!region_info.ParseFromString(value)) {
        DB_FATAL("region_id: %ld parse from pb fail when read region info", region_id);
        return -1;
    }
    return 0;
}

int MetaWriter::clear_doing_snapshot(int64_t region_id) {
    rocksdb::WriteBatch batch;
    rocksdb::WriteOptions options;
    batch.Delete(_meta_cf, doing_snapshot_key(region_id));
    auto s = _rocksdb->write(options, &batch);
    if (!s.ok()) {
        DB_FATAL("region_id: %ld drop region fail, err_msg: %s", 
                region_id, s.ToString().c_str());
        return -1;
    }
    return 0;
}

int MetaWriter::update_region_info(const pb::RegionInfo& region_info) {
    int64_t region_id = region_info.region_id();
    std::string region_info_str;
    if (!region_info.SerializeToString(&region_info_str)) {
        DB_FATAL("region_id: %ld serialze to string fail", region_id);
        return -1;
    }
    auto s = _rocksdb->put(MetaWriter::write_options, _meta_cf, 
            rocksdb::Slice(region_info_key(region_id)), 
            rocksdb::Slice(region_info_str));
    if (!s.ok()) {
        DB_FATAL("region_id: %ld put to rocksdb fail, err_msg: %s",
                region_id, s.ToString().c_str());
        return -1;
    }
    DB_WARNING("region_id: %ld write region_info: %s success to rocksdb", 
            region_id, region_info.ShortDebugString().c_str());
    return 0;
}

int MetaWriter::update_apply_index(int64_t region_id, int64_t applied_index, int64_t data_index) {
    auto s = _rocksdb->put(MetaWriter::write_options, _meta_cf,
            rocksdb::Slice(applied_index_key(region_id)),
            rocksdb::Slice(encode_applied_index(applied_index, data_index)));
    if (!s.ok()) {
        DB_FATAL("region_id: %ld write applied_index failed, err_msg: %s",
                region_id, s.ToString().c_str());
        return -1;
    }
    DB_FATAL("region_id: %ld write applied_index: %ld success",
            region_id, applied_index);
    return 0;
}
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
