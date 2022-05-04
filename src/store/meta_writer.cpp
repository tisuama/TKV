#include "common/table_key.h"
#include "store/meta_writer.h"
#include "common/mut_table_key.h"
#include <rocksdb/sst_file_reader.h>

namespace TKV {
const rocksdb::WriteOptions MetaWriter::write_options;
const std::string MetaWriter::META_IDENTIFY(1, 0x01);
// key: META_IDENTIFY + region_id + identify
const std::string MetaWriter::APPLIED_INDEX_IDENTIFY(1, 0x01);
const std::string MetaWriter::NUM_TABLE_LINE_IDENTIFY(1, 0x02);
// ...
const std::string MetaWriter::REGION_INFO_IDENTIFY(1, 0x05);
const std::string MetaWriter::DOING_SNAPSHOT_IDENTIFY(1, 0x07); 
const std::string MetaWriter::LEARNER_IDENTIFY(1, 0x0C);
const std::string MetaWriter::LOCAL_STORAGE_IDENTIFY(1, 0x0D);

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

} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
