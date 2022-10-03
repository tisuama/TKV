#include "common/schema_factory.h"
#include "common/common.h"
#include "proto/meta.pb.h"

namespace TKV {
int SchemaFactory::init() {
    if (_is_inited) {
        return 0;
    }

    int ret = bthread::execution_queue_start(&_region_queue_id, nullptr, 
            update_regions_double_buffer, (void*)this);
    if (ret != 0) {
        DB_FATAL("execution_queue_start error, %d", ret);
        return -1;
    }

    _is_inited = true;
    return 0;
}

int SchemaFactory::update_regions_double_buffer(void* meta, bthread::TaskIterator<RegionVec>& iter) {
    SchemaFactory* factory = (SchemaFactory*)meta;
    TimeCost cost;
    factory->update_regions_double_buffer(iter);
    return 0;
}

void SchemaFactory::update_regions_double_buffer(bthread::TaskIterator<RegionVec>& iter) {
    // table_id => (partition => (start_key => region_info)
    std::map<int64_t, std::map<int, std::map<std::string, const pb::RegionInfo*>>> table_key_region_map;
    for (; iter; ++iter) {
        for (auto& region: *iter) {
            int64_t region_id = region.region_id();
            int64_t table_id = region.table_id();
            int64_t partition_id = region.partition_id();
            DB_WARNING("region_id: %ld, update region info: %s", region_id, region.ShortDebugString().c_str());
            const std::string& start_key = region.start_key();
            if (!start_key.empty() && start_key == region.end_key()) {
                DB_WARNING("region_id: %ld table_id: %ld is empty", region_id, table_id);
                continue;
            }
            table_key_region_map[table_id][partition_id][start_key] = &region;
        }
    }

    for (auto& table_region:  table_key_region_map) {
        update_regions(_double_buffer_region, table_region.first, table_region.second);
    }
}

int SchemaFactory::update_table_internal(SchemaMapping& background, const pb::SchemaInfo& table) {
    BAIDU_SCOPED_LOCK(_table_mutex);
    auto& table_info_mapping = background.table_id_to_table_info;
    auto& table_name_id_mapping = background.table_name_to_id;
    auto& db_info_mapping = background.db_id_to_db_info;
    auto& db_name_id_mapping = background.db_name_to_db_id;
    
    if (!_is_inited) {
        DB_FATAL("schema_factory not inited");
        return -1;
    }
    if (table.has_deleted() && table.deleted()) {
        delete_table(table, background);
        return 1;
    }
    
    if (!table.has_database_id() ||
            !table.has_table_id() ||
            !table.has_database_name() ||
            !table.has_table_name()) {
        DB_FATAL("missing field in schemainfo: %s", table.ShortDebugString().data());
        return -1;
    }
    int64_t db_id = table.database_id();
    int64_t table_id = table.table_id();
    const std::string& db_name = table.database_name();
    const std::string& table_name = table.table_name();
    const std::string& namesp = table.namespace_name();

    // Copy the temp descriptorproto and build the proto
    DatabaseInfo db_info;
    db_info.id = db_id;
    db_info.name = db_name;
    db_info.namesp = namesp;

    // Create table if not exist
    std::string cur_table_name("table_" + std::to_string(table_id));
    SmartTable table_info_ptr = std::make_shared<TableInfo>();
    if (table_info_mapping.count(table_id)) {
        *table_info_ptr = *table_info_mapping[table_id];
        TableInfo& table_info = *table_info_ptr; 
        table_info.dists.clear();
        // Check table version is match
        if (table_info.version >= table.version()) {
            DB_WARNING("Not need to update, origin version: %ld, new version: %ld, table_id: %ld", 
                    table_info.version, table.version(), table_id);
            return 0;
        }
    }
    TableInfo& table_info = *table_info_ptr;
    table_info.id = table_id;
    table_info.db_id = db_id;
    table_info.partition_num = table.partition_num();
    table_info.timestamp = table.timestamp();
    if (!table.has_byte_size_per_record() || table.byte_size_per_record() < 1) {
        table_info.byte_size_per_record = 1;
    } else {
        table_info.byte_size_per_record = table.byte_size_per_record();
    }
    if (table.has_region_split_lines() && table.region_split_lines()) {
        table_info.region_split_lines = table.region_split_lines();
    } else {
        table_info.region_split_lines = table.region_size() / table_info.byte_size_per_record;
    }
    // SETTING PARTITION INFO HERE
    table_info.name = db_name + "." + table_name;
    table_info.short_name = table_name;
    table_info.namesp = namesp;
    table_info.resource_tag = table.resource_tag();
    table_info.main_logical_room = table.main_logical_room();
    if (table.has_comment()) {
        table_info.comment = table.comment();
    }
    table_info.engine = pb::ROCKSDB;
    if (table.has_engine()) {
        table_info.engine = table.engine();
    }
    if (table.has_region_num()) {
        table_info.region_num = table.region_num();
    }
    // SETTING TTL DURATION HERE
    for (auto& r : table.learner_resource_tags()) {
        table_info.learner_resource_tags.emplace_back(r);
    }
    for (auto& d : table.dists()) {
        DistInfo dist_info;
        dist_info.logical_room = d.logical_room();
        dist_info.count = d.count();
        table_info.dists.emplace_back(dist_info);
    }
    
    // create db_name -> db_id mapping
    db_name_id_mapping[transfer_to_lower(namesp + "." + db_name)] = db_id;
    DB_WARNING("db_name_id_mapping: %s -> %ld", std::string(namesp + "." + db_name).c_str(), db_id);
    
    // create table_name -> table_id
    std::string db_table(transfer_to_lower(namesp + "." + db_name + "." + table_name));
    table_name_id_mapping[db_table] = table_id;
    
    db_info_mapping[db_id] = db_info;
    table_info_mapping[table_id] = table_info_ptr;
    return 1; // SUCCESS NUM
}

void SchemaFactory::delete_table_region_map(const pb::SchemaInfo& table) {
    if (table.has_deleted() && table.deleted()) {
        DB_DEBUG("erase table: %s", table.ShortDebugString().data());
        double_buffer_table_region_erase(_double_buffer_region, table.table_id());
    }
}

void SchemaFactory::update_tables_double_buffer_sync(const SchemaVec& tables) {
    for (auto& table: tables) {
        update_table_internal(_double_buffer_table, table);
        delete_table_region_map(table);
    }
}

void SchemaFactory::delete_table(const pb::SchemaInfo& table, SchemaMapping& background) {
    if (!table.has_table_id()) {
        DB_FATAL("missing fields in schemainfo");
        return ;
    }
    auto& table_info_mapping = background.table_id_to_table_info;
    auto& table_name_id_mapping = background.table_name_to_id;
    int64_t id = table.table_id();
    if (table_info_mapping.count(id) == 0) {
        DB_FATAL("no table found with table_id: %ld", id);
        return ;
    }
    auto ptr = table_info_mapping[id];
    auto& table_info = *ptr;
    std::string full_name = transfer_to_lower(table_info.namesp + "." + table_info.name); 
    DB_WARNING("full name: %s is erase", full_name.c_str());
    table_name_id_mapping.erase(full_name);

    return ;
}

void SchemaFactory::get_all_table_version(std::unordered_map<int64_t, int64_t>& table_id_version_map) {
    BAIDU_SCOPED_LOCK(_table_mutex);
    for (auto t :_double_buffer_table.table_id_to_table_info) {
        table_id_version_map[t.first] = t.second->version;
    }
}

bool SchemaFactory::exist_table_id(int64_t table_id) {
    if (_double_buffer_table.global_index_id_mapping.count(table_id) == 0) {
        return false;
    }
    return true;
}

void SchemaFactory::update_table(const pb::SchemaInfo& table) {
    delete_table_region_map(table);
    update_table_internal(_double_buffer_table, table);
}


size_t SchemaFactory::update_regions(std::unordered_map<int64_t, TableRegionPtr>& table_region_mapping, int64_t table_id,
       std::map<int, std::map<std::string, const pb::RegionInfo*>>& key_region_map) {
    BAIDU_SCOPED_LOCK(_region_mutex);
    if (table_region_mapping.count(table_id) == 0) {
        table_region_mapping[table_id] = std::make_shared<TableRegionInfo>();
    }
    TableRegionPtr table_info_ptr = table_region_mapping[table_id];
    DB_NOTICE("table_id: %ld update region table", table_id);

    // key_region_map: partition => (start_key => region_info)
    for (auto& start_key_region: key_region_map) {
        // int64_t partition_id = start_key_region.first;
        auto& start_key_region_map = start_key_region.second;
        std::vector<const pb::RegionInfo*> last_regions;
        std::map<std::string, int64_t> clear_regions;
        
        std::string end_key;
        for (auto iter = start_key_region_map.begin(); iter != start_key_region_map.end(); iter++) {
            const pb::RegionInfo& region = *iter->second;
            DB_NOTICE("region_id: %ld udpate region info: %s", region.region_id(), region.ShortDebugString().c_str());

            pb::RegionInfo pre_region;
            int ret = table_info_ptr->get_region_info(region.region_id(), pre_region);            
            if (ret < 0) { // 之前不存在这个region的信息
                // 判断加入的region和现在region是否重叠
            } else if (region.version() > pre_region.version()) {
                DB_DEBUG("region_id: %ld, new vs old: (%ld, %s, %s) VS (%ld, %s, %s)",
                        region.region_id(), region.version(), to_hex_str(region.start_key()).c_str(),
                        to_hex_str(region.end_key()).c_str(), pre_region.version(),
                        to_hex_str(pre_region.start_key()).c_str(),
                        to_hex_str(pre_region.end_key()).c_str());
                clear_regions.clear();
                last_regions.clear();
                
                if (region.start_key() < pre_region.start_key() && 
                        end_key_compare(region.end_key(), pre_region.end_key()) < 0) {
                    // Case1: start_key和end_key都变小 => split 和 merge同时发生
                } else if (region.start_key() < pre_region.start_key() && 
                        end_key_compare(region.end_key(), pre_region.end_key()) == 0) {
                    // Case2: start_key变小，end_key不变 => merge
                } else if (region.start_key() == pre_region.start_key() && 
                        end_key_compare(region.end_key(), pre_region.end_key()) < 0) {
                    // Case3: start_key不变, end_key变小 => split
                } else if (region.start_key() == pre_region.start_key() && 
                        end_key_compare(region.start_key(), pre_region.end_key())) {
                    // 仅version变大
                }
            } else {
                DB_DEBUG("region info: %ld, pre_region info: %s",
                        region.ShortDebugString().c_str(), pre_region.ShortDebugString().c_str());
            }
        }
    }
    return 1;
}

size_t SchemaFactory::double_buffer_table_region_erase(
        std::unordered_map<int64_t, TableRegionPtr>& table_region_map, int64_t table_id) {
    DB_DEBUG("double bufer table region erase table id: %ld", table_id);
    
    BAIDU_SCOPED_LOCK(_region_mutex);
    auto it = table_region_map.find(table_id);
    if (it != table_region_map.end()) {
        return table_region_map.erase(table_id);
    } 
    return 0;
}

} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
