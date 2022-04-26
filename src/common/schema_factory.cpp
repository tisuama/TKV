#include "common/schema_factory.h"
#include "common/common.h"

namespace TKV {
int SchemaFactory::update_table_internal(SchemaMapping& background, const pb::SchemaInfo& table) {
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
            !table.has_database() ||
            !table.has_table_name()) {
        DB_FATAL("missing field in schemainfo: %s", table.ShortDebugString().data());
        return -1;
    }
    int64_t db_id = table.database_id();
    int64_t table_id = table.table_id();
    const std::string& db_name = table.database();
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
    // partition info 
    if (table.partition_num() > 1 && table.has_partition_info()) {
        // set partition info
    }
}

void SchemaFactory::delete_table_region_map(const pb::SchemaInfo& table) {
    if (table.has_deleted() && table.deleted()) {
        DB_DEBUG("erase table: %s", table.ShortDebugString().data());
        _double_buffer_table_region.Modify(double_buffer_table_region_erase, table.table_id());
    }
}

void SchemaFactory::update_tables_double_buffer_sync(const SchemaVec& tables) {
    for (auto& table: tables) {
        std::function<int(SchemaMapping& bg, const pb::SchemaInfo& table)> f = 
            std::bind(&SchemaFactory::update_table_internal, this, std::placeholders::_1, std::placeholders::_2);
        DB_NOTICE("schema info: %s udpate double buffer sync", table.ShortDebugString().data());
        delete_table_region_map(table);
        _double_buffer_table.Modify(f, table);
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

} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
