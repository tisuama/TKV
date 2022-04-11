#include "common/schema_factory.h"


namespace TKV {
void SchemaFactory::update_table_internal(SchemaMapping& background, const pb::SchemaInfo& table) {
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
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
