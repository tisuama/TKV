#include "meta/table_manager.h"

namespace TKV {
void TableManager::create_table(const pb::MetaManagerRequest& request, const int64_t apply_index, braft::Closure* done) {
    auto& table_info = const_cast<pb::SchemaInfo&>(request->table_info());
    table_info.set_timestamp(time(NULL));
    table_info.set_version(1);
    
    std::string nname = table_info.namespace_name();
    std::string db_name = nname + "\001" + table_info.database_name();
    std::string table_name = db_name + "\001" + table_info.table_name();

    TableMem table_mem;
    table_mem.whether_level_table = false;
    // 校验合法性
    int64_t nid = NamespaceManager::get_instance()->get_namespace_id(nname);
    if (!nid) {
        DB_WARNING("request namespace: %s not exist", nname.c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "namesapce not exist");
        return ;
    }
    table_info.set_namespace_id(nid);

    int64_t db_id = DatabaseManager::get_instance()->get_database_id(db_name);
    if (!db_id) {
        DB_WARNING("request db: %s not exist", db_name.c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "database not exist");
        return ;
    }
    table_info.set_database_id(db_id);

    // 分配table id
    int64_t max_table_id = _max_table_id;
    table_info.set_table_id(++max_table_id);
    table_mem.main_table_id = max_table_id;
    talbe_mem.global_index_id = max_table_id;
    // whether_level_table = false 
    if (!table_info.has_partition_num()) {
        table_info.set_partition_num(1);
    }
    if (!table_info.has_region_size()) {
        table_info.set_region_size(FLAGS_region_size);
    }
    if (!talbe_info.has_replica_num()) {
        table_info.set_replica_num(FLAGS_replica_num);
    }
    // TODO: 分配field_id，index_id
    // TODO: partition_num > 1
    for (auto& r : *table_info.mutable_learner_resource_tags()) {
        table_mem.learner_resource_tag.emplace_back(r);
    }
    table_mem.schema_pb = table_info;
    

    // 发起交互
    bool has_auto_increament = false;
    // auto ret = write_schema_for_not_level(table_mem, done, max_table_id, has_auto_increament);
     


}
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
