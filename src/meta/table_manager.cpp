#include "meta/table_manager.h"
#include "meta/namespace_manager.h"
#include "meta/database_manager.h"
#include "meta/region_manager.h"
#include "proto/store.pb.h"

namespace TKV {
DEFINE_int32(region_size, 100 * 1024 * 1024, "region capacity, default: 100M");
DEFINE_int32(replica_num, 3, "region replica num, default: 3");

void TableManager::create_table(const pb::MetaManagerRequest& request, const int64_t apply_index, braft::Closure* done) {
    auto& table_info = const_cast<pb::SchemaInfo&>(request.table_info());
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
    int64_t max_table_id = this->_max_table_id;
    table_info.set_table_id(++max_table_id);
    table_mem.main_table_id = max_table_id;
    table_mem.global_index_id = max_table_id;
    // whether_level_table = false 
    if (!table_info.has_partition_num()) {
        table_info.set_partition_num(1);
    }
    if (!table_info.has_region_size()) {
        table_info.set_region_size(FLAGS_region_size);
    }
    if (!table_info.has_replica_num()) {
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


void TableManager::write_schema_for_not_level(TableMem& table_mem, braft::Closure* done,
                                int64_t max_table_id, bool has_auto_increment) {
    // 如果创建成功，则不需要任何操作
    // 如果创建失败，则需要手动调用table接口删除
    std::vector<std::string> rocksdb_keys;
    std::vector<std::string> rocksdb_values;

    std::string max_table_id_value;
    max_table_id_value.append((char*)&max_table_id, sizeof(max_table_id));
    rocksdb_keys.push_back(this->construct_max_table_id_key());
    rocksdb_values.push_back(max_table_id_value);

    // 持久话region_info，与store交互
    // 准备partition_num个数的region_info
    int64_t max_region_id = RegionManager::get_instance()->get_max_region_id();
    int64_t start_region_id = max_region_id + 1;

    std::shared_ptr<std::vector<pb::InitRegion>> init_regions(new std::vector<pb::InitRegion>{});
    // schema_pb.init_store set in pre_process_for_create_table()
    init_regions->reserve(table_mem.schema_pb.init_store_size());
    int64_t instance_count = 0;
    pb::SchemaInfo schema_info = table_mem.schema_pb;
    int64_t main_table_id = schema_info.table_id();
    schema_info.clear_init_store();
    schema_info.clear_split_keys();
    // 全局索引和主键索引需要建region
    // 处理含有split_key的index
    for (int i = 0; i < table_mem.schema_pb.partition_num(); i++) {
        if (table_mem.schema_pb.engine() != pb::ROCKSDB ||
            table_mem.schema_pb.engine() != pb::ROCKSDB_CSTORE) {
            continue;
        }    

        for (auto& split_key : table_mem.schema_pb.split_keys()) {
            CHECK(!split_key.has_index_name());  
            for (auto j = 0; j <= split_key.split_keys_size(); j++) {
                pb::InitRegion region_request;
                pb::RegionInfo* region_info = region_request.mutable_region_info();                 
            }
        }
    }
}

} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
