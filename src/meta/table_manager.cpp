#include "meta/table_manager.h"
#include "meta/namespace_manager.h"
#include "meta/database_manager.h"
#include "meta/region_manager.h"
#include "meta/meta_rocksdb.h"
#include "proto/store.pb.h"
#include "store/store_server_interact.h"

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
    auto ret = write_schema_for_not_level(table_mem, done, max_table_id, has_auto_increament);
    if (ret) {
        DB_WARNING("write rocksdb fail when create table, table: %s", table_name.c_str());
        IF_DONE_SET_RESPONSE(done, pb::INTERNAL_ERROR, "write db fail");
        return ;
    }
    this->set_max_table_id(max_table_id);
    table_mem.schema_pb.clear_init_store();     
    table_mem.schema_pb.clear_split_keys();
    this->set_table_info(table_mem);

    DatabaseManager::get_instance()->add_table_id(db_id, table_info.table_id());
    DB_NOTICE("create table completely, _max_table_id: %ld, table_name: %s", _max_table_id, table_name.c_str());
}


int TableManager::write_schema_for_not_level(TableMem& table_mem, braft::Closure* done,
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
                region_info->set_region_id(++max_region_id);
                region_info->set_table_id(main_table_id);
                region_info->set_main_table_id(main_table_id);
                region_info->set_table_name(table_mem.schema_pb.table_name());
                this->construct_region_common(region_info, table_mem.schema_pb.replica_num());
                region_info->set_partition_id(i);
                region_info->add_peers(table_mem.schema_pb.init_store(instance_count));
                region_info->set_leader(table_mem.schema_pb.init_store(instance_count));
                region_info->set_partition_num(table_mem.schema_pb.partition_num());
                // region_info->set_is_binglog_region(table_mem.is_binglog);
                if (j) {
                    region_info->set_start_key(split_key.split_keys(j - 1));
                }
                if (j < split_key.split_keys_size()) {
                    region_info->set_end_key(split_key.split_keys(j));
                }
                *(region_request.mutable_schema_info()) = schema_info;
                region_request.set_snapshot_times(2);
                init_regions->push_back(region_request);
            }
        }
    }

    // persist region_id
    std::string max_region_id_key = RegionManager::get_instance()->construct_max_region_id_key();
    std::string max_region_value;
    max_region_value.append((char*)&max_region_id, sizeof(int64_t));
    rocksdb_keys.push_back(max_region_id_key);
    rocksdb_values.push_back(max_region_value);

    // persist schema_info
    int64_t table_id = table_mem.schema_pb.table_id();
    std::string table_value;
    if (!schema_info.SerializeToString(&table_value)) {
        DB_WARNING("request serialize to string fail when create not level table, request: %s",
                schema_info.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::PARSE_TO_PB_FAIL, "serialze to array fail");
        return -1;
    }
    rocksdb_keys.push_back(this->construct_table_key(table_id));
    rocksdb_values.push_back(table_value);

    // write to rocksdb
    int ret = MetaRocksdb::get_instance()->put_meta_info(rocksdb_keys, rocksdb_values);
    if (ret < 0) {
        DB_WARNING("Add new table %s to rocksdb fail", 
                schema_info.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INTERNAL_ERROR, "write to db fail");
        return -1;
    }
    RegionManager::get_instance()->set_max_region_id(max_region_id);
    
    // Leader发送请求
    std::string nname = table_mem.schema_pb.namespace_name();
    std::string db_name = table_mem.schema_pb.database_name();        
    std::string table_name = table_mem.schema_pb.table_name();
    if (done && table_mem.schema_pb.engine() == pb::ROCKSDB ||
        table_mem.schema_pb.engine() == pb::ROCKSDB_CSTORE) {

        auto create_table_fn = [this, nname, db_name, table_name, init_regions, table_id] () {
            send_create_table_request(nname, db_name, table_name, init_regions); 
        };
        Bthread bth;
        bth.run(create_table_fn);
    }
    IF_DONE_SET_RESPONSE(done, pb::SUCCESS, "success"); 
    DB_WARNING("Create table, db_name: %s, table_id: %ld, table_name: %s, max_table_id: %ld"
            " alloc [start_region_id, end_region_id]: [%ld, %ld]",
            db_name.c_str(), table_id, max_table_id, start_region_id, max_region_id);
    return 0;
}


void TableManager::send_create_table_request(const std::string& namespace_name, 
            const std::string& database_name, const std::string& table_name, 
            std::shared_ptr<std::vector<pb::InitRegion>> init_regions) {
    uint64_t log_id = butil::fast_rand();
    // 并发10线程发送
    BthreadCond concurrency_cond(-10);
    std::string full_table_name = namespace_name + "." + database_name + "." + table_name;
    bool success = false;
    for (auto& init_region_request: *init_regions) {
        auto send_init_region_fn = [&init_region_request, &success, &concurrency_cond, 
             log_id, full_table_name] () {
            std::shared_ptr<BthreadCond> auto_decrease(&concurrency_cond, 
                    [](BthreadCond *cond) { cond->decrease_signal(); }); // deleter
            auto& r = init_region_request.region_info();
            int64_t region_id = r.region_id();
            StoreInteract store_interact(r.leader().c_str());
            pb::StoreRes res;
            auto ret = store_interact.send_request(log_id, "init_region", init_region_request, res);
            if (ret < 0) {
                // Send error 
                DB_FATAL("Create table fail, address: %s, region_id: %ld",
                        r.leader().c_str(), region_id);
                success = false;
                return ;
            }
            DB_NOTICE("New region id: %ld success, full_table_name: %s", full_table_name.c_str());
        };
        if (!success) {
            break;
        }
        Bthread bth;
        concurrency_cond.increase();
        concurrency_cond.wait();
        bth.run(send_init_region_fn);
    }
    concurrency_cond.wait(-10);
    if (!success) {
        DB_FATAL("Create table %s fail", full_table_name.c_str());
        // TODO: drop table request
        // send_drop_table_request(namespace_name, database_name, table_name);
    } else {
        DB_NOTICE("Create table %s success", full_table_name.c_str());
    }
} 

} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
