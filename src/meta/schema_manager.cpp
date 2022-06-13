#include "meta/schema_manager.h"
#include "meta/namespace_manager.h"
#include "meta/database_manager.h"
#include "meta/table_manager.h"
#include "proto/meta.pb.h"
#include "meta/cluster_manager.h"

namespace TKV {
const std::string SchemaManager::MAX_NAMESPACE_ID_KEY = "max_namespace_id";
const std::string SchemaManager::MAX_DATABASE_ID_KEY = "max_database_id";
const std::string SchemaManager::MAX_TABLE_ID_KEY = "max_table_id";
const std::string SchemaManager::MAX_REGION_ID_KEY = "max_region_id";


int SchemaManager::check_and_get_for_privilege(pb::UserPrivilege& user_privilege) {
	std::string nname = user_privilege.namespace_name();
    int64_t nid = NamespaceManager::get_instance()->get_namespace_id(nname);
    if (!nid) {
        DB_FATAL("namespace not exist, namespace: %s, request: %s",
                nname.c_str(), user_privilege.ShortDebugString().c_str());
        return -1;
    }
    // get and set namespace id
    user_privilege.set_namespace_id(nid);
    for (auto& database: *user_privilege.mutable_privilege_database()) {
        std::string db_name = nname + "\001" + database.database();
        int64_t db_id = DatabaseManager::get_instance()->get_database_id(db_name);
        if (!db_id) {
            DB_FATAL("database %s not exist, request: %s",
                    db_name.c_str(), user_privilege.ShortDebugString().c_str());
            return -1;
        }
        // get and set database id
        database.set_database_id(db_id);
    }
    
    for (auto& table: *user_privilege.mutable_privilege_table()) {
        std::string db_name = nname + "\001" + table.database();
        std::string table_name = db_name + "\001" + table.table_name();
        int64_t db_id = DatabaseManager::get_instance()->get_database_id(db_name);
        int64_t table_id = TableManager::get_instance()->get_table_id(table_name);
        if (!db_id || !table_id) {
            DB_FATAL("database %s not exist, namespace: %s, request: %s",
                    db_name.c_str(), nname.c_str(), 
                    user_privilege.ShortDebugString().c_str());
            return -1;
        }
        table.set_database_id(db_id);
        table.set_table_id(table_id);
    }
    
	return 0;
}

void SchemaManager::process_schema_info(google::protobuf::RpcController* controller, 
                        const pb::MetaManagerRequest* request, 
                        pb::MetaManagerResponse* response, 
                        google::protobuf::Closure* done) {
    brpc::ClosureGuard done_gurad(done);    
    int64_t log_id = 0;
    brpc::Controller* cntl;
    if (controller) {
        cntl = static_cast<brpc::Controller*>(controller); 
        if (cntl->has_log_id()) {
            log_id = cntl->log_id();
        }
    }
    if (!_meta_state_machine->is_leader()) {
        ERROR_SET_RESPONSE(response, pb::NOT_LEADER, "not leader", request->op_type(), log_id);
        return ;
    }
    ON_SCOPED_EXIT(([cntl, log_id, response]() {
        if (response!= nullptr && response->errcode() != pb::SUCCESS) {
            const auto& remote_side = butil::endpoint2str(cntl->remote_side());
            DB_WARNING("response error, remote_side: %s, log_id: %ld", remote_side, log_id);
        }
    }));
    
    // do something
    switch(request->op_type()) {
        case pb::OP_CREATE_NAMESPACE:
            if (!request->has_namespace_info()) {
                ERROR_SET_RESPONSE(response, pb::INPUT_PARAM_ERROR, 
                        "no namespace info", request->op_type(), log_id);
                return ;
            }
            if (request->op_type() == pb::OP_MODIFY_NAMESPACE
                    && !request->namespace_info().has_quota()) {
                ERROR_SET_RESPONSE(response, pb::INPUT_PARAM_ERROR,
                        "no namesace quota", request->op_type(), log_id);
                return ;
            }
            _meta_state_machine->process(controller, request, response, done_gurad.release());
            return ;
        case pb::OP_CREATE_DATABASE:
            if (!request->has_database_info()) {
                ERROR_SET_RESPONSE(response, pb::INPUT_PARAM_ERROR,
                        "no database info", request->op_type(), log_id);
                return ;
            }
            if (request->op_type() == pb::OP_CREATE_DATABASE
                    && !request->database_info().has_quota()) {
                ERROR_SET_RESPONSE(response, pb::INPUT_PARAM_ERROR,
                        "no database quota", request->op_type(), log_id);
                return ;
            }
            _meta_state_machine->process(controller, request, response, done_gurad.release());
            return ;
        case pb::OP_CREATE_TABLE:
            if (!request->has_table_info()) {
                ERROR_SET_RESPONSE(response, pb::INPUT_PARAM_ERROR,
                        "no schema info", request->op_type(), log_id);
            }
            if (request->op_type() == pb::OP_CREATE_TABLE) {
                auto ret = pre_process_for_create_table(request, response, log_id);
                if (ret < 0) return ;
            }
            _meta_state_machine->process(controller, request, response, done_gurad.release()); 
            return ;
        default:
            ERROR_SET_RESPONSE(response, pb::INPUT_PARAM_ERROR,
                    "invalid op_type", request->op_type(), log_id);
            return;
    }
}

int SchemaManager::pre_process_for_create_table(const pb::MetaManagerRequest* request, 
       pb::MetaManagerResponse* response,
       uint64_t log_id) {
    auto& table_info = const_cast<pb::SchemaInfo&>(request->table_info());
    int partition_num = 1;
    if (table_info.has_partition_num()) {
        partition_num = table_info.partition_num();
    }
    // 分区表多region
    // Set split_key
    if (table_info.has_region_num()) {
        int32_t region_num = table_info.region_num();
        if (region_num > 1) {
            auto skey = table_info.add_split_keys();
            // split->keys->set_index_name(primary_index_name);
            for (auto r = 1; r < region_num; r++) {
                skey->add_split_keys(std::string(r + 1, 0x01));
            }
        }
    }
    // 校验split_key有序, split_key.index_name必须在index_name中
    // split_index
    // split_keys(0)
    // split_keys(1)
    int total_region_cnt = 0;
    for (auto& skey: request->table_info().split_keys()) {
        for (auto i = 1; i < skey.split_keys_size(); i++) {
            if (skey.split_keys(i) <= skey.split_keys(i - 1)) {
                ERROR_SET_RESPONSE(response, pb::INPUT_PARAM_ERROR, 
                        "split key not sorted", request->op_type(), log_id);
                return -1;
            }
        }
        total_region_cnt += skey.split_keys_size() + 1;
    }
    
    // Set schema main_logical_room
    auto mutable_request = const_cast<pb::MetaManagerRequest*>(request);
    std::string main_logical_room;
    auto ret = whether_dists_legal(mutable_request, response, main_logical_room, log_id);
    if (ret < 0) {
        DB_WARNING("select main logical room failed, exit now");
        return -1;
    }
    if (request->table_info().has_main_logical_room()) {
        main_logical_room = request->table_info().main_logical_room();
    } 

    // Set schema resource tag
    // partition_num * total_region_cnt 
    total_region_cnt += partition_num * total_region_cnt;
    std::string tag = request->table_info().resource_tag();
    if (!table_info.has_resource_tag()) {
        std::string nname = table_info.namespace_name();
        std::string db_name = nname + "\001" + table_info.database_name();
        int64_t db_id = DatabaseManager::get_instance()->get_database_id(db_name);
        std::string db_tag = DatabaseManager::get_instance()->get_resource_tag(db_id);
        if (tag != "") {
            tag = db_tag;
        }
    }
    mutable_request->mutable_table_info()->set_resource_tag(tag);
    DB_WARNING("create table should select intance count: %ld", total_region_cnt);
    
    // Add instance
    for (int i = 0; i < total_region_cnt; i++) {
        std::string instance;
        int ret = ClusterManager::get_instance()->select_instance_rolling(
                tag, {}, main_logical_room, instance);
        if (ret < 0) {
            ERROR_SET_RESPONSE(response, pb::INPUT_PARAM_ERROR, 
                    "select instance fail", request->op_type(), log_id);
            return -1;
        }
        mutable_request->mutable_table_info()->add_init_store(instance);
    }
    return 0;
} 

int whether_dists_legal(pb::MetaManagerRequest* request,
        pb::MetaManagerResponse* response, 
        std::string& candidate_logical_room,
        uint64_t log_id) {
    if (request->table_info().dists_size() == 0) {
        return -1;
    }
    // 检查逻辑机房是否存在
    uint64_t total_count = 0;
    for (auto& d : request->table_info().dists()) {
        std::string room = d.logical_room();
        if (ClusterManager::get_instance()->logical_room_exist(room)) {
            total_count += d.count();
            continue;
        }
        ERROR_SET_RESPONSE(response, pb::INPUT_PARAM_ERROR, 
                "logical room not exist, select intance fail",
                request->op_type(), log_id);
        return -1;
    }
    if (request->table_info().main_logical_room().size() != 0) {
        int max_count = 0;
        // 副本数量最多的是主机房
        for (auto& d : request->table_info().dists()) {
            if (d.count() > max_count) {
                max_count = d.count();
                candidate_logical_room = d.logical_room();
            }
        }
    }
    // sum(dists.count) = replica_num
    if (total_count != (uint64_t)request->table_info().replica_num()) {
        ERROR_SET_RESPONSE(response, pb::INPUT_PARAM_ERROR,
                "replica num not match", request->op_type(), log_id);
        return -1;
    }
    return 0;
}
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
