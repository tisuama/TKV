#include "meta/schema_manager.h"
#include "meta/namespace_manager.h"
#include "meta/database_manager.h"
#include "meta/table_manager.h"
#include "proto/meta.pb.h"

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
    // 校验split_key有序
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
    
    auto request = const_cast<pb::MetaManagerRequest*>(request);
    std::string logical_room;
    // 指定跨机房
    // TODO: next
} 
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
