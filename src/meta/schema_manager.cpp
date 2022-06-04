#include "meta/schema_manager.h"
#include "meta/namespace_manager.h"
#include "meta/database_manager.h"
#include "meta/table_manager.h"

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
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
