#pragma once
#include <unordered_map>
#include "proto/meta.pb.h"
#include "meta/meta_server.h"

namespace TKV {
class DatabaseManager {
public:
    ~DatabaseManager() {
        bthread_mutex_destroy(&_db_mutex);
    }
    static DatabaseManager* get_instance() {
        static DatabaseManager instance;
        return &instance;
    }
    
    int64_t get_database_id(const std::string& db_name) {
        BAIDU_SCOPED_LOCK(_db_mutex);
        if (_db_id_map.find(db_name) != _db_id_map.end()) {
            return _db_id_map[db_name];
        }
        return 0;
    }

private:
    DatabaseManager(): _max_db_id(0) {
        bthread_mutex_init(&_db_mutex, NULL);
    }
    
    std::string construct_database_key(int64_t db_id) {
        std::string db_key = MetaServer::SCHEMA_IDENTIFY +
           MetaServer::DATABASE_SCHEMA_IDENTIFY; 
        // byte order
        db_key.append((char*)&db_id, sizeof(db_id));
        return db_key;
    }
    
    std::string construct_max_database_id_key() {
        std::string max_db_id_key = MetaServer::SCHEMA_IDENTIFY + 
            MetaServer::MAX_ID_SCHEMA_IDENTIFY + 
            SchemaManager::MAX_DATABASE_ID_KEY;
        return max_db_id_key;
    }

    bthread_mutex_t _db_mutex;
    int64_t         _max_db_id;
    // database_name -> database_id
    std::unordered_map<std::string, int64_t> _db_id_map;
    std::unordered_map<std::string, pb::DatabaseInfo> _db_info_map;
    std::unordered_map<std::int64_t, std::set<int64_t>> _table_ids;
};
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
