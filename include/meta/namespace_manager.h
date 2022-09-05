#pragma once
#include <unordered_map>
#include <set>
#include <mutex>

#include "proto/meta.pb.h"
#include "meta/meta_server.h"
#include "meta/schema_manager.h"

namespace TKV {
class NamespaceManager {
public:
    ~NamespaceManager() {
        bthread_mutex_destroy(&_nmutex);
    }
    
    static NamespaceManager* get_instance() {
        static NamespaceManager instance;
        return &instance;
    }
    
    int64_t get_namespace_id(const std::string& nname) {
        BAIDU_SCOPED_LOCK(_nmutex);
        if (_nid_map.find(nname) == _nid_map.end()) {
            return 0;
        }
        return _nid_map[nname];
    }

    void set_namespace_info(const pb::NamespaceInfo& ninfo) {
        BAIDU_SCOPED_LOCK(_nmutex);
        _nid_map[ninfo.namespace_name()] = ninfo.namespace_id();
        _ninfo_map[ninfo.namespace_id()] = ninfo;
    }
    
    void set_max_namespace_id(int64_t max_nid) {
        BAIDU_SCOPED_LOCK(_nmutex);
        _max_nid = max_nid;
    }
    
    int64_t get_max_namespace_id() {
        BAIDU_SCOPED_LOCK(_nmutex);
        return _max_nid;
    }
    
    const std::string get_resource_tag(const int64_t nid) {
        BAIDU_SCOPED_LOCK(_nmutex);
        if (_ninfo_map.find(nid) == _ninfo_map.end()) {
            return "";
        }
        return _ninfo_map[nid].resource_tag();
    }

    void add_database_id(int64_t nid, int64_t db_id) {
        BAIDU_SCOPED_LOCK(_nmutex);
        _db_ids[nid].insert(db_id);
    }

    // Raft串行接口访问
    void create_namespace(const pb::MetaManagerRequest& request, braft::Closure* done);
    void drop_namespace(const pb::MetaManagerRequest& request, braft::Closure* done);
    void modify_namespace(const pb::MetaManagerRequest& request, braft::Closure* done);
    void load_namespace_snapshot(const std::string& value);
    
private:
    NamespaceManager(): _max_nid(0) {
        bthread_mutex_init(&_nmutex, NULL);
    }

    std::string construct_namespace_key(int64_t nid) {
        std::string nkey = MetaServer::SCHEMA_IDENTIFY + 
            MetaServer::NAMESPACE_SCHEMA_IDENTIFY;
        nkey.append((char*)&nid, sizeof(nid));
        return nkey;
    }
    
    std::string construct_max_namespace_id_key() {
        std::string max_nkey = MetaServer::SCHEMA_IDENTIFY + 
            MetaServer::MAX_ID_SCHEMA_IDENTIFY + 
            SchemaManager::MAX_NAMESPACE_ID_KEY;
        return max_nkey;
    }

    bthread_mutex_t         _nmutex;
    int64_t                 _max_nid;
    // namespace name -> namespace id
    std::unordered_map<std::string, int64_t> _nid_map;
    // namespace name -> namespace info
    std::unordered_map<int64_t, pb::NamespaceInfo> _ninfo_map;
    std::unordered_map<int64_t, std::set<int64_t>> _db_ids;
    
};
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
