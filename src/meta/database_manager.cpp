#include "meta/database_manager.h"
#include "meta/namespace_manager.h"
#include "meta/meta_rocksdb.h"

namespace TKV {
void DatabaseManager::create_database(const pb::MetaManagerRequest& request, braft::Closure* done) {
    auto& db_info = const_cast<pb::DatabaseInfo&>(request.database_info());
    std::string nname = db_info.namespace_name();
    std::string db_name = nname + "\001" + db_info.database_name();
    int64_t nid = NamespaceManager::get_instance()->get_namespace_id(nname);
    // namespace not exist 
    if (nid == 0) {
        DB_WARNING("request namespace: %s not exist", nname.c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "namespace not exist");
        return ;
    }
    // db has exist
    if (_db_id_map.find(db_name) != _db_id_map.end()) {
        DB_WARNING("database: %s has been exist", db_name.c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "database has been exist");
        return ;
    }
    std::vector<std::string> rkeys;
    std::vector<std::string> rvalues;
    // prepare database info
    int64_t db_id = _max_db_id + 1;
    db_info.set_database_id(db_id);
    db_info.set_namespace_id(nid);
    if (!db_info.has_resource_tag()) {
        std::string r_tag = NamespaceManager::get_instance()->get_resource_tag(nid);
        if (r_tag != "") {
            db_info.set_resource_tag(r_tag);
        }
    }
    db_info.set_version(1);
    // serialize now
    std::string db_value;
    if (!db_info.SerializeToString(&db_value)) {
        DB_WARNING("request serialzeToArray fail, request: %s", request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::PARSE_TO_PB_FAIL, "serialzeToArray fail");
        return ;
    }
    rkeys.push_back(construct_database_key(db_id));
    rvalues.push_back(db_value);

    // max_id
    std::string max_db_value;
    max_db_value.append((char*)&nid, sizeof(int64_t));
    rkeys.push_back(construct_max_database_id_key());
    rvalues.push_back(max_db_value);

    // write to db
    int ret = MetaRocksdb::get_instance()->put_meta_info(rkeys, rvalues);
    if (ret < 0) {
        IF_DONE_SET_RESPONSE(done, pb::INTERNAL_ERROR, "write to db fail");
        return ;
    }
    // update mem info
    this->set_database_info(db_info);
    this->set_max_database_id(db_id);
    NamespaceManager::get_instance()->add_database_id(nid, db_id);
    IF_DONE_SET_RESPONSE(done, pb::SUCCESS, "success");
    DB_NOTICE("create database success, request: %s", request.ShortDebugString().c_str());
}

int DatabaseManager::load_database_snapshot(const std::string& value) {
    pb::DatabaseInfo db_pb;
    if (!db_pb.ParseFromString(value)) {
        DB_FATAL("parse from pb fail when load database snapshot, value: %s", value.c_str());
        return -1;
    }
    DB_WARNING("load database info: %s", db_pb.ShortDebugString().c_str());
    set_database_info(db_pb);

    // 更新内存中namesapce info
    NamespaceManager::get_instance()->add_database_id(
            db_pb.namespace_id(),
            db_pb.database_id());
    return 0;
}
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
