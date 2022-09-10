#include "meta/namespace_manager.h"
#include "meta/meta_rocksdb.h"
#include "proto/meta.pb.h"

namespace TKV {
void NamespaceManager::create_namespace(const pb::MetaManagerRequest& request, braft::Closure* done) {
    auto& ninfo = const_cast<pb::NamespaceInfo&>(request.namespace_info());      
    std::string nname = ninfo.namespace_name();
    if (_nid_map.find(nname) != _nid_map.end()) {
        DB_WARNING("request namespace: %s has been exist", nname.c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "namespace already existed");
        return ;
    }
    std::vector<std::string> rkeys;
    std::vector<std::string> rvalues;
    int64_t nid = _max_nid + 1;
    ninfo.set_namespace_id(nid);
    ninfo.set_version(1);
    
    std::string nvalue;
    if (!ninfo.SerializeToString(&nvalue)) {
        DB_WARNING("request serialzed to string failed, request: %s", request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::PARSE_TO_PB_FAIL, "serialzed to string failed");
        return ;
    }
    rkeys.push_back(construct_namespace_key(nid));
    rvalues.push_back(nvalue);
    

    std::string max_value;
    max_value.append((char*)&nid, sizeof(nid));
    rkeys.push_back(construct_max_namespace_id_key());
    rvalues.push_back(max_value);

    // 持久化
    int ret = MetaRocksdb::get_instance()->put_meta_info(rkeys, rvalues);
    if (ret < 0) {
        IF_DONE_SET_RESPONSE(done, pb::INTERNAL_ERROR, "write db failed");
        return ;
    }
    // 更新内存
    this->set_namespace_info(ninfo);
    this->set_max_namespace_id(nid);
    IF_DONE_SET_RESPONSE(done, pb::SUCCESS, "success");
    DB_NOTICE("create namespace success, request: %s", request.ShortDebugString().c_str());
}

void NamespaceManager::drop_namespace(const pb::MetaManagerRequest& request, braft::Closure* done) {
}

void NamespaceManager::modify_namespace(const pb::MetaManagerRequest& request, braft::Closure* done) {
}

int NamespaceManager::load_namespace_snapshot(const std::string& value) {
    pb::NamespaceInfo namespace_pb;
    if (namespace_pb.ParseFromString(value)) {
        DB_FATAL("parse from pb fail when load namespace snapshot, value: %s", value.c_str());
        return -1;
    }
    DB_WARNING("load namespace info: %s", namespace_pb.ShortDebugString().c_str());
    set_namespace_info(namespace_pb);
    return 0;
}
} // namespace TKV 
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */

