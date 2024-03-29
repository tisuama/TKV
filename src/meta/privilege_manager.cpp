#include "meta/meta_rocksdb.h"
#include "meta/privilege_manager.h"
#include "meta/schema_manager.h"

namespace TKV {
void PrivilegeManager::process_user_privilege(google::protobuf::RpcController* controller,
                            const pb::MetaManagerRequest* request,
                            pb::MetaManagerResponse* response,
                            google::protobuf::Closure* done) {
    brpc::ClosureGuard done_gurad(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
    uint64_t log_id = 0;
    if (cntl->has_log_id()) {
        log_id = cntl->log_id();
    }
    if (!request->has_user_privilege()) {
        ERROR_SET_RESPONSE(response, 
                           pb::INPUT_PARAM_ERROR,
                           "no user privilege",
                           request->op_type(),
                           log_id);
        return ;
    }
    switch(request->op_type()) {
        case pb::OP_CREATE_USER:
            if (!request->user_privilege().has_password()) {
                ERROR_SET_RESPONSE(response,
                                   pb::INPUT_PARAM_ERROR,
                                   "no password",
                                   request->op_type(),
                                   log_id);
                return ;
            }    
            _meta_state_machine->process(controller, request, response, done_gurad.release());
            return ;
        default:
            ERROR_SET_RESPONSE(response,
                               pb::INPUT_PARAM_ERROR,
                               "invalid op_type",
                               request->op_type(),
                               log_id);
            return ;
    }
} 

void PrivilegeManager::create_user(const pb::MetaManagerRequest& request, braft::Closure* done) {
    auto& user_privilege = const_cast<pb::UserPrivilege&>(request.user_privilege());
    std::string user_name = user_privilege.username();
    if (_user_privilege.find(user_name) != _user_privilege.end()) {
        DB_WARNING("reuqest username has been exist, username: %s", user_privilege.username().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "username has been exist");
        return ;
    }
    int ret = SchemaManager::get_instance()->check_and_get_for_privilege(user_privilege);
    if (ret < 0) {
        DB_WARNING("request not illegal, request: %s", request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "request invalid");
        return ;
    }
    // set version
    user_privilege.set_version(1);
    // build key and value
    std::string value;
    if (!user_privilege.SerializeToString(&value)) {
        DB_WARNING("request serialize to string fail, request: %s", request.ShortDebugString().c_str());  
        IF_DONE_SET_RESPONSE(done, pb::PARSE_TO_PB_FAIL, "serialize to string failed");
        return ;
    }
    // write to db
    ret = MetaRocksdb::get_instance()->put_meta_info(construct_privilege_key(user_name), value);
    if (ret < 0) {
        DB_WARNING("add user_name: %s to rocksdb failed", user_name.c_str());
        IF_DONE_SET_RESPONSE(done, pb::INTERNAL_ERROR, "write to db failed");
        return ;
    }
    // update mem info
    BAIDU_SCOPED_LOCK(_user_mutex);
    _user_privilege[user_name] = user_privilege;
    IF_DONE_SET_RESPONSE(done, pb::SUCCESS, "success");
    DB_NOTICE("create user: %s success, request: %s", user_name.c_str(), request.ShortDebugString().c_str());
}

void PrivilegeManager::add_privilege(const pb::MetaManagerRequest& request, braft::Closure* done) {
}

int PrivilegeManager::load_snapshot() {
    _user_privilege.clear();
    std::string privilege_prefix = MetaServer::PRIVILEGE_IDENTIFY;
    rocksdb::ReadOptions read_options;
    read_options.prefix_same_as_start = true;
    read_options.total_order_seek = false;
    auto db = RocksWrapper::get_instance();
    std::unique_ptr<rocksdb::Iterator> iter(
            db->new_iterator(read_options, db->get_meta_info_handle()));
    iter->Seek(privilege_prefix);
    for (; iter->Valid(); iter->Next()) {
        std::string user_name(iter->key().ToString(), privilege_prefix.size());
        pb::UserPrivilege user_privilege_pb;
        if (!user_privilege_pb.ParseFromString(iter->value().ToString())) {
            DB_FATAL("parse from pb fail when load snapshot, key: %s",
                    iter->key().data());
            return -1;
        }
        DB_WARNING("privilege load snapshot, user_privilege: %s", user_privilege_pb.ShortDebugString().c_str()); 
        BAIDU_SCOPED_LOCK(_user_mutex);
        _user_privilege[user_name] = user_privilege_pb;
    }
    return 0;
}

} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
