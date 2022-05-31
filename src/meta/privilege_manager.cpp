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
    // TODO: next
}

void PrivilegeManager::add_privilege(const pb::MetaManagerRequest& request, braft::Closure* done) {
}

} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
