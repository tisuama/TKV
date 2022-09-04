#pragma once
#include <unordered_map>
#include <bthread/mutex.h>
#include "proto/meta.pb.h"

#include "meta/meta_server.h"

namespace TKV {
class PrivilegeManager{
public:
    static PrivilegeManager* get_instance() {
        static PrivilegeManager instance;
        return &instance;
    }

    ~PrivilegeManager() {
        bthread_mutex_destroy(&_user_mutex);
    }
    
    // called by meta_server before apply
    void process_user_privilege(google::protobuf::RpcController* controller,
                                const pb::MetaManagerRequest* request,
                                pb::MetaManagerResponse* response,
                                google::protobuf::Closure* done); 

    // called by meta_state_machine when on_apply
    void create_user(const pb::MetaManagerRequest& request, braft::Closure* done);
    void add_privilege(const pb::MetaManagerRequest& request, braft::Closure* done);
    
    int load_snapshot();

    void set_meta_state_machine(MetaStateMachine* s) {
        _meta_state_machine = s;
    }

private:
    PrivilegeManager() {
        bthread_mutex_init(&_user_mutex, NULL);
    }
    
    std::string construct_privilege_key(const std::string& user_name) {
        return MetaServer::PRIVILEGE_IDENTIFY + user_name;
    }
        
    bthread_mutex_t _user_mutex;
    std::unordered_map<std::string, pb::UserPrivilege> _user_privilege;
    MetaStateMachine* _meta_state_machine {nullptr};
};

} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */


