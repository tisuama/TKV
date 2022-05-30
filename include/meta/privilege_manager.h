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

    void set_meta_state_machine(MetaStateMachine* meta_state_machine) {
        _meta_state_machine = meta_state_machine;
    }

private:
    PrivilegeManager() {
        bthread_mutex_init(&_user_mutex, NULL);
    }
        
    bthread_mutex_t _user_mutex;
    std::unordered_map<std::string, pb::UserPrivilege> _user_privilege;
    MetaStateMachine* _meta_state_machine {nullptr};
};

} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */


