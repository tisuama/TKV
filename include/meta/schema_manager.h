#pragma once

#include "proto/meta.pb.h"
#include "proto/store.pb.h"
#include "meta/meta_state_machine.h"

namespace TKV {
typedef std::shared_ptr<pb::RegionInfo> SmartRegionInfo;	
class SchemaManager {
public:
	static const std::string MAX_NAMESPACE_ID_KEY;
	static const std::string MAX_DATABASE_ID_KEY;
	static const std::string MAX_TABLE_ID_KEY;
	static const std::string MAX_REGION_ID_KEY;
	
	static SchemaManager* get_instance() {
		static SchemaManager instance;
		return &instance;
	}
	~SchemaManager() {}
    
    void set_meta_state_machine(MetaStateMachine* s) {
        _meta_state_machine = s;
    }

	int check_and_get_for_privilege(pb::UserPrivilege& user_privilege);
    
    void process_schema_info(google::protobuf::RpcController* controller, 
                            const pb::MetaManagerRequest* request, 
                            pb::MetaManagerResponse* response, 
                            google::protobuf::Closure* done);

private:
	SchemaManager() {};

	MetaStateMachine* _meta_state_machine {nullptr};
};

} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
