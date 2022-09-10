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
    int whether_dists_legal(pb::MetaManagerRequest* request,
            pb::MetaManagerResponse* response, 
            std::string& logical_room,
            uint64_t log_id);
    
    // Raft接口之前调用
    void process_schema_info(google::protobuf::RpcController* controller, 
                            const pb::MetaManagerRequest* request, 
                            pb::MetaManagerResponse* response, 
                            google::protobuf::Closure* done);
    int load_snapshot();
        
private:
	SchemaManager() {};
    
    int pre_process_for_create_table(const pb::MetaManagerRequest* request, 
           pb::MetaManagerResponse* response,
           uint64_t log_id); 
    int load_max_id_snapshot(const std::string& max_id_prefix,
            const std::string& key,
            const std::string& value);


	MetaStateMachine* _meta_state_machine {nullptr};
};

} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
