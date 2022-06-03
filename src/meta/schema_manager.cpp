#include "meta/schema_manager.h"
#include "meta/namespace_manager.h"

namespace TKV {
const std::string SchemaManager::MAX_NAMESPACE_ID_KEY = "max_namespace_id";

int SchemaManager::check_and_get_for_privilege(pb::UserPrivilege& user_privilege) {
	std::string nname = user_privilege.namespace_name();
    int64_t nid = NamespaceManager::get_instance()->get_namespace_id(nname);
    if (!nid) {
        DB_FATAL("namespace not exist, namespace: %s, request: %s",
                nname.c_str(), user_privilege.ShortDebugString().c_str());
        return -1;
    }
    user_privilege.set_namespace_id(nid);
    // TODO: next
    
	return 0;
}
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
