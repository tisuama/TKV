#include "meta/namespace_manager.h"

namespace TKV {
void NamespaceManager::create_namespace(const pb::MetaManagerRequest& request, braft::Closure* done) {
      
}

void NamespaceManager::drop_namespace(const pb::MetaManagerRequest& request, braft::Closure* done) {
}

void NamespaceManager::modify_namespace(const pb::MetaManagerRequest& request, braft::Closure* done) {
}

void NamespaceManager::load_namespace_snapshot(const std::string& value) {
}
} // namespace TKV 
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */

