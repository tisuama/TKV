#include "raft/my_raft_meta_storage.h"

namespace TKV {
int MyRaftMetaStorage::set_term(const int64_t term) {
}

int64_t MyRaftMetaStorage::get_term() const {
}

int MyRaftMetaStorage::set_votedfor(const braft::PeerId& peer_id) {
}

int MyRaftMetaStorage::get_votedfor(braft::PeerId& peer_id) const {
}

butil::Status MyRaftMetaStorage::init() {
}

butil::Status MyRaftMetaStorage::set_term_and_votedfor(const int64_t term,
        const braft::PeerId& peer_id, const braft::VersionedGroupId& group) {
}

butil::Status MyRaftMetaStorage::get_term_and_votedfor(int64_t* term, 
        braft::PeerId* peer_id, const braft::VersionedGroupId& group) {
}

RaftMetaStorage* MyRaftMetaStorage::new_instance(const std::string& uri) const {
}
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
