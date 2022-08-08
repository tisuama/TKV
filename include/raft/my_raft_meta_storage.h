#pragma once
#include <string>

#include "engine/rocks_wrapper.h"
#include <braft/storage.h>
#include <braft/local_storage.pb.h>

namespace TKV {
// RAFT_LOG_HANDLE    
// LOG_META_IDENTIFY = 0x01;
// LOG_DATA_IDENTIFY = 0x02;
// RAFT_META_IDENTIFY = 0x03;
    
using braft::RaftMetaStorage;
class MyRaftMetaStorage: public RaftMetaStorage {
public:
    // region_id(8 bytes) + 0x03 // region_id开始的都是业务数据
    static const size_t RAFT_META_KEY_SIZE = sizeof(int64_t) + 1;
    static const uint8_t RAFT_META_IDENTIFY = 0x03;

    int set_term(const int64_t term);
    int64_t get_term() const;
    int set_votedfor(const braft::PeerId& peer_id);
    int get_votedfor(braft::PeerId& peer_id) const;
    int set_term_and_votedfor(const int64_t term, const braft::PeerId& peer_id);

    virtual butil::Status init() override;
    virtual butil::Status set_term_and_votedfor(const int64_t term,
            const braft::PeerId& peer_id, const braft::VersionedGroupId& group) override;
    virtual butil::Status get_term_and_votedfor(int64_t* term, 
            braft::PeerId* peer_id, const braft::VersionedGroupId& group) override;
    RaftMetaStorage* new_instance(const std::string& uri) const override;


private:
    MyRaftMetaStorage(int64_t region_id, RocksWrapper* db,
            rocksdb::ColumnFamilyHandle* handle)
        : _region_id(region_id)
        , _db(db)
        , _handle(handle)
    {}

    int load();
    int save();

    bool _is_inited = false;
    int64_t _region_id = 0;
    int64_t _term = -1;
    braft::PeerId _votedfor;
    RocksWrapper* _db = nullptr;
    rocksdb::ColumnFamilyHandle* _handle = nullptr;
};
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
