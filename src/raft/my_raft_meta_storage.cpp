#include "raft/my_raft_meta_storage.h"
#include "common/mut_table_key.h"
#include "common/log.h"

#include <braft/local_storage.pb.h>

namespace TKV {
// myraftmeta://my_raft_meta?id=
static int parse_my_raft_meta_uri(const std::string& uri, std::string& id) {
    size_t pos = uri.find("id=");
    if (pos == 0 || pos == std::string::npos) {
        return -1;
    }
    id = uri.substr(pos + 3);
    return 0;
}

int MyRaftMetaStorage::set_term(const int64_t term) {
    if (_is_inited) {
        _term = term;
        return save();
    } else {
        DB_FATAL("region_id: %ld raft meta storage not inited", _region_id);
        return -1;
    }   
}
 
int64_t MyRaftMetaStorage::get_term() const {
    if (_is_inited) {
        return _term;
    } else {
        DB_WARNING("region_id: %ld raft meta is not inited", _region_id);
        return -1;
    }
}

int MyRaftMetaStorage::set_votedfor(const braft::PeerId& peer_id) {
    if (_is_inited) {
        _votedfor = peer_id;
        return save();
    } else {
        DB_WARNING("region_id: %ld raft meta is not inited", _region_id);
        return -1;
    }
}

int MyRaftMetaStorage::get_votedfor(braft::PeerId& peer_id) const {
    if (_is_inited) {
        peer_id = _votedfor;
        return 0;
    } else {
        DB_WARNING("region_id: %ld raft meta is not inited", _region_id);
        return -1;
    }
}

butil::Status MyRaftMetaStorage::init() {
    butil::Status s;
    if (_is_inited) {
        return s;
    }
    int ret = load();
    if (ret == 0) {
        _is_inited = true;
        return s;
    }
    s.set_error(EINVAL, "region_id: %ld load pb meta error", _region_id);
    return s;
}

butil::Status MyRaftMetaStorage::set_term_and_votedfor(const int64_t term,
        const braft::PeerId& peer_id, const braft::VersionedGroupId& group) {
    butil::Status s;
    int ret = set_term_and_votedfor(term, peer_id);
    if (ret < 0) {
        s.set_error(EINVAL, "my raft meta is error, region_id: %ld", _region_id);
    }
    return s;
}

butil::Status MyRaftMetaStorage::get_term_and_votedfor(int64_t* term, 
        braft::PeerId* peer_id, const braft::VersionedGroupId& group) {
    butil::Status s;
    if (_is_inited) {
        *peer_id = _votedfor;
        *term = _term;
        return s;
    }
    s.set_error(EINVAL, "my raft meta is error, region_id: %ld", _region_id);
    return s;
}

RaftMetaStorage* MyRaftMetaStorage::new_instance(const std::string& uri) const {
    RocksWrapper* rocksdb = RocksWrapper::get_instance();
    if (rocksdb == nullptr) {
        DB_FATAL("rocksdb is not set");
        return nullptr;
    }
    std::string string_region_id;
    int ret = parse_my_raft_meta_uri(uri, string_region_id);
    if (ret) {
        DB_FATAL("parse uri fail, uri: %s", uri.c_str());
        return nullptr;
    }
    int64_t region_id = std::stol(string_region_id);
    auto handle = rocksdb->get_raft_log_handle();
    if (handle == nullptr) {
        DB_FATAL("get raft handle from rocksdb fail, uri: %s, region_id: %ld", 
                uri.c_str(), region_id);
        return nullptr;
    }
    auto* instance = new(std::nothrow) MyRaftMetaStorage(region_id, rocksdb, handle);
    if (instance == nullptr) {
        DB_FATAL("region_id: %ld, new raft meta storage instance fail", region_id);
        return nullptr;
    }
    DB_WARNING("region_id: %ld new raft meta storage success", region_id);
    return instance;
}

int MyRaftMetaStorage::load() {
    braft::StablePBMeta meta;
    MutableKey key;
    key.append_i64(_region_id);
    key.append_u8(RAFT_META_IDENTIFY);
    
    rocksdb::Slice skey(key.data());
    std::string value_str;
    rocksdb::ReadOptions read_option;
    auto s = _db->get(read_option, _handle, skey, &value_str);

    if (s.ok()) {
        meta.ParseFromString(value_str);
        _term = meta.term();
        if (_votedfor.parse(meta.votedfor())) {
            DB_WARNING("region_id: %ld parse from stable meta failed", _region_id);
            return -1;
        }
    } else {
        DB_WARNING("region_id: %ld failed to load stable meta from rocksdb", _region_id);
    }
    return 0;
}

int MyRaftMetaStorage::save() {
    butil::Timer timer;
    timer.start();
   
    braft::StablePBMeta meta;
    meta.set_term(_term);
    meta.set_votedfor(_votedfor.to_string());
    std::string value_str;
    meta.SerializeToString(&value_str);

    MutableKey key;
    key.append_i64(_region_id);
    key.append_u8(RAFT_META_IDENTIFY);
    
    rocksdb::Slice skey(key.data());
    rocksdb::Slice value(value_str);
    
    rocksdb::WriteOptions write_option;
    auto s = _db->put(write_option, _handle, skey, value);
    if (!s.ok()) {
        DB_FATAL("region_id: %ld fail to save my raft meta", _region_id);
        return -1;
    }
    timer.stop();
    DB_WARNING("region_id: %ld save my raft meta success", _region_id);
    return 0;
}

int MyRaftMetaStorage::set_term_and_votedfor(const int64_t term, const braft::PeerId& peer_id) {
    if (_is_inited) {
        _term = term;
        _votedfor = peer_id;
        return save();
    } else {
        DB_WARNING("region_id: %ld raft meta is not inited", _region_id);
        return -1;
    }
}
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
