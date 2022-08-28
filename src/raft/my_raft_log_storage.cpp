#include "raft/my_raft_log_storage.h"
#include <xxhash.h>
#include <braft/local_storage.pb.h>

namespace TKV {

static int parse_my_raft_log_uri(const std::string& uri, std::string& region_id_str) {
    size_t pos = uri.find("id=");
    if (pos == 0 || pos == std::string::npos) {
        return -1;
    }
    region_id_str = uri.substr(pos + 3);
    DB_WARNING("parse result, uri: %s, region_id: %s", uri.c_str(), region_id_str.c_str());
    return 0;
}

int MyRaftLogStorage::init(braft::ConfigurationManager* config_manager) {
	TimeCost time_cost;
	char log_meta_key[LOG_META_KEY_SIZE];
	_encode_log_meta_key(log_meta_key, LOG_META_KEY_SIZE);
    if (_raft_log_handle == nullptr) {
        DB_FATAL("region_id: %ld raft init without rocksdb handle", _region_id);
        return -1;
    }
    int64_t first_log_index = 1;
    int64_t last_log_index = 0;
    std::string first_log_index_str;
    auto s = _db->get(rocksdb::ReadOptions(), _raft_log_handle, 
            rocksdb::Slice(log_meta_key, LOG_META_KEY_SIZE),
            &first_log_index_str);
    if (!s.ok() && s.IsNotFound()) {
        // new region
        rocksdb::WriteOptions write_option;
        s = _db->put(write_option, _raft_log_handle, 
                rocksdb::Slice(log_meta_key, LOG_META_KEY_SIZE),
                rocksdb::Slice((char*)&first_log_index, sizeof(int64_t)));
        if (!s.ok()) {
            DB_WARNING("region_id: %ld update first_log_index to rocksdb fail, err_msg: %s",
                    _region_id, s.ToString().c_str());
            return -1;
        }
    } else if (!s.ok()) {
        DB_FATAL("region_id: %ld read meta info from rocksdb fail, err_msg: %s",
                _region_id, s.ToString().c_str());
        return -1;
    } else {
        first_log_index = *(int64_t*)first_log_index_str.c_str();
        DB_WARNING("region_id: %ld is old, first_log_index: %ld", _region_id, first_log_index);
    }

    // read log data
    char log_data_key[LOG_DATA_KEY_SIZE];
    _encode_log_data_key(log_data_key, LOG_DATA_KEY_SIZE, first_log_index);
    rocksdb::ReadOptions read_option;
    // LOG使用prefix加速查找，这里不需要total_order_seek来跳过prefix bloom
    read_option.prefix_same_as_start = true;
    read_option.total_order_seek = false;
    read_option.fill_cache = false;
    std::unique_ptr<rocksdb::Iterator> iter(_db->new_iterator(read_option, _raft_log_handle));
    iter->Seek(rocksdb::Slice(log_data_key, LOG_DATA_KEY_SIZE));

    int64_t expect_index = first_log_index;
    // 从first_log_index -> index拿到term_map和config_manager
    // 快照和config需要从最近的快照truncate_prefix
    while (iter->Valid()) {
        rocksdb::Slice key = iter->key();
        int64_t tmp_region_id, tmp_index;
        if (_decode_log_data_key(key, tmp_region_id, tmp_index) != 0) {
            DB_FATAL("region_id: %ld data is corrupted", _region_id);
            return -1;
        }
        if (tmp_region_id != _region_id) {
            DB_FATAL("rocksdb seek param my has problem, region_id: %ld != tmp_region_id: %ld",
                    _region_id, tmp_region_id);
            return -1;
        }
        if (expect_index != tmp_index) {
            rocksdb::Slice value = iter->value();
            LogHead head(value);
            value.remove_prefix(LOG_HEAD_SIZE);
            DB_FATAL("region_id: %ld found a hole, expect_index: %ld, read index: %ld", 
                    _region_id, expect_index, tmp_index);
            return -1;
        }
        rocksdb::Slice value = iter->value();
        if (value.size() < LOG_HEAD_SIZE) {
            DB_FATAL("region_id: %ld index: %ld is corrupted", _region_id, expect_index);
            return -1;
        }
        LogHead head(value);
        // head.index = tmp_index
        if (_term_map.append(braft::LogId(tmp_index, head.term)) != 0) {
            DB_FATAL("region_id: %ld fail to append term_map, index: %ld", _region_id, tmp_index); 
            return -1;
        }
        if (head.type == braft::ENTRY_TYPE_CONFIGURATION) {
            value.remove_prefix(LOG_HEAD_SIZE);
            scoped_refptr<braft::LogEntry> entry(new braft::LogEntry()); 
            entry->id.index = tmp_index;
            entry->id.term = head.term;
            if (_parse_meta(entry.get(), value)) {
                DB_FATAL("region_id: %ld fail to parse meta as index: %ld",
                        _region_id, tmp_index);
                return -1;
            }
            std::string peers_str = "new: ";
            for (size_t i = 0; i < entry->peers->size(); i++) {
                peers_str += (*(entry->peers))[i].to_string() + ",";
            }
            if (entry->old_peers) {
                peers_str += "old: ";
                for (size_t i = 0; i < entry->old_peers->size(); i++) {
                    // to_string: ip:port:idx形式 127.0.0.1:8110:0
                    peers_str += (*(entry->old_peers))[i].to_string() + ",";
                }
            }
            DB_WARNING("region_id: %ld add configuration, index: %ld, term: %ld, peers: %s", 
                    _region_id, tmp_index, head.term, peers_str.c_str());
            braft::ConfigurationEntry conf_entry;
            conf_entry.id = entry->id;
            conf_entry.conf = *(entry->peers);
            if (entry->old_peers) {
                conf_entry.old_conf = *(entry->old_peers);
            }
            config_manager->add(conf_entry);
        }
        last_log_index = tmp_index;
        ++expect_index;
        iter->Next();
    }
    if (!iter->status().ok()) {
        DB_FATAL("region_id: %ld fail to iter rocksdb", _region_id);
        return -1;
    }
    if (last_log_index == 0) {
        last_log_index = first_log_index - 1;
    }
    _first_log_index.store(first_log_index);
    _last_log_index.store(last_log_index);
    DB_WARNING("region_id: %ld, first_log_index: %ld, last_log_index: %ld time_cost: %ld",
        _region_id, _first_log_index.load(), _last_log_index.load(), time_cost.get_time());    
    return 0;
}

braft::LogEntry* MyRaftLogStorage::get_entry(const int64_t index) {
    char buf[LOG_DATA_KEY_SIZE];
    _encode_log_data_key(buf, LOG_DATA_KEY_SIZE, index);
    std::string value;
    auto s = _db->get(rocksdb::ReadOptions(), _raft_log_handle, rocksdb::Slice(buf, LOG_DATA_KEY_SIZE), &value);
    if (!s.ok()) {
        DB_WARNING("region_id: %d get_index: %ld fail, err_msg: %s", _region_id, index, s.ToString().c_str());
        return nullptr;
    }
    if (value.size() < LOG_HEAD_SIZE) {
        DB_FATAL("region_id: %ld log index: %ld is corrupted", _region_id, index);
        return nullptr;
    }
    rocksdb::Slice value_slice(value.data(), value.size());
    LogHead head(value_slice);
    value_slice.remove_prefix(LOG_HEAD_SIZE);
    braft::LogEntry* entry = new braft::LogEntry;
    entry->AddRef();
    entry->type = (braft::EntryType)head.type;
    entry->id = braft::LogId(index, head.term);
    switch(entry->type) {
        case braft::ENTRY_TYPE_DATA:
            entry->data.append(value_slice.data(), value_slice.size());
            break;
        case braft::ENTRY_TYPE_CONFIGURATION:
            if (_parse_meta(entry, value_slice) != 0) {
                entry->Release();
                entry = nullptr;
            }
            break;
        case braft::ENTRY_TYPE_NO_OP:
            if (value_slice.size() != 0) {
                DB_FATAL("region_id: %ld's NO OP DATA size must 0, index: %ld",
                        _region_id, index);
                entry->Release();
                entry = nullptr;
            }
            break;
        default:
            DB_FATAL("unknow entry type, index: %ld region_id: %ld",
                    index, _region_id);
            entry->Release();
            entry = nullptr;
            break;
    }
    return entry;
}

int64_t MyRaftLogStorage::get_term(const int64_t index) {
    int64_t term = 0;
    if (index < _first_log_index.load() || 
            index > _last_log_index.load()) {
        DB_WARNING("region_id: %ld get index is not in [first_log_index, last_log_index], "
                "index: %ld, first_log_index: %ld, last_log_index: %ld",
                _region_id, index, _first_log_index.load(), _last_log_index.load());
        return 0;
    }
    term = _term_map.get_term(index);
    return term;
}

/* 0: success; -1: error */
// entry->data类型是IOBuf
int MyRaftLogStorage::append_entry(const braft::LogEntry* entry) {
    std::vector<braft::LogEntry*> entries;
    entries.push_back(const_cast<braft::LogEntry*>(entry));
    return append_entries(entries, nullptr) == 1 ? 0 : -1;
}

int MyRaftLogStorage::append_entries(const std::vector<braft::LogEntry*>& entries, 
		braft::IOMetric* metric) {
    if (entries.empty()) {
        return 0;
    }
    if (_last_log_index + 1 != entries.front()->id.index) {
        DB_FATAL("region_id: %ld has gap between append entries, _last_log_index: %ld,"
                " entry_log_index: %ld, term: %ld", _region_id, _last_log_index.load(),
                entries.front()->id.index, entries.front()->id.term);
        return -1;
    }
    static thread_local int64_t total_time = 0;
    static thread_local int64_t count = 0;
    TimeCost time_cost;
    ON_SCOPED_EXIT(([&time_cost](){
        total_time += time_cost.get_time();
        count++;
    }));
    SlicePartsVec kv_raftlog_vec;
    kv_raftlog_vec.reserve(entries.size());
    butil::Arena arena;
    for (auto iter = entries.begin(); iter != entries.end(); iter++) {
        if (_build_key_value(kv_raftlog_vec, *iter, arena)) {
            DB_FATAL("Fail to build kv, log_id: %ld, log_term: %ld, region_id: %ld",
                    (*iter)->id.index, (*iter)->id.term, _region_id);
            return -1;
        }
    }
    
    // write to rocksdb
    rocksdb::WriteOptions options;
    rocksdb::WriteBatch batch;
    for (auto it = kv_raftlog_vec.begin(); it != kv_raftlog_vec.end(); it++) {
        batch.Put(_raft_log_handle, it->first, it->second);
    }
    auto s = _db->write(options, &batch);
    if (!s.ok()) {
        DB_FATAL("region_id: %ld write to rocksdb fail, err_msg: %s", _region_id, s.ToString().c_str());
        return -1;
    }

    // udpate term map
    for (auto it = entries.begin(); it != entries.end(); it++) {
        if (_term_map.append((*it)->id) != 0) {
            DB_FATAL("Fail to update term map, region_id: %ld", _region_id);
            _term_map.truncate_prefix(_last_log_index.load());
            return -1;
        }
    }
    _last_log_index.fetch_add(entries.size());
    return entries.size();
}

int MyRaftLogStorage::truncate_prefix(const int64_t first_log_index) {
    if (first_log_index < _first_log_index.load()) {
        return 0;
    }
    DB_WARNING("region_id: %ld truncate prefix to first_log_index: %ld, _first_log_index: %ld",
            _region_id, first_log_index, _first_log_index.load());
    _first_log_index.store(first_log_index);
    if (first_log_index > _last_log_index.load()) {
        _last_log_index.store(first_log_index - 1);
    }
    _term_map.truncate_prefix(first_log_index);
    // CanSet add peer
    char key_buf[LOG_META_KEY_SIZE];
    _encode_log_meta_key(key_buf, LOG_META_KEY_SIZE);
    
    // Write to rocksdb
    rocksdb::WriteOptions write_option;
    auto s = _db->put(write_option, _raft_log_handle, 
            rocksdb::Slice(key_buf, LOG_META_KEY_SIZE),
            rocksdb::Slice((char*)&first_log_index, sizeof(int64_t)));
    if (!s.ok()) {
        DB_FATAL("region_id: %ld update first_log_index: %ld fail, err_msg: %s",
                _region_id, first_log_index, s.ToString().c_str());
        return -1;
    } else {
        DB_FATAL("region_id: %ld update first_log_index: %ld success",
                _region_id, first_log_index);
    }
    // 替换为remove_range 
    char start_key[LOG_DATA_KEY_SIZE];
    _encode_log_data_key(start_key, LOG_DATA_KEY_SIZE, 0);
    char end_key[LOG_DATA_KEY_SIZE];
    _encode_log_data_key(end_key, LOG_DATA_KEY_SIZE, first_log_index);
    s = _db->remove_range(write_option, _raft_log_handle,
            rocksdb::Slice(start_key, LOG_DATA_KEY_SIZE),
            rocksdb::Slice(end_key, LOG_DATA_KEY_SIZE),
            true);
    
    if (!s.ok()) {
        DB_FATAL("region_id: %ld remove range first_log_index: %ld fail, err_msg: %s",
                _region_id, first_log_index, s.ToString().c_str());
        return -1;
    } else {
        DB_FATAL("region_id: %ld remove rangefirst_log_index: %ld success",
                _region_id, first_log_index);
    }
    return 0;
}

int MyRaftLogStorage::truncate_suffix(const int64_t last_log_index) {
    if (last_log_index >= _last_log_index.load()) {
        return 0;
    }
    const uint64_t pre_last_log_index = _last_log_index.load();
    DB_WARNING("region_id: %ld truncate_suffix to last_log_index: %ld",
            _region_id, last_log_index, _last_log_index.load());
    _term_map.truncate_suffix(last_log_index);
    _last_log_index.store(last_log_index);
    std::unique_ptr<char[]> buff(new char[2 * LOG_DATA_KEY_SIZE]); 
    char* data_buff = buff.get();
    
    // Write to rocksdb
    _encode_log_data_key(data_buff, LOG_DATA_KEY_SIZE, last_log_index);
    _encode_log_data_key(data_buff + LOG_DATA_KEY_SIZE, LOG_DATA_KEY_SIZE, pre_last_log_index);
    rocksdb::WriteOptions write_option;
    auto s = _db->remove_range(write_option, _raft_log_handle, 
                rocksdb::Slice(data_buff, LOG_DATA_KEY_SIZE),
                rocksdb::Slice(data_buff + LOG_DATA_KEY_SIZE, LOG_DATA_KEY_SIZE),
                true);   
    if (!s.ok()) {
        DB_FATAL("Fail to write to db, region_id: %ld, err_msg: %s",
                _region_id, s.ToString().c_str());
        return -1;
    }
    return 0;
}

int MyRaftLogStorage::reset(const int64_t next_log_index) {
    DB_WARNING("region_id: %ld reset to next log index: %ld", 
            _region_id, next_log_index);
    truncate_prefix(next_log_index);
    truncate_suffix(next_log_index - 1);
    _term_map.reset();
    return 0;
}

// raft_log: //my_raft_log?id=21
braft::LogStorage* MyRaftLogStorage::new_instance(const std::string& uri) const {
    RocksWrapper* rocksdb = RocksWrapper::get_instance();
    if (rocksdb == nullptr) {
        DB_FATAL("uri: %s init fail, rocksdb not init", uri.c_str());
        return nullptr;
    }
    std::string region_id_str;
    int ret = parse_my_raft_log_uri(uri, region_id_str);
    if (ret) {
        DB_FATAL("parse uri fail, uri: %s", uri.c_str());
        return nullptr;
    }
    int64_t region_id = stol(region_id_str);
    auto raftlog_handle = rocksdb->get_raft_log_handle();
    if (raftlog_handle == nullptr) {
        DB_FATAL("region_id: %ld get raft log handle fail", _region_id);
        return nullptr;
    }
    auto instance = new(std::nothrow) MyRaftLogStorage(region_id, rocksdb, raftlog_handle);
    if (instance == nullptr) {
        DB_FATAL("region_id: %ld new storage fail", _region_id);
    }
    // RaftLogCompactionFilter::get_instance()->update_first_index_map(region_id, 0);
    return instance;
}

int MyRaftLogStorage::_parse_meta(braft::LogEntry* entry, const rocksdb::Slice& value) {
    braft::ConfigurationPBMeta meta;
    if (!meta.ParseFromArray(value.data(), value.size())) {
        DB_FATAL("region_id: %ld fail to parse configuration", _region_id);
        return -1;
    }
    entry->peers = new std::vector<braft::PeerId>;
    for (int i = 0; i < meta.peers_size(); i++) {
        // 从string中转成braft::PeerId
        entry->peers->push_back(braft::PeerId(meta.peers(i)));
    }
    if (meta.old_peers_size() > 0) {
        entry->old_peers = new std::vector<braft::PeerId>;
        for (int i = 0; i < meta.old_peers_size(); i++) {
            entry->old_peers->push_back(braft::PeerId(meta.old_peers(i)));
        }
    }
    return 0;
}

int MyRaftLogStorage::_build_key_value(SlicePartsVec & kv_raftlog_vec, const braft::LogEntry* entry, butil::Arena& arena) {
    rocksdb::SliceParts raftlog_key;
    rocksdb::SliceParts raftlog_value;
    
    // construct key
    void* key_buf = arena.allocate(LOG_DATA_KEY_SIZE);
    if (key_buf == nullptr) {
        DB_FATAL("Fail to allocate mem, region_id: %ld", _region_id);
        return -1;
    }
    _encode_log_data_key(key_buf, LOG_DATA_KEY_SIZE, entry->id.index);
    void* key_slice_mem = arena.allocate(sizeof(rocksdb::Slice));
    if (key_slice_mem == nullptr) {
        DB_FATAL("Fail to allocate mem, region_id: %ld", _region_id);
        return -1;
    }
    new (key_slice_mem) rocksdb::Slice((char*)key_buf, LOG_DATA_KEY_SIZE);
    raftlog_key.parts = (rocksdb::Slice*)key_slice_mem;
    raftlog_key.num_parts = 1;

    // construct value
    LogHead head(entry->id.term, entry->type);
    void* head_buf = arena.allocate(LOG_HEAD_SIZE);
    if (head_buf == nullptr) {
        DB_FATAL("Fail to allocate mem, region_id: %ld", _region_id);
        return -1;
    }
    head.serialize_to(head_buf);

    raftlog_value.parts = nullptr;
    switch(entry->type) {
        case braft::ENTRY_TYPE_DATA:
            raftlog_value.parts = _construct_slice_array(head_buf, entry->data, arena);
            raftlog_value.num_parts = entry->data.backing_block_num() + 1;
            break;
        case braft::ENTRY_TYPE_CONFIGURATION:
            raftlog_value.parts = _construct_slice_array(head_buf, entry->peers, 
                    entry->old_peers, arena);
            raftlog_value.num_parts = (entry->peers ? 2 : 1);
            break;
        case braft::ENTRY_TYPE_NO_OP:
            raftlog_value.parts = _construct_slice_array(head_buf, nullptr, nullptr, arena);
            raftlog_value.num_parts = 1;
            break;
        default:
            DB_FATAL("unknow type: %d, region_id: %ld", entry->type, _region_id);
            return -1;
    }
    if (raftlog_value.parts == nullptr) {
        DB_FATAL("Fail to construct value, region_id: %ld", _region_id);
        return -1;
    }
    kv_raftlog_vec.push_back({raftlog_key, raftlog_value});
    return 0;
}

rocksdb::Slice* MyRaftLogStorage::_construct_slice_array(void* head_buf, const butil::IOBuf& buf, butil::Arena& arena) {
    const size_t block_num = buf.backing_block_num();
    rocksdb::Slice* slices = (rocksdb::Slice*)arena.allocate(sizeof(rocksdb::Slice) * (block_num));
    if (slices == nullptr) {
        DB_FATAL("Fail to allocate mem, region_id: %ld", _region_id);
        return nullptr;
    }
    new (slices) rocksdb::Slice((const char*)head_buf, LOG_HEAD_SIZE);
    for (size_t i = 0; i < block_num; i++) {
        auto block = buf.backing_block(i);
        new (slices + 1 + i) rocksdb::Slice(block.data(), block.size());
    }
    return slices;
}

rocksdb::Slice* MyRaftLogStorage::_construct_slice_array(void* head_buf, const std::vector<braft::PeerId>* peers, 
        const std::vector<braft::PeerId>* old_peers, butil::Arena& arena) {
    rocksdb::Slice* slices = (rocksdb::Slice*)arena.allocate(sizeof(rocksdb::Slice) * (peers ? 2 : 1));
    if (slices == nullptr) {
        DB_FATAL("Fail to allocate mem, region_id: %ld", _region_id);
        return nullptr;
    }
    new (slices) rocksdb::Slice((const char*)head_buf, LOG_HEAD_SIZE);
    if (peers) {
        braft::ConfigurationPBMeta meta;
        meta.mutable_peers()->Reserve(peers->size());
        for (auto it = peers->begin(); it != peers->end(); it++) {
            meta.add_peers(it->to_string());
        }
        if (old_peers) {
            meta.mutable_old_peers()->Reserve(old_peers->size());
            for (size_t i = 0; i < old_peers->size(); i++) {
                meta.add_old_peers((*old_peers)[i].to_string());
            }
        }
        const size_t byte_size = meta.ByteSize();
        void* meta_buf = arena.allocate(byte_size);
        if (!meta_buf) {
            DB_FATAL("Fail to allocate mem, region_id: %ld, byte_size: %ld", _region_id, byte_size);
            return nullptr;
        }
        if (!meta.SerializeToArray(meta_buf, byte_size)) {
            DB_FATAL("Fail to serialize to mem, region_id: %ld", _region_id);
            return nullptr;
        }
        new (slices + 1) rocksdb::Slice((const char*)meta_buf, byte_size);
    }
    return slices;
}
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
