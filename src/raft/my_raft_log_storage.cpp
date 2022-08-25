#include "raft/my_raft_log_storage.h"

namespace TKV {
int MyRaftLogStorage::init(braft::ConfigurationManager* config_manager) {
	TimeCost time_cost;
	char log_meta_key[LOG_META_KEY_SIZE];
	_encode_log_meta_key(log_meta_key, LOG_META_KEY_SIZE);
    if (_raft_log_handle == nullptr) {
        DB_FATAL("region_id: %ld raft init without rocksdb handle", _region_id);
        return -1;
    }
    int64_t first_log_index = -1;
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
        first_log_index = *(int64_t*)first_log_index.c_str();
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
            std::unique_ptr<braft::LogEntry> entry(new braft::LogEntry()); 
            entry->id.index = tmp_index;
            entry->id.term = head.term;
            if (_parse_meta(entry, value)) {
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

int64_t MyRaftLogStorage::first_log_index() {
	return _first_log_index.load(std::memory_order_relaxed);
}

int64_t MyRaftLogStorage::last_log_index() {
	return _last_log_index.load(std::memory_order_relaxed);
}

braft::LogEntry* MyRaftLogStorage::get_entry(const int64_t index) {
}

int64_t MyRaftLogStorage::get_term(const int64_t index) {
}

int MyRaftLogStorage::append_entry(const braft::LogEntry* entry) {
}

int MyRaftLogStorage::append_entries(const std::vector<braft::LogEntry*>& entries, 
		braft::IOMetric* metric) {
}

int MyRaftLogStorage::truncate_prefix(const int64_t first_log_index) {
}

int MyRaftLogStorage::truncate_suffix(const int64_t last_log_index) {
}

int MyRaftLogStorage::rest(const int64_t next_log_index) {
}

LogStorage* MyRaftLogStorage::new_instance(const std::string& uri) {
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
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
