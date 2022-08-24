#pragma once

#include <string>
#include <atomic>

#include <braft/storage.h>
#include <braft/configuration_manager.h>
#include <butil/raw_pack.h>
#include <rocksdb/slice.h>

namespace TKV {
struct LogHead {
    explicit LogHead(const rocksdb::Slice& raw) {
        butil::RawUnpacker(raw.data())
            .unpack64((uint64_t&)term)
            .unpack32((uint32_t&)type);
    }
    
    LogHead(int64_t term, int type): term(term), type(type) { }
    void serialize_to(void* data) {
        butil::RawPacker(data).pack64(term).pack32(type);
    }
    int64_t term;
    int type;
};

class MyRaftLogStorage: public braft::LogStorage {
	std::vector<std::pair<rocksdb::SliceParts, rocksdb::SliceParts>> SlicePartsVec;
public:
	// data format
	// region_id + 0x1 -> _first_log_index
	// region_id + 0x2 + logindex -> LogHead + data
	// data: DATA + configurationPBMeta
	static const size_t LOG_META_KEY_SIZE = sizeof(int64_t) + 1;
    static const size_t LOG_DATA_KEY_SIZE = sizeof(int64_t) + 1 + sizeof(int64_t);	
	static const uint8_t LOG_META_IDENRIFY = 0x1;
	static const uint8_t LOG_DATA_IDENTIFY = 0x2;
	static const size_t LOG_HEAD_SIZE = sizeof(int64_t) + sizeof(int);

	~MyRaftLogStoreage() {
		bthread_mutex_destory(&_mutex);
	}
	MyRaftLogStorage(): _db(nullptr),_raft_log_handle(nullptr) {
		bthread_muterx_init(&_mutex);
	}
	MyRarftLogStorage(int64_t region_id, RocksWrapper* db,
			rocksdb::ColumnFamilyHandle* raft_log_handle)
		: _first_log_index(0)
		, _last_log_index(0)
		, _region_id(region_id)
		, _db(db)
		, _raft_log_handle(raft_log_handle)
	{
		bthread_mutex_inti(&_mutex);
	}

	int init(braft::ConfigurationManager* config_manager) override;

	int64_t first_log_index() override {
		return _first_log_index.load(std::memory_order_relaxed);
	}

	int64_t last_log_index() override {
		return _last_log_index.load(std::memory_order_relaxed);
	}

	braft::LogEntry* get_entiry(const int64_t index) override;

	int64_t get_term(const int64_t index) override;

	int append_entry(const braft::LogEntry* entry) override;
	int append_entries(const std::vector<braft::LogEntry*>& entries, 
			braft::IOMetric* metric) override;

	int truncate_prefix(const int64_t first_log_index) override;
	
	int truncate_suffix(const int64_t last_log_index) override;

	int rest(const int64_t next_log_index) override;

	LogStorage* new_instance(const std::string& uri) const override;

private:
	int _encode_log_meta_key(void* key_buf, size_t n) {
        if (n < LOG_META_KEY_SIZE) {
            DB_WARNING("region_id: %ld key buf is not enough", _region_id);
            return -1;
        }
        uint64_t region_field = KeyEncoder::to_endian_u64(KeyEncoder::encode_i64(_region_id));
        memcpy(key_buf, (char*)&region_field, sizeof(int64_t));
        memcpy((char*)key_buf + sizeof(uint64_t), &LOG_META_IDENRIFY, 1);
        return 0;
    }

	int _encode_log_data_key(void* key_buf, size_t n, int64_t index) {
        if (n < LOG_DATA_KEY_SIZE) {
            DB_WARNING("region_id: %ld key buf is not enough", _region_id);
            return -1;
        }

        uint64_t region_field = KeyEncoder::to_endian_u64(KeyEncoder::encode_i64(_region_id));
        memcpy(key_buf, (char*)&region_field, sizeof(int64_t));
        memcpy((char*)key_buf + sizeof(uint64_t), &LOG_DATA_IDENTIFY, 1);
        uint64_t index_tmp = KeyEncoder::to_endian_u64(KeyEncoder::encode_i64(index));
        memcpy((char*)key_buf + sizeof(uint64_t) + 1, (char*)&index_tmp, sizeof(int64_t));
        return 0; 
    }

	int _decode_log_data_key(const rocksdb::Slice& data_key, 
			int64_t region_id, int64_t& index) {
        if (data_key.size() != LOG_DATA_KEY_SIZE) {
            DB_WARNING("region_id: %ld log data is corrupted", _region_id);
            return -1;
        }
        uint64_t region_field = *(uint64_t*)data_key.data();
        region_id = KeyEncoder::decode_i64(KeyEncoder::to_endian_u64(region_id));
    }

    std::atomic<int64_t> _first_log_index;
    std::atomic<int64_t> _last_log_index;
    int64_t _region_id;

    RocksWrapper* _db;
    rocksdb::ColumnFamilyHandle* _raft_log_handle;
    bool is_binlog_region {false};

    IndexTermMap _term_map;
    bthread_mutex_t _mutex;
};
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
