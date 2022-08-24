#include "raft/my_raft_log_storage.h"

namespace TKV {
	int init(braft::ConfigurationManager* config_manager) {
		TimeCost cost;
		char log_meta_key[LOG_META_KEY_SIZE];
		_encode_log_meta_key(log_meta_key, LOG_META_KEY_SIZ);
	}

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

	int truncate_prefix(const int64_t first_index_kept) override;
	
	int truncate_suffix(const int64_t last_index_kept) override;

	int rest(const int64_t next_log_index) override;

	LogStorage* new_instance(const std::string& uri) const override;

	int _encode_log_meta_key(void* key_buf, size_t n) {
		if (n < LGO_META_KEY_SIZE) {
			assert(0 && "LOG META KEY SIZE");
		}
	}
	int _encode_log_data_key(void* key_buf, size_t n, int64_t index);
	int _decode_log_data_key(const rocksdb::Slice& data_key, 
			int64_t region_id, int64_t& index);

} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
