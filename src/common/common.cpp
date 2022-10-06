#include "common/common.h"

#include <butil/files/file_path.h>
#include <butil/strings/string_split.h>
#include <rocksdb/slice.h>

namespace TKV {
int64_t parse_snapshot_index_from_path(const std::string& snapshot_path, bool use_dirname) {
	butil::FilePath path(snapshot_path);	
	std::string tmp_path;
	if (use_dirname) {
		// Last component of dir
		tmp_path = path.DirName().BaseName().value();
	} else {
		tmp_path = path.DirName().value();
	}
	std::vector<std::string> split_vec;
	std::vector<std::string> snapshot_index_vec;
	butil::SplitString(tmp_path, '/',  &split_vec);
	butil::SplitString(split_vec.back(), '_', &snapshot_index_vec);
	int64_t snapshot_index = 0;
	if (snapshot_index_vec.size() == 2) {
		// snapshot_123形式
		snapshot_index = atoll(snapshot_index_vec[1].c_str());
	}
	return snapshot_index;
}

std::string to_hex_str(const std::string& str) {
	return rocksdb::Slice(str).ToString(true /* hex = true */);
}

std::string transfer_to_lower(std::string str) {
    std::transform(str.begin(), str.end(), str.begin(), 
            [](unsigned char c) -> unsigned char { return std::tolower(c); });
    return str;
}

int end_key_compare(rocksdb::Slice key1, rocksdb::Slice key2) {
	if (key1 == key2) {
		return 0;
	}
	if (key1.empty()) {
		return 1;
	}
	if (key2.empty()) {
		return -1;
	}
	return key1.compare(key2);
}

void update_param(const std::string& name, const std::string& value) {
	std::string target;
	if (!google::GetCommandLineOption(name.c_str(), &target)) {
		DB_WARNING("get command line: %s failed", name.c_str());
		return ;
	}
	if (target == value) {
		return ;
	}
	if (google::SetCommandLineOption(name.c_str(), value.c_str()).empty()) {
		DB_WARNING("set command line failed, key: %s, value: %s", name.c_str(), value.c_str());
		return ;
	}
	DB_WARNING("set command line success, key: %s, value: %s", name.c_str(), value.c_str());
}
} // namespace TKV
