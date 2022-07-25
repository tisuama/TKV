#include "common/common.h"

#include <butil/files/file_path.h>
#include <butil/strings/string_split.h>

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
} // namespace TKV
