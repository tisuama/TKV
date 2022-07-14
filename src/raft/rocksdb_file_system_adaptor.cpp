#include "raft/rocksdb_file_system_adaptor.h"

namespace TKV {
bool RocksdbFileSystemAdaptor::delete_file(const std::string& path, bool recursive) {
    butil::FilePath file_path(path);
    return butil::DeleteFile(file_path, recursive);
}

bool RocksdbFileSystemAdaptor::rename(const std::string& old_path, const std::string& new_path) {
    return ::rename(old_path.c_str(), new_path.c_str()) == 0;
}

bool RocksdbFileSystemAdaptor::link(const std::string& old_path, const std::string& new_path) {
    return ::link(old_path.c_str(), new_path.c_str()) == 0;
}

bool RocksdbFileSystemAdaptor::create_directory(const std::string& path, 
    butil::File::Error* error, 
    bool create_parnet_directories) {
    butil::FilePath dir(path);
    return butil::CreateDirecotryAndGetError(dir, error, create_parnet_directories);
}

bool RocksdbFileSystemAdaptor::path_exists(const std::string& path) {
    butil::FilePath file_path(path);
    return butil::PathExists(file_path);
}

bool RocksdbFileSystemAdaptor::directory_exists(const std::string& path) {
    butil::FilePath file_path(path);
    return butil::DirectoryExists(file_path);
}

braft::DirReader* RocksdbFileSystemAdaptor::directory_reader(const std::string& path) {
    return new PosixDirReader(path.c_str());
}

braft::FileAdaptor* RocksdbFileSystemAdaptor::open(const std::string& path, int flag,
    const ::google::protobuf::Message* file_meta,
    butil::File::Error* e) {
}

bool RocksdbFileSystemAdaptor::open_snapshot(const std::string& snapshot_path) {
    DB_WARNING("region_id: %ld lock snapshot mutex before open snapshot", _region_id);
    BAIDU_SCOPED_LOCK(_snapshot_mutex);
    auto iter = _snapshot.find(path);
    if (iter != _snapshot.end()) {
        // raft InstallSnapshot 超时
        if (iter->second.cost.get_time() > 3600 * 1000 * 1000LL &&
                iter->second.count == 1 &&
                (iter->second.ptr->data_context == nullptr ||
                 iter->second.ptr->data_context->offset == 0)) {
            _snapshot.erase(iter);
            DB_WARNING("region_id: %ld snapshot path: %s has hang over one hour", _region_id, path.c_str());
        } else {
            DB_WARNING("region_id: %ld snapshot path: %s is busy", _region_id, path.c_str());
            _snapshot[path].count++;
            return false;
        }
    }
    _snapshot_cond.increase();
    // TODO: Create new rocksdb snapshot 
}

void RocksdbFileSystemAdaptor::close_snapshot(const std::string& snapshot_path) {
}

void RocksdbFileSystemAdaptor::close(const std::string& path) {
}

} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
