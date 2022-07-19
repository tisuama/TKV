#include "raft/rocksdb_file_system_adaptor.h"

namespace TKV {
SstWriterAdaptor::SstWriterAdaptor(int64_t region_id, 
        const std::string& path, const rocksdb::Options& option) 
    : _region_id(region_id)
    , _path(path)
    , _writer(new SstFileWriter(option))
    {}
    
SstWriterAdaptor::~SstWriterAdaptor {
    this->close();
}

int SstWriterAdaptor::open() {
    _region = Store::get_instance()->get_region(_region_id);
    if (!_region) {
        DB_FATAL("open sst file path: %s failed, region_id: %ld not exist",
                _path.c_str(), _region_id);
        return -1;
    }
    _is_meta = _path.find("meta") != std::string::npos;
    std::string path = _path;
    if (!_is_meta) {
        path += std::to_string(_sst_idx);
    }
    auto s = _writer->open(path);
    if (!s.ok()) {
        DB_FATAL("open sst file path: %s failed, err: %s, region_id: %ld",
                path.c_str(), s.ToString().c_str(), _region_id);
        return -1;
    }
    _closed = false;
    DB_WARNING("rocksdb sst writer open path: %s success, region_id: %ld", path.c_str(), _region_id);
    return 0;
}

ssize_t SstWriterAdaptor::write(const butil::IOBuf& data, off_t offset) {
    (void) offset;
    std::string path = _path;
    if (!_is_meta) {
        path += std::to_string(_sst_idx);
    }
    if (_closed) {
        DB_FATAL("write sst file path: %s  failed, file closed, data len: %lu, region_id: %ld",
                path.c_str(), data.size(), _region_id);
        return -1;
    }
    int ret = this->iobuf_to_sst(data);
    if (ret < 0) {
        DB_FATAL("write sst file path: %s failed, recieve invalid data, data len: %lu, region_id: %ld", 
                path.c_str(), data.size(), _region_id);
        return -1;
    }
    
    // 防止分裂失败
    _region->reset_timecost(); 
    _data_size += data.size();
    /* 超过SST_FILE_LENGTH，finish_sst后打开新的SST */
    if (!_is_meta && _writer->file_size() >= SST_FILE_LENGTH) {
        if (!this->finish_sst()) {
            return -1;
        }
        _count = 0;
        ++_sst_idx;
        path = _path + std::string(_sst_idx);
        auto s = _writer->open(path);
        if (!s.ok()) {
            DB_FATAL("open sst file path: %s failed, err: %s, region_id: %ld",
                    path.c_str(), s.ToString().c_str(), _region_id);
            return  -1;
        }
        DB_WARNING("rocksdb sst write, region_id: %ld, path: %s, offset: %lu",
                "file size: %lu, total_count: %lu, data size: %lu", 
                _region_id, path.c_str(), offset, _writer->file_size(), _count, _data_size);
    }
    // 返回写成功的size
    return data.size();
}

int SstWriterAdaptor::iobuf_to_sst(butil::IOBuf data) {
}

ssize_t SstWriterAdaptor::read(butil::IOBuf* portal, off_t offset, size_t size) {
}

ssize_t SstWriterAdaptor::size() {
    DB_FATAL("SstWriterAdaptor::size not impl");
    return -1;
}

bool SstWriterAdaptor::sync() {
    return true;
}

bool SstWriterAdaptor::close() {
    if (_closed) {
        DB_WARNING("file has been closed, path: %s", _path.c_str());
        return true;
    }
    _closed = true;
    if (_is_meta) {
        _region->set_snapshot_meta_size(_data_size);
    } else {
        _region->set_snapshot_data_size(_data_size);
    }
    return this->finish_sst();
}

/* 写SST成功，close并返回 */
bool SstWriterAdaptor::finish_sst() {
    std::string path = _path;
    if (!_is_meta) {
        path += std::to_string(_sst_idx);
    }
    if (_count > 0) {
        DB_WARNING("SstWriterAdaptor finished, path: %s, region_id: %ld, file_size: %lu, total_count: %ld, all_size: %ld",
            path.c_str(), _region_id, _writer->file_size(), _count, _data_size);
        auto s = _writer->finish();
        if (!s.ok()) {
            DB_FATAL("finish sst path: %s failed, err: %s, region_id: %ld",
                    path.c_str(), s.ToString().c_str(), _region_id);
            return false;
        }
    } else {
        bool ret = butil::DeleteFile(butil::FilePath(path), false);
        DB_WARNING("count is 0, delete path: %s, region_id: %ld", path.c_str(), _region_id);
        if (!ret) {
            DB_FATAL("delete sst file path: %s failed, region_id: %ld", path.c_str(), _region_id);
        }
    }
}

ssize_t RocksbReaderAdaptor::write(const butil::IOBuf& data, off_t offset) {
}

ssize_t RocksbReaderAdaptor::read(butil::IOBuf* portal, off_t offset, size_t size) {
}

ssize_t RocksbReaderAdaptor::size() {
}

bool RocksbReaderAdaptor::sync() {
}

bool RocksbReaderAdaptor::close() {
}
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
        // raft InstallSnapshot 超时，说明对方机器卡住了
        if (iter->second.cost.get_time() > 3600 * 1000 * 1000LL &&
                iter->second.count == 1 &&
                (iter->second.ptr->data_context == nullptr ||
                 iter->second.ptr->data_context->offset == 0)) {
            _snapshot.erase(iter);
            DB_WARNING("region_id: %ld snapshot path: %s has hang over one hour, exit now", _region_id, path.c_str());
        } else {
            DB_WARNING("region_id: %ld snapshot path: %s is doing snapshot", _region_id, path.c_str());
            _snapshot[path].count++;
            return false;
        }
    }
    _snapshot_cond.increase();
    // Create new snapshot
    auto region = Store::get_instance()->get_region(_region_id);
    // TODO: lock commit meta index
    _snapshot[path].ptr.reset(new SnapshotContext);
    int64_t data_index = region->get_data_index();
    _snapshot[path].ptr->data_index = data_index;
    _snapshot[path].count++;
    _snapshot[path].cost.reset();
    DB_WARNING("region_id: %ld data_index: %ld, open snapshot path: %s",
            _region_id, data_index, path.c_str());
    return true;
}

void RocksdbFileSystemAdaptor::close_snapshot(const std::string& snapshot_path) {
    DB_WARNING("region_id: %ld close snapshot path: %s", _region_id, path.c_str());
    BAIDU_SCOPED_LOCK(_snapshot_mutex);
    auto iter = _snapshot.find(path);
    if (iter != _snapshot.end()) {
        _snapshot[path].count--;
        if (_snapshot[path].count == 0) {
            _snapshot.erase(iter);
            _snapshot_cond.decrease_broadcast();            
            DB_WARNING("region_id: %ld close snapshot path: %s release", _region_id, path.c_str());
        }
    }
}

void RocksdbFileSystemAdaptor::close(const std::string& path) {
    // TODO: close 
}

braft::FileAdaptor* RocksdbFileSystemAdaptor::open_reader_adaptor(const std::string& path, int flag,
        const ::google::protobuf::Message* file_meta,
        butil::File::Error* e) {
}

braft::FileAdaptor* RocksdbFileSystemAdaptor::open_writer_adaptor(const std::string& path, int flag,
        const ::google::protobuf::Message* file_meta,
        butil::File::Error* e) {
    (void)file_meta;

    auto db = RocksWrapper::get_instance();
    rocksdb::Options options;
    if (is_snapshot_data_file(path)) {
        options = db->get_options(db->get_data_handle());
    } else {
        options = db->get_options(db->get_meta_info_handle());
    }
    options.bottommost_compression = rocksdb::kLZ4Compression;
    options.bottommost_compression_opts = rocksdb::CompressionOptions();
    
    // Create new SstWriterAdaptor
    SstWriterAdaptor* writer = new SstWriterAdaptor(_region_id, path, options);
}
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
