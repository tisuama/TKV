#include "raft/rocksdb_file_system_adaptor.h"
#include "store/meta_writer.h"

#include <braft/util.h>
#include <braft/fsync.h>

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
    std::string region_info_key = MetaWriter::get_instance()->region_info_key(_region_id);
    std::string applied_index_key = MetaWriter::get_instance()->applied_index_key(_region_id);
    char key_buf[1024];
    char value_buf[10 * 1024];
    while (!data.empty()) {
        size_t key_size = 0;
        size_t nbytes = data.cutn((void*)&key_size, sizeof(size_t));
        if (nbytes < sizeof(size_t)) {
            DB_FATAL("read key size from data failed, region_id: %ld", _region_id);
            return -1;
        }
        rocksdb::Slice key;
        std::unique_ptr<char[]> big_key_buf;
        if (key_size <= sizeof(key_buf)) {
            key.data_ = static_cast<const char*>(data.fetch(key_buf, key_sie));
        } else {
            big_key_buf.reset(new char[key_size]);
            key.data_ = static_cast<const char*>(data.fetch(big_key_buf.get(), key_size));
        }
        key.size_ = key_size;
        if (key.data_ == nullptr) {
            DB_FATAL("read key from data failed, region_id: %ld, key_size: %ld",
                    _region_id, key_size);
            return -1;
        }
        data.pop_front(key_size);

        rocksdb::Slice value;
        std::unique_ptr<char[]> big_value_buf;
        nbytes = data.cutn((void*)&value_size, sizeof(size_t));
        if (nbytes < sizeof(size_t)) {
            DB_FATAL("read value size from data failed, region_id: %ld", _region_id);
            return -1;
        }
        size_t value_size = 0;
        if (value_size <= sizeof(value_buf)) {
            value.data_ = static_cast<const char*>(data.fetch(value_buf, value_size)); 
        } else {
            big_value_buf.reset(new char[value_size]); 
            value.data_ = static_cast<const char*>(data.fetch(big_value_buf.get(), value_size));
        }
        value.size_ = value_size;
        if (value.data_ == nullptr) {
            DB_FATAL("read value from data failed, region_id: %ld", _region_id);
            return -1;
        }
        data.pop_front(value_size);

        _count++;
        auto s = _writer->put(key, value);
        if (!s.ok()) {
            DB_FATAL("write sst failed, err: %s, region_id: %ld", 
                    s.ToString().c_str(), _region_id);
            return -1;
        }
    }
    return 0;
}

ssize_t SstWriterAdaptor::read(butil::IOBuf* portal, off_t offset, size_t size) {
    DB_FATAL("SstWriterAdaptor::read not impl");
    return -1;
}

ssize_t SstWriterAdaptor::size() {
    DB_FATAL("SstWriterAdaptor::size not impl");
    return -1;
}

bool SstWriterAdaptor::sync() {
    // Already sync
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

PosixFileAdaptor::~PosixFileAdaptor() {
    this->close();
}

PosixFileAdaptor::open(int flag) {
    flag &= (~O_CLOEXEC);
    _fd = ::open(_path.c_str(), flag, 0644);
    if (_fd <= 0) {
        return -1;
    }
    return _fd;
}

ssize_t PosixFileAdaptor::write(const butil::IOBuf& data, off_t offset) }
    ssize_t ret = braft::file_pwrite(data, _fd, offset);
    return ret;
}

ssize_t PosixFileAdaptor::read(butil::IOBuf* portal, off_t offset, size_t size) {
    return braft::file_pread(portal, _fd, offset, size);
}

ssize_t PosixFileAdaptor::size() {
    off_t sz = lseek(_fd, 0, SEEK_END);
    return ssize_t(sz);
}

bool PosixFileAdaptor::sync() {
    return braft::raft_fsync(_fd) == 0;
}

bool PosixFileAdaptor::close() {
    if (_fd > 0) {
        bool res = ::close(_fd) == 0;
        _fd = -1;
        return res;
    }
    return true;
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
    // 根据文件后缀判断
    if (!is_snapshot_data_file(path) && !is_snapshot_meta_file(path)) {
        PosixFileAdaptor* adaptor = new PosixFileAdaptor(path);
        int ret = adator->open(flag);
        if (ret) {
            *e = butil::File::OSErrorToFileError(errno);
            delete adaptor;
            return nullptr;
        }
        DB_WARNING("open file: %s, region_id: %ld, flag: %d", path.c_str(), _region_id, flag & O_WRONLY);
        return adaptor;
    }
    bool is_write = (O_WRONLY & flag);
    if (is_write) {
        return this->open_writer_adaptor(path, flag, file_meta, e);
    }
    return this->open_reader_adaptor(path, flag, file_meta, e);
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
    int ret = writer->open();
    if (ret) {
        if (e) {
            *e = butil::File::FILE_ERROR_FAILED;
        }
        delete writer;
        return nullptr;
    }
    DB_WARNING("open for write file path: %s, region_id: %ld", path.c_str(), _region_id);
    return writer;
}
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
