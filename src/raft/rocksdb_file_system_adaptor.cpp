#include "common/common.h"
#include "raft/rocksdb_file_system_adaptor.h"
#include "store/meta_writer.h"
#include "store/store.h"

#include <braft/util.h>
#include <braft/fsync.h>

namespace TKV {
SstWriterAdaptor::SstWriterAdaptor(int64_t region_id, 
        const std::string& path, const rocksdb::Options& option) 
    : _region_id(region_id)
    , _path(path)
    , _writer(new SstFileWriter(option))
    {}
    
SstWriterAdaptor::~SstWriterAdaptor() {
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
        // 此SST是否有有效数据
        _count = 0;
        ++_sst_idx;
        path = _path + std::to_string(_sst_idx);
        // 重新打开新的sst
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
            key.data_ = static_cast<const char*>(data.fetch(key_buf, key_size));
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
        size_t value_size;
        std::unique_ptr<char[]> big_value_buf;
        nbytes = data.cutn((void*)&value_size, sizeof(size_t));
        if (nbytes < sizeof(size_t)) {
            DB_FATAL("read value size from data failed, region_id: %ld", _region_id);
            return -1;
        }
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

ssize_t SstWriterAdaptor::read(butil::IOPortal* portal, off_t offset, size_t size) {
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
    return true;
}

PosixFileAdaptor::~PosixFileAdaptor() {
    this->close();
}

int PosixFileAdaptor::open(int flag) {
    flag &= (~O_CLOEXEC);
    _fd = ::open(_path.c_str(), flag, 0644);
    DB_WARNING("PosixFileAdaptor read path: %s, _fd: %d", _path.c_str(), _fd);
    if (_fd <= 0) {
        return -1;
    }
    return 0;
}

ssize_t PosixFileAdaptor::write(const butil::IOBuf& data, off_t offset) {
    // braft/util.cpp
    DB_WARNING("PosixFileAdaptor file path: %s, _fd: %d, offset: %ld", _path.c_str(), _fd, offset);
    ssize_t ret = braft::file_pwrite(data, _fd, offset);
    return ret;
}
ssize_t PosixFileAdaptor::read(butil::IOPortal* portal, off_t offset, size_t size) {
    DB_WARNING("PosixFileAdaptor file path: %s, _fd: %d, offset: %ld", _path.c_str(), _fd, offset);
    return braft::file_pread(portal, _fd, offset, size);
}

ssize_t PosixFileAdaptor::size() {
    off_t sz = lseek(_fd, 0, SEEK_END);
    DB_WARNING("PosixFileAdaptor file path: %s, _fd: %d, size: %ld", _path.c_str(), _fd, sz);
    return ssize_t(sz);
}

bool PosixFileAdaptor::sync() {
    return braft::raft_fsync(_fd) == 0;
}

bool PosixFileAdaptor::close() {
    if (_fd > 0) {
        bool res = ::close(_fd) == 0;
        _fd = -1;
        DB_WARNING("PosixFileAdaptor file path: %s, _fd: %d close", _path.c_str(), _fd);
        return res;
    }
    return true;
}


bool PosixDirReader::is_valid() const {
    return _dir_reader.IsValid();
}

bool PosixDirReader::next() {
    bool ret = _dir_reader.Next();
    // 过滤掉..文件
    while (ret && (strcmp(name(), ".") == 0 || strcmp(name(), "..") == 0)) {
        ret = _dir_reader.Next();
    }
    return ret;
}

const char* PosixDirReader::name() const {
    DB_WARNING("PosixDirReader name: %s", _dir_reader.name());
    return _dir_reader.name();
}

ssize_t RocksdbReaderAdaptor::write(const butil::IOBuf& data, off_t offset) {
    (void)offset;
    (void)data;
    DB_FATAL("RocksdbReaderAdaptor::write not impl");
    return -1;
}

// Called by braft::FileService::get_file
ssize_t RocksdbReaderAdaptor::read(butil::IOPortal * portal, off_t offset, size_t size) {
    if (_closed) {
        DB_FATAL("rocksdb reader has been closed, region_id: %ld, offset: %ld",
                _region_id, offset);
        return -1;
    }
    if (offset < 0) {
        DB_FATAL("region_id: %ld ret error, offset: %ld", _region_id, offset);
        return -1;
    }
    
    // Start read file 
    TimeCost time_cost;
    // _context: snapshot_ctx
    IteratorContext* iter_context = _context->data_context;
    if (_is_meta_reader) {
        iter_context = _context->meta_context;
    } else if (!_context->need_copy_data) {
        iter_context->have_done = true;
        DB_WARNING("region_id: %ld need not copy data, time cost: %ld",
                _region_id, time_cost.get_time());
        return 0;
    }
    // iter_context->offset: 已经读的offset
    // offset: 这次要读的offset
    // _last_offset: 上次读完的offset
    if (offset > iter_context->offset) {
        DB_FATAL("region_id: %ld retry last_offset, offset bigger fail "
            "time cost: %ld, last_offset: %ld, off: %lu, ctx->off: %lu, size: %lu",
            _region_id, time_cost.get_time(), _last_offset, offset, iter_context->offset, size);
        return -1;
    }
    if (offset < iter_context->offset) {
        // 缓存的上次读的offset，此次可以恢复
        if (_last_offset == offset) {
            *portal = _last_package;
            DB_FATAL("[last_offset = offset]: region_id: %ld retry last_offset success, time_cost: %ld"
                    "offset: %lu, ctx->offset: %lu, size: %lu, ret_size: %lu",
                    _region_id, time_cost.get_time(), offset, iter_context->offset, size, _last_package.size());
            return _last_package.size();
        }
        DB_FATAL("[last_offset != offset]: region_id: %ld retry last_offset fail time_cost: %ld"
                "offset: %lu, ctx->offset: %lu, size: %lu, ret_size: %lu",
                _region_id, time_cost.get_time(), offset, iter_context->offset, size, _last_package.size());
        return -1;
    }
    
    // 开始读取 
    size_t count = 0;
    int64_t key_num = 0;
    auto meta_writer = MetaWriter::get_instance();
    std::string log_index_prefix = meta_writer->log_index_key_prefix(_region_id);
    std::string region_info_key = meta_writer->region_info_key(_region_id);
    //BRAFT限制最大size为raft_max_byte_count_per_rpc = 128K
    while (count < size) {
        if (!iter_context->iter->Valid() 
            || !iter_context->iter->key().starts_with(iter_context->prefix)) {
            iter_context->have_done = true;   
            DB_WARNING("region_id: %ld snapshot read over, total_size: %ld", _region_id, iter_context->offset);
            auto region = Store::get_instance()->get_region(_region_id);
            if (iter_context->is_meta_sst) {
                region->set_snapshot_meta_size(iter_context->offset);
            } else {
                region->set_snapshot_data_size(iter_context->offset);
            }
            break;
        }
        int64_t read_size = 0;
        read_size += serialize_to_iobuf(portal, iter_context->iter->key());
        read_size += serialize_to_iobuf(portal, iter_context->iter->value());
        // returned value
        count += read_size;
        // 统计本次读了多少行
        key_num++;
        // 统计总共读了多少行
        _num_lines++;
        iter_context->offset += read_size;
        iter_context->iter->Next();
    }
    DB_WARNING("region_id: %ld read done, count: %ld, key_num: %ld, time_cost: %ld"
            ", offset: %lu, size: %lu, last_offset: %lu, last_count: %lu",
            _region_id, count, key_num, time_cost.get_time(), offset, size, _last_offset, _last_package.size());
    _last_offset = offset;
    // 缓存一份数据防止网络失败
    _last_package = *portal;
    // count等于0时BRAFT设置EOF
    return count;
}

ssize_t RocksdbReaderAdaptor::size() {
    IteratorContext* ctx = _context->data_context;
    if (_is_meta_reader) {
        ctx = _context->meta_context;
    }
    // 和IteratorContext->done啥关系
    if (ctx->have_done) {
        return ctx->offset;
    }
    return std::numeric_limits<ssize_t>::max();
}

bool RocksdbReaderAdaptor::sync() {
    return true;
}

bool RocksdbReaderAdaptor::close() {
    if (_closed) {
        DB_WARNING("file has been closed, region_id: %ld, num_lines: %ld, path: %s",
                _region_id, _num_lines, _path.c_str());
        return true;
    }
    _fs->close(_path);
    _closed = true;
    return true;
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
    return butil::CreateDirectoryAndGetError(dir, error, create_parnet_directories);
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
    // 根据文件后缀判断是Meta还是Data
    if (!is_snapshot_data_file(path) && !is_snapshot_meta_file(path)) {
        PosixFileAdaptor* adaptor = new PosixFileAdaptor(path);
        int ret = adaptor->open(flag);
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

bool RocksdbFileSystemAdaptor::open_snapshot(const std::string& path) {
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
    
    region->commit_meta_lock();
    _snapshot[path].ptr.reset(new SnapshotContext);
    int64_t data_index = region->get_data_index();
    _snapshot[path].ptr->data_index = data_index;
    region->commit_meta_unlock();

    _snapshot[path].count++;
    _snapshot[path].cost.reset();
    DB_WARNING("region_id: %ld data_index: %ld, open snapshot path: %s",
            _region_id, data_index, path.c_str());
    return true;
}

void RocksdbFileSystemAdaptor::close_snapshot(const std::string& path) {
    DB_WARNING("region_id: %ld close snapshot path: %s", _region_id, path.c_str());
    BAIDU_SCOPED_LOCK(_snapshot_mutex);
    auto iter = _snapshot.find(path);
    if (iter != _snapshot.end()) {
        _snapshot[path].count--;
        if (_snapshot[path].count == 0) {
            _snapshot.erase(iter);
            _snapshot_cond.decrease_broadcast();            
            DB_WARNING("region_id: %ld close snapshot path: %s success and release", _region_id, path.c_str());
        }
    }
}

void RocksdbFileSystemAdaptor::close(const std::string& path) {
    size_t len = path.size();
    if (is_snapshot_data_file(path)) {
        len -= SNAPSHOT_DATA_FILE_WITH_SLASH.size();
    } else {
        CHECK(is_snapshot_meta_file(path));
        len -= SNAPSHOT_META_FILE_WITH_SLASH.size();
    }
    const std::string snapshot_path = path.substr(0, len);

    BAIDU_SCOPED_LOCK(_snapshot_mutex);
    auto iter = _snapshot.find(snapshot_path);
    if (iter == _snapshot.end()) {
        DB_FATAL("no snapshot found when close reader, path: %s, region_id: %ld",
                path.c_str(), _region_id);
        return ;
    }
    // set reading = false
    auto& snapshot_ctx = iter->second;
    if (is_snapshot_data_file(path) && snapshot_ctx.ptr->data_context != nullptr) {
        DB_WARNING("read snapshot data file close, path: %s", path.c_str());
        snapshot_ctx.ptr->data_context->reading = false;
    } else if (snapshot_ctx.ptr->meta_context != nullptr) {
        DB_WARNING("read snapshot meta file close, path: %s", path.c_str());
        snapshot_ctx.ptr->meta_context->reading = false;
    }
}

braft::FileAdaptor* RocksdbFileSystemAdaptor::open_reader_adaptor(const std::string& path, int flag,
        const ::google::protobuf::Message* file_meta,
        butil::File::Error* e) {
    TimeCost time_cost;
    (void) file_meta;
    std::string prefix;
    std::string upper_bound;
    size_t len = path.size();
    if (is_snapshot_data_file(path)) {
        len -= SNAPSHOT_DATA_FILE.size();
        MutableKey key;
        key.append_i64(_region_id);
        // prefix: region_id
        prefix = key.data();
        key.append_u64(UINT64_MAX);
        // key = upper_bound: region_id + UINT64_MAX
        upper_bound = key.data();
    } else {
        len -= SNAPSHOT_META_FILE.size();
        prefix = MetaWriter::get_instance()->meta_info_prefix(_region_id);
    }
    const std::string snapshot_path = path.substr(0, len - 1);
    SnapshotContextPtr snapshot_ctx = get_snapshot(snapshot_path);
    if (snapshot_ctx == nullptr) {
        DB_FATAL("snapshot not found, path: %s, region_id: %ld", snapshot_path.c_str(), _region_id);
        if (e) {
            *e = butil::File::FILE_ERROR_FAILED;
        }
        return nullptr;
    }

    bool is_meta_reader = false;
    IteratorContext* iter_context = nullptr;
    if (is_snapshot_data_file(path)) {
        is_meta_reader = false;
        iter_context = snapshot_ctx->data_context;
        // first open snapshot file
        // Set data_context
        if (iter_context == nullptr) {
            iter_context = new IteratorContext;
            iter_context->prefix = prefix;
            iter_context->is_meta_sst = false;
            iter_context->upper_bound = upper_bound;
            iter_context->upper_bound_slice = upper_bound;

            // rocksdb option
            rocksdb::ReadOptions read_option;
            read_option.snapshot = snapshot_ctx->snapshot;
            read_option.total_order_seek = true;
            read_option.fill_cache = false;
            // upper_bound: region_id{UINT64_MAX}
            read_option.iterate_upper_bound = &iter_context->upper_bound_slice;
            rocksdb::ColumnFamilyHandle* cf_handle = RocksWrapper::get_instance()->get_data_handle();
            iter_context->iter.reset(RocksWrapper::get_instance()->new_iterator(read_option, cf_handle));
            // Seek: first key at or past the target
            iter_context->iter->Seek(prefix);

            braft::NodeStatus status;
            auto region = Store::get_instance()->get_region(_region_id);
            region->get_node_status(&status);
            int64_t peer_next_index = 0;
            // 根据Peer状态和data_index判断是否需要复制数据
            // addpeer在unstable里，peer_next_index = 0就会走复制流程
            for (auto iter : status.stable_followers) {
                auto& peer = iter.second;
                DB_WARNING("region_id: %ld peer: %s installing_snapshot: %d, next_index: %ld",
                       _region_id, iter.first.to_string().c_str(), peer.installing_snapshot, peer.next_index); 
                if (peer.installing_snapshot) {
                    peer_next_index = peer.next_index;
                    break;
                }
            }
            if (snapshot_ctx->data_index < peer_next_index) {
                // 当前leader的log_index小于peer需要的index时无法拷贝数据
                snapshot_ctx->need_copy_data = false;
            }
            snapshot_ctx->data_context = iter_context;
            DB_WARNING("region_id: %ld open reader, data_index: %ld, peer_next_index: %ld, path: %s, time_cost: %ld",
                    _region_id, snapshot_ctx->data_index, peer_next_index, path.c_str(), time_cost.get_time());
        }
    }
    
    int64_t applied_index = 0;
    int64_t data_index = 0;
    int64_t snapshot_index = 0;
    if (is_snapshot_meta_file(path)) {
        is_meta_reader = true;
        iter_context = snapshot_ctx->meta_context;
        // Set meta_context 
        if (iter_context == nullptr) {
            iter_context = new IteratorContext;
            iter_context->prefix = prefix;
            iter_context->is_meta_sst = true;

            // rocksdb options
            rocksdb::ReadOptions read_option;
            read_option.snapshot = snapshot_ctx->snapshot;
            // iter over same prefix 
            read_option.prefix_same_as_start = true;
            read_option.total_order_seek = false;
            read_option.fill_cache = false;
            rocksdb::ColumnFamilyHandle* cf_handle = RocksWrapper::get_instance()->get_meta_info_handle();
            iter_context->iter.reset(RocksWrapper::get_instance()->new_iterator(read_option, cf_handle));
            iter_context->iter->Seek(prefix);
            snapshot_ctx->meta_context = iter_context;
            snapshot_index = parse_snapshot_index_from_path(path, true);
            iter_context->snapshot_index = snapshot_index;
            MetaWriter::get_instance()->read_applied_index(_region_id, read_option, &applied_index, &data_index);
            iter_context->applied_index = std::max(iter_context->snapshot_index, applied_index);
        }
    }
    if (iter_context->reading) {
        DB_WARNING("snapshot reader is busy, path: %s, region_id: %ld", path.c_str(), _region_id);
        if (e != nullptr) {
            *e = butil::File::FILE_ERROR_FAILED;
        }
        return nullptr;
    }
    iter_context->reading = true;
    // init a reader
    auto reader = new RocksdbReaderAdaptor(_region_id, path, this, snapshot_ctx, is_meta_reader);
    // open this reader
    reader->open();
    DB_WARNING("region_id: %ld open reader: %s, snapshot_index: %ld, applied_index: %ld, data_index: %ld, time cost: %ld",
            _region_id, path.c_str(), snapshot_index, applied_index, data_index, time_cost.get_time());
    return reader;
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

SnapshotContextPtr RocksdbFileSystemAdaptor::get_snapshot(const std::string& path) {
    BAIDU_SCOPED_LOCK(_snapshot_mutex);
    auto iter = _snapshot.find(path);
    if (iter != _snapshot.end()) {
        return iter->second.ptr;
    }
    return nullptr;
}
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
