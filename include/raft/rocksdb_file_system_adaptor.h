#pragma once
#include <map>
#include <braft/file_system_adaptor.h>
#include "engine/rocks_wrapper.h"
#include "engine/sst_file_writer.h"


namespace TKV {
const std::string SNAPSHOT_DATA_FILE = "region_data_snapshot.sst";
const std::string SNAPSHOT_META_FILE = "region_meta_snapshot.sst";
const std::string SNAPSHOT_DATA_FILE_WITH_SLASH = "/" + SNAPSHOT_DATA_FILE;
const std::string SNAPSHOT_META_FILE_WITH_SLASH = "/" + SNAPSHOT_META_FILE;
const size_t SST_FILE_LENGTH = 128 * 1024 * 1024;

class RocksdbFileSystemAdaptor;
class Region;
typedef std::shared_ptr<Region> SmartRegion;

struct IteratorContext {
    bool    reading = false;
    bool    is_meta_sst = false;
    bool    done = false;
    std::string prefix;
    std::string upper_bound;
    rocksdb::Slice upper_bound_slice;
    std::unique_ptr<rocksdb::Iterator> iter;

    int64_t offset = 0;
    int64_t snapshot_index = 0;
    int64_t applied_index = 0;
};

struct SnapshotContext {
    SnapshotContext()
        : snapshot(RocksWrapper::get_instance()->get_snapshot()) 
    {}
    ~SnapshotContext() {
        if (snapshot) {
            RocksWrapper::get_instance()->release_snapshot(snapshot);
        }
        if (data_context) {
            delete data_context;
        }
        if (meta_conext) {
            delete meta_conext;
        }
    }

    const rocksdb::Snapshot* snapshot = nullptr;
    IteratorContext* data_context = nullptr;
    IteratorContext* meta_conext = nullptr;
    int64_t data_index = 0;
    bool    need_copy_data = true;
};

typedef std::shared_ptr<SnapshotContext> SnapshotContextPtr;

// Writer and Reader
class SstWriterAdaptor: public braft::FileAdaptor {
friend class RocksdbFileSystemAdaptor;
public:
    virtual ssize_t write(const butil::IOBuf& data, off_t offset) override;
    virtual ssize_t read(butil::IOBuf* portal, off_t offset, size_t size) override;
    virtual ssize_t size() override;
    virtual bool sync() override;
    virtual bool close() override;

    SstWriterAdaptor(int64_t region_id, const std::string& path, const rocksdb::Options& option);

private:
    bool filish_sst();
    int iobuf_to_sst(butil::IOBuf data);
    int64_t _region_id;
    SmartRegion _regin_ptr;
    std::string _path;
    int _sst_idx = 0;
    size_t _count = 0;
    size_t _data_size = 0;
    bool   _closed = true;
    bool   _is_meta = false;
    std::unique_ptr<SstFileWriter> _writer; 
};

class PosixDirReader: public braft::DirReader {
    friend class RocksdbFileSystemAdaptor;
public:
    virtual ~PosixDirReader() { }
    virtual bool is_valid() const override;
    virtual bool next() override;
    virtual const char* name() const override;
    PosixDirReader(const std::string& path)
        : _dir_reader(path.c_str()) 
    {}
    
private:
    butil::DirReaderPosix _dir_reader;
};

class PosixFileAdaptor: public braft::FileAdaptor {
    friend class RocksdbFileSystemAdaptor;
public:
    virtual ~PosixFileAdaptor();
    int open(int flag);
    virtual ssize_t write(const butil::IOBuf& data, off_t offset) override;
    virtual ssize_t read(butil::IOBuf* portal, off_t offset, size_t size) override;
    virtual ssize_t size() override;
    virtual bool sync() override;
    virtual bool close() override;
    
    PosixFileAdaptor(const std::string& path)
        : _path(path)
        , _fd(-1)
    {}
private:
    std::string _path;
    int _fd;
};


// 从rocksdb中读取region的全部信息，包括data和meta信息
class RocksdbReaderAdaptor: public braft::FileAdaptor {
    friend class RocksdbFileSystemAdaptor;
public:
    virtual ~RocksdbReaderAdaptor();
    virtual ssize_t read(butil::IOPortal* portal, off_t offset, size_t size) override;
    virtual ssize_t size() override;
    virtual bool close() override;
    virtual ssize_t write(const butil::IOBuf& data, off_t offset) override;
    virtual bool sync() override;

    void open() { _closed = false; }

    RocksdbReaderAdaptor(int64_t region_id,
            const std::string& path,
            RocksdbFileSystemAdaptor* fs,
            SnapshotContextPtr context,
            bool is_meta_reader);
private:
    int64_t serialize_to_iobuf(butil::IOPortal* portal, const rocksdb::Slice& key) {
        if (portal != nullptr) {
            portal->append((void*)&key.size_, sizeof(size_t));
            portal->append((void*)key.data_, key.size_);
        }
        return sizeof(size_t) + key.size_;
    }

    int64_t     _region_id;
    std::string _path;
    RocksdbFileSystemAdaptor* _fs = nullptr;
    SnapshotContextPtr _context = nullptr;
    bool        _is_meta_reader = false;
    bool        _closed = true;
    size_t      _num_lines = 0;
    butil::IOPortal _last_package;
    off_t       _last_offset = 0;
};

/* Manage snapshot for each region */
class RocksdbFileSystemAdaptor: public braft::FileSystemAdaptor {
public:
    RocksdbFileSystemAdaptor(int64_t region_id)
        : _region_id(region_id)
    {}

    virtual ~RocksdbFileSystemAdaptor() {
        _snapshot_cond.wait();
        DB_WARNING("region_id: %ld rocksdb file_system_adaptor release", _region_id);
    } 

    // override virtual function
    virtual bool delete_file(const std::string& path, bool recursive) override;
    virtual bool rename(const std::string& old_path, const std::string& new_path) override;
    virtual bool link(const std::string& old_path, const std::string& new_path) override;
    virtual bool create_directory(const std::string& path, 
                butil::File::Error* error, 
                bool create_parnet_directories) override;
    virtual bool path_exists(const std::string& path) override;
    virtual bool directory_exists(const std::string& path) override;
    virtual braft::DirReader* directory_reader(const std::string& path) override;
    virtual braft::FileAdaptor* open(const std::string& path, int flag,
                const ::google::protobuf::Message* file_meta,
                butil::File::Error* e) override;
    virtual bool open_snapshot(const std::string& snapshot_path) override;
    virtual void close_snapshot(const std::string& snapshot_path) override;
    void close(const std::string& path);

private:
    braft::FileAdaptor* open_reader_adaptor(const std::string& path, int flag,
            const ::google::protobuf::Message* file_meta,
            butil::File::Error* e);
    braft::FileAdaptor* open_writer_adaptor(const std::string& path, int flag,
            const ::google::protobuf::Message* file_meta,
            butil::File::Error* e);
    
    SnapshotContextPtr get_snapshot(const std::string& path);

    struct ContextEnv {
        SnapshotContextPtr ptr;
        int64_t            count = 0;
        TimeCost           cost;
    };

    int64_t     _region_id;
    bthread::Mutex _snapshot_mutex;
    bthread::Mutex _open_reader_adaptor_mutex;
    BthreadCond    _snapshot_cond;
    // path -> snapshot
    typedef std::map<std::string, ContextEnv> SnapshotMap;
    SnapshotMap    _snapshots;
};

} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
