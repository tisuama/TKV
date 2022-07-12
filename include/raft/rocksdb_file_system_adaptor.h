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
class std::shared_ptr<Region> SmartRegion;

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

private:
    struct ContextEnv {
        SnapshotContextPtr ptr;
        int64_t            count = 0;
        TimeCost           cost;
    };

    int64_t     _region_id;
    bthread::Mutex _snapshot_mutex;
    bthread::Mutex _open_reader_adaptor_mutex;
    BthreadCond    _snapshot_cond;
    typedef std::map<std::string, ContextEnv> SnapshotMap;
    SnapshotMap    _snapshots;
};

} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
