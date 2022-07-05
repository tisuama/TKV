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

/* TODO: snapshot
struct SnapshotContext {
    SnapshotContext()
        : snapshot(RocksdbWrapper::get_instance()->get_snapshot()) 
    {}
    ~SnapshotContext() {
        if (snapshot) {
            RocksdbWrapper::get_instance()->release_snapshot();
        }
    }

    const rocksdb::Snapshot* snapshot = nullptr;
};
*/

typedef std::shared_ptr<SnapshotContext> SnapshotContextPtr;

class RocksdbFileSystemAdaptor: public braft::FileSystemAdaptor {
public:
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
