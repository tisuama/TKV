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

class RocksdbFileSystemAdaptor: public braft::FileSytemAdaptor {
public:
private:
    int64_t     _region_id;
    bthread::Mutex _snapshot_mutex;
        
};

} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
