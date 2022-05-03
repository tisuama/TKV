#include "common/table_key.h"
#include "store/meta_writer.h"
#include <rocksdb/sst_file_reader.h>

namespace TKV {
const std::string MetaWriter::META_IDENTIFY(1, 0x01);
// key: META_IDENTIFY + region_id + identify
const std::string MetaWriter::APPLIED_INDEX_IDENTIFY(1, 0x01);
const std::string MetaWriter::NUM_TABLE_LINE_IDENTIFY(1, 0x02);
// ...
const std::string MetaWriter::REGION_INFO_IDENTIFY(1, 0x05);
const std::string MetaWriter::DOING_SNAPSHOT_IDENTIFY(1, 0x07); 
const std::string MetaWriter::LEARNER_IDENTIFY(1, 0x0C);
const std::string MetaWriter::LOCAL_STORAGE_IDENTIFY(1, 0x0D);
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
