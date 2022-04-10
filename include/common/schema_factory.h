#pragma once
#include <mutex>
#include <set>
#include <google/protobuf/dynamic_message.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/descriptor.pb.h>
#include "common/statis.h"
#include "proto/meta.pb.h"
#include "common/common.h"

using ::google::protobuf::DescriptorProto;
using ::google::protobuf::Descriptor;
using ::google::protobuf::RepeatedPtrField;
using ::google::protobuf::Message;
using ::google::protobuf::FieldDescriptorProto;
using ::google::protobuf::FieldDescriptor;
using ::google::protobuf::DescriptorPool;
using ::google::protobuf::DynamicMessageFactory;


namespace TKV {
typedef std::map<std::string, int64_t> KeyRegionMap; 
DECLARE_string(default_physical_room);

struct RegionInfo {
    RegionInfo() {}
    explicit RegionInfo(const RegionInfo& other) {
        region_info = other.region_info;
    }

    pb::RegionInfo region_info;
};

struct DistInfo {
    std::string logical_room;
    int64_t     count;
};

struct TableInfo {
    int64_t     id = -1;
    int64_t     db_id = -1;
    int64_t     version = -1;
    int64_t     partition_num;
    int64_t     region_split_lines;
    int64_t     byte_size_per_record;
    std::string name; // db.table
    std::string namesp;
    std::string resource_tag;
    std::string main_logical_room;
    std::vector<DistInfo> dists;
    int64_t     replica_num = 3;

    int32_t     region_num;
    const Descriptor* tb_desc = nullptr;
    const Message*    message_proto = nullptr;
    DescriptorProto*  tb_proto = nullptr;
    FieldDescriptorProto* file_proto = nullptr;
    DynamicMessageFactory* factory = nullptr;

    uint32_t    timestamp = 0;
};

struct IndexInfo {
    int64_t     id = -1;
    int64_t     version = -1;
    std::string name; // db.index
    std::string short_name;
};

struct DatabaseInfo {
    int64_t     id = -1;
    int64_t     version = -1;
    std::string name;
    std::string namesp;
};

typedef std::shared_ptr<TableInfo> SmartTable;
typedef std::shared_ptr<IndexInfo> SmartIndex;
typedef std::shared_ptr<StatisticsInfo> SmartStatistics;

struct SchemaMapping {
    // namespace.database -> database_id
    std::unordered_map<std::string, int64_t>    db_name_to_db_id;
    // database_id -> database_info
    std::unordered_map<int64_t, DatabaseInfo>   db_id_to_db_info;
    // namespace.database.table_name -> table_id
    std::unordered_map<std::string, int64_t>    table_name_to_id;
    // table_id -> table_info
    std::unordered_map<int64_t, SmartTable>     table_id_to_table_info;
    // namespace.database.table.index -> index_id
    std::unordered_map<std::string, int64_t>    index_name_to_index_id;
    // index_id -> index_info
    std::unordered_map<int64_t, SmartIndex>     index_id_to_index_info;
    // table_id -> 代价统计信息
    std::map<int64_t, SmartStatistics>          table_id_to_statis;

};


struct TableRegionInfo {
    void update_leader(int64_t region_id, const std::string& leader) {
        if (region_info_map.find(region_id) != region_info_map.end()) {
            region_info_map[region_id].region_info.set_leader(leader);
            DB_DEBUG("Table region_id: %ld set leader: %s", region_id, leader.c_str());
        }
    }
    
    int get_region_info(int64_t region_id, pb::RegionInfo& info) {
        if (region_info_map.find(region_id) != region_info_map.end()) {
            info = region_info_map[region_id].region_info;
            return 0;
        } else {
            return -1;
        }
    }
    
    void inset_region_info(const pb::RegionInfo& info) {
        region_info_map[info.region_id()].region_info = info;
        DB_DEBUG("Table inset region_info, region_id: %ld, region_info: %s",
                info.region_id(), info.ShortDebugString().c_str());
    }
     
    std::unordered_map<int64_t, RegionInfo>   region_info_map;
    std::unordered_map<int64_t, KeyRegionMap> key_region_map; 
};

using DoubleBufferedTable = butil::DoublyBufferedData<SchemaMapping>;
typedef std::shared_ptr<TableRegionInfo> TableRegionPtr;
using DoubleBufferedTableRegionInfo = butil::DoublyBufferedData<std::unordered_map<int64_t, TableRegionPtr>>;

class SchemaFactory {
typedef RepeatedPtrField<pb::RegionInfo> RegionVec;
typedef RepeatedPtrField<pb::SchemaInfo> SchemaVec;
typedef RepeatedPtrField<pb::DataBaseInfo>  DataBaseVec;
public:
    virtual ~SchemaFactory() {
        bthread_mutex_destroy(&_update_slow_db_mutex);
    }

    static SchemaFactory* get_instance() {
        static SchemaFactory instance;
        return &instance; 
    }

    void update_table_internal(SchemaMapping& background, const pb::SchemaInfo& table);
    void update_tables_double_buffer_sync(const SchemaVec& tables);
    
private:
    SchemaFactory() {
        _is_inited = false;
        bthread_mutex_init(&_update_slow_db_mutex, NULL);
        _physical_room = FLAGS_default_physical_room;
    }

    bool                            _is_inited {false};
    bthread_mutex_t                 _update_slow_db_mutex;
    std::map<int64_t, DatabaseInfo> _slow_db_info;

    DoubleBufferedTable             _double_buffer_table;
    DoubleBufferedTableRegionInfo   _double_buffer_table_region;

    bthread::ExecutionQueueId<RegionVec> _region_queue_id {0};
    
    std::string                     _physical_room;
    std::string                     _logical_room;
    int64_t                         _last_update_index {0};
};
} // namespace TKV
  
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
