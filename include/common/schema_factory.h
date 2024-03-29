#pragma once
#include <mutex>
#include <set>
#include <map>
#include <google/protobuf/dynamic_message.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/descriptor.pb.h>
#include "common/statis.h"
#include "proto/meta.pb.h"
#include "proto/common.pb.h"
#include "common/common.h"

using ::google::protobuf::RepeatedPtrField;
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
    uint32_t    timestamp = 0;
    std::string comment;
    pb::Engine  engine;
    std::string short_name;
    std::vector<std::string> learner_resource_tags;
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

struct InstanceDBStatus {
    pb::Status status = pb::NORMAL;
    bool need_cacle = false;
    std::string     logical_room;
    // CHECK_COUNT次后才设置normal
    int64_t         normal_count = 0;
    // 探测到FAULT_COUNT次后才设置FAULT
    int64_t         fault_count = 0;
    TimeCost        last_update_time;
    static const int64_t CHECK_OUNT = 10;
};

struct StatusMapping {
    // store -> logical_room
    std::unordered_map<int64_t, InstanceDBStatus> ins_info_map;
    // physical_room -> logical_room
    std::unordered_map<std::string, std::string>  phy_log_map;
};

typedef std::shared_ptr<TableInfo> SmartTable;
typedef std::shared_ptr<IndexInfo> SmartIndex;
typedef std::shared_ptr<StatisticsInfo> SmartStatistics;
typedef std::shared_ptr<InstanceDBStatus> SmartDBStatus;

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
    // 全局二级索引与主表id的映射
    // 全局二级索引有不同的table_id，主表主键索引的table_id是主表table_id
    std::unordered_map<int64_t, int64_t>        global_index_id_mapping;
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

typedef std::shared_ptr<TableRegionInfo> TableRegionPtr;

class SchemaFactory {
typedef RepeatedPtrField<pb::RegionInfo> RegionVec;
typedef RepeatedPtrField<pb::SchemaInfo> SchemaVec;
typedef RepeatedPtrField<pb::DatabaseInfo>  DataBaseVec;

public:
    virtual ~SchemaFactory() {
        bthread_mutex_destroy(&_update_slow_db_mutex);
        bthread_mutex_destroy(&_region_mutex);
        bthread_mutex_destroy(&_table_mutex);
    }

    static SchemaFactory* get_instance() {
        static SchemaFactory instance;
        return &instance; 
    }

    int init();

    void update_tables_double_buffer_sync(const SchemaVec& tables);

    static int update_regions_double_buffer(void* meta, bthread::TaskIterator<RegionVec>& iter);

    void update_regions_double_buffer(bthread::TaskIterator<RegionVec>& iter);

    void get_all_table_version(std::unordered_map<int64_t, int64_t>& table_id_version_map); 

    bool exist_table_id(int64_t table_id);

    void update_table(const pb::SchemaInfo& table);

    size_t update_regions(std::unordered_map<int64_t, TableRegionPtr>& table_region_mapping, int64_t table_id,
            std::map<int, std::map<std::string, const pb::RegionInfo*>>& key_region_map);

    
private:
    SchemaFactory() {
        _is_inited = false;
        bthread_mutex_init(&_update_slow_db_mutex, NULL);
        bthread_mutex_init(&_region_mutex, NULL);
        bthread_mutex_init(&_table_mutex, NULL);
        _physical_room = FLAGS_default_physical_room;
    }

    void delete_table_region_map(const pb::SchemaInfo& table);
    int  update_table_internal(SchemaMapping& background, const pb::SchemaInfo& table);
    void delete_table(const pb::SchemaInfo& table, SchemaMapping& background);
    size_t double_buffer_table_region_erase(
        std::unordered_map<int64_t, TableRegionPtr>& table_region_map, 
        int64_t table_id);

    bool                            _is_inited {false};
    bthread_mutex_t                 _update_slow_db_mutex;
    // use of slow data base
    std::map<int64_t, DatabaseInfo> _slow_db_info;

    bthread_mutex_t                 _table_mutex;
    // table info
    SchemaMapping                   _double_buffer_table;
    // table region info
    bthread_mutex_t                 _region_mutex;
    
    std::unordered_map<int64_t, TableRegionPtr> _double_buffer_region;
    // ExecutionQueue
    bthread::ExecutionQueueId<RegionVec>        _region_queue_id = {0};
    
    std::string                     _physical_room;
    std::string                     _logical_room;
    int64_t                         _last_update_index {0};
};

} // namespace TKV
  
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
