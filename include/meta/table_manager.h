#pragma once
#include <unordered_map>
#include <unordered_set>
#include <set>

#include "meta/schema_manager.h"
#include "meta/meta_server.h"
#include "common/table_key.h"
#include "proto/meta.pb.h"

namespace TKV {
enum MergeStatus {
    MERGE_IDLE = 0,
    MERGE_SRC = 1,// 用于Merge源 
    MERGE_DST = 2 // 用于Merge目标
};

struct RegionDesc {
    int64_t region_id;
    MergeStatus merge_status;
};

struct TableMem {
    bool whether_level_table; // 暂时不支持
    pb::SchemaInfo schema_pb;
    std::unordered_map<int64_t, std::set<int64_t>> partition_regions; // 只存在于内存
    // Not support field and index now
    std::unordered_map<std::string, int32_t> field_id_map; 
    std::unordered_map<std::string, int64_t> index_id_map; 
    // start_key -> region_id
    std::map<int64_t, std::map<std::string, RegionDesc>> skey_to_region_map;

    // 发生split或者merge时，用以下三个map暂存心跳上报的region信息，保证整体更新。
    // start_key -> region： 存放new region, new region为分裂出来的region
    std::map<int64_t, std::map<std::string, SmartRegionInfo>> skey_to_new_region_map;
    // region_id -> none region： 存放空region
    std::map<int64_t, SmartRegionInfo> id_to_none_map;
    // region_id -> region
    // 存放key发生变化的region，以该region为准，查找merge和split涉及到的所有region
    std::map<int64_t, SmartRegionInfo> id_to_region_map; 
    int64_t main_table_id {0}; 
    int64_t global_index_id {0};
    bool    is_partition {false};
    std::vector<std::string> learner_resource_tag;
    int64_t statis_version {0};


    // clear
    void clear_regions() {
        partition_regions.clear();
        skey_to_region_map.clear();
        skey_to_new_region_map.clear();
        id_to_none_map.clear();
        id_to_region_map.clear();
    }
};

class TableManager {
public:
    ~TableManager() {
        bthread_mutex_destroy(&_table_mutex);
    }
    static TableManager* get_instance() {
        static TableManager instance;
        return &instance;
    }
    
    int64_t get_table_id(const std::string& table_name) {
        BAIDU_SCOPED_LOCK(_table_mutex);
        if (_table_id_map.find(table_name) != _table_id_map.end()) {
            return _table_id_map[table_name];
        }
        return 0;
    }

    // Raft 串行调用接口
    void create_table(const pb::MetaManagerRequest& request, const int64_t apply_index, braft::Closure* done);
    void write_schema_for_not_level(TableMem& table_mem, braft::Closure* done,
                                    int64_t max_table_id, bool has_auto_increment = false);

private:
    std::string construct_max_table_id_key() {
        std::string max_table_id_key = MetaServer::SCHEMA_IDENTIFY + 
                MetaServer::MAX_ID_SCHEMA_IDENTIFY + SchemaManager::MAX_TABLE_ID_KEY;
        return max_table_id_key;
    }

    TableManager(): _max_table_id(0) {
        bthread_mutex_init(&_table_mutex, NULL);
    }
    
    bthread_mutex_t     _table_mutex;
    int64_t             _max_table_id;
    // table_name -> id [name: namespace\001database\001\table_name]
    std::unordered_map<std::string, int64_t>    _table_id_map;
    std::unordered_map<int64_t, TableMem>       _table_info_map;
    std::map<int64_t, TableMem>     _table_tombstone_map;
    std::set<int64_t>               _need_apply_raft_table_ids;
};

} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
