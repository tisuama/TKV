#include <gflags/gflags.h>

namespace TKV {
DEFINE_string(conf_path, "/etc/TKV/TKV.conf", "TKV conf path");
    
// Meta Service
DEFINE_string(log_path, "./", "log_file of meta or store");
DEFINE_bool(enable_debug, true, "if enable debug");
DEFINE_bool(enable_self_trace, false, "if enable self trace");
DEFINE_string(meta_server_bns, "127.0.0.1:8010,127.0.0.1:8011,127.0.0.1:8012", "meta server bns");
DEFINE_int32(meta_replica_number, 3, "replicas of meta server");
DEFINE_bool(meta_with_any_ip, false, "meta server ip is ip_any or meta_ip");
DEFINE_int32(meta_id, 0, "meta id in conf file");
DEFINE_string(meta_ip, "127.0.0.1", "meta ip");
DEFINE_int32(meta_port, 8010, "meta port");
DEFINE_int32(disk_used_percent, 95, "max disk used percent, default: 80");

// Sotre Service
DEFINE_string(default_logical_room, "default_logical_room", "default logical room");
DEFINE_string(default_physical_room, "default_physical_room", "default physical room");
DEFINE_string(resource_tag, "default_resource_tag", "resource_tag");
DEFINE_int32(store_id, 0, "store id in conf file");
DEFINE_string(store_ip, "127.0.0.1", "store ip");
DEFINE_int32(store_port, 8110, "store port");

// Raft Service
// raft log数据
DEFINE_string(raftlog_uri, "raft_log://myraftlog?id=", "raft_log uri");
// raft meta持久化数据
DEFINE_string(stable_uri, "raft_meta://myraftmeta?id=", "raft stable path");
// snapshot_last_log_id为后缀的文件
DEFINE_string(snapshot_uri, "local://raft_data/snapshot", "raft snapshot uri");
// rocksdb path
DEFINE_string(db_path, "./rocks_db", "rocksdb db path of store data");

// Concurrency
DEFINE_int32(snapshot_load_num, 4, "snapshot_load_num, default: 4");
DEFINE_int32(raft_write_concurrency, 40, "raft_write_concurrency, default: 40");
DEFINE_int32(service_write_concurrency, 40, "service_write_concurrency");
DEFINE_int32(balance_periodicity, 60, "times of store heart beat");
DEFINE_int64(store_heart_beat_interval, 30LL * 1000 * 1000, "store heart beat interval, default: 30s");
DEFINE_int32(snapshot_interval_s, 180, "raft snapshot interval, default: 600s");
DEFINE_int32(election_timeout_ms, 1000, "raft election timeout(ms), default: 1s");

} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */

