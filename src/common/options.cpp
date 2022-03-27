#include <gflags/gflags.h>

namespace TKV {
DEFINE_string(log_file, "meta.log", "log_file of meta or store");
DEFINE_bool(enable_debug, true, "if enable debug");
DEFINE_bool(enable_self_trace, false, "if enable self trace");
DEFINE_int32(meta_port, 8010, "meta port");
DEFINE_string(meta_server_bns, "127.0.0.1", "meta server bns");
DEFINE_int32(meta_replica_number, 3, "replicas of meta server");
DEFINE_bool(meta_with_any_ip, false ,"meta server ip is ip_any or meta_ip");
DEFINE_string(meta_ip, "127.0.0.1", "meta serer ip when meta_with_any_ip = false");


// rocksdb
}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */

