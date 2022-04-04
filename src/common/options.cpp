#include <gflags/gflags.h>

namespace TKV {
DEFINE_string(log_path, "./", "log_file of meta or store");
DEFINE_bool(enable_debug, true, "if enable debug");
DEFINE_bool(enable_self_trace, false, "if enable self trace");
DEFINE_string(meta_server_bns, "127.0.0.1:8010", "meta server bns");
DEFINE_int32(meta_replica_number, 3, "replicas of meta server");
DEFINE_bool(meta_with_any_ip, false ,"meta server ip is ip_any or meta_ip");
DEFINE_string(meta_ip, "127.0.0.1", "meta serer ip when meta_with_any_ip = false");
DEFINE_int32(meta_port, 8010, "meta port");


DEFINE_string(default_logical_room, "default", "default logical room");
DEFINE_string(default_physical_room, "default", "default physical room");
}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */

