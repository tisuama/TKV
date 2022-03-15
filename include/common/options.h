#pragma once

#include <gflags/gflags.h>

namespace TKV {
DEFINE_string(log_path, "log/", "log_file of meta or store");
DEFINE_int32(meta_port, 8010, "meta port");
DEFINE_string(meta_server_bns, "127.0.0.1", "meta server bns");
DEFINE_int32(meta_replica_number, 3, "replicas of meta server");
}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */


