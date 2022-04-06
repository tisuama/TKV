#include <gflags/gflags.h>
#include <stdio.h>
#include <signal.h>
#include <string>

#include "common/common.h"
#include "store/store.h"

namespace TKV {
DECLARE_int32(store_port);
} // namespace TKV

brpc::Server server;
int main(int argc, char** argv) {
    google::SetCommandLineOption("flagfile", "/etc/TKV/store_flags.conf");
    google::ParseCommandLineFlags(&argc, &argv, true);
    // init log first
    if (TKV::init_log("store.log") != 0) {
        fprintf(stderr, "init store log failed, exit now");
        return -1;
    }
    DB_WARNING("TKV init log success");
    // init something
    //
    // add service
    butil::EndPoint addr;
    addr.ip = butil::IP_ANY;
    addr.port = TKV::FLAGS_store_port;
    if (braft::add_service(&server, addr) != 0) {
        DB_FATAL("Fail to init raft service");
        return -1;
    }
    DB_WARNING("Add raft to brpc server success");
    // register sotre service  
    TKV::Store* store = TKV::Store::get_instance();
    (void)store;
}
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
