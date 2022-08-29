#include <gflags/gflags.h>
#include <stdio.h>
#include <signal.h>
#include <string>

#include "common/common.h"
#include "store/store.h"
#include "raft/my_raft_log.h"
#include "common/conf.h"

namespace TKV {
DECLARE_int32(store_id);
DECLARE_int32(store_port);
DECLARE_string(conf_path);
} // namespace TKV

brpc::Server server;
int main(int argc, char** argv) {
    // google::SetCommandLineOption("flagfile", "/etc/TKV/sto.conf");
    google::ParseCommandLineFlags(&argc, &argv, true);
    // Conf Parse GFLAGS options
    TKV::Conf store_conf(TKV::FLAGS_conf_path, false /* is_meta */, TKV::FLAGS_store_id); 
    if (store_conf.parse()) {
        DB_DEBUG("TKVMeta parse conf failed");
        return -1;
    }
    // init log first
    std::string store_log = "store" + std::to_string(TKV::FLAGS_store_id);
    if (TKV::init_log(store_log.c_str()) != 0) {
        fprintf(stderr, "init store log failed, exit now");
        return -1;
    }
    DB_WARNING("TKV init log success");
    // init something
    TKV::register_myraft_extension();

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
    std::vector<int64_t> init_region_ids;
    store->init_before_listen(init_region_ids);
}
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
