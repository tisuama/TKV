#include <string>
#include <iostream>
#include <fstream>
#include <gflags/gflags.h>
#include <brpc/server.h>
#include <braft/raft.h>
#include "common/util.h"
#include "common/common.h"
#include "meta/meta_server.h"

namespace TKV {
DECLARE_int32(meta_port);
DECLARE_string(meta_server_bns);
DECLARE_int32(meta_replica_num);
}
int main(int argc, char** argv) {
    // read config parse
    google::SetCommandLineOption("flagfile", "/etc/TKV/meta_flags.conf");
    google::ParseCommandLineFlags(&argc, &argv, true); 
    // init log first
    if (TKV::init_log("meta") !=  0) {
        fprintf(stderr, "init meta log failed.");
        return -1;
    } 
    DB_DEBUG("TKV init log success");
    brpc::Server server;
    butil::EndPoint addr;
    addr.ip = butil::IP_ANY;
    addr.port = TKV::FLAGS_meta_port;
    if (braft::add_service(&server, addr)) {
        DB_FATAL("Fail to add raft  service");
        return -1;
    }
    DB_WARNING("Add raft to brpc server sucess");
    std::vector<std::string> raft_peers;
    std::vector<braft::PeerId> peers;
    TKV::split_string(raft_peers, TKV::FLAGS_meta_server_bns, ',');
    for (auto& r : raft_peers) {
        DB_WARNING("raft peer: %s", r.data());
        braft::PeerId peer(r);
        peers.push_back(peer); 
    }
    TKV::MetaServer* meta_server = TKV::MetaServer::get_instance();
    if (server.AddService(meta_server, brpc::SERVER_DOESNT_OWN_SERVICE)) {
        DB_FATAL("Fail to Add meta_server Service");
        return -1;
    }
    if (server.Start(addr, NULL)) {
        DB_FATAL("Fail to start server");
        return -1;
    }
    if (meta_server->init(peers)) {
        DB_FATAL("Meta server init failed");
        return -1;
    }

    while (!brpc::IsAskedToQuit()) {
        bthread_usleep(1000000L);
    }
    DB_WARNING("receive kill signal, begin to quit");
    
    server.Stop(0);
    server.Join();
    DB_WARNING("meta server quit sucess");
    return 0;
}


// End of file
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
