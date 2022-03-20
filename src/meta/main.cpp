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
    if (TKV::init_log(argv[0]) !=  0) {
        fprintf(stderr, "init meta log failed.");
        return -1;
    } 
    DB_DEBUG("TKV init log sucess");
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

    // Start server at port
}


// End of file
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
