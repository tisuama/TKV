#include <iostream>
#include <string>
#include <gflags/gflags.h>
#include <brpc/server.h>
#include <brpc/channel.h>
#include "common/log.h"
#include "common/closure.h"
#include "meta/meta_server_interact.h"
#include "proto/meta.pb.h"

using namespace TKV;

int main(int argc, char** argv) {
    google::ParseCommandLineFlags(&argc, &argv, true); 
    auto meta = MetaServerInteract::get_instance();
    if (!meta->is_inited()) {
        meta->init();
    }
    if (TKV::init_log("test") != 0) {
        fprintf(stderr, "init log failed");
        return -1;
    }
    
    // meta manager requets 
    pb::MetaManagerRequest request;
    pb::MetaManagerResponse response;
    request.set_op_type(pb::OP_NONE);
    
    meta->send_request("meta_manager", request, response);

    DB_DEBUG("All test complete");
}
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
