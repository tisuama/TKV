#include <string>
#include <fstream>
#include "gflags/gflags.h"

namespace TKV {
DECLARE_int32(meta_port);
DECLARE_string(meta_server_bns);
DECLARE_int32(meta_replica_num);
    
int main(int argc, char** argv) {
    // read config parse
    google::ParseCommandLineFlags(&argc, &argv, true); 
    // init log first
    if (TKV::init_log(argv[0]) !=  0) {
        fprintf(stderr, "init meta log failed.");
        return -1;
    } 
}

}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
