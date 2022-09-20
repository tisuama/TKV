#include <iostream>
#include <string>
#include <gflags/gflags.h>
#include <brpc/server.h>
#include <brpc/channel.h>
#include <gtest/gtest.h>
#include "common/log.h"
#include "common/closure.h"
#include "proto/store.pb.h"
#include "store/store_server_interact.h"

DEFINE_string(cmd, "test_op_op", "cmd type");

using namespace TKV;
const std::string store_addr = "127.0.0.1:8110";

void test_no_op() {
    // store manager request
    pb::InitRegion init_region_req;
    pb::StoreRes   store_res;
    auto info = init_region_req.mutable_region_info();
    info->set_region_id(1);
    info->set_table_name("TEST_TABLE");
    info->set_table_id(1);
    info->set_replica_num(3);
    info->set_version(1);
    info->add_peers("127.0.0.1:8110");
    info->set_leader("127.0.0.1:8110");
    info->set_can_add_peer(false);
    info->set_conf_version(1);
    info->set_main_table_id(1);
    info->set_partition_id(0);
    info->set_partition_num(1);
    auto schema = init_region_req.mutable_schema_info();
    schema->set_table_id(1);
    schema->set_table_name("TEST_TABLE");
    schema->set_database_name("TEST_DB");
    schema->set_database_id(1);
    schema->set_namespace_name("TEST_NAMESPACE");
    schema->set_namespace_id(1);
    schema->set_region_size(104857600);
    schema->set_replica_num(3);
    schema->set_partition_num(1);
    schema->set_resource_tag("");
    schema->set_engine(pb::ROCKSDB);
    schema->set_version(1);
    init_region_req.set_snapshot_times(2);
    
    
    StoreInteract store(store_addr.c_str());
    std::cout << "StoreInteract, request info: " << init_region_req.ShortDebugString() << std::endl;
    int r = store.send_request(0, "init_region", init_region_req, store_res);
    std::cout << "send result: " << r << std::endl;
}

int main(int argc, char** argv) {
    google::ParseCommandLineFlags(&argc, &argv, true); 
    test_no_op();
}
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
