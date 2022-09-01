#include <iostream>
#include <string>
#include <gflags/gflags.h>
#include <brpc/server.h>
#include <brpc/channel.h>
#include <gtest/gtest.h>
#include "common/log.h"
#include "common/closure.h"
#include "meta/meta_server_interact.h"
#include "proto/meta.pb.h"

using namespace TKV;

const std::string meta_bns = "127.0.0.1:8010";
TEST(test_mata_server, no_op) {
    // meta manager requets 
    pb::MetaManagerRequest request;
    pb::MetaManagerResponse response;
    request.set_op_type(pb::OP_NONE);
    
    auto meta = MetaServerInteract::get_instance();
    int r = meta->init_internal(meta_bns);
    std::cout << "meta interact init: " << r << std::endl;
    r = meta->send_request("meta_manager", request, response);
    std::cout << "send request: " << r << std::endl;
}

TEST(test_mata_server, careate_namespace) {
    pb::MetaManagerRequest request;
    pb::MetaManagerResponse response;
    request.set_op_type(pb::OP_CREATE_NAMESPACE);
    auto info = request.mutable_namespace_info();
    info->set_namespace_name("TEST_NAMESPACE");
    info->set_quota(1024);
    
    auto meta = MetaServerInteract::get_instance();
    int r = meta->init_internal(meta_bns);
    std::cout << "meta interact init: " << r << std::endl;
    r = meta->send_request("meta_manager", request, response);
    std::cout << "send request: " << r << std::endl;
}

int main(int argc, char** argv) {
    google::ParseCommandLineFlags(&argc, &argv, true); 
	::testing::InitGoogleTest(&argc, argv);
    ::testing::GTEST_FLAG(filter) = "test_mata_server.careate_namespace";
	srand((unsigned)time(NULL));
	return RUN_ALL_TESTS();
}
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
