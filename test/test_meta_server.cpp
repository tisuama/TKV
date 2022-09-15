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

TEST(test_mata_server, create_database) {
    pb::MetaManagerRequest request;
    pb::MetaManagerResponse response;
    request.set_op_type(pb::OP_CREATE_DATABASE);
    auto db = request.mutable_database_info();
    db->set_namespace_name("TEST_NAMESPACE");
    db->set_database_name("TEST_DB");
    db->set_quota(1024);
    
    auto meta = MetaServerInteract::get_instance();
    int r = meta->init_internal(meta_bns);
    std::cout << "meta interact init: " << r << std::endl;
    r = meta->send_request("meta_manager", request, response);
    std::cout << "send request: " << r << std::endl;
}

TEST(test_mata_server, add_instance) {
    pb::MetaManagerRequest request;
    pb::MetaManagerResponse response;
    request.set_op_type(pb::OP_ADD_INSTANCE);
    auto info = request.mutable_instance();
    info->set_address("127.0.0.1:8010");
    info->set_capacity(1024000);
    info->set_used_size(0);
    info->set_resource_tag("LOCAL");
    info->set_physical_room("LAB511");
    info->set_status(pb::Status::NORMAL);
    
    auto meta = MetaServerInteract::get_instance();
    int r = meta->init_internal(meta_bns);
    std::cout << "meta interact init: " << r << std::endl;
    r = meta->send_request("meta_manager", request, response);
    std::cout << "send request: " << r << std::endl;
}

TEST(test_mata_server, create_table) {
    pb::MetaManagerRequest request;
    pb::MetaManagerResponse response;
    request.set_op_type(pb::OP_CREATE_TABLE);
    auto info = request.mutable_table_info();
    info->set_table_name("TEST_TABLE");
    info->set_database_name("TEST_DB");
    info->set_namespace_name("TEST_NAMESPACE");
    info->set_resource_tag("LOCAL");
    
    auto meta = MetaServerInteract::get_instance();
    int r = meta->init_internal(meta_bns);
    std::cout << "meta interact init: " << r << std::endl;
    r = meta->send_request("meta_manager", request, response);
    std::cout << "send request: " << r << std::endl;
}

TEST(test_mata_server, create_user) {
    pb::MetaManagerRequest request;
    pb::MetaManagerResponse response;
    request.set_op_type(pb::OP_CREATE_USER);
    auto info = request.mutable_user_privilege();
    info->set_username("root");
    info->set_password("passwrod");
    info->set_namespace_name("TEST_NAMESPACE");
    auto privilege_db = info->add_privilege_database();
    privilege_db->set_database("TEST_DB");
    privilege_db->set_rw(pb::RW::WRITE);
    
    auto meta = MetaServerInteract::get_instance();
    int r = meta->init_internal(meta_bns);
    std::cout << "meta interact init: " << r << std::endl;
    // r = meta->send_request("meta_manager", request, response);
    std::cout << "send request: " << r << std::endl;
}

int main(int argc, char** argv) {
    google::ParseCommandLineFlags(&argc, &argv, true); 
	::testing::InitGoogleTest(&argc, argv);
    ::testing::GTEST_FLAG(filter) = "test_mata_server.*";
	srand((unsigned)time(NULL));
	return RUN_ALL_TESTS();
}
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
