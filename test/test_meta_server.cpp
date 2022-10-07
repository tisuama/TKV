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

DEFINE_string(cmd, "test_op_op", "cmd type");

using namespace TKV;
const std::string meta_bns = "127.0.0.1:8010";

void test_no_op() {
    // meta manager request 
    pb::MetaManagerRequest request;
    pb::MetaManagerResponse response;
    request.set_op_type(pb::OP_NONE);
    
    auto meta = MetaServerInteract::get_instance();
    int r = meta->init_internal(meta_bns);
    std::cout << "meta interact init: " << r << std::endl;
    r = meta->send_request("meta_manager", request, response);
    std::cout << "send request: " << r << std::endl;
}

void add_namespace() {
    pb::MetaManagerRequest request;
    pb::MetaManagerResponse response;
    request.set_op_type(pb::OP_CREATE_NAMESPACE);
    auto info = request.mutable_namespace_info();
    info->set_resource_tag("default_resource_tag");
    info->set_namespace_name("TEST_NAMESPACE");
    info->set_quota(1024);
    
    auto meta = MetaServerInteract::get_instance();
    int r = meta->init_internal(meta_bns);
    std::cout << "meta interact init: " << r << std::endl;
    r = meta->send_request("meta_manager", request, response);
    std::cout << "send request: " << r << std::endl;
}

void add_database() {
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

void add_logical() {
    pb::MetaManagerRequest request;
    pb::MetaManagerResponse response;
    request.set_op_type(pb::OP_ADD_LOGICAL);
    auto info = request.mutable_logical_rooms();
    info->add_logical_rooms("TEST_LOGICAL");
    
    auto meta = MetaServerInteract::get_instance();
    int r = meta->init_internal(meta_bns);
    std::cout << "meta interact init: " << r << std::endl;
    r = meta->send_request("meta_manager", request, response);
    std::cout << "send request: " << r << std::endl;
}

void add_physical() {
    pb::MetaManagerRequest request;
    pb::MetaManagerResponse response;
    request.set_op_type(pb::OP_ADD_PHYSICAL);
    auto info = request.mutable_physical_rooms();
    info->set_logical_room("TEST_LOGICAL");
    info->add_physical_rooms("TEST_PHYSICAL");
    
    auto meta = MetaServerInteract::get_instance();
    int r = meta->init_internal(meta_bns);
    std::cout << "meta interact init: " << r << std::endl;
    r = meta->send_request("meta_manager", request, response);
    std::cout << "send request: " << r << std::endl;
}

void add_instance() {
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

void add_user() {
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
    r = meta->send_request("meta_manager", request, response);
    std::cout << "send request: " << r << std::endl;
}

void add_table() {
    pb::MetaManagerRequest request;
    pb::MetaManagerResponse response;
    request.set_op_type(pb::OP_CREATE_TABLE);
    auto info = request.mutable_table_info();
    info->set_table_name("TEST_TABLE");
    info->set_database_name("TEST_DB");
    info->set_namespace_name("TEST_NAMESPACE");
    
    auto meta = MetaServerInteract::get_instance();
    int r = meta->init_internal(meta_bns);
    std::cout << "meta interact init: " << r << std::endl;
    r = meta->send_request("meta_manager", request, response);
    std::cout << "send request: " << r << std::endl;
}

int main(int argc, char** argv) {
    google::ParseCommandLineFlags(&argc, &argv, true); 
    if (FLAGS_cmd == "prepare") {
        add_namespace();
        add_database();
        add_logical();
        add_physical();
        add_user();
    } else if (FLAGS_cmd == "add_table") {
        add_table();
    } else if (FLAGS_cmd == "add_namespace") {
        add_namespace();
    } else if (FLAGS_cmd == "add_database") {
        add_database();
    } else if (FLAGS_cmd == "add_user") { 
        add_user();
    } else if (FLAGS_cmd == "add_logical") {
        add_logical();
    } else if (FLAGS_cmd == "add_physical") {
        add_physical();
    } else if (FLAGS_cmd == "add_user") {
        add_user();
    } else {
        assert(0);
    }
}
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
