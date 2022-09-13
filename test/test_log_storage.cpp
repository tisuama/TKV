#include "raft/my_raft_log_storage.h"
#include "raft/my_raft_meta_storage.h"
#include "engine/rocks_wrapper.h"
#include <gtest/gtest.h>
#include "proto/store.pb.h"

namespace TKV {

TEST(test_log_storage, case_call) {
    ::system("rm -rf test_log");  
    const std::string db_path = "test_log";
    auto db = TKV::RocksWrapper::get_instance();
    int ret = db->init(db_path);
    CHECK(!ret);
    
    static TKV::MyRaftLogStorage my_raft_log_storage;  
    std::string uri = "raftlog?id=32";
    auto raft_log = my_raft_log_storage.new_instance(uri);
    braft::ConfigurationManager* conf_manager = new braft::ConfigurationManager;
    ret = raft_log->init(conf_manager);
    CHECK(!ret);
    {
        // add configuration
        braft::LogEntry* entry = new braft::LogEntry;
        entry->type = braft::ENTRY_TYPE_CONFIGURATION;
        entry->id = braft::LogId(1, 1); // log_index(1), term(1);
        std::vector<braft::PeerId> peers;
        braft::PeerId peer_id;
        peer_id.parse("127.0.0.1:8010");
        peers.push_back(peer_id);
        peer_id.parse("127.0.0.1:8011");
        peers.push_back(peer_id);
        peer_id.parse("127.0.0.1:8012");
        peers.push_back(peer_id);
        entry->peers = &peers;
        ret = raft_log->append_entry(entry);
        CHECK(!ret);
        std::cout << "index: 1, term: " << raft_log->get_term(1) << std::endl;
        std::cout << "first_log_index: " << raft_log->first_log_index() << std::endl;
        std::cout << "last_log_index: " << raft_log->last_log_index() << std::endl;
        auto read_entry = raft_log->get_entry(1);
        std::cout << "log_entry 1 type: " << read_entry->type << std::endl;
        std::cout << "log_entry 1 index: " << read_entry->id.index << std::endl;
        std::cout << "log_entry 1 term: " << read_entry->id.term << std::endl;
        auto term = raft_log->get_term(1);
        std::cout << "log_entry 1 get_term: " << term << std::endl;
    }
    {
        // no op
        auto entry = new braft::LogEntry;
        entry->type = braft::ENTRY_TYPE_NO_OP;
        entry->id = braft::LogId(2, 1);
        ret = raft_log->append_entry(entry);
        CHECK(!ret);
        std::cout << "log_entry 2 term: " << raft_log->get_term(2) << std::endl;
        std::cout << "first_log_index: " << raft_log->first_log_index() << std::endl;
        std::cout << "last_log_index: " << raft_log->last_log_index() << std::endl;
        
        auto read_entry = raft_log->get_entry(2);
        std::cout << "log_entry 2 type: " << read_entry->type << std::endl;
        std::cout << "log_entry 2 index: " << read_entry->id.index << std::endl;
        std::cout << "log_entry 2 term: " << read_entry->id.term << std::endl;
    }
    {
        // add batch data
        std::cout << "===== put multi data =====" << std::endl;
        for (size_t i = 3; i <= 10000; i++) {
            braft::LogEntry* entry = new braft::LogEntry;
            entry->type = braft::ENTRY_TYPE_DATA;
            entry->id = braft::LogId(i, 2);
            // construct data
            pb::StoreReq request;
            request.set_op_type(pb::OP_UPDATE);
            request.set_region_id(32);
            request.set_region_version(1);
            butil::IOBuf data;
            butil::IOBufAsZeroCopyOutputStream wrapper(&data);
            request.SerializeToZeroCopyStream(&wrapper);
            entry->data = data;
            ret = raft_log->append_entry(entry);
            CHECK(!ret);
            
        }
        // 随便选一个
        std::cout << "log_entry 10000 term: " << raft_log->get_term(10000) << std::endl;
        std::cout << "first_log_index: " << raft_log->first_log_index() << std::endl;
        std::cout << "last_log_index: " << raft_log->last_log_index() << std::endl;
        
        auto read_entry = raft_log->get_entry(5000);
        std::cout << "log_entry 5000 type: " << read_entry->type << std::endl;
        std::cout << "log_entry 5000 index: " << read_entry->id.index << std::endl;
        std::cout << "log_entry 5000 term: " << read_entry->id.term << std::endl;
    }
    {
        // add append_entries test
        std::vector<braft::LogEntry*> entries;
        pb::StoreReq request;
        request.set_op_type(pb::OP_UPDATE);
        request.set_region_id(32);
        request.set_region_version(1);
        butil::IOBuf data;
        butil::IOBufAsZeroCopyOutputStream wrapper(&data);
        request.SerializeToZeroCopyStream(&wrapper);
        for (size_t i = 10001; i <= 10010; i++) {
            braft::LogEntry* entry = new braft::LogEntry;
            entry->type = braft::ENTRY_TYPE_NO_OP;
            entry->id = braft::LogId(i, i);
            // construct data
            entry->data = data;
            entries.push_back(entry);
        }
        ret = raft_log->append_entries(entries, nullptr);
        std::cout << "append entries ret: " << ret << std::endl;
        // 随便选一个
        std::cout << "log_entry 10005 term: " << raft_log->get_term(10005) << std::endl;
        std::cout << "first_log_index: " << raft_log->first_log_index() << std::endl;
        std::cout << "last_log_index: " << raft_log->last_log_index() << std::endl;

        for (int i = 10000; i < 10010; i++) {        
            auto read_entry = raft_log->get_entry(i);
            std::cout << "log_entry " << i << " type: " << read_entry->type << std::endl;
            std::cout << "log_entry " << i << " index: " << read_entry->id.index << std::endl;
            std::cout << "log_entry " << i << " term: " << read_entry->id.term << std::endl;
        }
    }
    {
        // add truncate_prefix
        ret = raft_log->truncate_prefix(10001);
        CHECK(!ret);
        std::cout << "log_entry 10000 term: " << raft_log->get_term(10000) << std::endl;
        std::cout << "log_entry 10001 term: " << raft_log->get_term(10001) << std::endl;
        std::cout << "first_log_index: " << raft_log->first_log_index() << std::endl;
        std::cout << "last_log_index: " << raft_log->last_log_index() << std::endl;
    }
    {
        // add truncate_suffix
        ret = raft_log->truncate_suffix(10003);
        CHECK(!ret);
        std::cout << "log_entry 10006 term: " << raft_log->get_term(10006) << std::endl;
        std::cout << "log_entry 10003 term: " << raft_log->get_term(10003) << std::endl;
        std::cout << "first_log_index: " << raft_log->first_log_index() << std::endl;
        std::cout << "last_log_index: " << raft_log->last_log_index() << std::endl;
    }
    {
        // add reset
        /*
        ret = raft_log->reset(10000);
        CHECK(!ret);
        std::cout << "log_entry 10006 term: " << raft_log->get_term(10006) << std::endl;
        std::cout << "log_entry 10003 term: " << raft_log->get_term(10003) << std::endl;
        std::cout << "first_log_index: " << raft_log->first_log_index() << std::endl;
        std::cout << "last_log_index: " << raft_log->last_log_index() << std::endl;
        */
    }
    std::cout << "==== raft meta ==== " << std::endl;
    static MyRaftMetaStorage my_raft_meta_storage;
    braft::VersionedGroupId group;
    auto meta_s = my_raft_meta_storage.new_instance(uri);
    auto meta_log = static_cast<MyRaftMetaStorage*>(meta_s);
    meta_log->init();
    braft::PeerId peer("127.0.0.1:8010");
    meta_log->set_term_and_votedfor(2, peer, group);
    std::cout << "meta log term: " << meta_log->get_term() << std::endl;
    braft::PeerId tmp_peer;
    meta_log->get_votedfor(tmp_peer);
    std::cout << "meta log votedfor: " << tmp_peer.to_string() << std::endl;
    
     

    db->close();

    // reopen
    std::cout << "====== reopen test ======= " << std::endl;
    auto db1 = RocksWrapper::get_instance();
    ret = db1->init(db_path);
    std::cout << "rocksdb reopen result: " << ret << std::endl;
    CHECK(!ret);
    raft_log = my_raft_log_storage.new_instance(uri);
    raft_log->init(conf_manager);
    CHECK(raft_log);
    std::cout << "log_entry 10006 term: " << raft_log->get_term(10006) << std::endl;
    std::cout << "log_entry 10003 term: " << raft_log->get_term(10003) << std::endl;
    std::cout << "first_log_index: " << raft_log->first_log_index() << std::endl;
    std::cout << "last_log_index: " << raft_log->last_log_index() << std::endl;


    meta_s = my_raft_meta_storage.new_instance(uri);
    auto meta_log_1 = static_cast<MyRaftMetaStorage*>(meta_s);
    meta_log_1->init();
    std::cout << "meta log term: " << meta_log->get_term() << std::endl;
    meta_log->get_votedfor(tmp_peer);
    std::cout << "meta log votedfor: " << tmp_peer.to_string() << std::endl;

    db1->close();
}

TEST(test_log_storage, test_index_map) {
    IndexTermMap index_map;
    for (int i = 0; i < 100; i++) {
        braft::LogId log(i, i);
        index_map.append(log);
    }
    std::cout << index_map.get_term(1) << std::endl;
    index_map.truncate_prefix(100);
    index_map.truncate_prefix(100);
    std::cout << index_map.get_term(100) << std::endl;
}

} // namespcae TKV

int main(int argc, char** argv) {
	::testing::InitGoogleTest(&argc, argv);
    ::testing::GTEST_FLAG(filter) = "test_log_storage.test_index_map";
	srand((unsigned)time(NULL));
	return RUN_ALL_TESTS();
}
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
