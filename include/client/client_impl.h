#pragma once
#include <braft/raft.h>
#include <string>


namespace TKV {
class ClientImpl {
public:
    ClientImpl(const std::string& meta_server_bns);

    int init();
    
    void put(const std::string& key,
             const std::string& value,
             braft::Closure* done);

    void get(const std::string& key, 
             std::string* value,
             braft::Closure* done);
private: 
    std::string     _meta_server_bns;    
}; 
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
