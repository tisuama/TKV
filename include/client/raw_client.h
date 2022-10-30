#pragma once
#include "client/client_impl.h"
#include "common/closure.h"

namespace TKV {
struct ClientClosure: braft::Closure {
    braft::Closure* done;  // 外部closure
                           
    ClientClosure(braft::Closure* done)
        : done(done)
    {}
    
    virtual void set_status(const butil::Status& s) {
        status() = s;
    }
    virtual void set_result(std::string& result) { 
        /* Nothing To do */
    }
    
    void Run() {
        if (!stauts().ok()) {
            DB_WARNING("KV request error, errmsg: %s", status().error_cstart());
        }
    }

};    


struct GetClosure: public ClientClosure {
    std::string*    result;
    braft::Closure* done;
    
    GetClosure(const std::string* result, braft::Closure* done)
        : result(result), done(done)
    {}

    void set_result(std::string& res) {
        *result = res;
    }

    void Run() {
        if (!stauts().ok()) {
            DB_WARNING("KV request error, errmsg: %s", status().error_cstart());
        }
        if (done) {
            done->status() = status();
            done->Run();
        }
        delete this;
    }
};

class RawKVClient: public Client {
public:
    RawKVClient(const std::string& meta_server_bns)
        : _meta_server_bns(meta_server_bns)
    {} 
    
    int init() override;
    
    void put(const std::string& key,
             const std::string& value) override;

    void get(const std::string& key, 
             std::string* value) override;


private:
    ClientImpl*     _kv; 
    std::string     _meta_server_bns; 
};
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
