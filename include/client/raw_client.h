#pragma once
#include "client/client_impl.h"
#include "common/closure.h"

namespace TKV {
struct TKVClosure: braft::Closure {
    braft::Closure* done;  // 外部closure
                           
    virtual void set_status(butil::Status s) {
        status() = s;
    }
    virtual void set_result(std::string& result);
};    


struct GetClosure: public TKVClosure {
    std::string*    result;
    braft::Closure* done;
    
    GetClosure(const std::string* result, braft::Closure* done)
        : result(result), done(done)
    {}

    void set_result(std::string& res) {
        *result = res;
    }

    void Run() {
        if (done) {
            done->Run();
        }
        delete this;
    }
};

struct PutClosure: public TKVClosure {
    braft::Closure* done;

    PutClosure(braft::Closure* done)
        : done(done)
    {}

    void RUn() {
        if (done) {
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
             const std::string& value) override {
        SyncClosure* done = new SyncClosure;
        _kv->put(key, value, new PutClosure(done));
        done->Wait();  
    }

    void get(const std::string& key, 
             std::string* value) override {
        SyncClosure* done = new SyncClosure;
        _kv->get(key, new GetClosure(value, done)); 
        done->wait();
    }


private:
    ClientImpl*     _kv; 
    std::string     _meta_server_bns; 
};
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
