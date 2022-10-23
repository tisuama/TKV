#pragma once

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


class Client {
public:
    Client(const std::string& meta_server_bns)
       : _impl(meta_server_bns) 
    {} 
    
    int init();

    void get(const std::string& key, 
             std::string* value) {
        GetClosure* done = new GetClosure(value, nullptr);
        _impl->get(key, done); 
    }


private:
    ClientImpl _impl; 
};
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
