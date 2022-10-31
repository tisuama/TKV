#pragma once

#include <string>
#include <braft/util.h>

#include "proto/store.pb.h"
#include "client/region_cache.h"

namespace TKV {

class BatchData {
friend class Iterator;
public:
    BatchData() = default;
    
    void put(const std::string& key, 
            const std::string& value,
            KeyLocation& key_location,
            braft::Closure* done); 

    void get(const std::string& key, 
            std::string* value,
            KeyLocation& key_location,
            braft::Closure* done);

    class Iterator;
    std::shared_ptr<Iterator> new_iterator() {
        return std::make_shared<Iterator>(this);
    }

    class Iterator {
        public:
            Iterator(BatchData* batch)
                : _batch(batch) 
            {
                _iter = _batch->_batch_request.cbegin();
            }

            bool valid() {
                return _iter != _batch->_batch_request.cend();
            }

            int64_t region_id() {
                return _iter->first;
            }

            pb::StoreReq* request() {
                return _batch->get_request(_iter->first);
            }

            pb::StoreRes* response() {
                return _batch->get_response(_iter->first);
            }

            std::vector<braft::Closure*>* done() {
                return _batch->get_closure(_iter->first);
            }
            
            RegionVerId& version() {
                return _batch->get_version(_iter->first);
            }
            
            void next() {
                _iter++;
            }

        private:
            BatchData* _batch;

            // 内置迭代器
            std::map<int64_t, pb::StoreReq>::const_iterator _iter;
    };

    pb::StoreReq* get_request(int64_t region_id) {
        return &_batch_request[region_id];
    }

    pb::StoreRes* get_response(int64_t region_id) {
        return &_batch_response[region_id];
    }
    
    RegionVerId& get_version(int64_t region_id) {
        return _batch_ver[region_id];
    }
     
    std::vector<braft::Closure*>* get_closure(int64_t region_id) {
        return &_batch_closure[region_id];
    }

private:
    
    // region_id -> request
    std::map<int64_t, pb::StoreReq> _batch_request;
    // region_id -> response
    std::map<int64_t, pb::StoreRes> _batch_response;
    // region_id -> closures
    std::map<int64_t, std::vector<braft::Closure*>> _batch_closure;
    // region_id -> region_ver_id
    std::map<int64_t, RegionVerId>  _batch_ver;
};
	
} // namespace TKV 
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
