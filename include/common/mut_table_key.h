#pragma once

#include "common/key_encoder.h"
#include <rocksdb/slice.h>

namespace TKV {
class TableKey;
class MutableKey {
public:
    virtual ~MutableKey() {}
    MutableKey(): _full(false) {}
    
    MutableKey(rocksdb::Slice key, bool full = false)
        : _full(full)
        , _data(key.data_, key.size_) 
    {}
    
    MutableKey(const TableKey& key)
        : _full(key.get_full())
        , _data(key.data().data_, key.data().size_)
    {}
    
    MutableKey& append_i8(int8_t val) {
        uint8_t encode = KeyEncoder::encode_i8(val);
        _data.append((char*)&encode, sizeof(uint8_t));
        return *this;
    }

    MutableKey& append_u8(uint8_t val) {
        _data.append((char*)&val, sizeof(uint8_t));
        return *this;
    }
    
    MutableKey& replace_u8(uint8_t val, int pos) {
        _data.replace(pos, 1, (char*)&val, 1);
        return *this;
    }
    
    MutableKey& append_i16(int16_t val) {
        uint16_t encode = KeyEncoder::to_endian_u16(KeyEncoder::encode_i16(val));
        _data.append((char*)&encode, sizeof(uint16_t));
        return *this;
    }
    MutableKey& append_u16(uint16_t val) {
        uint16_t encode = KeyEncoder::to_endian_u16(val);
        _data.append((char*)&encode, sizeof(uint16_t));
        return *this;
    }

    MutableKey& append_i32(int32_t val) {
        uint32_t encode = KeyEncoder::to_endian_u32(KeyEncoder::encode_i32(val));
        _data.append((char*)&encode, sizeof(uint32_t));
        return *this;
    }
    MutableKey& append_u32(uint32_t val) {
        uint16_t encode = KeyEncoder::to_endian_u16(val);
        _data.append((char*)&encode, sizeof(uint16_t));
        return *this;
    }

private:
    bool _full;// full key or just a prefix
    std::string _data;
}
}// namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
