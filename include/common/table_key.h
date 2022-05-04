#pragma once
#include "common/key_encoder.h"
#include "common/schema_factory.h"
#include <rocksdb/slice.h>

namespace TKV {
class MutableKey;
class TableKey {
public:
    virtual ~TableKey() {}
    TableKey() : _full(false) {}
    TableKey(rocksdb::Slice key, bool full = false) 
        : _full(false)
        , _data(key) 
    {}

    TableKey(const TableKey& key)
        : _full(key._full)
        , _data(key._data)  
    {}
    
    TableKey(const MutableKey& key);
    
    int8_t extract_i8(int pos) const {
        char* c = const_cast<char*>(_data.data_ + pos);
        return KeyEncoder::decode_i8(*reinterpret_cast<uint8_t*>(c));
    }  
    uint8_t extract_u8(int pos) const {
        char* c = const_cast<char*>(_data.data_ + pos);
        return *reinterpret_cast<uint8_t*>(c);
    }

    int16_t extract_i16(int pos) const {
        char* c = const_cast<char*>(_data.data_ + pos);
        return KeyEncoder::decode_i16(KeyEncoder::to_endian_u16(*reinterpret_cast<uint16_t*>(c)));
    }
    uint16_t extract_u16(int pos) const {
        char* c = const_cast<char*>(_data.data_ + pos);
        return KeyEncoder::to_endian_u16(*reinterpret_cast<uint16_t*>(c));
    }
    
    int32_t extract_i32(int pos) const {
        char* c = const_cast<char*>(_data.data_ + pos);
        return KeyEncoder::decode_i32(KeyEncoder::to_endian_u32(*reinterpret_cast<uint32_t*>(c)));
    }
    uint32_t extract_u32(int pos) const {
        char* c = const_cast<char*>(_data.data_ + pos);
        return KeyEncoder::to_endian_u32(*reinterpret_cast<uint32_t*>(c));
    }
    
    int32_t extract_i64(int pos) const {
        char* c = const_cast<char*>(_data.data_ + pos);
        return KeyEncoder::decode_i64(KeyEncoder::to_endian_u64(*reinterpret_cast<uint64_t*>(c)));
    }
    uint32_t extract_u64(int pos) const {
        char* c = const_cast<char*>(_data.data_ + pos);
        return KeyEncoder::to_endian_u64(*reinterpret_cast<uint64_t*>(c));
    }

    float extract_float(int pos) const {
        char* c = const_cast<char*>(_data.data_ + pos);
        return KeyEncoder::decode_f32(KeyEncoder::to_endian_u32(*reinterpret_cast<uint32_t*>(c)));
    }
    
    bool extract_boolean(int pos) const {
       char* c = const_cast<char*>(_data.data_ + pos); 
       return *reinterpret_cast<uint8_t*>(c) != 0;
    }
    
    void extract_string(int pos, std::string& out) const {
        out.assign(_data.data_ + pos);
    }
    
    void extract_char(int pos, size_t len, std::string& out) {
        out.assign(_data.data_ + pos, len);
    }
    
    void set_full(bool full) {
        _full = full;
    }
    bool get_full() const {
        return _full;
    }
    size_t size() const {
        return _data.size();
    }
    
    const rocksdb::Slice& data() const {
        return _data;
    }

private:
    bool _full; // full key or just a prefix
    rocksdb::Slice _data;
};
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */

