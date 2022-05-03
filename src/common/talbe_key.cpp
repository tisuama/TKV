#include "common/table_key.h"
#include "common/mut_table_key.h"

namespace TKV {
TableKey::TableKey(const MutableKey& key)
	: _full(key.get_full())
    , _data(key.data())
	{}
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
