#pragma once
#include <cstdint>

namespace TKV {
class UpdateRegionStatus {
public:
	virtual ~UpdateRegionStatus() {}

	static UpdateRegionStatus* get_instance() {
		static UpdateRegionStatus instance;
		return &instance;
	}
	
	void reset_region_status(int64_t region_id);
private:
	UpdateRegionStatus() {}
};

class CanAddPeerSetter {
public:
	virtual ~CanAddPeerSetter() {}

	static CanAddPeerSetter* get_instance() {
		static CanAddPeerSetter instance;
		return &instance;
	}
	
	void set_can_add_peer(int64_t region_id);
private:
	CanAddPeerSetter() {}
};
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
