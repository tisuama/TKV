#include "common/table_key.h"
#include "common/mut_table_key.h"

#include <iostream>
#include <climits>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <cstring>
#include <gtest/gtest.h>
#include <butil/sys_byteorder.h>

using namespace TKV;

TEST(test_append_i8, case_call) {
	MutableKey mut_key;
	for (int16_t i = SCHAR_MIN; i <= SCHAR_MAX; i++) {
		mut_key.append_i8((int8_t)i);
	}
	EXPECT_EQ(256 * sizeof(int8_t), mut_key.size());
	TableKey key(mut_key);
	int pos = 0;
	for (int16_t i = SCHAR_MIN; i <= SCHAR_MAX; i++) {
		EXPECT_EQ((int8_t)i, key.extract_i8(pos));
		pos += sizeof(uint8_t);
	}
}

TEST(test_append_float, case_call) {
	MutableKey mut_key;
	int count = 0;
	srand((unsigned)time(NULL));
	std::vector<float> float_vals;
	for (int i = 0; i < 10000; i++) {
		float val = (rand() - RAND_MAX / 2 + 0.0f) / RAND_MAX;
		mut_key.append_float(val);
		float_vals.push_back(val);
		count++;
	}
	EXPECT_EQ(count * sizeof(float), mut_key.size());
	TableKey key(mut_key);
	int pos = 0;
	for (int i = 0; i < 10000; i++) {
		float diff = key.extract_float(pos) - float_vals[i];
		EXPECT_EQ(true, diff < 1e-6);
		pos += sizeof(float);
	}
}

TEST(test_append_value, case_call) {
	uint32_t value = 129 << 24 | 12;
	uint32_t v2 = butil::HostToNet32(value);
	char buf[4];
	memcpy(buf, &v2, sizeof(v2));
	auto debug_format = [](uint8_t x) {
		for (int j = 0; j < 8; j++) {
			if (x & (1 << j)) {
				std::cout << 1;
			} else {
				std::cout << 0;
			}
		}
		std::cout << std::endl;
	};
	uint8_t y = -128;
	debug_format(y);	
	std::cout << (uint)y << std::endl;
	std::cout << std::hex << " " << value << " " << v2 << std::endl;
}

int main(int argc, char** argv) {
	::testing::InitGoogleTest(&argc, argv);
	srand((unsigned)time(NULL));
	return RUN_ALL_TESTS();
}
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
