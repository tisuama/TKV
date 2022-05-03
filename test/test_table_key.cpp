#include "common/table_key.h"
#include "common/mut_table_key.h"

#include <iostream>
#include <climits>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <cstring>
#include <gtest/gtest.h>

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

int main(int argc, char** argv) {
	::testing::InitGoogleTest(&argc, argv);
	srand((unsigned)time(NULL));
	return RUN_ALL_TESTS();
}
