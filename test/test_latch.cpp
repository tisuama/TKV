#include <iostream>
#include "common/latch.h"

using namespace TKV;

int main() {
	Latches latches(2);
	std::vector<std::string> keyA{"a", "b", "c", "d"};
	auto lockA = latches.gen_lock(1, keyA);
	bool okA = latches.acquire(lockA);
	std::cout << "acquire A: " << okA << " " << lockA << std::endl;

	std::vector<std::string> keyB{"e", "a", "b", "f"};
	auto lockB = latches.gen_lock(2, keyA);
	bool okB = latches.acquire(lockB);
	std::cout << "acquire B: " << okB << " " << lockB << std::endl;
	
	lockA->commit_ts = 3;
	auto weakup_list = latches.release(lockA);
	for (auto x: weakup_list) {
		std::cout << "release info: " << x << std::endl;
		for (auto key: x->keys) {
			std::cout << "weakup key: " << key << std::endl;
		}
	}

	okB = latches.acquire(weakup_list[0]);
	std::cout << "relock result: " << okB << std::endl;
}
