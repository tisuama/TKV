#include <iostream>

#include "client/client.h"

using namespace TKV;
std::shared_ptr<TKV::Client> client;

void put() {
	std::string key = "test_key";
	std::string value = "test_value";
	int ret = client->put(key, value);
	std::cout << "put result: " << ret << std::endl;
}

int main() {
	client = TKV::NewRawKVClient("127.0.0.1:8010", "TEST_TABLE");
	int ret = client->init();
	if (ret < 0) {
		std::cout << "RawClient init failed" << std::endl;
	}

	put();
}
