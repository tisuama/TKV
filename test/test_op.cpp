#include <iostream>

#include "client/client.h"

using namespace TKV;
std::shared_ptr<TKV::Client> client;


int main() {
	client = TKV::NewRawKVClient("127.0.0.1", "TEST_TABLE");
	client->init();
	DB_WARNING("=== Test Op success");
}
