#include <iostream>

#include "client/client.h"
#include "client/txn.h"

using namespace TKV;
std::shared_ptr<TKV::Client> client;

int main() {
    auto cluster = NewCluster("127.0.0.1:8010", "TEST_TABLE");
    Txn txn1(cluster);
    txn1.set("a", "a");
    txn1.commit();
}
