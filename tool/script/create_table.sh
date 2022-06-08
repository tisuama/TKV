#!/bin/sh
#Created on 2017-11-22 
#测试场景：完成的meta_server流程

echo -e "\n"
#创建table
echo -e "创建table\n"
curl -d '{
    "op_type":"OP_CREATE_TABLE",
    "table_info": {
        "table_name": "TestTable",
        "database": "TestDB",
        "namespace_name": "TEST_NAMESPACE",
        "fields": [],
        "indexs": [] 
    }
}' http://$1/MetaService/meta_manager
echo -e "\n"

#查询table
curl -d '{
    "op_type" : "QUERY_SCHEMA" 
}' http://$1/MetaService/query

