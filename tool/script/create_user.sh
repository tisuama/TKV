#!/bin/sh

# 完成meta_server 流程

# 创建用户
echo -e "create user\n"

curl -s -d '{
	"op_type":"OP_CREATE_USER",
	"user_privilege": {
		"username": "root",
		"password": "password",
		"namespace_name": "TEST_NAMESPACE",
		"database": [{
				"datebase": "TEST_DB",
				"rw": "WRITE"
			}],
	    "bns": ["offline"],
		"ip": ["127.0.0.1", "127.0.0.1"]
		}
	}' http://$1/MetaService/meta_manager		
	
# 查询权限

curl -s -d '{
	"op_type": "QUERY_USERRPRIVILEG"
}' http://$1/MetaService/query

echo -e "\n"
	

