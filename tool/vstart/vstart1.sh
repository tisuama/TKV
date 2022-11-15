build_dir=/root/TKV/build/

# delete dir
cd $build_dir
pkill TKVMeta
rm -rf meta*
mkdir meta0 meta1 meta2
pkill TKVStore
rm -rf store*
mkdir store0 store1 store2

# restart meta server
nohup ./TKVMeta --meta_id=0 > /dev/null 2>&1 &
nohup ./TKVMeta --meta_id=1 > /dev/null 2>&1 &
nohup ./TKVMeta --meta_id=2 > /dev/null 2>&1 &

sleep 5

echo "=== Prepare Base Env"
nohup ./test_meta_server --cmd=prepare > /dev/null 2>&1 &

sleep 3

echo "=== START TKVStore"
nohup ./TKVStore --store_id=0 > /dev/null 2>&1 &
nohup ./TKVStore --store_id=1 > /dev/null 2>&1 &
nohup ./TKVStore --store_id=2 > /dev/null 2>&1 &

sleep 3

echo "=== ADD TABLE"
nohup ./test_meta_server --cmd=add_table > /dev/null 2>&1 &

echo "=== TKV INIT SUCCESS"
