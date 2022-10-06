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
nohup ./TKVMeta --meta_id=0 &
nohup ./TKVMeta --meta_id=1 &
nohup ./TKVMeta --meta_id=2 &

