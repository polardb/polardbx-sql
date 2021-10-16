### Overview

This document explains the development process of PolarDB-X, covering code compilation, deployment and other processes.

> Note: This document is mainly for CentOS 7, other Linux distributions have similar processes.

### Before Starting

On your CentOS, get the source code of PolarDB-X from github repositories:

[GalaxyEngine](https://github.com/ApsaraDB/galaxyengine)

[GalaxySQL](https://github.com/ApsaraDB/galaxysql)

[GalaxyGlue](https://github.com/ApsaraDB/galaxyglue)

[GalaxyCDC](https://github.com/ApsaraDB/galaxycdc)

Check out the `main` branch of each repository.

### Compiling PolarDB-X Data Node (GalaxyEngine)

```shell
# install cmake3
yum install cmake3
ln -s /usr/bin/cmake3 /usr/bin/cmake


# install GCC7
yum install centos-release-scl
yum install devtoolset-7-gcc devtoolset-7-gcc-c++ devtoolset-7-binutils
echo "source /opt/rh/devtoolset-7/enable" >>/etc/profile

# install dependencies
yum install make automake git openssl-devel ncurses-devel bison libaio-devel


# enter galaxyengine directory
cd galaxyengine

# install boost 1.70 (Note: Put boost into the repository to avoid downloading)
wget https://boostorg.jfrog.io/artifactory/main/release/1.70.0/source/boost_1_70_0.tar.gz
mkdir extra/boost
cp boost_1_70_0.tar.gz extra/boost/

# compile & install
./build.sh
make -j8
make install
```

### Compiling PolarDB-X Compute Node (GalaxySQL&GalaxyGlue)

```shell
# install jdk 1.8, and configure the environment variables JAVA_HOME, PATH

# install maven 3.8
wget https://dlcdn.apache.org/maven/maven-3/3.8.3/binaries/apache-maven-3.8.3-bin.tar.gz
tar zxvf apache-maven-3.8.3-bin.tar.gz
export PATH=`pwd`/apache-maven-3.8.3/bin/:$PATH

# confirm maven version is 3.8.3
mvn -v

# update subtree
mv galaxyglue galaxysql/polardbx-rpc

# enter the galaxysql directory 
cd galaxysql/

# compile&install
mvn install -D maven.test.skip=true -D env=release 

# prepare to run polardb-x
tar zxvf target/polardbx-server-5.4.12-SNAPSHOT.tar.gz
```

### Compiling PolarDB-X CDC (GalaxyCDC)

```shell
cd galaxycdc

# compile&install
mvn install -D maven.test.skip=true -D env=release 

# prepare to run CDC
tar zxvf polardbx-cdc-assemble/target/polardbx-cdc*.tar.gz
```

### Start PolarDB-X DN
> NOTE: GalaxyEngine is a derivative work of MySQL, the following will use mysql and galaxyengine without distinction
> 
- This step starts a mysql process that acts as the metadb and DN
- Refer to the mysql configuration file in the appendix to modify it accordingly. By default, 4886 is used as mysql port and 32886 as private protocol port
- By default, /u01/my3306 is used as mysql data directory, you can change it to other directory


Start MySQL：
```shell
mkdir -p /u01/my3306/{data,log,run,tmp,mysql}
/u01/mysql/bin/mysqld --defaults-file=my.cnf --initialize-insecure
/u01/mysql/bin/mysqld --defaults-file=my.cnf
```

### Start PolarDB-X CN
Once the mysql process is started, PolarDB-X can be initialized and the following configurations need to be prepared:

- metadb user：`my_polarx`
- metadb database: `polardbx_meta_db_polardbx`
- dnPasswordKey： `855a7043dfcb1f9b`
- PolarDB-X default root user：`polarx_root`
- PolarDB-X default password：`123456`, you can reset it by `-polarxRootPasswd`


update conf/server.properties:
```text
# PolarDB-X Port
serverPort=8527
# PolarDB-X RPC Port
rpcPort=9090
 # MetaDB Address
metaDbAddr=127.0.0.1:4886
# MetaDB X-Protocol Port
metaDbXprotoPort=32886
# MetaDB account
metaDbUser=my_polarx
metaDbName=polardbx_meta_db_polardbx
# PolarDB-X Instance Name
instanceId=polardbx-polardbx
```

Initialize PolarDB-X:

- -I: Initialization mode
- -P: dnPasswordKey
- -d: DataNode's address list, in standalone mode, is the port and address of the previously started mysql process
- -r: metadb password
- -u: PolarDB-X root user
- -S: PolarDB-X root user password

```sql
bin/startup.sh \
	-I \
	-P 855a7043dfcb1f9b \
  -d 127.0.0.1:4886:32886 \
  -r "" \
  -u polardbx_root \
  -S "123456"
```

This step generates the internal and encrypted passwords, which need to be filled in the configuration file conf/server.properties for subsequent access:
```sql
======== Paste following configurations to conf/server.properties ! ======= 
metaDbPasswd=HMqvkvXZtT7XedA6t2IWY8+D7fJWIJir/mIY1Nf1b58=
```


Final Step, Run PolarDB-X:
```shell
bin/startup.sh -P 855a7043dfcb1f9b
```


Connect to PolarDB-X to verify, if you can connect, it means the database is started successfully, try some SQLs.
```sql
mysql -h127.1 -P8527 -upolardbx_root
```

### Run PolarDB-X CDC
After the PolarDB-X is running, the PolarDB-X CDC component can be initialized, and the following configurations need to be prepared.

- metadb user：same as before `my_polarx`
- metadb database: same as before `polardbx_meta_db_polardbx`
- metadb password：same as before
- metadb port：same as before `3306`
- dnPasswordKey：same as before `855a7043dfcb1f9b`
- PolarDB-X user：same as before `polarx_root`
- PolarDB-X password：same as before `123456`
- PolarDB-X Port：same as before `8527`
- 


Modify the configuration file conf/config.properties and replace ${HOME} in the following example with the current user's home directory, e.g. /home/mysql
```shell
polardbx.instance.id=polardbx-polardbx
mem_size=16000
metaDb_url=jdbc:mysql://127.0.0.1:3306/polardbx_meta_db_polardbx?useSSL=false
metaDb_username=my_polarx
metaDb_password=xxx
polarx_url=jdbc:mysql://127.0.0.1:8527/__cdc__
polarx_username=polardbx_root
polarx_password=123456
dnPasswordKey=855a7043dfcb1f9b
storage.persistBasePath=${HOME}/logs/rocksdb
binlog.dir.path=${HOME}/binlog/
```
Next, you can start the CDC daemon process with the command shown below. 
After starting, check the process status by `jps` command, CDC will have 3 processes, namely DaemonBootStrap, TaskBootStrap and DumperBootStrap.
The system log of CDC will be output to ${HOME}/logs directory, and the global binary log will be output to directory configured by binlog.dir.path.
```shell
bin/daemon.sh start
```
Connect to PolarDB-X, do some DDL or DML operations, then execute `SHOW BINARY LOGS` and `SHOW BINLOG EVENTS` commands to verify the global binlog, enjoy it!

### Appendix
#### mysql configuration file
```yaml
[mysqld]
socket = /u01/my3306/run/mysql.sock
datadir = /u01/my3306/data
tmpdir = /u01/my3306/tmp
log-bin = /u01/my3306/mysql/mysql-bin.log
log-bin-index = /u01/my3306/mysql/mysql-bin.index
# log-error = /u01/my3306/mysql/master-error.log
relay-log = /u01/my3306/mysql/slave-relay.log
relay-log-info-file = /u01/my3306/mysql/slave-relay-log.info
relay-log-index = /u01/my3306/mysql/slave-relay-log.index
master-info-file = /u01/my3306/mysql/master.info
slow_query_log_file = /u01/my3306/mysql/slow_query.log
innodb_data_home_dir = /u01/my3306/mysql
innodb_log_group_home_dir = /u01/my3306/mysql

port = 4886
loose_polarx_port = 32886
loose_galaxyx_port = 32886
loose_polarx_max_connections = 5000

loose_server_id = 476984231
loose_cluster-info = 127.0.0.1:14886@1
loose_cluster-id = 5431
loose_enable_gts = 1
loose_innodb_undo_retention=1800



core-file
loose_log_sql_info=1
loose_log_sql_info_index=1
loose_indexstat=1
loose_tablestat=1
default_authentication_plugin=mysql_native_password

# close 5.6 variables for 5.5
binlog_checksum=CRC32
log_bin_use_v1_row_events=on
explicit_defaults_for_timestamp=OFF
binlog_row_image=FULL
binlog_rows_query_log_events=ON
binlog_stmt_cache_size=32768

#innodb
innodb_data_file_path=ibdata1:100M;ibdata2:200M:autoextend
innodb_buffer_pool_instances=8
innodb_log_files_in_group=4
innodb_log_file_size=200M
innodb_log_buffer_size=200M
innodb_flush_log_at_trx_commit=1
#innodb_additional_mem_pool_size=20M #deprecated in 5.6
innodb_max_dirty_pages_pct=60
innodb_io_capacity_max=10000
innodb_io_capacity=6000
innodb_thread_concurrency=64
innodb_read_io_threads=8
innodb_write_io_threads=8
innodb_open_files=615350
innodb_file_per_table=1
innodb_flush_method=O_DIRECT
innodb_change_buffering=none
innodb_adaptive_flushing=1
#innodb_adaptive_flushing_method=keep_average #percona
#innodb_adaptive_hash_index_partitions=1      #percona
#innodb_fast_checksum=1                       #percona
#innodb_lazy_drop_table=0                     #percona
innodb_old_blocks_time=1000
innodb_stats_on_metadata=0
innodb_use_native_aio=1
innodb_lock_wait_timeout=50
innodb_rollback_on_timeout=0
innodb_purge_threads=1
innodb_strict_mode=1
#transaction-isolation=READ-COMMITTED
innodb_disable_sort_file_cache=ON
innodb_lru_scan_depth=2048
innodb_flush_neighbors=0
innodb_sync_array_size=16
innodb_print_all_deadlocks
innodb_checksum_algorithm=CRC32
innodb_max_dirty_pages_pct_lwm=10
innodb_buffer_pool_size=500M

#myisam
concurrent_insert=2
delayed_insert_timeout=300

#replication
slave_type_conversions="ALL_NON_LOSSY"
slave_net_timeout=4
skip-slave-start=OFF
sync_master_info=10000
sync_relay_log_info=1
master_info_repository=TABLE
relay_log_info_repository=TABLE
relay_log_recovery=0
slave_exec_mode=STRICT
#slave_parallel_type=DATABASE
slave_parallel_type=LOGICAL_CLOCK
loose_slave_pr_mode=TABLE
slave-parallel-workers=32

#binlog
server_id=193317851
binlog_cache_size=32K
max_binlog_cache_size=2147483648
loose_consensus_large_trx=ON
max_binlog_size=500M
max_relay_log_size=500M
relay_log_purge=OFF
binlog-format=ROW
sync_binlog=1
sync_relay_log=1
log-slave-updates=0
expire_logs_days=0
rpl_stop_slave_timeout=300
slave_checkpoint_group=1024
slave_checkpoint_period=300
slave_pending_jobs_size_max=1073741824
slave_rows_search_algorithms='TABLE_SCAN,INDEX_SCAN'
slave_sql_verify_checksum=OFF
master_verify_checksum=OFF

# parallel replay
binlog_transaction_dependency_tracking = WRITESET
transaction_write_set_extraction = XXHASH64


#gtid
gtid_mode=OFF
enforce_gtid_consistency=OFF

loose_consensus-io-thread_cnt=8
loose_consensus-worker-thread_cnt=8
loose_consensus_max_delay_index=10000
loose_consensus-election-timeout=10000
loose_consensus_max_packet_size=131072
loose_consensus_max_log_size=20M
loose_consensus_auto_leader_transfer=ON
loose_consensus_log_cache_size=536870912
loose_consensus_prefetch_cache_size=268435456
loose_consensus_prefetch_window_size=100
loose_consensus_auto_reset_match_index=ON
loose_cluster-mts-recover-use-index=ON
loose_async_commit_thread_count=128
loose_replicate-same-server-id=on
loose_commit_lock_done_count=1
loose_binlog_order_commits=OFF
loose_cluster-log-type-node=OFF

#thread pool
# thread_pool_size=32
# thread_pool_stall_limit=30
# thread_pool_oversubscribe=10
# thread_handling=pool-of-threads

#server
default-storage-engine=INNODB
character-set-server=utf8
lower_case_table_names=1
skip-external-locking
open_files_limit=615350
safe-user-create
local-infile=1
sql_mode='NO_ENGINE_SUBSTITUTION'
performance_schema=0


log_slow_admin_statements=1
loose_log_slow_verbosity=full
long_query_time=1
slow_query_log=0
general_log=0
loose_rds_check_core_file_enabled=ON

table_definition_cache=32768
eq_range_index_dive_limit=200
table_open_cache_instances=16
table_open_cache=32768

thread_stack=1024k
binlog_cache_size=32K
net_buffer_length=16384
thread_cache_size=256
read_rnd_buffer_size=128K
sort_buffer_size=256K
join_buffer_size=128K
read_buffer_size=128K

# skip-name-resolve
#skip-ssl
max_connections=36000
max_user_connections=35000
max_connect_errors=65536
max_allowed_packet=1073741824
connect_timeout=8
net_read_timeout=30
net_write_timeout=60
back_log=1024

loose_boost_pk_access=1
log_queries_not_using_indexes=0
log_timestamps=SYSTEM
innodb_read_ahead_threshold=0

loose_io_state=1
loose_use_myfs=0
loose_daemon_memcached_values_delimiter=':;:'
loose_daemon_memcached_option="-t 32 -c 8000 -p15506"

innodb_doublewrite=1
```
