### 概述
作为一个普通开发者，通常使用CentOS等操作系统进行开发，如何从零参与到PolarDB-X的开发中呢？

本文档对PolarDB-X的开发流程进行了说明，覆盖代码编译、数据库安装、部署等流程。

备注：本文档主要针对CentOS7和Ubuntu20操作系统，其他Linux发行版原理类似。

### 准备工作

- 下载galaxyengine代码: https://github.com/ApsaraDB/galaxyengine ，main分支
- 下载galaxysql代码：https://github.com/ApsaraDB/galaxysql ，main分支
- 下载galaxyglue代码：https://github.com/ApsaraDB/galaxyglue ，main分支
- 下载galaxycdc代码：https://github.com/ApsaraDB/galaxycdc ，main分支

### 编译 PolarDB-X DN (存储节点，代号GalaxyEngine)

此步骤编译和安装GalaxyEngine（mysql）

**安装依赖（CentOS7)**

```bash
yum install cmake3
ln -s /usr/bin/cmake3 /usr/bin/cmake


# 安装GCC7
yum install centos-release-scl
yum install devtoolset-7-gcc devtoolset-7-gcc-c++ devtoolset-7-binutils
echo "source /opt/rh/devtoolset-7/enable" >>/etc/profile

# 安装依赖
yum install make automake git openssl-devel ncurses-devel bison libaio-devel
```

**安装依赖（Ubuntu20）**

```bash
# 安装GCC7
apt install -y gcc-7 g++-7
update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-7 60 \
                         --slave /usr/bin/g++ g++ /usr/bin/g++-7 
update-alternatives --config gcc
gcc --version
g++ --version

# 安装依赖
apt install make automake git bison libaio-dev libncurses-dev libsasl2-dev libldap2-dev
```

**编译**

```bash
# 进入 galaxyengine 代码路径
cd galaxyengine

# 安装boost1.70 (注：把boost放到仓库里避免下载）
wget https://boostorg.jfrog.io/artifactory/main/release/1.70.0/source/boost_1_70_0.tar.gz
mkdir extra/boost
cp boost_1_70_0.tar.gz extra/boost/

# 编译安装
# 详细参数请参考 https://dev.mysql.com/doc/refman/8.0/en/source-configuration-options.html
cmake .                               	\
    -DFORCE_INSOURCE_BUILD=ON           \
    -DCMAKE_BUILD_TYPE="Debug"          \
    -DSYSCONFDIR="/u01/mysql"           \
    -DCMAKE_INSTALL_PREFIX="/u01/mysql" \
    -DMYSQL_DATADIR="/u01/mysql/data"   \
    -DWITH_BOOST="./extra/boost/boost_1_70_0.tar.gz"
make -j8
make install
```


### 编译 PolarDB-X CN (计算节点，代号GalaxySQL)
此步骤编译和安装galaxysql & galaxyglue代码。
```yaml
# 安装jdk1.8, 并配置环境变量JAVA_HOME、PATH

# 安装maven3.8
wget https://dlcdn.apache.org/maven/maven-3/3.8.3/binaries/apache-maven-3.8.3-bin.tar.gz
tar zxvf apache-maven-3.8.3-bin.tar.gz
export PATH=`pwd`/apache-maven-3.8.3/bin/:$PATH

# 确认Maven版本为3.8.3
mvn -v

# 确保 polardbx-rpc (galaxyglue) 已经初始化
git submodule update --init

# 进入代码目录 
cd galaxysql/

# 编译打包
mvn install -D maven.test.skip=true -D env=release 

# 解压运行
tar zxvf target/polardbx-server-5.4.12-SNAPSHOT.tar.gz
```

### 编译 PolarDB-X CDC（日志节点，代号GalaxyCDC）
此步骤编译和安装galaxycdc代码。
```yaml
# 进入CDC代码

# 编译打包
mvn install -D maven.test.skip=true -D env=release 

# 包在/polardbx-cdc-assemble/target/

# 解压运行
tar zxvf polardbx-binlog.tar.gz
```

### 启动PolarDB-X DN

- 此步骤启动一个mysql进程，作为metadb和dn
- 参考附录中的mysql配置文件，可进行相应修改，默认使用 4886 作为 mysql端口，32886 作为私有协议端口
- 默认使用 /u01/my3306 作为mysql数据目录，可以修改成其他目录
  
> 注意：启动 DN 需要使用非 root 账号完成

启动mysql：
```yaml
mkdir -p /u01/my3306/{data,log,run,tmp,mysql}
/u01/mysql/bin/mysqld --defaults-file=my.cnf --initialize-insecure
/u01/mysql/bin/mysqld --defaults-file=my.cnf
```


### 启动PolarDB-X CN
启动mysql进程之后，便可以初始化PolarDB-X，需要准备以下几个配置：

- metadb user：以下采用`my_polarx`
- metadb database：创建metadb库，以下采用 `polardbx_meta_db_polardbx`
- 密码加密key（dnPasswordKey)：以下采用 `asdf1234ghjk5678`
- PolarDB-X默认用户名：默认为 `polarx_root`
- PolarDB-X默认用户密码：默认为 `123456`，可通过 `-S` 参数修改

> 注意：启动 CN 需要使用非 root 账号完成

修改配置文件 conf/server.properties:
```basic
# PolarDB-X 服务端口
serverPort=8527
# PolarDB-X RPC 端口
rpcPort=9090
 # MetaDB地址
metaDbAddr=127.0.0.1:4886
# MetaDB私有协议端口
metaDbXprotoPort=32886
# MetaDB用户
metaDbUser=my_polarx
metaDbName=polardbx_meta_db_polardbx
# PolarDB-X实例名
instanceId=polardbx-polardbx
```


初始化PolarDB-X：

- -I: 进入初始化模式
- -P: 之前准备的dnPasswordKey
- -d: DataNode的地址列表，单机模式下就是之前启动的mysql进程的端口和地址
- -r: 连接metadb的密码
- -u: 为PolarDB-X创建的根用户
- -S: 为PolarDB-X创建的根用户密码

```sql
bin/startup.sh \
    -I \
    -P asdf1234ghjk5678 \
    -d 127.0.0.1:4886:32886 \
    -r "" \
    -u polardbx_root \
    -S "123456"
```

此步骤中会生成内部密码及加密密码，需要将其填写配置文件 conf/server.properties 中，用于后续访问:
```sql
Generate password for user: my_polarx && M8%V5%K9^$5%oY0%yC0+&1!J7@8+R6)
Encrypted password: DB84u4UkU/OYlMzu3aj9NFdknvxYgedFiW9z59bVnoc=
Root user for polarx with password: polardbx_root && 123456
Encrypted password for polarx: H1AzXc2NmCs61dNjH5nMvA==
======== Paste following configurations to conf/server.properties ! ======= 
metaDbPasswd=HMqvkvXZtT7XedA6t2IWY8+D7fJWIJir/mIY1Nf1b58=
```


最后一步，启动PolarDB-X：
```yaml
bin/startup.sh -P asdf1234ghjk5678 
```


连接PolarDB-X验证，如果能连上，说明数据库启动成功啦，可以愉快地运行各种SQL啦：
```sql
mysql -h127.1 -P8527 -upolardbx_root
```


### 启动PolarDB-X CDC
启动PolarDB-X进程之后，便可以初始化PolarDB-X CDC组件，需要准备以下几个配置：

- metadb user：和启动PolarDB-X时设置的值保持一致，以下采用`my_polarx`
- metadb database：和启动PolarDB-X时设置的值保持一致，以下采用 `polardbx_meta_db_polardbx`
- metadb password：和启动PolarDB-X时设置的值保持一致，需使用密文，以下采用`HMqvkvXZtT7XedA6t2IWY8+D7fJWIJir/mIY1Nf1b58=`
- metadb port：和启动MySQL时设置的值保持一致，以下采用 `4886`
- 密码加密key（dnPasswordKey)：和启动PolarDB-X时设置的值保持一致，以下采用 `asdf1234ghjk5678`
- PolarDB-X用户名：和启动PolarDB-X时设置的值保持一致，以下采用默认值 `polarx_root`
- PolarDB-X用户密码：和启动PolarDB-X时设置的值保持一致，需使用密文，以下采用默认值`H1AzXc2NmCs61dNjH5nMvA==`
- PolarDB-X端口：和启动PolarDB-X时设置的值保持一致，以下采用默认值 `8527`
- 当前机器分配给CDC使用的内存大小：以下采用16000代指，单位为M，实际配置值请替换为真实值

> 注意：启动 CDC 需要使用非 root 账号完成

修改配置文件 conf/config.properties，将如下示例中的${HOME}替换为当前用户的根目录，如/home/mysql
```shell
useEncryptedPassword=true
polardbx.instance.id=polardbx-polardbx
mem_size=16000
metaDb_url=jdbc:mysql://127.0.0.1:4886/polardbx_meta_db_polardbx?useSSL=false
metaDb_username=my_polarx
metaDbPasswd=HMqvkvXZtT7XedA6t2IWY8+D7fJWIJir/mIY1Nf1b58=
polarx_url=jdbc:mysql://127.0.0.1:8527/__cdc__
polarx_username=polardbx_root
polarx_password=H1AzXc2NmCs61dNjH5nMvA==
dnPasswordKey=asdf1234ghjk5678
storage.persistBasePath=${HOME}/logs/rocksdb
binlog.dir.path=${HOME}/binlog/
```
接下来，即可启动CDC daemon进程，命令如下所示。启动之后，通过jps命令查看进程状态，CDC会有3个附属进程，分别是DaemonBootStrap、TaskBootStrap和DumperBootStrap，CDC的系统日志会输出到${HOME}/logs目录下，全局binlog日志会输出到binlog.dir.path参数配置的目录下，TaskBootStrap进程和DumperBootStrap进程被kill后，会被Daemon进程自动拉起。
```shell
bin/daemon.sh start
```
登录PolarDB-X，执行一些DDL或DML操作，然后执行show binary logs 和show binlog events命令，验证全局binlog的生成状态，enjoy it！

### 附录
#### mysql配置文件
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
