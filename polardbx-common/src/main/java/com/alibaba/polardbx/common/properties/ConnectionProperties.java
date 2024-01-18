/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.common.properties;

/**
 * 用于执行的ExtraCmd
 *
 * @author Dreamond
 */
public class ConnectionProperties {

    public static final String PLAN_CACHE = "PLAN_CACHE";
    public static final String PHY_SQL_TEMPLATE_CACHE = "PHY_SQL_TEMPLATE_CACHE";
    public static final String PREPARE_OPTIMIZE = "PREPARE_OPTIMIZE";

    public static final String ENABLE_RECYCLEBIN = "ENABLE_RECYCLEBIN";

    public static final String SHOW_TABLES_CACHE = "SHOW_TABLES_CACHE";

    public final static String MERGE_CONCURRENT = "MERGE_CONCURRENT";

    public final static String MERGE_UNION = "MERGE_UNION";

    public final static String MERGE_UNION_SIZE = "MERGE_UNION_SIZE";

    /**
     * minimum count of union physical sqls in a query
     */
    public final static String MIN_MERGE_UNION_SIZE = "MIN_MERGE_UNION_SIZE";

    /**
     * 表的meta超时时间，单位毫秒，默认5分钟
     */
    public static final String TABLE_META_CACHE_EXPIRE_TIME = "TABLE_META_CACHE_EXPIRE_TIME";

    public static final String OPTIMIZER_CACHE_EXPIRE_TIME = "OPTIMIZER_CACHE_EXPIRE_TIME";

    public static final String OPTIMIZER_CACHE_SIZE = "OPTIMIZER_CACHE_SIZE";

    public static final String ALLOW_FULL_TABLE_SCAN = "ALLOW_FULL_TABLE_SCAN";

    public static final String CHOOSE_STREAMING = "CHOOSE_STREAMING";

    public static final String CHOOSE_BROADCAST_WRITE = "CHOOSE_BROADCAST_WRITE";

    public static final String HBASE_MAPPING_FILE = "HBASE_MAPPING_FILE";

    public static final String FETCH_SIZE = "FETCH_SIZE";

    public static final String INIT_CONCURRENT_POOL_EVERY_CONNECTION = "INIT_CONCURRENT_POOL_EVERY_CONNECTION";

    public static final String CONCURRENT_THREAD_SIZE = "CONCURRENT_THREAD_SIZE";

    public static final String MAX_CONCURRENT_THREAD_SIZE = "MAX_CONCURRENT_THREAD_SIZE";

    public static final String PROCESS_AUTO_INCREMENT_BY_SEQUENCE = "PROCESS_AUTO_INCREMENT_BY_SEQUENCE";

    public static final String COLUMN_LABEL_INSENSITIVE = "COLUMN_LABEL_INSENSITIVE";

    public static final String RECORD_SQL = "RECORD_SQL";

    public static final String SOCKET_TIMEOUT = "SOCKET_TIMEOUT";

    public static final String TRANSACTION_POLICY = "TRANSACTION_POLICY";

    public static final String SHARE_READ_VIEW = "SHARE_READ_VIEW";

    public static final String ENABLE_TRX_SINGLE_SHARD_OPTIMIZATION = "ENABLE_TRX_SINGLE_SHARD_OPTIMIZATION";

    public static final String ENABLE_TRX_READ_CONN_REUSE = "ENABLE_TRX_READ_CONN_REUSE";

    public static final String GET_TSO_TIMEOUT = "GET_TSO_TIMEOUT";

    public static final String PURGE_HISTORY_MS = "PURGE_HISTORY_MS";

    /**
     * Max single TSO/XA/2PC transaction time
     */
    public static final String MAX_TRX_DURATION = "MAX_TRX_DURATION";

    public static final String EXPLAIN_X_PLAN = "EXPLAIN_X_PLAN";

    public static final String TRANSACTION_ISOLATION = "TRANSACTION_ISOLATION";

    public static final String TX_ISOLATION = "TX_ISOLATION";

    /**
     * 并行模式是否等所有节点返回数据后再返回，默认false
     */
    public static final String BLOCK_CONCURRENT = "BLOCK_CONCURRENT";

    public static final String GROUP_CONCURRENT_BLOCK = "GROUP_CONCURRENT_BLOCK";

    public static final String OSS_FILE_CONCURRENT = "OSS_FILE_CONCURRENT";

    /**
     * 是否强制串行执行，默认false
     */
    public static final String SEQUENTIAL_CONCURRENT_POLICY = "SEQUENTIAL_CONCURRENT_POLICY";

    public static final String FIRST_THEN_CONCURRENT_POLICY = "FIRST_THEN_CONCURRENT_POLICY";

    public static final String DML_EXECUTION_STRATEGY = "DML_EXECUTION_STRATEGY";

    public static final String DML_PUSH_DUPLICATE_CHECK = "DML_PUSH_DUPLICATE_CHECK";

    public static final String DML_SKIP_TRIVIAL_UPDATE = "DML_SKIP_TRIVIAL_UPDATE";

    public static final String DML_SKIP_DUPLICATE_CHECK_FOR_PK = "DML_SKIP_DUPLICATE_CHECK_FOR_PK";

    public static final String DML_SKIP_CRUCIAL_ERR_CHECK = "DML_SKIP_CRUCIAL_ERR_CHECK";

    /**
     * 是否允许在 RC 的隔离级别下下推 REPLACE
     */
    public static final String DML_FORCE_PUSHDOWN_RC_REPLACE = "DML_FORCE_PUSHDOWN_RC_REPLACE";
    /**
     * 是否使用 returning 优化
     */
    public static final String DML_USE_RETURNING = "DML_USE_RETURNING";

    /**
     * 是否使用 GSI 检查冲突的插入值
     */
    public static final String DML_GET_DUP_USING_GSI = "DML_GET_DUP_USING_GSI";

    /**
     * DML 检查冲突列时下发 DN 的一条 SQL 所能包含的最大 UNION 数量，<= 0 表示无限制
     */
    public static final String DML_GET_DUP_UNION_SIZE = "DML_GET_DUP_UNION_SIZE";

    /**
     * DML 检查冲突列时，在允许时是否使用 IN 来代替 UNION；会增加死锁概率
     */
    public static final String DML_GET_DUP_USING_IN = "DML_GET_DUP_USING_IN";

    /**
     * DML 检查冲突列时下发 DN 的一条 SQL 所能包含的最大 IN 数量，<= 0 表示无限制
     */
    public static final String DML_GET_DUP_IN_SIZE = "DML_GET_DUP_IN_SIZE";

    /**
     * 是否使用 duplicated row count 作为 INSERT IGNORE 的 affected rows
     */
    public static final String DML_RETURN_IGNORED_COUNT = "DML_RETURN_IGNORED_COUNT";

    /**
     * 在 Logical Relocate 时是否跳过没有发生变化的行，不下发任何物理 SQL
     */
    public static final String DML_RELOCATE_SKIP_UNCHANGED_ROW = "DML_RELOCATE_SKIP_UNCHANGED_ROW";

    /**
     * DML 执行时是否检查主键冲突
     */
    public static final String PRIMARY_KEY_CHECK = "PRIMARY_KEY_CHECK";

    /**
     * 是否开启 Foreign Key
     */
    public static final String ENABLE_FOREIGN_KEY = "ENABLE_FOREIGN_KEY";

    /**
     * 是否开启 Foreign Constraint Check
     */
    public static final String FOREIGN_KEY_CHECKS = "FOREIGN_KEY_CHECKS";

    /**
     * CN 是否开启 Foreign Constraint Check, 优先级最高
     * 0 -> 关闭
     * 1 -> 开启
     * 2 -> 未设置
     */
    public static final String CN_FOREIGN_KEY_CHECKS = "CN_FOREIGN_KEY_CHECKS";

    /**
     * 是否开启 UPDATE/DELETE 语句的 Foreign Constraint Check
     */
    public static final String FOREIGN_KEY_CHECKS_FOR_UPDATE_DELETE = "FOREIGN_KEY_CHECKS_FOR_UPDATE_DELETE";

    /**
     * 在 RelocateWriter 中是否通过 PartitionField 判断拆分键是否变化
     */
    public static final String DML_USE_NEW_SK_CHECKER = "DML_USE_NEW_SK_CHECKER";

    public static final String DML_PRINT_CHECKER_ERROR = "DML_PRINT_CHECKER_ERROR";

    /**
     * 在 INSERT IGNORE、REPLACE、UPSERT 的时候使用 PartitionField 判断重复值
     */
    public static final String DML_USE_NEW_DUP_CHECKER = "DML_USE_NEW_DUP_CHECKER";

    /**
     * 在 REPLACE、UPSERT 的时候是否跳过相同行比较（将导致 affected rows 不正确）
     */
    public static final String DML_SKIP_IDENTICAL_ROW_CHECK = "DML_SKIP_IDENTICAL_ROW_CHECK";

    /**
     * 在 REPLACE、UPSERT 的时候是否跳过含有 JSON 的相同行比较（因为 CN 不支持 JSON 比较）
     */
    public static final String DML_SKIP_IDENTICAL_JSON_ROW_CHECK = "DML_SKIP_IDENTICAL_JSON_ROW_CHECK";

    public static final String DML_SELECT_SAME_ROW_ONLY_COMPARE_PK_UK_SK = "DML_SELECT_SAME_ROW_ONLY_COMPARE_PK_UK_SK";

    /**
     * INSERT 中的 VALUES 出现列名时是否替换为插入值而不是默认值，以兼容 MySQL 行为；会对 INSERT 的 INPUT 按 VALUES 顺序排序
     */
    public static final String DML_REF_PRIOR_COL_IN_VALUE = "DML_REF_PRIOR_COL_IN_VALUE";

    /**
     * 在逻辑DDL中校验建表语句时，主动在物理连接上等待的时间，仅用于测试
     */
    public static final String GET_PHY_TABLE_INFO_DELAY = "GET_PHY_TABLE_INFO_DELAY";

    /**
     * 在逻辑DDL中发起物理DDL操作时，主动延迟的时间，仅用于测试
     */
    public static final String EMIT_PHY_TABLE_DDL_DELAY = "EMIT_PHY_TABLE_DDL_DELAY";

    /**
     * 在建表语句中跳过CDC
     */
    public static final String CREATE_TABLE_SKIP_CDC = "CREATE_TABLE_SKIP_CDC";

    /**
     * 是否强制使用 Online Modify Column，即使列类型没有改变，或者不是支持的类型
     */
    public static final String OMC_FORCE_TYPE_CONVERSION = "OMC_FORCE_TYPE_CONVERSION";

    /**
     * 是否在有 GSI 的表上使用 Online Modify Column
     */
    public static final String OMC_ALTER_TABLE_WITH_GSI = "OMC_ALTER_TABLE_WITH_GSI";

    /**
     * Online Modify Column 回填时是否使用 returning 优化
     */
    public static final String OMC_BACK_FILL_USE_RETURNING = "OMC_BACK_FILL_USE_RETURNING";

    /**
     * Online Modify Column / Add Generated Column 回填后是否进行检查
     */
    public static final String COL_CHECK_AFTER_BACK_FILL = "COL_CHECK_AFTER_BACK_FILL";

    /**
     * Online Modify Column / Add Generated Column 检查是否使用 Simple Checker（只进行 NULL 值判断）
     */
    public static final String COL_USE_SIMPLE_CHECKER = "COL_USE_SIMPLE_CHECKER";

    /**
     * Online Modify Column / Add Generated Column 是否跳过回填阶段（只用来 debug）
     */
    public static final String COL_SKIP_BACK_FILL = "COL_SKIP_BACK_FILL";

    /**
     * Add Generated Column 是否强制 CN 计算表达式
     */
    public static final String GEN_COL_FORCE_CN_EVAL = "COL_FORCE_CN_EVAL";

    /**
     * 是否允许在含有 Generated Column 的表上使用 OMC
     */
    public static final String ENABLE_OMC_WITH_GEN_COL = "ENABLE_OMC_WITH_GEN_COL";

    /**
     * 是否将条件中的表达式替换为 Generated Column
     */
    public static final String GEN_COL_SUBSTITUTION = "GEN_COL_SUBSTITUTION";

    /**
     * 是否在进行 Generated Column 表达式替换的时候进行类型判断
     */
    public static final String GEN_COL_SUBSTITUTION_CHECK_TYPE = "GEN_COL_SUBSTITUTION_CHECK_TYPE";

    /**
     * 是否允许在 DN Generated Column 上创建 Unique Key
     */
    public static final String ENABLE_UNIQUE_KEY_ON_GEN_COL = "ENABLE_UNIQUE_KEY_ON_GEN_COL";

    /**
     * 是否允许使用表达式索引的语法创建索引（创建生成列 + 创建索引）
     */
    public static final String ENABLE_CREATE_EXPRESSION_INDEX = "ENABLE_CREATE_EXPRESSION_INDEX";

    /**
     * Online Modify Column Checker 并行策略
     */
    public static final String OMC_CHECKER_CONCURRENT_POLICY = "OMC_CHECKER_CONCURRENT_POLICY";

    /**
     * 是否开启DDL
     */
    public static final String ENABLE_DDL = "ENABLE_DDL";

    /**
     * 是否开启DDL
     */
    public static final String ENABLE_ALTER_DDL = "ENABLE_ALTER_DDL";

    /**
     * 是否允许执行self-join的跨库join,默认为false. 避免用户以前的sql出现性能问题
     */
    public static final String ENABLE_SELF_CROSS_JOIN = "ENABLE_SELF_CROSS_JOIN";

    public static final String ENABLE_COMPATIBLE_DATETIME_ROUNDDOWN = "ENABLE_COMPATIBLE_DATETIME_ROUNDDOWN";

    public static final String ENABLE_COMPATIBLE_TIMESTAMP_ROUNDDOWN = "ENABLE_COMPATIBLE_TIMESTAMP_ROUNDDOWN";

    public static final String BROADCAST_DML = "BROADCAST_DML";

    /**
     * Enable/Disable New Sequence cache on CN
     */
    public static final String ENABLE_NEW_SEQ_CACHE_ON_CN = "ENABLE_NEW_SEQ_CACHE_ON_CN";

    /**
     * Cache size for New Sequence on CN. Only valid when ENABLE_NEW_SEQ_CACHE_ON_CN is true.
     */
    public static final String NEW_SEQ_CACHE_SIZE_ON_CN = "NEW_SEQ_CACHE_SIZE_ON_CN";

    /**
     * Cache size for New Sequence on DN
     */
    public static final String NEW_SEQ_CACHE_SIZE = "NEW_SEQ_CACHE_SIZE";

    /**
     * Enable grouping for New Sequence
     */
    public static final String ENABLE_NEW_SEQ_GROUPING = "ENABLE_NEW_SEQ_GROUPING";

    /**
     * Grouping timeout for New Sequence
     */
    public static final String NEW_SEQ_GROUPING_TIMEOUT = "NEW_SEQ_GROUPING_TIMEOUT";

    /**
     * The number of task queues shared by all New Sequence objects in the same database
     */
    public static final String NEW_SEQ_TASK_QUEUE_NUM_PER_DB = "NEW_SEQ_TASK_QUEUE_NUM_PER_DB";

    /**
     * Check if New Sequence value handler should merge requests from different sequences.
     */
    public static final String ENABLE_NEW_SEQ_REQUEST_MERGING = "ENABLE_NEW_SEQ_REQUEST_MERGING";

    /**
     * Idle time before a value handler is terminated
     */
    public static final String NEW_SEQ_VALUE_HANDLER_KEEP_ALIVE_TIME = "NEW_SEQ_VALUE_HANDLER_KEEP_ALIVE_TIME";

    /**
     * 2.0 only: check if simple sequence is allowed to be created. False by default.
     */
    public static final String ALLOW_SIMPLE_SEQUENCE = "ALLOW_SIMPLE_SEQUENCE";

    /**
     * Step for Group Sequence
     */
    public static final String SEQUENCE_STEP = "SEQUENCE_STEP";

    /**
     * Unit Count for Group Sequence
     */
    public static final String SEQUENCE_UNIT_COUNT = "SEQUENCE_UNIT_COUNT";

    /**
     * Unit Index for Group Sequence
     */
    public static final String SEQUENCE_UNIT_INDEX = "SEQUENCE_UNIT_INDEX";

    /**
     * Check if Group Sequence Catcher is enabled
     */
    public static final String ENABLE_GROUP_SEQ_CATCHER = "ENABLE_GROUP_SEQ_CATCHER";

    /**
     * Check Interval for Group Sequence Catcher
     */
    public static final String GROUP_SEQ_CHECK_INTERVAL = "GROUP_SEQ_CHECK_INTERVAL";

    /**
     * merge 查询的超时时间, 默认是0，不超时
     */
    public static final String MERGE_DDL_TIMEOUT = "MERGE_DDL_TIMEOUT";

    /**
     * merge ddl是否采用全并行模式,设置为false,默认为库间并行
     */
    public static final String MERGE_DDL_CONCURRENT = "MERGE_DDL_CONCURRENT";

    public static final String SLOW_SQL_TIME = "SLOW_SQL_TIME";

    public static final String LOAD_DATA_BATCH_INSERT_SIZE = "LOAD_DATA_BATCH_INSERT_SIZE";

    public static final String LOAD_DATA_CACHE_BUFFER_SIZE = "LOAD_DATA_CACHE_BUFFER_SIZE";

    public static final String SELECT_INTO_OUTFILE_BUFFER_SIZE = "SELECT_INTO_OUTFILE_BUFFER_SIZE";

    public static final String LOAD_DATA_USE_BATCH_MODE = "LOAD_DATA_USE_BATCH_MODE";

    public static final String SQL_DELAY_CUTOFF = "SQL_DELAY_CUTOFF";

    public static final String DB_PRIV = "DB_PRIV";

    public static final String ENABLE_VERSION_CHECK = "ENABLE_VERSION_CHECK";

    public static final String MAX_ALLOWED_PACKET = "MAX_ALLOWED_PACKET";

    public static final String NET_WRITE_TIMEOUT = "NET_WRITE_TIMEOUT";

    public static final String KILL_CLOSE_STREAM = "KILL_CLOSE_STREAM";

    public static final String SHOW_TABLES_FROM_RULE_ONLY = "SHOW_TABLES_FROM_RULE_ONLY";

    /**
     * rule 的兼容性配置
     */
    public static final String IS_CROSS_RULE = "IS_CROSS_RULE";

    /**
     * Check if logical information_schema query is supported. The default value
     * is TRUE.
     */
    public static final String ENABLE_LOGICAL_INFO_SCHEMA_QUERY = "ENABLE_LOGICAL_INFO_SCHEMA_QUERY";

    public static final String INFO_SCHEMA_QUERY_STAT_BY_GROUP = "INFO_SCHEMA_QUERY_STAT_BY_GROUP";

    public static final String DB_INSTANCE_TYPE = "DB_INSTANCE_TYPE";

    /**
     * 是否推送Group层与Atom层的数据源的监控数据, 默认不推送，而是只推送MYSQL实例及上层的的监控数据
     */
    public static final String ENABLE_PUSH_GROUP_STATS = "ENABLE_PUSH_GROUP_STATS";

    public static final String AUTO_ADD_APP_MODE = "AUTO_ADD_APP_MODE";

    /**
     * 是否需要将在Calcite上执行异常的SQL在老Server的逻辑上进行重试，默认是打开，在随机SQL测试时要关闭
     */
    public static final String RETRY_ERROR_SQL_ON_OLD_SERVER = "RETRY_ERROR_SQL_ON_OLD_SERVER";

    public static final String COLLECT_SQL_ERROR_INFO = "COLLECT_SQL_ERROR_INFO";

    public static final String ENABLE_PARAMETERIZED_SQL_LOG = "ENABLE_PARAMETERIZED_SQL_LOG";

    public static final String MAX_PARAMETERIZED_SQL_LOG_LENGTH = "MAX_PARAMETERIZED_SQL_LOG_LENGTH";

    public static final String HINT_PARSER_FLAG = "HINT_PARSER_FLAG";

    public static final String FORBID_EXECUTE_DML_ALL = "FORBID_EXECUTE_DML_ALL";

    public static final String ENABLE_RULE_DB_STORE = "ENABLE_RULE_DB_STORE";

    public static final String SCHEDULED_RULE_TASK_CLOCK = "SCHEDULED_RULE_TASK_CLOCK";

    public static final String RULE_CHECK_INTERVAL = "RULE_CHECK_INTERVAL";

    public static final String VERSION_PREFIX = "VERSION_PREFIX";

    /**
     * BlockIndexNLJoin : block size
     */
    public static final String JOIN_BLOCK_SIZE = "JOIN_BLOCK_SIZE";

    public static final String LOOKUP_JOIN_MAX_BATCH_SIZE = "LOOKUP_JOIN_MAX_BATCH_SIZE";

    public static final String LOOKUP_JOIN_MIN_BATCH_SIZE = "LOOKUP_JOIN_MIN_BATCH_SIZE";

    public static final String COLD_HOT_LIMIT_COUNT = "COLD_HOT_LIMIT_COUNT";

    public static final String ALLOW_EXTRA_READ_CONN = "ALLOW_EXTRA_READ_CONN";

    public static final String FAILURE_INJECTION = "FAILURE_INJECTION";

    public static final String PURGE_TRANS_INTERVAL = "PURGE_TRANS_INTERVAL";

    public static final String PURGE_TRANS_BEFORE = "PURGE_TRANS_BEFORE";

    public static final String ENABLE_DEADLOCK_DETECTION = "ENABLE_DEADLOCK_DETECTION";

    public static final String DEADLOCK_DETECTION_INTERVAL = "DEADLOCK_DETECTION_INTERVAL";

    public static final String ALLOW_CROSS_DB_QUERY = "ALLOW_CROSS_DB_QUERY";

    public static final String XA_RECOVER_INTERVAL = "XA_RECOVER_INTERVAL";

    public static final String TSO_HEARTBEAT_INTERVAL = "TSO_HEARTBEAT_INTERVAL";

    public static final String PURGE_TRANS_START_TIME = "PURGE_TRANS_START_TIME";

    public static final String PURGE_TRANS_BATCH_SIZE = "PURGE_TRANS_BATCH_SIZE";

    public static final String PURGE_TRANS_BATCH_PERIOD = "PURGE_TRANS_BATCH_PERIOD";

    /**
     * GROUP_CONCAT 展示最大长度
     */
    public static final String GROUP_CONCAT_MAX_LEN = "GROUP_CONCAT_MAX_LEN";

    public static final String BINLOG_ROWS_QUERY_LOG_EVENTS = "BINLOG_ROWS_QUERY_LOG_EVENTS";

    /**
     * max show length of group concat, only work for cn
     */
    public static final String CN_GROUP_CONCAT_MAX_LEN = "CN_GROUP_CONCAT_MAX_LEN";

    /**
     * 2 policies for batch insert: NONE, SPLIT.
     */
    public static final String BATCH_INSERT_POLICY = "BATCH_INSERT_POLICY";

    public static final String MAX_BATCH_INSERT_SQL_LENGTH = "MAX_BATCH_INSERT_SQL_LENGTH";

    public static final String BATCH_INSERT_CHUNK_SIZE = "BATCH_INSERT_CHUNK_SIZE";

    public static final String INSERT_SELECT_BATCH_SIZE = "INSERT_SELECT_BATCH_SIZE";

    public static final String INSERT_SELECT_LIMIT = "INSERT_SELECT_LIMIT";

    /**
     * Insert/update/delete select执行策略为Insert/update/delete多线程执行
     */
    public static final String MODIFY_SELECT_MULTI = "MODIFY_SELECT_MULTI";

    /**
     * Insert/update/delete select执行策略为select 和 Insert/update/delete 并行
     */
    public static final String MODIFY_WHILE_SELECT = "MODIFY_WHILE_SELECT";

    /**
     * Insert select执行策略为MPP执行
     */
    public static final String INSERT_SELECT_MPP = "INSERT_SELECT_MPP";

    public static final String INSERT_SELECT_MPP_BY_PARALLEL = "INSERT_SELECT_MPP_BY_PARALLEL";

    /**
     * Insert select self_table; insert 和 select 操作同一个表时,非事务下可能会导致数据 > 2倍，默认自身表时，先select 再insert
     */
    public static final String INSERT_SELECT_SELF_BY_PARALLEL = "INSERT_SELECT_SELF_BY_PARALLEL";

    /**
     * MODIFY_SELECT_MULTI策略时 逻辑任务执行 的线程个数
     */
    public static final String MODIFY_SELECT_LOGICAL_THREADS = "MODIFY_SELECT_LOGICAL_THREADS";

    /**
     * MODIFY_SELECT_MULTI策略时 物理任务执行 的线程个数
     */
    public static final String MODIFY_SELECT_PHYSICAL_THREADS = "MODIFY_SELECT_PHYSICAL_THREADS";

    /**
     * MODIFY_SELECT_MULTI策略时 内存buffer大小，
     */
    public static final String MODIFY_SELECT_BUFFER_SIZE = "MODIFY_SELECT_BUFFER_SIZE";

    public static final String SQL_SELECT_LIMIT = "SQL_SELECT_LIMIT";

    /**
     * Batch size for select size for update or delete.
     */
    public static final String UPDATE_DELETE_SELECT_BATCH_SIZE = "UPDATE_DELETE_SELECT_BATCH_SIZE";

    public static final String UPDATE_DELETE_SELECT_LIMIT = "UPDATE_DELETE_SELECT_LIMIT";

    public static final String PL_MEMORY_LIMIT = "PL_MEMORY_LIMIT";

    public static final String PL_CURSOR_MEMORY_LIMIT = "PL_CURSOR_MEMORY_LIMIT";

    public static final String ENABLE_UDF = "ENABLE_UDF";

    public static final String ENABLE_JAVA_UDF = "ENABLE_JAVA_UDF";

    public static final String CHECK_INVALID_JAVA_UDF = "CHECK_INVALID_JAVA_UDF";

    public static final String MAX_JAVA_UDF_NUM = "MAX_JAVA_UDF_NUM";

    public static final String FORCE_DROP_JAVA_UDF = "FORCE_DROP_JAVA_UDF";

    public static final String PL_INTERNAL_CACHE_SIZE = "PL_INTERNAL_CACHE_SIZE";

    public static final String MAX_PL_DEPTH = "MAX_PL_DEPTH";

    public static final String ORIGIN_CONTENT_IN_ROUTINES = "ORIGIN_CONTENT_IN_ROUTINES";

    public static final String FORCE_DROP_PROCEDURE = "FORCE_DROP_PROCEDURE";

    public static final String FORCE_DROP_SQL_UDF = "FORCE_DROP_SQL_UDF";

    /**
     * recyclebin table retain hours
     */
    public static final String RECYCLEBIN_RETAIN_HOURS = "RECYCLEBIN_RETAIN_HOURS";

    public static final String MAX_UPDATE_NUM_IN_GSI = "MAX_UPDATE_NUM_IN_GSI";

    public static final String ENABLE_JOIN_CLUSTERING = "ENABLE_JOIN_CLUSTERING";

    public static final String JOIN_CLUSTERING_CONDITION_PROPAGATION_LIMIT =
        "JOIN_CLUSTERING_CONDITION_PROPAGATION_LIMIT";

    public static final String ENABLE_JOIN_CLUSTERING_AVOID_CROSS_JOIN = "ENABLE_JOIN_CLUSTERING_AVOID_CROSS_JOIN";

    public static final String ENABLE_BACKGROUND_STATISTIC_COLLECTION = "ENABLE_BACKGROUND_STATISTIC_COLLECTION";

    public static final String BACKGROUND_STATISTIC_COLLECTION_START_TIME =
        "BACKGROUND_STATISTIC_COLLECTION_START_TIME";

    public static final String BACKGROUND_STATISTIC_COLLECTION_END_TIME = "BACKGROUND_STATISTIC_COLLECTION_END_TIME";

    public static final String STATISTIC_CORRECTIONS = "STATISTIC_CORRECTIONS";

    /**
     * background ttl expire end time default 05:00
     */
    public static final String BACKGROUND_TTL_EXPIRE_END_TIME = "BACKGROUND_TTL_EXPIRE_END_TIME";

    /**
     * background collect table staistic period default 12h = 12 * 60 min = 720 min
     */
    public static final String STATISTIC_VISIT_DN_TIMEOUT = "STATISTIC_VISIT_DN_TIMEOUT";

    public static final String BACKGROUND_STATISTIC_COLLECTION_EXPIRE_TIME =
        "BACKGROUND_STATISTIC_COLLECTION_EXPIRE_TIME";

    public static final String STATISTIC_SAMPLE_RATE = "STATISTIC_SAMPLE_RATE";

    public static final String SAMPLE_PERCENTAGE = "SAMPLE_PERCENTAGE";

    /**
     * backfill sample_percentage
     */
    public static final String BACKFILL_MAX_SAMPLE_PERCENTAGE = "BACKFILL_MAX_SAMPLE_PERCENTAGE";

    /**
     * enable innodb btree sampling
     */
    public static final String ENABLE_INNODB_BTREE_SAMPLING = "ENABLE_INNODB_BTREE_SAMPLING";

    public static final String HISTOGRAM_MAX_SAMPLE_SIZE = "HISTOGRAM_MAX_SAMPLE_SIZE";

    public static final String AUTO_ANALYZE_ALL_COLUMN_TABLE_LIMIT = "AUTO_ANALYZE_ALL_COLUMN_TABLE_LIMIT";

    public static final String AUTO_ANALYZE_TABLE_SLEEP_MILLS = "AUTO_ANALYZE_TABLE_SLEEP_MILLS";

    public static final String AUTO_ANALYZE_PERIOD_IN_HOURS = "AUTO_ANALYZE_PERIOD_IN_HOURS";

    public static final String HISTOGRAM_BUCKET_SIZE = "HISTOGRAM_BUCKET_SIZE";

    public static final String ANALYZE_TABLE_SPEED_LIMITATION = "ANALYZE_TABLE_SPEED_LIMITATION";

    public static final String ENABLE_SORT_MERGE_JOIN = "ENABLE_SORT_MERGE_JOIN";

    public static final String ENABLE_BKA_JOIN = "ENABLE_BKA_JOIN";

    /**
     * enable remove join condition default true
     */
    public static final String ENABLE_REMOVE_JOIN_CONDITION = "ENABLE_REMOVE_JOIN_CONDITION";

    /**
     * enable dynamic pruning in bka join default false
     */
    public static final String ENABLE_BKA_PRUNING = "ENABLE_BKA_PRUNING";

    /**
     * enable dynamic in values pruning in bka join default true
     */
    public static final String ENABLE_BKA_IN_VALUES_PRUNING = "ENABLE_BKA_IN_VALUES_PRUNING";

    /**
     * enable hash join default true
     */
    public static final String ENABLE_HASH_JOIN = "ENABLE_HASH_JOIN";

    public static final String FORCE_OUTER_DRIVER_HASH_JOIN = "FORCE_OUTER_DRIVER_HASH_JOIN";
    public static final String FORBID_OUTER_DRIVER_HASH_JOIN = "FORBID_OUTER_DRIVER_HASH_JOIN";

    public static final String ENABLE_NL_JOIN = "ENABLE_NL_JOIN";

    public static final String ENABLE_SEMI_NL_JOIN = "ENABLE_SEMI_NL_JOIN";

    public static final String ENABLE_SEMI_HASH_JOIN = "ENABLE_SEMI_HASH_JOIN";

    public static final String ENABLE_SEMI_BKA_JOIN = "ENABLE_SEMI_BKA_JOIN";

    public static final String ENABLE_SEMI_SORT_MERGE_JOIN = "ENABLE_SEMI_SORT_MERGE_JOIN";

    public static final String MATERIALIZED_ITEMS_LIMIT = "MATERIALIZED_ITEMS_LIMIT";

    public static final String ENABLE_MATERIALIZED_SEMI_JOIN = "ENABLE_MATERIALIZED_SEMI_JOIN";

    public static final String ENABLE_MYSQL_HASH_JOIN = "ENABLE_MYSQL_HASH_JOIN";

    public static final String ENABLE_MYSQL_SEMI_HASH_JOIN = "ENABLE_MYSQL_SEMI_HASH_JOIN";

    public static final String CBO_TOO_MANY_JOIN_LIMIT = "CBO_TOO_MANY_JOIN_LIMIT";

    public static final String CBO_LEFT_DEEP_TREE_JOIN_LIMIT = "CBO_LEFT_DEEP_TREE_JOIN_LIMIT";

    public static final String CBO_ZIG_ZAG_TREE_JOIN_LIMIT = "CBO_ZIG_ZAG_TREE_JOIN_LIMIT";

    public static final String CBO_BUSHY_TREE_JOIN_LIMIT = "CBO_BUSHY_TREE_JOIN_LIMIT";

    public static final String RBO_HEURISTIC_JOIN_REORDER_LIMIT = "RBO_HEURISTIC_JOIN_REORDER_LIMIT";

    public static final String MYSQL_JOIN_REORDER_EXHAUSTIVE_DEPTH = "MYSQL_JOIN_REORDER_EXHAUSTIVE_DEPTH";

    public static final String ENABLE_LV_SUBQUERY_UNWRAP = "ENABLE_LV_SUBQUERY_UNWRAP";

    public static final String ENABLE_FILTER_REORDER = "ENABLE_FILTER_REORDER";

    /**
     * enable semi join reorder default true
     */
    public static final String ENABLE_SEMI_JOIN_REORDER = "ENABLE_SEMI_JOIN_REORDER";

    public static final String ENABLE_OUTER_JOIN_REORDER = "ENABLE_OUTER_JOIN_REORDER";

    public static final String ENABLE_STATISTIC_FEEDBACK = "ENABLE_STATISTIC_FEEDBACK";

    public static final String ENABLE_HASH_AGG = "ENABLE_HASH_AGG";

    public static final String ENABLE_SORT_AGG = "ENABLE_SORT_AGG";

    public static final String ENABLE_PARTIAL_AGG = "ENABLE_PARTIAL_AGG";

    public static final String PARTIAL_AGG_SELECTIVITY_THRESHOLD = "PARTIAL_AGG_SELECTIVITY_THRESHOLD";

    public static final String PARTIAL_AGG_BUCKET_THRESHOLD = "PARTIAL_AGG_BUCKET_THRESHOLD";

    public static final String AGG_MAX_HASH_TABLE_FACTOR = "AGG_MAX_HASH_TABLE_FACTOR";

    public static final String AGG_MIN_HASH_TABLE_FACTOR = "AGG_MIN_HASH_TABLE_FACTOR";

    public static final String ENABLE_HASH_WINDOW = "ENABLE_HASH_WINDOW";

    public static final String ENABLE_SORT_WINDOW = "ENABLE_SORT_WINDOW";

    /**
     * enable push join default true
     */
    public static final String ENABLE_PUSH_JOIN = "ENABLE_PUSH_JOIN";

    /**
     * ignore un pushable function when join
     */
    public static final String IGNORE_UN_PUSHABLE_FUNC_IN_JOIN = "IGNORE_UN_PUSHABLE_FUNC_IN_JOIN";

    /**
     * enable push project default true
     */
    public static final String ENABLE_PUSH_PROJECT = "ENABLE_PUSH_PROJECT";

    public static final String ENABLE_CBO_PUSH_JOIN = "ENABLE_CBO_PUSH_JOIN";

    /**
     * cbo restrict push join, enable when join in cn is >= CBO_RESTRICT_PUSH_JOIN_LIMIT
     */
    public static final String CBO_RESTRICT_PUSH_JOIN_LIMIT = "CBO_RESTRICT_PUSH_JOIN_LIMIT";

    /**
     * cbo restrict push join rule counter, restrict the rule if it has been invoked CBO_RESTRICT_PUSH_JOIN_LIMIT times
     */
    public static final String CBO_RESTRICT_PUSH_JOIN_COUNT = "CBO_RESTRICT_PUSH_JOIN_COUNT";

    /**
     * enable rbo push agg default true
     */
    public static final String ENABLE_PUSH_AGG = "ENABLE_PUSH_AGG";

    public static final String PUSH_AGG_INPUT_ROW_COUNT_THRESHOLD = "PUSH_AGG_INPUT_ROW_COUNT_THRESHOLD";

    public static final String ENABLE_CBO_PUSH_AGG = "ENABLE_CBO_PUSH_AGG";

    public static final String ENABLE_PUSH_SORT = "ENABLE_PUSH_SORT";

    public static final String ENABLE_CBO_GROUP_JOIN = "ENABLE_CBO_GROUP_JOIN";

    public static final String CBO_AGG_JOIN_TRANSPOSE_LIMIT = "CBO_AGG_JOIN_TRANSPOSE_LIMIT";

    public static final String ENABLE_EXPAND_DISTINCTAGG = "ENABLE_EXPAND_DISTINCTAGG";

    public static final String ENABLE_SORT_JOIN_TRANSPOSE = "ENABLE_SORT_JOIN_TRANSPOSE";

    public static final String ENABLE_SORT_OUTERJOIN_TRANSPOSE = "ENABLE_SORT_OUTERJOIN_TRANSPOSE";

    public static final String CBO_JOIN_TABLELOOKUP_TRANSPOSE_LIMIT = "CBO_JOIN_TABLELOOKUP_TRANSPOSE_LIMIT";

    public static final String CBO_START_UP_COST_JOIN_LIMIT = "CBO_START_UP_COST_JOIN_LIMIT";

    public static final String ENABLE_START_UP_COST = "ENABLE_START_UP_COST";

    public static final String JOIN_HINT = "JOIN_HINT";

    public static final String SQL_SIMPLE_MAX_LENGTH = "SQL_SIMPLE_MAX_LENGTH";

    public static final String MASTER = "MASTER";

    public static final String SLAVE = "SLAVE";

    /**
     * 强制走follower备库
     */
    public static final String FOLLOWER = "FOLLOWER";

    /**
     * allow DDL on Global Secondary Index
     */
    public static final String DDL_ON_GSI = "DDL_ON_GSI";

    public static final String DML_ON_GSI = "DML_ON_GSI";

    public static final String PUSHDOWN_HINT_ON_GSI = "PUSHDOWN_HINT_ON_GSI";

    /**
     * allow NODE/SCAN of dml on broadcast table
     */
    public static final String PUSHDOWN_HINT_ON_BROADCAST = "PUSHDOWN_HINT_ON_BROADCAST";

    /**
     * allow Global Secondary Index DDL or DML on MySQL 5.6
     */
    public static final String STORAGE_CHECK_ON_GSI = "STORAGE_CHECK_ON_GSI";

    public static final String DISTRIBUTED_TRX_REQUIRED = "DISTRIBUTED_TRX_REQUIRED";

    public static final String TRX_CLASS_REQUIRED = "TRX_CLASS_REQUIRED";

    public static final String TSO_OMIT_GLOBAL_TX_LOG = "TSO_OMIT_GLOBAL_TX_LOG";

    public static final String TRUNCATE_TABLE_WITH_GSI = "TRUNCATE_TABLE_WITH_GSI";

    public static final String ALLOW_ADD_GSI = "ALLOW_ADD_GSI";

    public static final String GSI_DEBUG = "GSI_DEBUG";

    /**
     * debug mode on column, including hidden column, column multi-write, etc.
     */
    public static final String COLUMN_DEBUG = "COLUMN_DEBUG";

    /**
     * allow gsi stop at a specific status
     */
    public static final String GSI_FINAL_STATUS_DEBUG = "GSI_FINAL_STATUS_DEBUG";

    public static final String REPARTITION_SKIP_CUTOVER = "REPARTITION_SKIP_CUTOVER";

    /**
     * skip the repartition unchanged check
     */
    public static final String REPARTITION_SKIP_CHECK = "REPARTITION_SKIP_CHECK";

    /**
     * enable rebuild gsi for repartition
     */
    public static final String REPARTITION_ENABLE_REBUILD_GSI = "REPARTITION_ENABLE_REBUILD_GSI";

    public static final String REPARTITION_SKIP_CLEANUP = "REPARTITION_SKIP_CLEANUP";

    public static final String REPARTITION_FORCE_GSI_NAME = "REPARTITION_FORCE_GSI_NAME";

    public static final String SCALEOUT_BACKFILL_BATCH_SIZE = "SCALEOUT_BACKFILL_BATCH_SIZE";

    public static final String SCALEOUT_BACKFILL_SPEED_LIMITATION = "SCALEOUT_BACKFILL_SPEED_LIMITATION";

    public static final String SCALEOUT_BACKFILL_SPEED_MIN = "SCALEOUT_BACKFILL_SPEED_MIN";

    public static final String SCALEOUT_BACKFILL_PARALLELISM = "SCALEOUT_BACKFILL_PARALLELISM";

    /**
     * parallelism tasks of logical table for scaleout
     */
    public static final String SCALEOUT_TASK_PARALLELISM = "SCALEOUT_TASK_PARALLELISM";

    /**
     * parallelism tasks of logical table for tablegroup
     */
    public static final String TABLEGROUP_TASK_PARALLELISM = "TABLEGROUP_TASK_PARALLELISM";

    /**
     * batch size for scaleout check procedure
     */
    public static final String SCALEOUT_CHECK_BATCH_SIZE = "SCALEOUT_CHECK_BATCH_SIZE";

    public static final String SCALEOUT_CHECK_SPEED_LIMITATION = "SCALEOUT_CHECK_SPEED_LIMITATION";

    public static final String SCALEOUT_CHECK_SPEED_MIN = "SCALEOUT_CHECK_SPEED_MIN";

    public static final String SCALEOUT_CHECK_PARALLELISM = "SCALEOUT_CHECK_PARALLELISM";

    public static final String SCALEOUT_FASTCHECKER_PARALLELISM = "SCALEOUT_FASTCHECKER_PARALLELISM";

    public static final String SCALEOUT_EARLY_FAIL_NUMBER = "SCALEOUT_EARLY_FAIL_NUMBER";

    public static final String SCALEOUT_BACKFILL_POSITION_MARK = "GSI_BACKFILL_POSITION_MARK";

    public static final String SCALE_OUT_DEBUG = "SCALE_OUT_DEBUG";

    public static final String SCALE_OUT_DEBUG_WAIT_TIME_IN_WO = "SCALE_OUT_DEBUG_WAIT_TIME_IN_WO";

    public static final String SCALE_OUT_FINAL_TABLE_STATUS_DEBUG = "SCALE_OUT_FINAL_TABLE_STATUS_DEBUG";

    public static final String SCALE_OUT_FINAL_DB_STATUS_DEBUG = "SCALE_OUT_FINAL_DB_STATUS_DEBUG";

    /**
     * to split physical table for backfill
     */
    public static final String PHYSICAL_TABLE_START_SPLIT_SIZE = "PHYSICAL_TABLE_START_SPLIT_SIZE";

    /**
     * the parallelism for backfill
     */
    public static final String BACKFILL_PARALLELISM = "BACKFILL_PARALLELISM";

    /**
     * max sample size of backfill physical table
     */
    public static final String BACKFILL_MAX_SAMPLE_SIZE = "BACKFILL_MAX_SAMPLE_SIZE";

    /**
     * enable split physical table for backfill
     */
    public static final String ENABLE_PHYSICAL_TABLE_PARALLEL_BACKFILL = "ENABLE_PHYSICAL_TABLE_PARALLEL_BACKFILL";

    public static final String PHYSICAL_TABLE_BACKFILL_PARALLELISM = "PHYSICAL_TABLE_BACKFILL_PARALLELISM";

    public static final String ENABLE_SLIDE_WINDOW_BACKFILL = "ENABLE_SLIDE_WINDOW_BACKFILL";

    public static final String SLIDE_WINDOW_TIME_INTERVAL = "SLIDE_WINDOW_TIME_INTERVAL";

    public static final String SLIDE_WINDOW_SPLIT_SIZE = "SLIDE_WINDOW_SPLIT_SIZE";

    /**
     * check target table after scaleout's backfill
     */
    public static final String SCALEOUT_CHECK_AFTER_BACKFILL = "SCALEOUT_CHECK_AFTER_BACKFILL";

    public static final String SCALEOUT_BACKFILL_USE_FASTCHECKER = "SCALEOUT_BACKFILL_USE_FASTCHECKER";

    public static final String USE_FASTCHECKER = "USE_FASTCHECKER";

    public static final String GSI_BACKFILL_USE_FASTCHECKER = "GSI_BACKFILL_USE_FASTCHECKER";

    public static final String CHECK_GLOBAL_INDEX_USE_FASTCHECKER = "CHECK_GLOBAL_INDEX_USE_FASTCHECKER";

    public static final String FASTCHECKER_RETRY_TIMES = "FASTCHECKER_RETRY_TIMES";

    /**
     * when fastchecker failed to calculate hash value because of timeout,
     * we will decrease the batch size and retry
     */
    public static final String FASTCHECKER_BATCH_TIMEOUT_RETRY_TIMES = "FASTCHECKER_BATCH_TIMEOUT_RETRY_TIMES";

    /**
     * when fastchecker use xa check, it must acquire table lock on phyTables.
     * this argument will limit the acquire_lock_timeout.
     */
    public static final String FASTCHECKER_LOCK_TIMEOUT = "FASTCHECKER_LOCK_TIMEOUT";

    /**
     * if a physical table's row count exceed FASTCHECKER_BATCH_SIZE, we will start to check by batch
     */
    public static final String FASTCHECKER_BATCH_SIZE = "FASTCHECKER_BATCH_SIZE";

    /**
     * fastchecker max batch file size (bytes)
     */
    public static final String FASTCHECKER_BATCH_FILE_SIZE = "FASTCHECKER_BATCH_FILE_SIZE";

    /**
     * when fastchecker check table by batch, we limit the max sample percentage
     */
    public static final String FASTCHECKER_MAX_SAMPLE_PERCENTAGE = "FASTCHECKER_MAX_SAMPLE_PERCENTAGE";

    /**
     * when fastchecker check table by batch, we limit the max sample size
     */
    public static final String FASTCHECKER_MAX_SAMPLE_SIZE = "FASTCHECKER_MAX_SAMPLE_SIZE";

    /**
     * parallelism limit for GsiFastChecker
     */
    public static final String GSI_FASTCHECKER_PARALLELISM = "GSI_FASTCHECKER_PARALLELISM";

    public static final String SCALEOUT_DML_PUSHDOWN_OPTIMIZATION = "SCALEOUT_DML_PUSHDOWN_OPTIMIZATION";

    public static final String SCALEOUT_DML_PUSHDOWN_BATCH_LIMIT = "SCALEOUT_DML_PUSHDOWN_BATCH_LIMIT";

    public static final String ENABLE_SCALE_OUT_FEATURE = "ENABLE_SCALE_OUT_FEATURE";

    public static final String ENABLE_SCALE_OUT_ALL_PHY_DML_LOG = "ENABLE_SCALE_OUT_ALL_PHY_DML_LOG";

    public static final String ENABLE_SCALE_OUT_GROUP_PHY_DML_LOG = "ENABLE_SCALE_OUT_GROUP_PHY_DML_LOG";

    public static final String SCALE_OUT_WRITE_DEBUG = "SCALE_OUT_WRITE_DEBUG";

    public static final String SCALE_OUT_WRITE_PERFORMANCE_TEST = "SCALE_OUT_WRITE_PERFORMANCE_TEST";

    public static final String SCALE_OUT_DROP_DATABASE_AFTER_SWITCH_DATASOURCE =
        "SCALE_OUT_DROP_DATABASE_AFTER_SWITCH_DATASOURCE";

    public static final String SCALEOUT_TASK_RETRY_TIME = "SCALEOUT_TASK_RETRY_TIME";

    public static final String ALLOW_DROP_DATABASE_IN_SCALEOUT_PHASE = "ALLOW_DROP_DATABASE_IN_SCALEOUT_PHASE";

    /**
     * force execute the drop database when the drop lock of the database is already fetched
     */

    public static final String ALLOW_DROP_DATABASE_FORCE = "ALLOW_DROP_DATABASE_FORCE";

    /**
     * reload the database/tables status from metadb for debug purpose.
     */
    public static final String RELOAD_SCALE_OUT_STATUS_DEBUG = "RELOAD_SCALE_OUT_STATUS_DEBUG";

    public static final String ALLOW_ALTER_GSI_INDIRECTLY = "ALLOW_ALTER_GSI_INDIRECTLY";

    public static final String ALLOW_ALTER_MODIFY_SK = "ALLOW_ALTER_MODIFY_SK";

    /**
     * allow drop part unique constrain(drop some not all columns in composite unique constrain) in primary table or UGSI.
     */
    public static final String ALLOW_DROP_OR_MODIFY_PART_UNIQUE_WITH_GSI = "ALLOW_DROP_OR_MODIFY_PART_UNIQUE_WITH_GSI";

    public static final String ALLOW_LOOSE_ALTER_COLUMN_WITH_GSI = "ALLOW_LOOSE_ALTER_COLUMN_WITH_GSI";

    /**
     * the default partition mode
     */
    public static final String DEFAULT_PARTITION_MODE = "DEFAULT_PARTITION_MODE";

    /**
     * allow auto partition.
     */
    public static final String AUTO_PARTITION = "AUTO_PARTITION";

    public static final String AUTO_PARTITION_PARTITIONS = "AUTO_PARTITION_PARTITIONS";

    public static final String GSI_DEFAULT_CURRENT_TIMESTAMP = "GSI_DEFAULT_CURRENT_TIMESTAMP";

    public static final String GSI_ON_UPDATE_CURRENT_TIMESTAMP = "GSI_ON_UPDATE_CURRENT_TIMESTAMP";

    public static final String GSI_IGNORE_RESTRICTION = "GSI_IGNORE_RESTRICTION";

    public static final String GSI_CHECK_AFTER_CREATION = "GSI_CHECK_AFTER_CREATION";

    public static final String GENERAL_DYNAMIC_SPEED_LIMITATION = "GENERAL_DYNAMIC_SPEED_LIMITATION";

    /**
     * batch size for check oss data procedure
     */
    public static final String CHECK_OSS_BATCH_SIZE = "CHECK_OSS_BATCH_SIZE";

    /**
     * batch size for backfill procedure
     */
    public static final String GSI_BACKFILL_BATCH_SIZE = "GSI_BACKFILL_BATCH_SIZE";

    public static final String GSI_BACKFILL_SPEED_LIMITATION = "GSI_BACKFILL_SPEED_LIMITATION";

    public static final String GSI_BACKFILL_SPEED_MIN = "GSI_BACKFILL_SPEED_MIN";

    public static final String GSI_BACKFILL_PARALLELISM = "GSI_BACKFILL_PARALLELISM";

    public static final String GSI_CHECK_BATCH_SIZE = "GSI_CHECK_BATCH_SIZE";

    public static final String GSI_CHECK_SPEED_LIMITATION = "GSI_CHECK_SPEED_LIMITATION";

    public static final String GSI_CHECK_SPEED_MIN = "GSI_CHECK_SPEED_MIN";

    public static final String GSI_CHECK_PARALLELISM = "GSI_CHECK_PARALLELISM";

    public static final String GSI_EARLY_FAIL_NUMBER = "GSI_EARLY_FAIL_NUMBER";

    public static final String GSI_BACKFILL_POSITION_MARK = "GSI_BACKFILL_POSITION_MARK";

    public static final String GSI_CONCURRENT_WRITE_OPTIMIZE = "GSI_CONCURRENT_WRITE_OPTIMIZE";

    /**
     * batch size for create database as
     */
    public static final String CREATE_DATABASE_AS_BATCH_SIZE = "CREATE_DATABASE_AS_BATCH_SIZE";

    /**
     * speed limit for CTAS
     */
    public static final String CREATE_DATABASE_AS_BACKFILL_SPEED_LIMITATION =
        "CREATE_DATABASE_AS_BACKFILL_SPEED_LIMITATION";

    /**
     * min speed for CTAS
     */
    public static final String CREATE_DATABASE_AS_BACKFILL_SPEED_MIN = "CREATE_DATABASE_AS_BACKFILL_SPEED_MIN";

    /**
     * backfill parallelism for CDAS
     */
    public static final String CREATE_DATABASE_AS_BACKFILL_PARALLELISM = "CREATE_DATABASE_AS_BACKFILL_PARALLELISM";

    /**
     * DDL Tasks parallelism for CTAS
     */
    public static final String CREATE_DATABASE_AS_TASKS_PARALLELISM = "CREATE_DATABASE_AS_TASKS_PARALLELISM";
    /**
     * whether use fastchecker for CTAS
     */
    public static final String CREATE_DATABASE_AS_USE_FASTCHECKER = "CREATE_DATABASE_AS_USE_FASTCHECKER";
    /**
     * fastchecker parallelism for CTAS
     */
    public static final String CREATE_DATABASE_AS_FASTCHECKER_PARALLELISM =
        "CREATE_DATABASE_AS_FASTCHECKER_PARALLELISM";
    /**
     * fastchecker retry times for CTAS
     */
    public static final String CREATE_DATABASE_AS_FASTCHECKER_RETRY_TIMES =
        "CREATE_DATABASE_AS_FASTCHECKER_RETRY_TIMES";

    public static final String CREATE_DATABASE_MAX_PARTITION_FOR_DEBUG = "CREATE_DATABASE_MAX_PARTITION_FOR_DEBUG";
    /**
     * Write primary and gsi concurrently for load data
     */
    public static final String LOAD_DATA_IGNORE_IS_SIMPLE_INSERT = "LOAD_DATA_IGNORE_IS_SIMPLE_INSERT";

    public static final String ENABLE_LOAD_DATA_TRACE = "ENABLE_LOAD_DATA_TRACE";

    public static final String LOAD_DATA_AUTO_FILL_AUTO_INCREMENT_COLUMN = "LOAD_DATA_AUTO_FILL_AUTO_INCREMENT_COLUMN";

    public static final String LOAD_DATA_PURE_INSERT_MODE = "LOAD_DATA_PURE_INSERT_MODE";

    /**
     * handle empty char for load data
     */
    public static final String LOAD_DATA_HANDLE_EMPTY_CHAR = "LOAD_DATA_HANDLE_EMPTY_CHAR";

    public static final String GSI_CONCURRENT_WRITE = "GSI_CONCURRENT_WRITE";

    /**
     * the read/write parallelism of one phy group
     */
    public static final String GROUP_PARALLELISM = "GROUP_PARALLELISM";

    public static final String GSI_STATISTICS_COLLECTION = "GSI_STATISTICS_COLLECTION";

    /**
     * the switch of the read/write parallelism of one phy group
     */
    public static final String ENABLE_GROUP_PARALLELISM = "ENABLE_GROUP_PARALLELISM";

    /**
     * the switch of log the computing group connection key of all phyTableOperation
     */
    public static final String ENABLE_LOG_GROUP_CONN_KEY = "ENABLE_LOG_GROUP_CONN_KEY";

    /**
     * allow use group parallelism for select-query on autocommit=true trans when shareReadView is closed
     */
    public static final String ALLOW_GROUP_PARALLELISM_WITHOUT_SHARE_READVIEW =
        "ALLOW_GROUP_PARALLELISM_WITHOUT_SHARE_READVIEW";

    /**
     * for table lookup replicate all filter from index table to primary table
     */
    public static final String REPLICATE_FILTER_TO_PRIMARY = "REPLICATE_FILTER_TO_PRIMARY";

    /**
     * Reschedule failed DDL job caused by exception after certain minutes
     */
    public static final String PAUSED_DDL_RESCHEDULE_INTERVAL_IN_MINUTES = "PAUSED_DDL_RESCHEDULE_INTERVAL_IN_MINUTES";

    /**
     * enable MDL
     */
    public static final String ENABLE_MDL = "ENABLE_MDL";

    public static final String ALWAYS_REBUILD_PLAN = "ALWAYS_REBUILD_PLAN";

    public static final String PARALLELISM = "PARALLELISM";

    /**
     * Number of producers to run oss load data
     */
    public static final String OSS_LOAD_DATA_PRODUCERS = "OSS_LOAD_DATA_PRODUCERS";

    /**
     * Number of max consumers to run oss load data
     */
    public static final String OSS_LOAD_DATA_MAX_CONSUMERS = "OSS_LOAD_DATA_MAX_CONSUMERS";

    /**
     * Number of uploaders to run oss load data
     */
    public static final String OSS_LOAD_DATA_FLUSHERS = "OSS_LOAD_DATA_FLUSHERS";

    /**
     * Number of uploaders to run oss load data
     */
    public static final String OSS_LOAD_DATA_UPLOADERS = "OSS_LOAD_DATA_UPLOADERS";

    public static final String OSS_EXPORT_MAX_ROWS_PER_FILE = "OSS_EXPORT_MAX_ROWS_PER_FILE";

    /**
     * Number of shards to prefetch (only take effect under parallel query)
     */
    public static final String PREFETCH_SHARDS = "PREFETCH_SHARDS";

    public static final String MAX_CACHE_PARAMS = "MAX_CACHE_PARAMS";

    public static final String MAX_EXECUTE_MEMORY = "MAX_EXECUTE_MEMORY";

    public static final String CHUNK_SIZE = "CHUNK_SIZE";

    public static final String INDEX_ADVISOR_BROADCAST_THRESHOLD = "INDEX_ADVISOR_BROADCAST_THRESHOLD";

    public static final String SHARDING_ADVISOR_MAX_NODE_NUM = "SHARDING_ADVISOR_MAX_NODE_NUM";

    public static final String SHARDING_ADVISOR_APPRO_THRESHOLD = "SHARDING_ADVISOR_APPRO_THRESHOLD";

    public static final String SHARDING_ADVISOR_BROADCAST_THRESHOLD = "SHARDING_ADVISOR_BROADCAST_THRESHOLD";

    public static final String SHARDING_ADVISOR_SHARD = "SHARDING_ADVISOR_SHARD";

    public static final String SHARDING_ADVISOR_RECORD_PLAN = "SHARDING_ADVISOR_RECORD_PLAN";

    public static final String SHARDING_ADVISOR_RETURN_ANSWER = "SHARDING_ADVISOR_RETURN_ANSWER";

    /**
     * VECTORIZATION
     */
    public static final String ENABLE_EXPRESSION_VECTORIZATION = "ENABLE_EXPRESSION_VECTORIZATION";

    /**
     * Allow constant fold when binding the vectorized expression.
     */
    public static final String ENABLE_EXPRESSION_CONSTANT_FOLD = "ENABLE_EXPRESSION_CONSTANT_FOLD";

    /**
     * SPM
     */
    public static final String PLAN_EXTERNALIZE_TEST = "PLAN_EXTERNALIZE_TEST";

    public static final String ENABLE_SPM = "ENABLE_SPM";

    public static final String ENABLE_MODULE_CHECK = "ENABLE_MODULE_CHECK";

    public static final String ENABLE_SPM_EVOLUTION_BY_TIME = "ENABLE_SPM_EVOLUTION_BY_TIME";

    public static final String ENABLE_SPM_BACKGROUND_TASK = "ENABLE_SPM_BACKGROUND_TASK";

    public static final String SPM_MAX_BASELINE_SIZE = "SPM_MAX_BASELINE_SIZE";

    public static final String SPM_DIFF_ESTIMATE_TIME = "SPM_DIFF_ESTIMATE_TIME";

    public static final String SPM_MAX_ACCEPTED_PLAN_SIZE_PER_BASELINE = "SPM_MAX_ACCEPTED_PLAN_SIZE_PER_BASELINE";

    public static final String SPM_MAX_UNACCEPTED_PLAN_SIZE_PER_BASELINE = "SPM_MAX_UNACCEPTED_PLAN_SIZE_PER_BASELINE";

    public static final String SPM_EVOLUTION_RATE = "SPM_EVOLUTION_RATE";

    public static final String SPM_PQO_STEADY_CHOOSE_TIME = "SPM_PQO_STEADY_CHOOSE_TIME";

    public static final String SPM_MAX_UNACCEPTED_PLAN_EVOLUTION_TIMES = "SPM_MAX_UNACCEPTED_PLAN_EVOLUTION_TIMES";

    public static final String SPM_MAX_BASELINE_INFO_SQL_LENGTH = "SPM_MAX_BASELINE_INFO_SQL_LENGTH";

    public static final String SPM_MAX_PLAN_INFO_PLAN_LENGTH = "SPM_MAX_PLAN_INFO_PLAN_LENGTH";

    public static final String SPM_MAX_PLAN_INFO_ERROR_COUNT = "SPM_MAX_PLAN_INFO_ERROR_COUNT";

    public static final String SPM_RECENTLY_EXECUTED_PERIOD = "SPM_RECENTLY_EXECUTED_PERIOD";

    public static final String EXPLAIN_OUTPUT_FORMAT = "EXPLAIN_OUTPUT_FORMAT";

    public static final String SPM_MAX_PQO_PARAMS_SIZE = "SPM_MAX_PQO_PARAMS_SIZE";

    public static final String SPM_ENABLE_PQO = "SPM_ENABLE_PQO";

    /**
     * max length of sql text in sql.log, default is 4096
     */
    public static final String SQL_LOG_MAX_LENGTH = "SQL_LOG_MAX_LENGTH";

    public static final String DNF_REX_NODE_LIMIT = "DNF_REX_NODE_LIMIT";

    public static final String CNF_REX_NODE_LIMIT = "CNF_REX_NODE_LIMIT";

    public static final String REX_MEMORY_LIMIT = "REX_MEMORY_LIMIT";

    public static final String ENABLE_ALTER_SHARD_KEY = "ENABLE_ALTER_SHARD_KEY";

    public static final String USING_RDS_RESULT_SKIP = "USING_RDS_RESULT_SKIP";

    public static final String CONN_TIME_ZONE = "CONN_TIME_ZONE";

    public static final String BLOCK_ENCRYPTION_MODE = "block_encryption_mode";

    public static final String ENABLE_RANDOM_PHY_TABLE_NAME = "ENABLE_RANDOM_PHY_TABLE_NAME";

    /**
     * Check if asynchronous DDL is supported. It's FALSE by default.
     */
    public static final String ENABLE_ASYNC_DDL = "ENABLE_ASYNC_DDL";

    /**
     * Force DDLs to run on the legacy DDL engine (Async DDL).
     */
    public static final String FORCE_DDL_ON_LEGACY_ENGINE = "FORCE_DDL_ON_LEGACY_ENGINE";

    public static final String DDL_ENGINE_DEBUG = "DDL_ENGINE_DEBUG";

    public static final String DDL_SHARD_CHANGE_DEBUG = "DDL_SHARD_CHANGE_DEBUG";

    /**
     * Check if asynchronous DDL is pure, i.e. A DDL execution returns
     * immediately.
     */
    public static final String PURE_ASYNC_DDL_MODE = "PURE_ASYNC_DDL_MODE";

    public static final String ENABLE_OPERATE_SUBJOB = "ENABLE_OPERATE_SUBJOB";

    public static final String SKIP_VALIDATE_STORAGE_INST_IDLE = "SKIP_VALIDATE_STORAGE_INST_IDLE";

    public static final String CANCEL_SUBJOB = "CANCEL_SUBJOB";

    public static final String EXPLAIN_DDL_PHYSICAL_OPERATION = "EXPLAIN_DDL_PHYSICAL_OPERATION";
    public static final String ENABLE_CONTINUE_RUNNING_SUBJOB = "ENABLE_CONTINUE_RUNNING_SUBJOB";
    /**
     * Check if the "INSTANT ADD COLUMN" feature is supported.
     */
    public static final String SUPPORT_INSTANT_ADD_COLUMN = "SUPPORT_INSTANT_ADD_COLUMN";

    /**
     * DDL job request timeout.
     */
    public static final String DDL_JOB_REQUEST_TIMEOUT = "DDL_JOB_REQUEST_TIMEOUT";

    /**
     * Indicate that how many logical DDLs are allowed to execute concurrently.
     */
    public static final String LOGICAL_DDL_PARALLELISM = "LOGICAL_DDL_PARALLELISM";

    /**
     * The number of async ddl job schedulers
     */
    public static final String NUM_OF_JOB_SCHEDULERS = "NUM_OF_JOB_SCHEDULERS";

    /**
     * Waiting time when no job is being handled (i.e. idle)
     */
    public static final String DDL_JOB_IDLE_WAITING_TIME = "DDL_JOB_IDLE_WAITING_TIME";

    public static final String ENABLE_ASYNC_PHY_OBJ_RECORDING = "ENABLE_ASYNC_PHY_OBJ_RECORDING";

    /**
     * Physical DDL MDL WAITING TIMEOUT
     */
    public static final String PHYSICAL_DDL_MDL_WAITING_TIMEOUT = "PHYSICAL_DDL_MDL_WAITING_TIMEOUT";

    public static final String MAX_TABLE_PARTITIONS_PER_DB = "MAX_TABLE_PARTITIONS_PER_DB";

    public static final String LOGICAL_DB_TIME_ZONE = "LOGICAL_DB_TIME_ZONE";
    public static final String SHARD_ROUTER_TIME_ZONE = "SHARD_ROUTER_TIME_ZONE";

    /**
     * The default collation of database if creating database without specifying any collation
     */
    public static final String COLLATION_SERVER = "COLLATION_SERVER";

    /**
     * Allow sharding with constant expression
     */
    public static final String ENABLE_SHARD_CONST_EXPR = "ENABLE_SHARD_CONST_EXPR";

    public static final String FORBID_APPLY_CACHE = "FORBID_APPLY_CACHE";

    public static final String FORCE_APPLY_CACHE = "FORCE_APPLY_CACHE";

    public static final String SKIP_READONLY_CHECK = "SKIP_READONLY_CHECK";

    public static final String WINDOW_FUNC_OPTIMIZE = "WINDOW_FUNC_OPTIMIZE";

    public static final String WINDOW_FUNC_SUBQUERY_CONDITION = "WINDOW_FUNC_SUBQUERY_CONDITION";

    /**
     * the num limit for correlate materialized judgement
     */
    public static final String PUSH_CORRELATE_MATERIALIZED_LIMIT = "PUSH_CORRELATE_MATERIALIZED_LIMIT";

    /**
     * force window and join reorder
     */
    public static final String WINDOW_FUNC_REORDER_JOIN = "WINDOW_FUNC_REORDER_JOIN";

    public static final String STATISTIC_COLLECTOR_FROM_RULE = "STATISTIC_COLLECTOR_FROM_RULE";

    public static final String ENABLE_MPP = "ENABLE_MPP";

    public static final String SLAVE_SOCKET_TIMEOUT = "SLAVE_SOCKET_TIMEOUT";

    public static final String MPP_JOIN_BROADCAST_NUM = "MPP_JOIN_BROADCAST_NUM";

    public static final String MPP_QUERY_MANAGER_THREAD_SIZE = "MPP_QUERY_MANAGER_THREAD_SIZE";

    public static final String MPP_QUERY_EXECUTION_THREAD_SIZE = "MPP_QUERY_EXECUTION_THREAD_SIZE";

    public static final String MPP_REMOTE_TASK_CALLBACK_THREAD_SIZE = "MPP_REMOTE_TASK_CALLBACK_THREAD_SIZE";

    public static final String MPP_TASK_NOTIFICATION_THREAD_SIZE = "MPP_TASK_NOTIFICATION_THREAD_SIZE";

    public static final String MPP_TASK_YIELD_THREAD_SIZE = "MPP_TASK_YIELD_THREAD_SIZE";

    public static final String MPP_MAX_WORKER_THREAD_SIZE = "MPP_MAX_WORKER_THREAD_SIZE";

    public static final String MPP_TASK_FUTURE_CALLBACK_THREAD_SIZE = "MPP_TASK_FUTURE_CALLBACK_THREAD_SIZE";

    public static final String MPP_EXCHANGE_CLIENT_THREAD_SIZE = "MPP_EXCHANGE_CLIENT_THREAD_SIZE";

    public static final String MPP_HTTP_RESPONSE_THREAD_SIZE = "MPP_HTTP_RESPONSE_THREAD_SIZE";

    public static final String MPP_HTTP_TIMEOUT_THREAD_SIZE = "MPP_HTTP_TIMEOUT_THREAD_SIZE";

    public static final String MPP_PARALLELISM = "MPP_PARALLELISM";

    public static final String MPP_HTTP_SERVER_MAX_THREADS = "MPP_HTTP_SERVER_MAX_THREADS";
    public static final String MPP_HTTP_SERVER_MIN_THREADS = "MPP_HTTP_SERVER_MIN_THREADS";
    public static final String MPP_HTTP_CLIENT_MAX_THREADS = "MPP_HTTP_CLIENT_MAX_THREADS";
    public static final String MPP_HTTP_CLIENT_MIN_THREADS = "MPP_HTTP_CLIENT_MIN_THREADS";
    public static final String MPP_HTTP_MAX_REQUESTS_PER_DESTINATION = "MPP_HTTP_MAX_REQUESTS_PER_DESTINATION";
    public static final String MPP_HTTP_CLIENT_MAX_CONNECTIONS = "MPP_HTTP_CLIENT_MAX_CONNECTIONS";
    public static final String MPP_HTTP_CLIENT_MAX_CONNECTIONS_PER_SERVER =
        "MPP_HTTP_CLIENT_MAX_CONNECTIONS_PER_SERVER";

    public static final String DATABASE_PARALLELISM = "DATABASE_PARALLELISM";

    public static final String POLARDBX_PARALLELISM = "POLARDBX_PARALLELISM";

    public static final String POLARDBX_SLAVE_INSTANCE_FIRST = "POLARDBX_SLAVE_INSTANCE_FIRST";

    public static final String SEGMENTED = "SEGMENTED";

    public static final String SEGMENTED_COUNT = "SEGMENTED_COUNT";

    public static final String PUSH_POLICY = "PUSH_POLICY";

    public static final String SUPPORT_PUSH_AMONG_DIFFERENT_DB = "SUPPORT_PUSH_AMONG_DIFFERENT_DB";

    public static final String SIMPLIFY_MULTI_DB_SINGLE_TB_PLAN = "SIMPLIFY_MULTI_DB_SINGLE_TB_PLAN";

    public static final String MPP_QUERY_MAX_RUN_TIME = "MPP_QUERY_MAX_RUN_TIME";

    public static final String MPP_QUERY_MAX_DELAY_TIME = "MPP_QUERY_MAX_DELAY_TIME";

    public static final String MPP_QUERY_MIN_DELAY_TIME = "MPP_QUERY_MIN_DELAY_TIME";

    public static final String MPP_QUERY_DELAY_COUNT = "MPP_QUERY_DELAY_COUNT";

    public static final String MPP_QUERY_MAX_DELAY_PENDING_RATIO = "MPP_QUERY_MAX_DELAY_PENDING_RATIO";

    public static final String MPP_QUERY_MIN_DELAY_PENDING_RATIO = "MPP_QUERY_MIN_DELAY_PENDING_RATIO";

    public static final String MPP_TASK_MAX_RUN_TIME = "MPP_TASK_MAX_RUN_TIME";

    public static final String MPP_CPU_CFS_PERIOD_US = "MPP_CPU_CFS_PERIOD_US";

    public static final String MPP_CPU_CFS_QUOTA = "MPP_CPU_CFS_QUOTA";

    public static final String MPP_CPU_CFS_MIN_QUOTA = "MPP_CPU_CFS_MIN_QUOTA";

    public static final String MPP_CPU_CFS_MAX_QUOTA = "MPP_CPU_CFS_MAX_QUOTA";

    public static final String MPP_AP_PRIORITY = "MPP_AP_PRIORITY";

    public static final String MPP_MIN_QUERY_EXPIRE_TIME = "MPP_MIN_QUERY_EXPIRE_TIME";

    public static final String MPP_MAX_QUERY_EXPIRED_RESERVETION_TIME = "MPP_MAX_QUERY_EXPIRED_RESERVETION_TIME";

    public static final String MPP_MAX_QUERY_HISTORY = "MPP_MAX_QUERY_HISTORY";

    public static final String MPP_QUERY_CLIENT_TIMEOUT = "MPP_QUERY_CLIENT_TIMEOUT";

    public static final String MPP_QUERY_REMOTE_TASK_MIN_ERROR = "MPP_QUERY_REMOTETASK_MIN_ERROR_DURATION";

    public static final String MPP_QUERY_REMOTE_TASK_MAX_ERROR = "MPP_QUERY_REMOTETASK_MAX_ERROR_DURATION";

    public static final String MPP_QUERY_MAX_CPU_TIME = "MPP_QUERY_MAX_CPU_TIME";

    public static final String MPP_MAX_PARALLELISM = "MPP_MAX_PARALLELISM";

    public static final String MPP_MIN_PARALLELISM = "MPP_MIN_PARALLELISM";

    public static final String MPP_QUERY_ROWS_PER_PARTITION = "MPP_QUERY_ROWS_PER_PARTITION";

    public static final String MPP_QUERY_IO_PER_PARTITION = "MPP_QUERY_IO_PER_PARTITION";

    public static final String LOOKUP_JOIN_PARALLELISM_FACTOR = "LOOKUP_JOIN_PARALLELISM_FACTOR";

    public static final String MPP_PARALLELISM_AUTO_ENABLE = "MPP_PARALLELISM_AUTO_ENABLE";

    public static final String MPP_QUERY_PHASED_EXEC_SCHEDULE_ENABLE = "MPP_QUERY_PHASED_EXEC_SCHEDULE_ENABLE";
    public static final String MPP_SCHEDULE_MAX_SPLITS_PER_NODE = "MPP_SCHEDULE_MAX_SPLITS_PER_NODE";

    public static final String MPP_SCHEMA_MAX_MEM = "MPP_SCHEMA_MAX_MEMORY";

    public static final String MPP_TP_TASK_WORKER_THREADS_RATIO = "MPP_TP_WORKER_THREADS_RATIO";

    public static final String MPP_TASK_WORKER_THREADS_RATIO = "MPP_WORKER_THREADS_RATIO";

    public static final String MPP_SPLIT_RUN_QUANTA = "MPP_SPLIT_RUN_QUANTA";

    public static final String MPP_STATUS_REFRESH_MAX_WAIT = "MPP_STATUS_REFRESH_MAX_WAIT";

    public static final String MPP_INFO_UPDATE_INTERVAL = "MPP_INFO_UPDATE_INTERVAL";

    public static final String MPP_OUTPUT_MAX_BUFFER_SIZE = "MPP_TASK_OUTPUT_MAX_BUFFER_SIZE";

    public static final String MPP_TASK_CLIENT_TIMEOUT = "MPP_TASK_CLIENT_TIMEOUT";

    public static final String MPP_TASKINFO_CACHE_MAX_ALIVE_MILLIS = "MPP_TASKINFO_CACHE_MAX_ALIVE_MILLIS";

    public static final String MPP_LOW_PRIORITY_ENABLED = "MPP_TASK_LOW_PRIORITY_ENABLED";

    public static final String MPP_TASK_LOCAL_MAX_BUFFER_SIZE = "MPP_TASK_LOCAL_MAX_BUFFER_SIZE";

    public static final String MPP_TASK_LOCAL_BUFFER_ENABLED = "MPP_TASK_LOCAL_BUFFER_ENABLED";

    public static final String MPP_TABLESCAN_DS_MAX_SIZE = "MPP_TABLESCAN_DS_MAX_SIZE";

    public static final String MPP_TABLESCAN_CONNECTION_STRATEGY = "MPP_TABLESCAN_CONNECTION_STRATEGY";

    public static final String MPP_EXCHANGE_MAX_BUFFER_SIZE = "MPP_EXCHANGE_MAX_BUFFER_SIZE";

    public static final String MPP_EXCHANGE_CONCURRENT_MULTIPLIER = "MPP_EXCHANGE_CONCURRENT_REQUEST_MULTIPLIER";

    public static final String MPP_EXCHANGE_MIN_ERROR_DURATION = "MPP_EXCHANGE_MIN_ERROR_DURATION";

    public static final String MPP_EXCHANGE_MAX_ERROR_DURATION = "MPP_EXCHANGE_MAX_ERROR_DURATION";

    public static final String MPP_EXCHANGE_MAX_RESPONSE_SIZE = "MPP_EXCHANGE_MAX_RESPONSE_SIZE";

    public static final String MPP_RPC_LOCAL_ENABLED = "MPP_RPC_LOCAL_ENABLED";

    public static final String MPP_PRINT_ELAPSED_LONG_QUERY_ENABLED = "MPP_PRINT_ELAPSED_QUERY_ENABLED";

    public static final String MPP_ELAPSED_QUERY_THRESHOLD_MILLS = "MPP_ELAPSED_QUERY_THRESHOLD_MILLS";

    public static final String MPP_METRIC_LEVEL = "MPP_METRIC_LEVEL";

    public static final String MPP_QUERY_NEED_RESERVE = "MPP_QUERY_NEED_RESERVE";

    public static final String ENABLE_MODIFY_SHARDING_COLUMN = "ENABLE_MODIFY_SHARDING_COLUMN";

    @Override
    protected Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

    public static final String ENABLE_MODIFY_LIMIT_OFFSET_NOT_ZERO = "ENABLE_MODIFY_LIMIT_OFFSET_NOT_ZERO";

    /**
     * Allow multi update/delete cross db
     */
    public static final String ENABLE_COMPLEX_DML_CROSS_DB = "ENABLE_COMPLEX_DML_CROSS_DB";

    public static final String COMPLEX_DML_WITH_TRX = "COMPLEX_DML_WITH_TRX";

    public static final String ENABLE_INDEX_SELECTION = "ENABLE_INDEX_SELECTION";

    public static final String ENABLE_INDEX_SKYLINE = "ENABLE_INDEX_SKYLINE";

    public static final String ENABLE_MERGE_INDEX = "ENABLE_MERGE_INDEX";

    public static final String ENABLE_OSS_INDEX_SELECTION = "ENABLE_OSS_INDEX_SELECTION";

    /**
     * Enable index selection
     */
    public static final String SWITCH_GROUP_ONLY = "SWITCH_GROUP_ONLY";

    public static final String PLAN = "PLAN";

    public static final String ENABLE_SQL_PROFILE_LOG = "ENABLE_SQL_PROFILE_LOG";

    public static final String ENABLE_CPU_PROFILE = "ENABLE_CPU_PROFILE";

    public static final String ENABLE_MEMORY_POOL = "ENABLE_MEMORY_POOL";

    public static final String PER_QUERY_MEMORY_LIMIT = "PER_QUERY_MEMORY_LIMIT";

    public static final String SCHEMA_MEMORY_LIMIT = "SCHEMA_MEMORY_LIMIT";

    public static final String GLOBAL_MEMORY_LIMIT = "GLOBAL_MEMORY_LIMIT";

    public static final String ENABLE_MEMORY_LIMITATION = "ENABLE_MEMORY_LIMITATION";

    public static final String ENABLE_POST_PLANNER = "ENABLE_POST_PLANNER";

    public static final String ENABLE_DIRECT_PLAN = "ENABLE_DIRECT_PLAN";

    public static final String MPP_MEMORY_REVOKING_THRESHOLD = "MPP_MEMORY_REVOKING_THRESHOLD";

    public static final String MPP_MEMORY_REVOKING_TARGET = "MPP_MEMORY_REVOKING_TARGET";

    public static final String MPP_NOTIFY_BLOCKED_QUERY_MEMORY = "MPP_NOTIFY_BLOCKED_QUERY_MEMORY";

    public static final String TP_LOW_MEMORY_PROPORTION = "TP_LOW_MEMORY_PROPORTION";

    public static final String TP_HIGH_MEMORY_PROPORTION = "TP_HIGH_MEMORY_PROPORTION";

    public static final String AP_LOW_MEMORY_PROPORTION = "AP_LOW_MEMORY_PROPORTION";

    public static final String AP_HIGH_MEMORY_PROPORTION = "AP_HIGH_MEMORY_PROPORTION";

    public static final String ENABLE_SPILL = "ENABLE_SPILL";

    public static final String MPP_MAX_SPILL_THREADS = "MPP_MAX_SPILL_THREADS";

    public static final String MPP_SPILL_PATHS = "MPP_SPILL_PATHS";

    public static final String MPP_MAX_SPILL_SPACE_THRESHOLD = "MPP_MAX_SPILL_SPACE_THRESHOLD";

    public static final String MPP_AVAILABLE_SPILL_SPACE_THRESHOLD = "MPP_AVAILABLE_SPILL_SPACE_THRESHOLD";

    public static final String MPP_MAX_QUERY_SPILL_SPACE_THRESHOLD = "MPP_MAX_QUERY_SPILL_SPACE_THRESHOLD";

    public static final String MPP_MAX_SPILL_FD_THRESHOLD = "MPP_MAX_SPILL_FD_THRESHOLD";

    public static final String HYBRID_HASH_JOIN_BUCKET_NUM = "HYBRID_HASH_JOIN_BUCKET_NUM";

    public static final String HYBRID_HASH_JOIN_RECURSIVE_BUCKET_NUM = "HYBRID_HASH_JOIN_RECURSIVE_BUCKET_NUM";

    public static final String HYBRID_HASH_JOIN_MAX_RECURSIVE_DEPTH = "HYBRID_HASH_JOIN_MAX_RECURSIVE_DEPTH";

    public static final String MPP_LESS_REVOKE_BYTES = "MPP_LESS_REVOKE_BYTES";

    public static final String MPP_ALLOCATOR_SIZE = "MPP_ALLOCATOR_SIZE";

    public static final String MPP_CLUSTER_NAME = "MPP_CLUSTER_NAME";

    public static final String ENABLE_PARAMETER_PLAN = "ENABLE_PARAMETER_PLAN";

    public static final String ENABLE_CROSS_VIEW_OPTIMIZE = "ENABLE_CROSS_VIEW_OPTIMIZE";

    public static final String MPP_GLOBAL_MEMORY_LIMIT_RATIO = "MPP_GLOBAL_MEMORY_LIMIT_RATIO";

    public static final String CONN_POOL_PROPERTIES = "CONN_POOL_PROPERTIES";
    public static final String CONN_POOL_MIN_POOL_SIZE = "CONN_POOL_MIN_POOL_SIZE";
    public static final String CONN_POOL_MAX_POOL_SIZE = "CONN_POOL_MAX_POOL_SIZE";
    public static final String CONN_POOL_MAX_WAIT_THREAD_COUNT = "CONN_POOL_MAX_WAIT_THREAD_COUNT";
    public static final String CONN_POOL_IDLE_TIMEOUT = "CONN_POOL_IDLE_TIMEOUT";
    public static final String CONN_POOL_BLOCK_TIMEOUT = "CONN_POOL_BLOCK_TIMEOUT";
    public static final String CONN_POOL_XPROTO_CONFIG = "CONN_POOL_XPROTO_CONFIG";
    public static final String CONN_POOL_XPROTO_FLAG = "CONN_POOL_XPROTO_FLAG";
    public static final String CONN_POOL_XPROTO_META_DB_PORT = "CONN_POOL_XPROTO_META_DB_PORT";
    public static final String CONN_POOL_XPROTO_STORAGE_DB_PORT = "CONN_POOL_XPROTO_STORAGE_DB_PORT";
    public static final String CONN_POOL_XPROTO_MAX_CLIENT_PER_INST = "CONN_POOL_XPROTO_MAX_CLIENT_PER_INST";
    public static final String CONN_POOL_XPROTO_MAX_SESSION_PER_CLIENT = "CONN_POOL_XPROTO_MAX_SESSION_PER_CLIENT";
    public static final String CONN_POOL_XPROTO_MAX_POOLED_SESSION_PER_INST =
        "CONN_POOL_XPROTO_MAX_POOLED_SESSION_PER_INST";
    public static final String CONN_POOL_XPROTO_MIN_POOLED_SESSION_PER_INST =
        "CONN_POOL_XPROTO_MIN_POOLED_SESSION_PER_INST";
    public static final String CONN_POOL_XPROTO_SESSION_AGING_TIME = "CONN_POOL_XPROTO_SESSION_AGING_TIME";
    public static final String CONN_POOL_XPROTO_SLOW_THRESH = "CONN_POOL_XPROTO_SLOW_THRESH";
    public static final String CONN_POOL_XPROTO_AUTH = "CONN_POOL_XPROTO_AUTH";
    public static final String CONN_POOL_XPROTO_AUTO_COMMIT_OPTIMIZE = "CONN_POOL_XPROTO_AUTO_COMMIT_OPTIMIZE";
    public static final String CONN_POOL_XPROTO_XPLAN = "CONN_POOL_XPROTO_XPLAN";
    public static final String CONN_POOL_XPROTO_XPLAN_EXPEND_STAR = "CONN_POOL_XPROTO_XPLAN_EXPEND_STAR";
    public static final String CONN_POOL_XPROTO_XPLAN_TABLE_SCAN = "CONN_POOL_XPROTO_XPLAN_TABLE_SCAN";
    public static final String CONN_POOL_XPROTO_TRX_LEAK_CHECK = "CONN_POOL_XPROTO_TRX_LEAK_CHECK";
    public static final String CONN_POOL_XPROTO_MESSAGE_TIMESTAMP = "CONN_POOL_XPROTO_MESSAGE_TIMESTAMP";
    public static final String CONN_POOL_XPROTO_PLAN_CACHE = "CONN_POOL_XPROTO_PLAN_CACHE";
    public static final String CONN_POOL_XPROTO_CHUNK_RESULT = "CONN_POOL_XPROTO_CHUNK_RESULT";
    public static final String CONN_POOL_XPROTO_PURE_ASYNC_MPP = "CONN_POOL_XPROTO_PURE_ASYNC_MPP";
    public static final String CONN_POOL_XPROTO_CHECKER = "CONN_POOL_XPROTO_CHECKER";
    public static final String CONN_POOL_XPROTO_DIRECT_WRITE = "CONN_POOL_XPROTO_DIRECT_WRITE";
    public static final String CONN_POOL_XPROTO_FEEDBACK = "CONN_POOL_XPROTO_FEEDBACK";
    public static final String CONN_POOL_XPROTO_MAX_PACKET_SIZE = "CONN_POOL_XPROTO_MAX_PACKET_SIZE";
    public static final String CONN_POOL_XPROTO_QUERY_TOKEN = "CONN_POOL_XPROTO_QUERY_TOKEN";
    public static final String CONN_POOL_XPROTO_PIPE_BUFFER_SIZE = "CONN_POOL_XPROTO_PIPE_BUFFER_SIZE";

    public static final String XPROTO_MAX_DN_CONCURRENT = "XPROTO_MAX_DN_CONCURRENT";

    public static final String XPROTO_MAX_DN_WAIT_CONNECTION = "XPROTO_MAX_DN_WAIT_CONNECTION";

    /**
     * X-Protocol always keep upper filter when use XPlan
     */
    public static final String XPROTO_ALWAYS_KEEP_FILTER_ON_XPLAN_GET = "XPROTO_ALWAYS_KEEP_FILTER_ON_XPLAN_GET";

    /**
     * x-protocol probe timeout.
     */
    public static final String XPROTO_PROBE_TIMEOUT = "XPROTO_PROBE_TIMEOUT";

    /**
     * Galaxy prepare config.
     */
    public static final String XPROTO_GALAXY_PREPARE = "XPROTO_GALAXY_PREPARE";

    /**
     * X-Protocol / XRPC flow control pipe max size(in KB, 1024 means 1MB).
     */
    public static final String XPROTO_FLOW_CONTROL_SIZE_KB = "XPROTO_FLOW_CONTROL_SIZE_KB";

    /**
     * X-Protocol / XRPC TCP aging time in seconds.
     */
    public static final String XPROTO_TCP_AGING = "XPROTO_TCP_AGING";

    /**
     * The storage inst list of all single groups when creating new database
     */
    public static final String SINGLE_GROUP_STORAGE_INST_LIST = "SINGLE_GROUP_STORAGE_INST_LIST";

    public static final String SHARD_DB_COUNT_EACH_STORAGE_INST = "SHARD_DB_COUNT_EACH_STORAGE_INST";

    public static final String SHARD_DB_COUNT_EACH_STORAGE_INST_FOR_STMT =
        "SHARD_DB_COUNT_EACH_STORAGE_INST_FOR_STMT";

    public static final String MAX_LOGICAL_DB_COUNT = "MAX_LOGICAL_DB_COUNT";

    public static final String PASSWORD_RULE_CONFIG = "PASSWORD_RULE_CONFIG";
    public static final String MAX_AUDIT_LOG_CLEAN_KEEP_DAYS = "MAX_AUDIT_LOG_CLEAN_KEEP_DAYS";
    public static final String MAX_AUDIT_LOG_CLEAN_DELAY_DAYS = "MAX_AUDIT_LOG_CLEAN_DELAY_DAYS";
    public static final String LOGIN_ERROR_MAX_COUNT_CONFIG = "LOGIN_ERROR_MAX_COUNT_CONFIG";
    public static final String ENABLE_LOGIN_AUDIT_CONFIG = "ENABLE_LOGIN_AUDIT_CONFIG";

    public static final String ENABLE_FORBID_PUSH_DML_WITH_HINT = "ENABLE_FORBID_PUSH_DML_WITH_HINT";

    public static final String VARIABLE_EXPIRE_TIME = "VARIABLE_EXPIRE_TIME";

    public static final String MERGE_SORT_BUFFER_SIZE = "MERGE_SORT_BUFFER_SIZE";

    public static final String ENABLE_AGG_PRUNING = "ENABLE_AGG_PRUNING";

    public static final String WORKLOAD_CPU_THRESHOLD = "WORKLOAD_CPU_THRESHOLD";

    public static final String WORKLOAD_MEMORY_THRESHOLD = "WORKLOAD_MEMORY_THRESHOLD";

    public static final String WORKLOAD_IO_THRESHOLD = "WORKLOAD_IO_THRESHOLD";

    public static final String WORKLOAD_OSS_NET_THRESHOLD = "WORKLOAD_OSS_NET_THRESHOLD";

    public static final String WORKLOAD_TYPE = "WORKLOAD_TYPE";

    public static final String EXECUTOR_MODE = "EXECUTOR_MODE";

    public static final String ENABLE_MASTER_MPP = "ENABLE_MASTER_MPP";

    public static final String ENABLE_TEMP_TABLE_JOIN = "ENABLE_TEMP_TABLE_JOIN";

    public static final String LOOKUP_IN_VALUE_LIMIT = "LOOKUP_IN_VALUE_LIMIT";

    public static final String LOOKUP_JOIN_BLOCK_SIZE_PER_SHARD = "LOOKUP_JOIN_BLOCK_SIZE_PER_SHARD";

    public static final String ENABLE_CONSISTENT_REPLICA_READ = "ENABLE_CONSISTENT_REPLICA_READ";

    public static final String EXPLAIN_LOGICALVIEW = "EXPLAIN_LOGICALVIEW";

    public static final String ENABLE_HTAP = "ENABLE_HTAP";

    public static final String IN_SUB_QUERY_THRESHOLD = "IN_SUB_QUERY_THRESHOLD";

    public static final String ENABLE_OR_OPT = "ENABLE_OR_OPT";

    public static final String ENABLE_IN_SUB_QUERY_FOR_DML = "ENABLE_IN_SUB_QUERY_FOR_DML";

    public static final String ENABLE_RUNTIME_FILTER = "ENABLE_RUNTIME_FILTER";

    public static final String FORCE_ENABLE_RUNTIME_FILTER_COLUMNS = "FORCE_ENABLE_RUNTIME_FILTER_COLUMNS";

    public static final String FORCE_DISABLE_RUNTIME_FILTER_COLUMNS = "FORCE_DISABLE_RUNTIME_FILTER_COLUMNS";

    public static final String BLOOM_FILTER_BROADCAST_NUM = "BLOOM_FILTER_BROADCAST_NUM";

    public static final String BLOOM_FILTER_MAX_SIZE = "BLOOM_FILTER_MAX_SIZE";

    public static final String BLOOM_FILTER_RATIO = "BLOOM_FILTER_RATIO";

    public static final String RUNTIME_FILTER_PROBE_MIN_ROW_COUNT = "RUNTIME_FILTER_PROBE_MIN_ROW_COUNT";

    public static final String BLOOM_FILTER_GUESS_SIZE = "BLOOM_FILTER_GUESS_SIZE";

    public static final String BLOOM_FILTER_MIN_SIZE = "BLOOM_FILTER_MIN_SIZE";

    public static final String ENABLE_PUSH_RUNTIME_FILTER_SCAN = "ENABLE_PUSH_RUNTIME_FILTER_SCAN";

    public static final String WAIT_RUNTIME_FILTER_FOR_SCAN = "WAIT_RUNTIME_FILTER_FOR_SCAN";

    public static final String ENABLE_RUNTIME_FILTER_INTO_BUILD_SIDE = "ENABLE_RUNTIME_FILTER_INTO_BUILD_SIDE";

    public static final String ENABLE_RUNTIME_FILTER_XXHASH = "ENABLE_RUNTIME_FILTER_XXHASH";

    public static final String ENABLE_SPLIT_RUNTIME_FILTER = "ENABLE_SPLIT_RUNTIME_FILTER";

    public static final String ENABLE_OPTIMIZE_SCAN_WITH_RUNTIME_FILTER = "ENABLE_OPTIMIZE_SCAN_WITH_RUNTIME_FILTER";

    public static final String RUNTIME_FILTER_FPP = "RUNTIME_FILTER_FPP";

    public static final String STORAGE_SUPPORTS_BLOOM_FILTER = "STORAGE_SUPPORTS_BLOOM_FILTER";

    public static final String WAIT_BLOOM_FILTER_TIMEOUT_MS = "WAIT_BLOOM_FILTER_TIMEOUT_MS";

    public static final String RESUME_SCAN_STEP_SIZE = "RESUME_SCAN_STEP_SIZE";

    public static final String ENABLE_SPILL_OUTPUT = "ENABLE_SPILL_OUTPUT";

    public static final String SPILL_OUTPUT_MAX_BUFFER_SIZE = "SPILL_OUTPUT_MAX_BUFFER_SIZE";

    public static final String SUPPORT_READ_FOLLOWER_STRATEGY = "SUPPORT_READ_FOLLOWER_STRATEGY";

    public static final String ENABLE_BROADCAST_RANDOM_READ = "ENABLE_BROADCAST_RANDOM_READ";

    public static final String TABLEGROUP_DEBUG = "TABLEGROUP_DEBUG";

    public static final String DDL_ON_PRIMARY_GSI_TYPE = "DDL_ON_PRIMARY_GSI_TYPE";

    public static final String SLEEP_TIME_BEFORE_NOTIFY_DDL = "SLEEP_TIME_BEFORE_NOTIFY_DDL";

    public static final String SHOW_IMPLICIT_ID = "SHOW_IMPLICIT_ID";

    public static final String ENABLE_DRIVING_STREAM_SCAN = "ENABLE_DRIVING_STREAM_SCAN";

    public static final String ENABLE_SIMPLIFY_TRACE_SQL = "ENABLE_SIMPLIFY_TRACE_SQL";

    public static final String CALCULATE_ACTUAL_SHARD_COUNT_FOR_COST = "CALCULATE_ACTUAL_SHARD_COUNT_FOR_COST";

    public static final String PARAMETRIC_SIMILARITY_ALGO = "PARAMETRIC_SIMILARITY_ALGO";

    public static final String FEEDBACK_WORKLOAD_AP_THRESHOLD = "FEEDBACK_WORKLOAD_AP_THRESHOLD";

    public static final String FEEDBACK_WORKLOAD_TP_THRESHOLD = "FEEDBACK_WORKLOAD_TP_THRESHOLD";

    public static final String MASTER_READ_WEIGHT = "MASTER_READ_WEIGHT";

    public static final String STORAGE_DELAY_THRESHOLD = "STORAGE_DELAY_THRESHOLD";

    public static final String STORAGE_BUSY_THRESHOLD = "STORAGE_BUSY_THRESHOLD";

    /**
     * set the operation strategy when the slave delay
     * <0 means nothing, =1 change master, =2 throw exception
     */
    public static final String DELAY_EXECUTION_STRATEGY = "DELAY_EXECUTION_STRATEGY";

    public static final String KEEP_DELAY_EXECUTION_STRATEGY = "KEEP_DELAY_EXECUTION_STRATEGY";

    public static final String USE_CDC_CON = "USE_CDC_CON";

    /**
     * top record size
     */
    public static final String TOPN_SIZE = "TOPN_SIZE";

    public static final String TOPN_MIN_NUM = "TOPN_MIN_NUM";

    public static final String SELECT_INTO_OUTFILE_STATISTICS_DUMP = "SELECT_INTO_OUTFILE_STATISTICS_DUMP";

    /**
     * Whether ignore histogram of string column
     */
    public static final String STATISTICS_DUMP_IGNORE_STRING = "STATISTICS_DUMP_IGNORE_STRING";

    /**
     * 是否开启 SELECT INTO OUTFILE 默认关闭
     */
    public static final String ENABLE_SELECT_INTO_OUTFILE = "ENABLE_SELECT_INTO_OUTFILE";

    public static final String SHOW_HASH_PARTITIONS_BY_RANGE = "SHOW_HASH_PARTITIONS_BY_RANGE";

    public static final String SHOW_TABLE_GROUP_NAME = "SHOW_TABLE_GROUP_NAME";

    public static final String MAX_PHYSICAL_PARTITION_COUNT = "MAX_PHYSICAL_PARTITION_COUNT";

    public static final String MAX_PARTITION_COLUMN_COUNT = "MAX_PARTITION_COLUMN_COUNT";

    /**
     * Label if auto use range-key subpart for index of auto-part table, default is true
     */
    public static final String ENABLE_AUTO_USE_RANGE_FOR_TIME_INDEX = "ENABLE_AUTO_USE_RANGE_FOR_TIME_INDEX";

    /**
     * Label if auto use range/list columns partitions for "part by range/list", default is true
     */
    public static final String ENABLE_AUTO_USE_COLUMNS_PARTITION = "ENABLE_AUTO_USE_COLUMNS_PARTITION";

    /**
     * Balancer parameters
     */
    public static final String ENABLE_BALANCER = "ENABLE_BALANCER";
    public static final String BALANCER_MAX_PARTITION_SIZE = "BALANCER_MAX_PARTITION_SIZE";
    public static final String BALANCER_WINDOW = "BALANCER_WINDOW";

    /**
     * switch for partition management
     */
    public static final String ENABLE_PARTITION_MANAGEMENT = "ENABLE_PARTITION_MANAGEMENT";

    /**
     * Allow move the single table with locality='balance_single_table=on' during scale-out/scale-in
     */
    public static final String ALLOW_MOVING_BALANCED_SINGLE_TABLE = "ALLOW_MOVING_BALANCED_SINGLE_TABLE";

    /**
     * The default value of default_single when create auto-db without specify default_single option
     */
    public static final String DATABASE_DEFAULT_SINGLE = "DATABASE_DEFAULT_SINGLE";

    /**
     * switch for partition pruning, only use for qatest and debug
     */
    public static final String ENABLE_PARTITION_PRUNING = "ENABLE_PARTITION_PRUNING";

    public static final String ENABLE_AUTO_MERGE_INTERVALS_IN_PRUNING = "ENABLE_AUTO_MERGE_INTERVALS_IN_PRUNING";

    public static final String ENABLE_INTERVAL_ENUMERATION_IN_PRUNING = "ENABLE_INTERVAL_ENUMERATION_IN_PRUNING";

    public static final String PARTITION_PRUNING_STEP_COUNT_LIMIT = "PARTITION_PRUNING_STEP_COUNT_LIMIT";

    public static final String USE_FAST_SINGLE_POINT_INTERVAL_MERGING = "USE_FAST_SINGLE_POINT_INTERVAL_MERGING";

    public static final String ENABLE_CONST_EXPR_EVAL_CACHE = "ENABLE_CONST_EXPR_EVAL_CACHE";

    public static final String MAX_ENUMERABLE_INTERVAL_LENGTH = "MAX_ENUMERABLE_INTERVAL_LENGTH";

    /**
     * The max size of in value from the InSubQuery pruning
     */
    public static final String MAX_IN_SUBQUERY_PRUNING_SIZE = "MAX_IN_SUBQUERY_PRUNING_SIZE";

    /**
     * Enable do pruning log in pruner.log
     */
    public static final String ENABLE_LOG_PART_PRUNING = "ENABLE_LOG_PART_PRUNING";

    public static final String ENABLE_OPTIMIZER_ALERT = "ENABLE_OPTIMIZER_ALERT";

    public static final String ENABLE_OPTIMIZER_ALERT_LOG = "ENABLE_OPTIMIZER_ALERT_LOG";

    public static final String OPTIMIZER_ALERT_LOG_INTERVAL = "OPTIMIZER_ALERT_LOG_INTERVAL";

    public static final String ALERT_BKA_BASE = "ALERT_BKA_BASE";

    public static final String ALERT_TP_BASE = "ALERT_TP_BASE";

    public static final String ENABLE_ALERT_TEST = "ENABLE_ALERT_TEST";

    public static final String ENABLE_BRANCH_AND_BOUND_OPTIMIZATION = "ENABLE_BRANCH_AND_BOUND_OPTIMIZATION";

    public static final String ENABLE_BROADCAST_JOIN = "ENABLE_BROADCAST_JOIN";

    public static final String BROADCAST_SHUFFLE_COUNT = "BROADCAST_SHUFFLE_COUNT";

    public static final String BROADCAST_SHUFFLE_PARALLELISM = "BROADCAST_SHUFFLE_PARALLELISM";

    public static final String ENABLE_PASS_THROUGH_TRAIT = "ENABLE_PASS_THROUGH_TRAIT";

    public static final String ENABLE_DERIVE_TRAIT = "ENABLE_DERIVE_TRAIT";

    public static final String ENABLE_SHUFFLE_BY_PARTIAL_KEY = "ENABLE_SHUFFLE_BY_PARTIAL_KEY";

    public static final String ADVISE_TYPE = "ADVISE_TYPE";

    public static final String ENABLE_HLL = "ENABLE_HLL";

    public static final String STRICT_ENUM_CONVERT = "STRICT_ENUM_CONVERT";

    public static final String STRICT_YEAR_CONVERT = "STRICT_YEAR_CONVERT";

    /**
     * feedback minor tolerance value
     */
    public static final String MINOR_TOLERANCE_VALUE = "MINOR_TOLERANCE_VALUE";

    /**
     * upper bound for baseline sync
     */
    public static final String MAX_BASELINE_SYNC_PLAN_SIZE = "MAX_BASELINE_SYNC_PLAN_SIZE";

    public static final String SPM_OLD_PLAN_CHOOSE_COUNT_LEVEL = "SPM_OLD_PLAN_CHOOSE_COUNT_LEVEL";

    /**
     * bytes upper bound for baseline sync
     */
    public static final String MAX_BASELINE_SYNC_BYTE_SIZE = "MAX_BASELINE_SYNC_BYTE_SIZE";

    /**
     * the period of storage ha task of each dn, unit:ms
     */
    public static final String STORAGE_HA_TASK_PERIOD = "STORAGE_HA_TASK_PERIOD";

    public static final String STORAGE_HA_SOCKET_TIMEOUT = "STORAGE_HA_SOCKET_TIMEOUT";

    public static final String STORAGE_HA_CONNECT_TIMEOUT = "STORAGE_HA_CONNECT_TIMEOUT";

    public static final String ENABLE_HA_CHECK_TASK_LOG = "ENABLE_HA_CHECK_TASK_LOG";

    public static final String STATISTIC_NDV_SKETCH_EXPIRE_TIME = "STATISTIC_NDV_SKETCH_EXPIRE_TIME";

    public static final String STATISTIC_NDV_SKETCH_QUERY_TIMEOUT = "STATISTIC_NDV_SKETCH_QUERY_TIMEOUT";

    public static final String STATISTIC_NDV_SKETCH_MAX_DIFFERENT_VALUE = "STATISTIC_NDV_SKETCH_MAX_DIFFERENT_VALUE";

    public static final String STATISTIC_NDV_SKETCH_MAX_DIFFERENT_RATIO = "STATISTIC_NDV_SKETCH_MAX_DIFFERENT_RATIO";

    public static final String STATISTIC_NDV_SKETCH_SAMPLE_RATE = "STATISTIC_NDV_SKETCH_SAMPLE_RATE";

    public static final String ENABLE_CHECK_STATISTICS_EXPIRE = "ENABLE_CHECK_STATISTICS_EXPIRE";

    public static final String INDEX_ADVISOR_CARDINALITY_BASE = "INDEX_ADVISOR_CARDINALITY_BASE";

    public static final String AUTO_COLLECT_NDV_SKETCH = "AUTO_COLLECT_NDV_SKETCH";

    public static final String CDC_STARTUP_MODE = "CDC_STARTUP_MODE";
    /**
     * CDC模块是否开启metadata snapshot 能力
     */
    public static final String ENABLE_CDC_META_BUILD_SNAPSHOT = "ENABLE_CDC_META_BUILD_SNAPSHOT";

    public static final String SHARE_STORAGE_MODE = "SHARE_STORAGE_MODE";

    public static final String SHOW_ALL_PARAMS = "SHOW_ALL_PARAMS";

    public static final String ENABLE_SET_GLOBAL = "ENABLE_SET_GLOBAL";

    public static final String ENABLE_PREEMPTIVE_MDL = "ENABLE_PREEMPTIVE_MDL";

    public static final String SHOW_STORAGE_POOL = "SHOW_STORAGE_POOL";

    public static final String SHOW_FULL_LOCALITY = "SHOW_FULL_LOCALITY";
    public static final String PREEMPTIVE_MDL_INITWAIT = "PREEMPTIVE_MDL_INITWAIT";
    public static final String PREEMPTIVE_MDL_INTERVAL = "PREEMPTIVE_MDL_INTERVAL";

    public static final String RENAME_PREEMPTIVE_MDL_INITWAIT = "RENAME_PREEMPTIVE_MDL_INITWAIT";
    public static final String RENAME_PREEMPTIVE_MDL_INTERVAL = "RENAME_PREEMPTIVE_MDL_INTERVAL";

    public static final String TG_PREEMPTIVE_MDL_INITWAIT = "TG_PREEMPTIVE_MDL_INITWAIT";
    public static final String TG_PREEMPTIVE_MDL_INTERVAL = "TG_PREEMPTIVE_MDL_INTERVAL";

    public static final String FORCE_READ_OUTSIDE_TX = "FORCE_READ_OUTSIDE_TX";

    public static final String SCHEDULER_SCAN_INTERVAL_SECONDS = "SCHEDULER_SCAN_INTERVAL_SECONDS";
    public static final String SCHEDULER_CLEAN_UP_INTERVAL_HOURS = "SCHEDULER_CLEAN_UP_INTERVAL_HOURS";
    public static final String SCHEDULER_RECORD_KEEP_HOURS = "SCHEDULER_RECORD_KEEP_HOURS";
    public static final String SCHEDULER_MIN_WORKER_COUNT = "SCHEDULER_MIN_WORKER_COUNT";
    public static final String SCHEDULER_MAX_WORKER_COUNT = "SCHEDULER_MAX_WORKER_COUNT";

    public static final String DEFAULT_LOCAL_PARTITION_SCHEDULE_CRON_EXPR =
        "DEFAULT_LOCAL_PARTITION_SCHEDULE_CRON_EXPR";

    /**
     * check target table after alter tablegroup's backfill
     */
    public static final String TABLEGROUP_REORG_CHECK_AFTER_BACKFILL = "TABLEGROUP_REORG_CHECK_AFTER_BACKFILL";

    /**
     * TABLEGROUP_REORG_BACKFILL_USE_FASTCHECKER
     */
    public static final String TABLEGROUP_REORG_BACKFILL_USE_FASTCHECKER = "TABLEGROUP_REORG_BACKFILL_USE_FASTCHECKER";

    public static final String TABLEGROUP_REORG_CHECK_BATCH_SIZE = "TABLEGROUP_REORG_CHECK_BATCH_SIZE";

    public static final String TABLEGROUP_REORG_CHECK_SPEED_LIMITATION = "TABLEGROUP_REORG_CHECK_SPEED_LIMITATION";

    public static final String TABLEGROUP_REORG_CHECK_SPEED_MIN = "TABLEGROUP_REORG_CHECK_SPEED_MIN";

    public static final String TABLEGROUP_REORG_CHECK_PARALLELISM = "TABLEGROUP_REORG_CHECK_PARALLELISM";

    public static final String TABLEGROUP_REORG_FASTCHECKER_PARALLELISM = "TABLEGROUP_REORG_FASTCHECKER_PARALLELISM";
    /**
     * number of error for check early fail.
     */
    public static final String TABLEGROUP_REORG_EARLY_FAIL_NUMBER = "TABLEGROUP_REORG_EARLY_FAIL_NUMBER";

    /**
     * set the table's final status for alter tablegroup debug purpose.
     */
    public static final String TABLEGROUP_REORG_FINAL_TABLE_STATUS_DEBUG = "TABLEGROUP_REORG_FINAL_TABLE_STATUS_DEBUG";

    public static final String INTERRUPT_DDL_WHILE_LOSING_LEADER = "INTERRUPT_DDL_WHILE_LOSING_LEADER";

    public static final String RECORD_SQL_COST = "RECORD_SQL_COST";

    public static final String ENABLE_LOGICALVIEW_COST = "ENABLE_LOGICALVIEW_COST";

    public static final String FORCE_RECREATE_GROUP_DATASOURCE = "FORCE_RECREATE_GROUP_DATASOURCE";

    public static final String ENABLE_PLAN_TYPE_DIGEST = "ENABLE_PLAN_TYPE_DIGEST";

    public static final String ENABLE_PLAN_TYPE_DIGEST_STRICT_MODE = "ENABLE_PLAN_TYPE_DIGEST_STRICT_MODE";

    /**
     * flag that if auto warming logical db
     */
    public static final String ENABLE_LOGICAL_DB_WARMMING_UP = "ENABLE_LOGICAL_DB_WARMMING_UP";

    /**
     * pool size of auto-warming-logical-db-executor
     */
    public static final String LOGICAL_DB_WARMMING_UP_EXECUTOR_POOL_SIZE = "LOGICAL_DB_WARMMING_UP_EXECUTOR_POOL_SIZE";

    public static final String FLASHBACK_RENAME = "FLASHBACK_RENAME";

    public static final String PURGE_FILE_STORAGE_TABLE = "PURGE_FILE_STORAGE_TABLE";
    /* ================ For OSS Table ORC File ================ */

    public static final String OSS_BACKFILL_PARALLELISM = "OSS_BACKFILL_PARALLELISM";

    public static final String OSS_ORC_INDEX_STRIDE = "OSS_ORC_INDEX_STRIDE";

    public static final String OSS_BLOOM_FILTER_FPP = "OSS_BLOOM_FILTER_FPP";

    public static final String OSS_MAX_ROWS_PER_FILE = "OSS_MAX_ROWS_PER_FILE";

    public static final String OSS_REMOVE_TMP_FILES = "OSS_REMOVE_TMP_FILES";

    public static final String OSS_ORC_COMPRESSION = "OSS_ORC_COMPRESSION";

    /* ================ For OSS Table File System ================ */

    public static final String OSS_FS_MAX_READ_RATE = "OSS_FS_MAX_READ_RATE";

    public static final String OSS_FS_MAX_WRITE_RATE = "OSS_FS_MAX_WRITE_RATE";

    public static final String OSS_FS_VALIDATION_ENABLE = "OSS_FS_VALIDATION_ENABLE";

    public static final String OSS_FS_CACHE_TTL = "OSS_FS_CACHE_TTL";

    public static final String OSS_FS_MAX_CACHED_ENTRIES = "OSS_FS_MAX_CACHED_ENTRIES";

    public static final String OSS_FS_HOT_CACHE_TTL = "OSS_FS_HOT_CACHE_TTL";

    public static final String OSS_FS_MAX_HOT_CACHED_ENTRIES = "OSS_FS_MAX_HOT_CACHED_ENTRIES";

    public static final String OSS_ORC_MAX_MERGE_DISTANCE = "OSS_ORC_MAX_MERGE_DISTANCE";

    public static final String FILE_LIST = "FILE_LIST";

    public static final String FILE_PATTERN = "FILE_PATTERN";

    public static final String ENABLE_EXPIRE_FILE_STORAGE_PAUSE = "ENABLE_EXPIRE_FILE_STORAGE_PAUSE";

    public static final String ENABLE_CHECK_DDL_FILE_STORAGE = "ENABLE_CHECK_DDL_FILE_STORAGE";

    public static final String ENABLE_CHECK_DDL_BINDING_FILE_STORAGE = "ENABLE_CHECK_DDL_BINDING_FILE_STORAGE";

    public static final String ENABLE_EXPIRE_FILE_STORAGE_TEST_PAUSE = "ENABLE_EXPIRE_FILE_STORAGE_TEST_PAUSE";

    public static final String FILE_STORAGE_TASK_PARALLELISM = "FILE_STORAGE_TASK_PARALLELISM";
    public static final String ENABLE_FILE_STORE_CHECK_TABLE = "ENABLE_FILE_STORE_CHECK_TABLE";

    public static final String ENABLE_OSS_BUFFER_POOL = "ENABLE_OSS_BUFFER_POOL";

    public static final String ENABLE_OSS_DELAY_MATERIALIZATION = "ENABLE_OSS_DELAY_MATERIALIZATION";

    public static final String ENABLE_OSS_ZERO_COPY = "ENABLE_OSS_ZERO_COPY";

    public static final String ENABLE_OSS_COMPATIBLE = "ENABLE_OSS_COMPATIBLE";

    public static final String ENABLE_OSS_DELAY_MATERIALIZATION_ON_EXCHANGE =
        "ENABLE_OSS_DELAY_MATERIALIZATION_ON_EXCHANGE";

    public static final String ENABLE_OSS_FILE_CONCURRENT_SPLIT_ROUND_ROBIN =
        "ENABLE_OSS_FILE_CONCURRENT_SPLIT_ROUND_ROBIN";

    public static final String ENABLE_REUSE_VECTOR = "ENABLE_REUSE_VECTOR";

    public static final String ENABLE_DECIMAL_FAST_VEC = "ENABLE_DECIMAL_FAST_VEC";

    public static final String ENABLE_UNIQUE_HASH_KEY = "ENABLE_UNIQUE_HASH_KEY";

    public static final String BLOCK_BUILDER_CAPACITY = "BLOCK_BUILDER_CAPACITY";

    public static final String ENABLE_HASH_TABLE_BLOOM_FILTER = "ENABLE_HASH_TABLE_BLOOM_FILTER";

    public static final String ENABLE_COMMON_SUB_EXPRESSION_TREE_ELIMINATE =
        "ENABLE_COMMON_SUB_EXPRESSION_TREE_ELIMINATE";

    public static final String OSS_FILE_ORDER = "OSS_FILE_ORDER";

    public static final String MAX_SESSION_PREPARED_STMT_COUNT = "MAX_SESSION_PREPARED_STMT_COUNT";

    public static final String ALLOW_REPLACE_ARCHIVE_TABLE = "ALLOW_REPLACE_ARCHIVE_TABLE";

    public static final String ALLOW_CREATE_TABLE_LIKE_FILE_STORE = "ALLOW_CREATE_TABLE_LIKE_FILE_STORE";

    /**
     * is enable collect partitions heatmap, dynamic, default:true
     */
    public static final String ENABLE_PARTITIONS_HEATMAP_COLLECTION = "ENABLE_PARTITIONS_HEATMAP_COLLECTION";

    /**
     * set schemas and tables of partitions heatmap collect
     * exp: 'schema_01#table1&table12,schema_02#table1' or  'schema_01,schema_02' or ''
     */
    public static final String PARTITIONS_HEATMAP_COLLECTION_ONLY = "PARTITIONS_HEATMAP_COLLECTION_ONLY";

    /**
     * if partitions numbers that has been collected more than PARTITIONS_HEATMAP_COLLECTION_MAX_SCAN, then do not collect others.
     */
    public static final String PARTITIONS_HEATMAP_COLLECTION_MAX_SCAN = "PARTITIONS_HEATMAP_COLLECTION_MAX_SCAN";

    /**
     * if single logic schema count more than PARTITIONS_HEATMAP_COLLECTION_MAX_SINGLE_LOGIC_SCHEMA_COUNT, then do not collect it.
     */
    public static final String PARTITIONS_HEATMAP_COLLECTION_MAX_SINGLE_LOGIC_SCHEMA_COUNT =
        "PARTITIONS_HEATMAP_COLLECTION_MAX_SINGLE_LOGIC_SCHEMA_COUNT";

    /**
     * if partitions numbers more than PARTITIONS_HEATMAP_COLLECTION_MAX_MERGE_NUM, then merge this.
     */
    public static final String PARTITIONS_HEATMAP_COLLECTION_MAX_MERGE_NUM =
        "PARTITIONS_HEATMAP_COLLECTION_MAX_MERGE_NUM";

    /**
     * extreme performance mode
     */
    public static final String ENABLE_EXTREME_PERFORMANCE = "ENABLE_EXTREME_PERFORMANCE";

    public static final String ENABLE_CLEAN_FAILED_PLAN = "ENABLE_CLEAN_FAILED_PLAN";

    /**
     * the min size of IN expr that would be pruned
     */
    public static final String IN_PRUNE_SIZE = "IN_PRUNE_SIZE";

    /**
     * the batch size of IN expr being pruned
     */
    public static final String IN_PRUNE_STEP_SIZE = "IN_PRUNE_STEP_SIZE";

    public static final String IN_PRUNE_MAX_TIME = "IN_PRUNE_MAX_TIME";

    /**
     * the max num of pruning info cache by logical view
     */
    public static final String MAX_IN_PRUNE_CACHE_SIZE = "MAX_IN_PRUNE_CACHE_SIZE";

    /**
     * the max table num of cache pruning info for logical view
     */
    public static final String MAX_IN_PRUNE_CACHE_TABLE_SIZE = "MAX_IN_PRUNE_CACHE_TABLE_SIZE";

    public static final String REBALANCE_TASK_PARALISM = "REBALANCE_TASK_PARALISM";

    /**
     * params for statement summary
     */
    public static final String ENABLE_STATEMENTS_SUMMARY = "ENABLE_STATEMENTS_SUMMARY";
    /**
     * the interval of flush the current statement summary set to the history set.  unit: seconds
     */
    public static final String STATEMENTS_SUMMARY_PERIOD_SEC = "STATEMENTS_SUMMARY_PERIOD_SEC";
    /**
     * the period count which the history contains
     */
    public static final String STATEMENTS_SUMMARY_HISTORY_PERIOD_NUM = "STATEMENTS_SUMMARY_HISTORY_PERIOD_NUM";
    /**
     * the max statement template count which statement summary support.
     */
    public static final String STATEMENTS_SUMMARY_MAX_SQL_TEMPLATE_COUNT = "STATEMENTS_SUMMARY_MAX_SQL_TEMPLATE_COUNT";

    public static final String STATEMENTS_SUMMARY_RECORD_INTERNAL = "STATEMENTS_SUMMARY_RECORD_INTERNAL";

    /**
     * only collect local data when it is false.
     */
    public static final String ENABLE_REMOTE_SYNC_ACTION = "ENABLE_REMOTE_SYNC_ACTION";

    /**
     * the max length of sql sample stored in statement summary.
     */
    public static final String STATEMENTS_SUMMARY_MAX_SQL_LENGTH = "STATEMENTS_SUMMARY_MAX_SQL_LENGTH";

    /**
     * the percent of queries being summarized.
     * when the percent is 0, only slow sql is summarized.
     */
    public static final String STATEMENTS_SUMMARY_PERCENT = "STATEMENTS_SUMMARY_PERCENT";

    public static final String ENABLE_STORAGE_TRIGGER = "enable_storage_trigger";

    public static final String ENABLE_TRANS_LOG = "ENABLE_TRANS_LOG";

    public static final String PLAN_CACHE_EXPIRE_TIME = "PLAN_CACHE_EXPIRE_TIME";

    public static final String SKIP_MOVE_DATABASE_VALIDATOR = "SKIP_MOVE_DATABASE_VALIDATOR";

    public static final String ENABLE_MPP_FILE_STORE_BACKFILL = "ENABLE_MPP_FILE_STORE_BACKFILL";

    public static final String PARTITION_NAME = "PARTITION_NAME";

    public static final String FORBID_REMOTE_DDL_TASK = "FORBID_REMOTE_DDL_TASK";

    public static final String ENABLE_STANDBY_BACKFILL = "ENABLE_STANDBY_BACKFILL";

    public static final String PHYSICAL_DDL_IGNORED_ERROR_CODE = "PHYSICAL_DDL_IGNORED_ERROR_CODE";

    public static final String DDL_PAUSE_DURING_EXCEPTION = "DDL_PAUSE_DURING_EXCEPTION";

    public static final String CHANGE_SET_REPLAY_TIMES = "CHANGE_SET_REPLAY_TIMES";

    public static final String CHANGE_SET_APPLY_BATCH = "CHANGE_SET_APPLY_BATCH";

    public static final String CHANGE_SET_MEMORY_LIMIT = "CHANGE_SET_MEMORY_LIMIT";

    public static final String ENABLE_CHANGESET = "ENABLE_CHANGESET";

    public static final String CN_ENABLE_CHANGESET = "CN_ENABLE_CHANGESET";

    public static final String CHANGE_SET_APPLY_SPEED_LIMITATION = "CHANGE_SET_APPLY_SPEED_LIMITATION";

    public static final String CHANGE_SET_APPLY_SPEED_MIN = "CHANGE_SET_APPLY_SPEED_MIN";

    public static final String CHANGE_SET_APPLY_PARALLELISM = "CHANGE_SET_APPLY_PARALLELISM";

    public static final String CHANGE_SET_APPLY_PHY_PARALLELISM = "CHANGE_SET_APPLY_PHY_PARALLELISM";

    public static final String CHANGE_SET_APPLY_OPTIMIZATION = "CHANGE_SET_APPLY_OPTIMIZATION";

    /**
     * for change set debug
     */
    public static final String SKIP_CHANGE_SET_CHECKER = "SKIP_CHANGE_SET_CHECKER";

    public static final String CHANGE_SET_CHECK_TWICE = "CHANGE_SET_CHECK_TWICE";

    public static final String SKIP_CHANGE_SET = "SKIP_CHANGE_SET";

    public static final String CHANGE_SET_DEBUG_MODE = "CHANGE_SET_DEBUG_MODE";

    public static final String SKIP_CHANGE_SET_APPLY = "SKIP_CHANGE_SET_APPLY";

    public static final String SKIP_CHANGE_SET_FETCH = "SKIP_CHANGE_SET_FETCH";

    public static final String PURGE_OSS_FILE_CRON_EXPR = "PURGE_OSS_FILE_CRON_EXPR";

    public static final String PURGE_OSS_FILE_BEFORE_DAY = "PURGE_OSS_FILE_BEFORE_DAY";

    public static final String BACKUP_OSS_PERIOD = "BACKUP_OSS_PERIOD";

    public static final String FILE_STORAGE_FILES_META_QUERY_PARALLELISM = "FILE_STORAGE_FILES_META_QUERY_PARALLELISM";

    public static final String ENBALE_BIND_PARAM_TYPE = "ENBALE_BIND_PARAM_TYPE";

    public static final String ENBALE_BIND_COLLATE = "ENBALE_BIND_COLLATE";

    public static final String SKIP_TABLEGROUP_VALIDATOR = "SKIP_TABLEGROUP_VALIDATOR";

    /**
     * Enable auto savepoint. If it is TRUE, failed DML statements will be rollbacked automatically.
     */
    public static final String ENABLE_AUTO_SAVEPOINT = "ENABLE_AUTO_SAVEPOINT";

    public static final String CURSOR_FETCH_CONN_MEMORY_LIMIT = "CURSOR_FETCH_CONN_MEMORY_LIMIT";

    public static final String FORCE_RESHARD = "FORCE_RESHARD";

    public static final String REMOVE_DDL_JOB_REDUNDANCY_RELATIONS = "REMOVE_DDL_JOB_REDUNDANCY_RELATIONS";

    public static final String TG_MDL_SEGMENT_SIZE = "TG_MDL_SEGMENT_SIZE";

    public static final String DB_MDL_SEGMENT_SIZE = "DB_MDL_SEGMENT_SIZE";

    public static final String ENABLE_FAST_MOCK = "ENABLE_FAST_MOCK";

    public static final String ENABLE_TRIGGER_DIRECT_INFORMATION_SCHEMA_QUERY =
        "ENABLE_TRIGGER_DIRECT_INFORMATION_SCHEMA_QUERY";

    public static final String ENABLE_LOWER_CASE_TABLE_NAMES = "ENABLE_LOWER_CASE_TABLE_NAMES";

    /**
     * second when ddl plan scheduler wait for polling ddl plan record.
     */
    public static final String DDL_PLAN_SCHEDULER_DELAY = "DDL_PLAN_SCHEDULER_DELAY";

    public static final String USE_PARAMETER_DELEGATE = "USE_PARAMETER_DELEGATE";

    public static final String ENABLE_NODE_HINT_REPLACE = "ENABLE_NODE_HINT_REPLACE";

    public static final String USE_JDK_DEFAULT_SER = "USE_JDK_DEFAULT_SER";

    public static final String OPTIMIZE_TABLE_PARALLELISM = "OPTIMIZE_TABLE_PARALLELISM";

    public static final String OPTIMIZE_TABLE_USE_DAL = "OPTIMIZE_TABLE_USE_DAL";

    /**
     * module conf
     */
    public static final String MAINTENANCE_TIME_START = "MAINTENANCE_TIME_START";

    public static final String MAINTENANCE_TIME_END = "MAINTENANCE_TIME_END";

    public static final String ENABLE_MODULE_LOG = "ENABLE_MODULE_LOG";

    public static final String MAX_MODULE_LOG_PARAMS_SIZE = "MAX_MODULE_LOG_PARAMS_SIZE";

    public static final String MAX_MODULE_LOG_PARAM_SIZE = "MAX_MODULE_LOG_PARAM_SIZE";

    /**
     * speed limit for oss backfill procedure
     */
    public static final String OSS_BACKFILL_SPEED_LIMITATION = "OSS_BACKFILL_SPEED_LIMITATION";

    /**
     * speed lower bound for oss backfill procedure
     */
    public static final String OSS_BACKFILL_SPEED_MIN = "OSS_BACKFILL_SPEED_MIN";

    public static final String ONLY_MANUAL_TABLEGROUP_ALLOW = "ONLY_MANUAL_TABLEGROUP_ALLOW";
    public static final String MANUAL_TABLEGROUP_NOT_ALLOW_AUTO_MATCH = "MANUAL_TABLEGROUP_NOT_ALLOW_AUTO_MATCH";

    public static final String ACQUIRE_CREATE_TABLE_GROUP_LOCK = "ACQUIRE_CREATE_TABLE_GROUP_LOCK";

    public static final String ENABLE_DRUID_FOR_SYNC_CONN = "ENABLE_DRUID_FOR_SYNC_CONN";

    public static final String PASSWORD_CHECK_PATTERN = "PASSWORD_CHECK_PATTERN";

    public static final String DEPRECATE_EOF = "DEPRECATE_EOF";

    public static final String ENABLE_AUTO_SPLIT_PARTITION = "ENABLE_AUTO_SPLIT_PARTITION";

    public static final String ENABLE_FORCE_PRIMARY_FOR_TSO = "ENABLE_FORCE_PRIMARY_FOR_TSO";

    public static final String ENABLE_FORCE_PRIMARY_FOR_FILTER = "ENABLE_FORCE_PRIMARY_FOR_FILTER";

    public static final String ENABLE_FORCE_PRIMARY_FOR_GROUP_BY = "ENABLE_FORCE_PRIMARY_FOR_GROUP_BY";

    /**
     * Whether rollback a branch of XA trx if its primary group is unknown.
     */
    public static final String ROLLBACK_UNKNOWN_PRIMARY_GROUP_XA_TRX = "ROLLBACK_UNKNOWN_PRIMARY_GROUP_XA_TRX";

    public static final String PREFETCH_EXECUTE_POLICY = "PREFETCH_EXECUTE_POLICY";

    public static final String MAX_RECURSIVE_TIME = "MAX_RECURSIVE_COUNT";
    public static final String MAX_RECURSIVE_CTE_MEM_BYTES = "MAX_RECURSIVE_CTE_MEM_BYTES";

    public static final String ENABLE_REPLICA = "ENABLE_REPLICA";

    public static final String GROUPING_LSN_THREAD_NUM = "GROUPING_LSN_THREAD_NUM";

    public static final String GROUPING_LSN_TIMEOUT = "GROUPING_LSN_TIMEOUT";

    public static final String ENABLE_ASYNC_COMMIT = "ENABLE_ASYNC_COMMIT";

    public static final String ENABLE_TRANSACTION_RECOVER_TASK = "ENABLE_TRANSACTION_RECOVER_TASK";

    public static final String ASYNC_COMMIT_TASK_LIMIT = "ASYNC_COMMIT_TASK_LIMIT";

    public static final String ASYNC_COMMIT_PUSH_MAX_SEQ_ONLY_LEADER = "ASYNC_COMMIT_PUSH_MAX_SEQ_ONLY_LEADER";

    public static final String ASYNC_COMMIT_OMIT_PREPARE_TS = "ASYNC_COMMIT_OMIT_PREPARE_TS";

    public static final String ENABLE_SINGLE_SHARD_WRITE = "ENABLE_SINGLE_SHARD_WRITE";

    public static final String ENABLE_FOLLOWER_READ = "ENABLE_FOLLOWER_READ";

    public static final String CREATE_TABLE_WITH_CHARSET_COLLATE = "CREATE_TABLE_WITH_CHARSET_COLLATE";

    public static final String ENABLE_SIMPLIFY_SUBQUERY_SQL = "ENABLE_SIMPLIFY_SUBQUERY_SQL";
    public static final String ENABLE_SIMPLIFY_SHARDING_SQL = "ENABLE_SIMPLIFY_SHARDING_SQL";

    public static final String MAX_PHYSICAL_SLOW_SQL_PARAMS_TO_PRINT = "MAX_PHYSICAL_SLOW_SQL_PARAMS_TO_PRINT";

    public static final String SERVER_ID = "SERVER_ID";

    public static final String ENABLE_REMOTE_CONSUME_LOG = "ENABLE_REMOTE_CONSUME_LOG";

    public static final String REMOTE_CONSUME_LOG_BATCH_SIZE = "REMOTE_CONSUME_LOG_BATCH_SIZE";

    public static final String ENABLE_TRANSACTION_STATISTICS = "ENABLE_TRANSACTION_STATISTICS";

    public static final String SLOW_TRANS_THRESHOLD = "SLOW_TRANS_THRESHOLD";

    public static final String TRANSACTION_STATISTICS_TASK_INTERVAL = "TRANSACTION_STATISTICS_TASK_INTERVAL";

    public static final String IDLE_TRANSACTION_TIMEOUT = "IDLE_TRANSACTION_TIMEOUT";

    public static final String IDLE_WRITE_TRANSACTION_TIMEOUT = "IDLE_WRITE_TRANSACTION_TIMEOUT";

    public static final String IDLE_READONLY_TRANSACTION_TIMEOUT = "IDLE_READONLY_TRANSACTION_TIMEOUT";

    public static final String MAX_CACHED_SLOW_TRANS_STATS = "MAX_CACHED_SLOW_TRANS_STATS";

    public static final String ENABLE_TRX_IDLE_TIMEOUT_TASK = "ENABLE_TRX_IDLE_TIMEOUT_TASK";

    public static final String TRX_IDLE_TIMEOUT_TASK_INTERVAL = "TRX_IDLE_TIMEOUT_TASK_INTERVAL";

    /**
     * -1 mean the learner only allow read, this is the default value;
     */
    public static final String LEARNER_LEVEL = "LEARNER_LEVEL";

    public static final String PLAN_CACHE_SIZE = "PLAN_CACHE_SIZE";

    public static final String ENABLE_X_PROTO_OPT_FOR_AUTO_SP = "ENABLE_X_PROTO_OPT_FOR_AUTO_SP";

    public static final String SIM_CDC_FAILED = "SIM_CDC_FAILED";

    public static final String SKIP_DDL_RESPONSE = "SKIP_DDL_RESPONSE";

    public static final String ENABLE_ROLLBACK_TO_READY = "ENABLE_ROLLBACK_TO_READY";

    public static final String TRX_LOG_CLEAN_PARALLELISM = "TRX_LOG_CLEAN_PARALLELISM";

    public static final String CHECK_RESPONSE_IN_MEM = "CHECK_RESPONSE_IN_MEM";

    public static final String ASYNC_PAUSE = "ASYNC_PAUSE";

    public static final String PHYSICAL_DDL_TASK_RETRY = "PHYSICAL_DDL_TASK_RETRY";

    public static final String ENABLE_2PC_OPT = "ENABLE_2PC_OPT";
}
