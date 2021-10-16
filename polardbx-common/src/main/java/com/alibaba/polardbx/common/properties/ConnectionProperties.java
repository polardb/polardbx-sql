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


public class ConnectionProperties {

    public static final String PLAN_CACHE = "PLAN_CACHE";
    public static final String PREPARE_OPTIMIZE = "PREPARE_OPTIMIZE";

    public static final String ENABLE_RECYCLEBIN = "ENABLE_RECYCLEBIN";

    public static final String SHOW_TABLES_CACHE = "SHOW_TABLES_CACHE";

    public final static String MERGE_CONCURRENT = "MERGE_CONCURRENT";

    public final static String MERGE_UNION = "MERGE_UNION";

    public final static String MERGE_UNION_SIZE = "MERGE_UNION_SIZE";

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

    public static final String MAX_TRX_DURATION = "MAX_TRX_DURATION";

    public static final String EXPLAIN_X_PLAN = "EXPLAIN_X_PLAN";

    public static final String TRANSACTION_ISOLATION = "TRANSACTION_ISOLATION";

    public static final String BLOCK_CONCURRENT = "BLOCK_CONCURRENT";

    public static final String GROUP_CONCURRENT_BLOCK = "GROUP_CONCURRENT_BLOCK";

    public static final String SEQUENTIAL_CONCURRENT_POLICY = "SEQUENTIAL_CONCURRENT_POLICY";

    public static final String FIRST_THEN_CONCURRENT_POLICY = "FIRST_THEN_CONCURRENT_POLICY";

    public static final String DML_EXECUTION_STRATEGY = "DML_EXECUTION_STRATEGY";

    public static final String DML_PUSH_DUPLICATE_CHECK = "DML_PUSH_DUPLICATE_CHECK";

    public static final String DML_SKIP_TRIVIAL_UPDATE = "DML_SKIP_TRIVIAL_UPDATE";

    public static final String DML_SKIP_DUPLICATE_CHECK_FOR_PK = "DML_SKIP_DUPLICATE_CHECK_FOR_PK";

    public static final String DML_SKIP_CRUCIAL_ERR_CHECK = "DML_SKIP_CRUCIAL_ERR_CHECK";

    public static final String DML_USE_RETURNING = "DML_USE_RETURNING";

    public static final String DML_RETURN_IGNORED_COUNT = "DML_RETURN_IGNORED_COUNT";

    public static final String ENABLE_SELF_CROSS_JOIN = "ENABLE_SELF_CROSS_JOIN";

    public static final String ENABLE_COMPATIBLE_DATETIME_ROUNDDOWN = "ENABLE_COMPATIBLE_DATETIME_ROUNDDOWN";

    public static final String ENABLE_COMPATIBLE_TIMESTAMP_ROUNDDOWN = "ENABLE_COMPATIBLE_TIMESTAMP_ROUNDDOWN";

    public static final String BROADCAST_DML = "BROADCAST_DML";

    public static final String SEQUENCE_STEP = "SEQUENCE_STEP";

    public static final String MERGE_DDL_TIMEOUT = "MERGE_DDL_TIMEOUT";

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

    public static final String ENABLE_LOGICAL_INFO_SCHEMA_QUERY = "ENABLE_LOGICAL_INFO_SCHEMA_QUERY";

    public static final String INFO_SCHEMA_QUERY_STAT_BY_GROUP = "INFO_SCHEMA_QUERY_STAT_BY_GROUP";

    public static final String DB_INSTANCE_TYPE = "DB_INSTANCE_TYPE";

    public static final String ALLOW_SIMPLE_SEQUENCE = "ALLOW_SIMPLE_SEQUENCE";

    public static final String PURE_ASYNC_DDL_MODE = "PURE_ASYNC_DDL_MODE";

    public static final String AUTO_ADD_APP_MODE = "AUTO_ADD_APP_MODE";

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

    public static final String GROUP_SEQ_CHECK_INTERVAL = "GROUP_SEQ_CHECK_INTERVAL";

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

    public static final String SEQUENCE_UNIT_COUNT = "SEQUENCE_UNIT_COUNT";

    public static final String SEQUENCE_UNIT_INDEX = "SEQUENCE_UNIT_INDEX";

    public static final String GROUP_CONCAT_MAX_LEN = "GROUP_CONCAT_MAX_LEN";

    public static final String BINLOG_ROWS_QUERY_LOG_EVENTS = "BINLOG_ROWS_QUERY_LOG_EVENTS";

    public static final String BATCH_INSERT_POLICY = "BATCH_INSERT_POLICY";

    public static final String MAX_BATCH_INSERT_SQL_LENGTH = "MAX_BATCH_INSERT_SQL_LENGTH";

    public static final String BATCH_INSERT_CHUNK_SIZE = "BATCH_INSERT_CHUNK_SIZE";

    public static final String INSERT_SELECT_BATCH_SIZE = "INSERT_SELECT_BATCH_SIZE";

    public static final String INSERT_SELECT_LIMIT = "INSERT_SELECT_LIMIT";

    public static final String UPDATE_DELETE_SELECT_BATCH_SIZE = "UPDATE_DELETE_SELECT_BATCH_SIZE";

    public static final String UPDATE_DELETE_SELECT_LIMIT = "UPDATE_DELETE_SELECT_LIMIT";

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

    public static final String BACKGROUND_STATISTIC_COLLECTION_PERIOD = "BACKGROUND_STATISTIC_COLLECTION_PERIOD";

    public static final String BACKGROUND_STATISTIC_COLLECTION_EXPIRE_TIME =
        "BACKGROUND_STATISTIC_COLLECTION_EXPIRE_TIME";

    public static final String STATISTIC_SAMPLE_RATE = "STATISTIC_SAMPLE_RATE";

    public static final String SAMPLE_PERCENTAGE = "SAMPLE_PERCENTAGE";

    public static final String ENABLE_INNODB_BTREE_SAMPLING = "ENABLE_INNODB_BTREE_SAMPLING";

    public static final String HISTOGRAM_MAX_SAMPLE_SIZE = "HISTOGRAM_MAX_SAMPLE_SIZE";

    public static final String AUTO_ANALYZE_ALL_COLUMN_TABLE_LIMIT = "AUTO_ANALYZE_ALL_COLUMN_TABLE_LIMIT";

    public static final String AUTO_ANALYZE_TABLE_SLEEP_MILLS = "AUTO_ANALYZE_TABLE_SLEEP_MILLS";

    public static final String AUTO_ANALYZE_PERIOD_IN_HOURS = "AUTO_ANALYZE_PERIOD_IN_HOURS";

    public static final String HISTOGRAM_BUCKET_SIZE = "HISTOGRAM_BUCKET_SIZE";

    public static final String ANALYZE_TABLE_SPEED_LIMITATION = "ANALYZE_TABLE_SPEED_LIMITATION";

    public static final String ENABLE_SORT_MERGE_JOIN = "ENABLE_SORT_MERGE_JOIN";

    public static final String ENABLE_BKA_JOIN = "ENABLE_BKA_JOIN";

    public static final String ENABLE_BKA_PRUNING = "ENABLE_BKA_PRUNING";

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

    public static final String ENABLE_SEMI_JOIN_REORDER = "ENABLE_SEMI_JOIN_REORDER";

    public static final String ENABLE_OUTER_JOIN_REORDER = "ENABLE_OUTER_JOIN_REORDER";

    public static final String ENABLE_STATISTIC_FEEDBACK = "ENABLE_STATISTIC_FEEDBACK";

    public static final String ENABLE_HASH_AGG = "ENABLE_HASH_AGG";

    public static final String ENABLE_SORT_AGG = "ENABLE_SORT_AGG";

    public static final String ENABLE_PARTIAL_AGG = "ENABLE_PARTIAL_AGG";

    public static final String PARTIAL_AGG_SELECTIVITY_THRESHOLD = "PARTIAL_AGG_SELECTIVITY_THRESHOLD";

    public static final String PARTIAL_AGG_BUCKET_THRESHOLD = "PARTIAL_AGG_BUCKET_THRESHOLD";

    public static final String ENABLE_PUSH_JOIN = "ENABLE_PUSH_JOIN";

    public static final String ENABLE_PUSH_PROJECT = "ENABLE_PUSH_PROJECT";

    public static final String ENABLE_CBO_PUSH_JOIN = "ENABLE_CBO_PUSH_JOIN";

    public static final String ENABLE_PUSH_AGG = "ENABLE_PUSH_AGG";

    public static final String PUSH_AGG_INPUT_ROW_COUNT_THRESHOLD = "PUSH_AGG_INPUT_ROW_COUNT_THRESHOLD";

    public static final String ENABLE_CBO_PUSH_AGG = "ENABLE_CBO_PUSH_AGG";

    public static final String ENABLE_PUSH_SORT = "ENABLE_PUSH_SORT";

    public static final String ENABLE_CBO_GROUP_JOIN = "ENABLE_CBO_GROUP_JOIN";

    public static final String CBO_AGG_JOIN_TRANSPOSE_LIMIT = "CBO_AGG_JOIN_TRANSPOSE_LIMIT";

    public static final String ENABLE_EXPAND_DISTINCTAGG = "ENABLE_EXPAND_DISTINCTAGG";

    public static final String ENABLE_SORT_JOIN_TRANSPOSE = "ENABLE_SORT_JOIN_TRANSPOSE";

    public static final String CBO_JOIN_TABLELOOKUP_TRANSPOSE_LIMIT = "CBO_JOIN_TABLELOOKUP_TRANSPOSE_LIMIT";

    public static final String CBO_START_UP_COST_JOIN_LIMIT = "CBO_START_UP_COST_JOIN_LIMIT";

    public static final String ENABLE_START_UP_COST = "ENABLE_START_UP_COST";

    public static final String JOIN_HINT = "JOIN_HINT";

    public static final String SQL_SIMPLE_MAX_LENGTH = "SQL_SIMPLE_MAX_LENGTH";

    public static final String MASTER = "MASTER";

    public static final String SLAVE = "SLAVE";

    public static final String DDL_ON_GSI = "DDL_ON_GSI";

    public static final String DML_ON_GSI = "DML_ON_GSI";

    public static final String PUSHDOWN_HINT_ON_GSI = "PUSHDOWN_HINT_ON_GSI";

    public static final String STORAGE_CHECK_ON_GSI = "STORAGE_CHECK_ON_GSI";

    public static final String DISTRIBUTED_TRX_REQUIRED = "DISTRIBUTED_TRX_REQUIRED";

    public static final String TRX_CLASS_REQUIRED = "TRX_CLASS_REQUIRED";

    public static final String TSO_OMIT_GLOBAL_TX_LOG = "TSO_OMIT_GLOBAL_TX_LOG";

    public static final String TRUNCATE_TABLE_WITH_GSI = "TRUNCATE_TABLE_WITH_GSI";

    public static final String ALLOW_ADD_GSI = "ALLOW_ADD_GSI";

    public static final String GSI_DEBUG = "GSI_DEBUG";

    public static final String GSI_FINAL_STATUS_DEBUG = "GSI_FINAL_STATUS_DEBUG";

    public static final String REPARTITION_SKIP_CUTOVER = "REPARTITION_SKIP_CUTOVER";

    public static final String REPARTITION_ENABLE_REBUILD_GSI = "REPARTITION_ENABLE_REBUILD_GSI";

    public static final String REPARTITION_SKIP_CLEANUP = "REPARTITION_SKIP_CLEANUP";

    public static final String REPARTITION_FORCE_GSI_NAME = "REPARTITION_FORCE_GSI_NAME";

    public static final String SCALEOUT_BACKFILL_BATCH_SIZE = "SCALEOUT_BACKFILL_BATCH_SIZE";

    public static final String SCALEOUT_BACKFILL_SPEED_LIMITATION = "SCALEOUT_BACKFILL_SPEED_LIMITATION";

    public static final String SCALEOUT_BACKFILL_SPEED_MIN = "SCALEOUT_BACKFILL_SPEED_MIN";

    public static final String SCALEOUT_BACKFILL_PARALLELISM = "SCALEOUT_BACKFILL_PARALLELISM";

    public static final String SCALEOUT_TASK_PARALLELISM = "SCALEOUT_TASK_PARALLELISM";

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

    public static final String SCALEOUT_CHECK_AFTER_BACKFILL = "SCALEOUT_CHECK_AFTER_BACKFILL";

    public static final String SCALEOUT_BACKFILL_USE_FASTCHECKER = "SCALEOUT_BACKFILL_USE_FASTCHECKER";

    public static final String USE_FASTCHECKER = "USE_FASTCHECKER";

    public static final String GSI_BACKFILL_USE_FASTCHECKER = "GSI_BACKFILL_USE_FASTCHECKER";

    public static final String CHECK_GLOBAL_INDEX_USE_FASTCHECKER = "CHECK_GLOBAL_INDEX_USE_FASTCHECKER";

    public static final String FASTCHECKER_RETRY_TIMES = "FASTCHECKER_RETRY_TIMES";

    public static final String FASTCHECKER_LOCK_TIMEOUT = "FASTCHECKER_LOCK_TIMEOUT";

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

    public static final String RELOAD_SCALE_OUT_STATUS_DEBUG = "RELOAD_SCALE_OUT_STATUS_DEBUG";

    public static final String ALLOW_ALTER_GSI_INDIRECTLY = "ALLOW_ALTER_GSI_INDIRECTLY";

    public static final String ALLOW_DROP_OR_MODIFY_PART_UNIQUE_WITH_GSI = "ALLOW_DROP_OR_MODIFY_PART_UNIQUE_WITH_GSI";

    public static final String ALLOW_LOOSE_ALTER_COLUMN_WITH_GSI = "ALLOW_LOOSE_ALTER_COLUMN_WITH_GSI";

    public static final String AUTO_PARTITION = "AUTO_PARTITION";

    public static final String AUTO_PARTITION_PARTITIONS = "AUTO_PARTITION_PARTITIONS";

    public static final String GSI_DEFAULT_CURRENT_TIMESTAMP = "GSI_DEFAULT_CURRENT_TIMESTAMP";

    public static final String GSI_ON_UPDATE_CURRENT_TIMESTAMP = "GSI_ON_UPDATE_CURRENT_TIMESTAMP";

    public static final String GSI_IGNORE_RESTRICTION = "GSI_IGNORE_RESTRICTION";

    public static final String GSI_CHECK_AFTER_CREATION = "GSI_CHECK_AFTER_CREATION";

    public static final String GENERAL_DYNAMIC_SPEED_LIMITATION = "GENERAL_DYNAMIC_SPEED_LIMITATION";

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

    public static final String LOAD_DATA_IGNORE_IS_SIMPLE_INSERT = "LOAD_DATA_IGNORE_IS_SIMPLE_INSERT";

    public static final String LOAD_DATA_HANDLE_EMPTY_CHAR = "LOAD_DATA_HANDLE_EMPTY_CHAR";

    public static final String GSI_CONCURRENT_WRITE = "GSI_CONCURRENT_WRITE";

    public static final String REPLICATE_FILTER_TO_PRIMARY = "REPLICATE_FILTER_TO_PRIMARY";

    public static final String ENABLE_MDL = "ENABLE_MDL";

    public static final String ALWAYS_REBUILD_PLAN = "ALWAYS_REBUILD_PLAN";

    public static final String PARALLELISM = "PARALLELISM";

    public static final String PREFETCH_SHARDS = "PREFETCH_SHARDS";

    public static final String MAX_CACHE_PARAMS = "MAX_CACHE_PARAMS";

    public static final String MAX_EXECUTE_MEMORY = "MAX_EXECUTE_MEMORY";

    public static final String CHUNK_SIZE = "CHUNK_SIZE";

    public static final String ENABLE_EXPRESSION_VECTORIZATION = "ENABLE_EXPRESSION_VECTORIZATION";

    public static final String PLAN_EXTERNALIZE_TEST = "PLAN_EXTERNALIZE_TEST";

    public static final String ENABLE_SPM = "ENABLE_SPM";

    public static final String ENABLE_SPM_BACKGROUND_TASK = "ENABLE_SPM_BACKGROUND_TASK";

    public static final String SPM_MAX_BASELINE_SIZE = "SPM_MAX_BASELINE_SIZE";

    public static final String SPM_MAX_ACCEPTED_PLAN_SIZE_PER_BASELINE = "SPM_MAX_ACCEPTED_PLAN_SIZE_PER_BASELINE";

    public static final String SPM_MAX_UNACCEPTED_PLAN_SIZE_PER_BASELINE = "SPM_MAX_UNACCEPTED_PLAN_SIZE_PER_BASELINE";

    public static final String SPM_EVOLUTION_RATE = "SPM_EVOLUTION_RATE";

    public static final String SPM_MAX_UNACCEPTED_PLAN_EVOLUTION_TIMES = "SPM_MAX_UNACCEPTED_PLAN_EVOLUTION_TIMES";

    public static final String SPM_MAX_BASELINE_INFO_SQL_LENGTH = "SPM_MAX_BASELINE_INFO_SQL_LENGTH";

    public static final String SPM_MAX_PLAN_INFO_PLAN_LENGTH = "SPM_MAX_PLAN_INFO_PLAN_LENGTH";

    public static final String SPM_MAX_PLAN_INFO_ERROR_COUNT = "SPM_MAX_PLAN_INFO_ERROR_COUNT";

    public static final String SPM_RECENTLY_EXECUTED_PERIOD = "SPM_RECENTLY_EXECUTED_PERIOD";

    public static final String EXPLAIN_OUTPUT_FORMAT = "EXPLAIN_OUTPUT_FORMAT";

    public static final String SQL_LOG_MAX_LENGTH = "SQL_LOG_MAX_LENGTH";

    public static final String DNF_REX_NODE_LIMIT = "DNF_REX_NODE_LIMIT";

    public static final String CNF_REX_NODE_LIMIT = "CNF_REX_NODE_LIMIT";

    public static final String REX_MEMORY_LIMIT = "REX_MEMORY_LIMIT";

    public static final String ENABLE_ALTER_SHARD_KEY = "ENABLE_ALTER_SHARD_KEY";

    public static final String USING_RDS_RESULT_SKIP = "USING_RDS_RESULT_SKIP";

    public static final String CONN_TIME_ZONE = "CONN_TIME_ZONE";

    public static final String BLOCK_ENCRYPTION_MODE = "block_encryption_mode";

    public static final String ENABLE_RANDOM_PHY_TABLE_NAME = "ENABLE_RANDOM_PHY_TABLE_NAME";

    public static final String LOGICAL_DDL_PARALLELISM = "LOGICAL_DDL_PARALLELISM";

    public static final String PHYSICAL_DDL_MDL_WAITING_TIMEOUT = "PHYSICAL_DDL_MDL_WAITING_TIMEOUT";

    public static final String MAX_TABLE_PARTITIONS_PER_DB = "MAX_TABLE_PARTITIONS_PER_DB";

    public static final String LOGICAL_DB_TIME_ZONE = "LOGICAL_DB_TIME_ZONE";

    public static final String SHARD_ROUTER_TIME_ZONE = "SHARD_ROUTER_TIME_ZONE";

    public static final String ENABLE_SHARD_CONST_EXPR = "ENABLE_SHARD_CONST_EXPR";

    public static final String FORBID_APPLY_CACHE = "FORBID_APPLY_CACHE";

    public static final String FORCE_APPLY_CACHE = "FORCE_APPLY_CACHE";

    public static final String SKIP_READONLY_CHECK = "SKIP_READONLY_CHECK";

    public static final String WINDOW_FUNC_OPTIMIZE = "WINDOW_FUNC_OPTIMIZE";

    public static final String WINDOW_FUNC_SUBQUERY_CONDITION = "WINDOW_FUNC_SUBQUERY_CONDITION";

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

    public static final String ENABLE_COMPLEX_DML_CROSS_DB = "ENABLE_COMPLEX_DML_CROSS_DB";

    public static final String COMPLEX_DML_WITH_TRX = "COMPLEX_DML_WITH_TRX";

    public static final String ENABLE_INDEX_SELECTION = "ENABLE_INDEX_SELECTION";

    public static final String ENABLE_INDEX_SKYLINE = "ENABLE_INDEX_SKYLINE";

    public static final String ENABLE_MERGE_INDEX = "ENABLE_MERGE_INDEX";

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

    public static final String MPP_LEARNER_DELAY_THRESHOLD = "MPP_LEARNER_DELAY_THRESHOLD";

    public static final String MPP_LEARNER_LOAD_THRESHOLD = "MPP_LEARNER_LOAD_THRESHOLD";

    public static final String ENABLE_AWARE_LEARNER_DELAY = "ENABLE_AWARE_LEARNER_DELAY";

    public static final String ENABLE_AWARE_LEARNER_LOAD = "ENABLE_AWARE_LEARNER_LOAD";

    public static final String MASTER_READ_WEIGHT = "MASTER_READ_WEIGHT";

    public static final String TOPN_SIZE = "TOPN_SIZE";

    public static final String TOPN_MIN_NUM = "TOPN_MIN_NUM";

    public static final String ENABLE_SELECT_INTO_OUTFILE = "ENABLE_SELECT_INTO_OUTFILE";

    public static final String SHOW_HASH_PARTITIONS_BY_RANGE = "SHOW_HASH_PARTITIONS_BY_RANGE";

    public static final String SHOW_TABLE_GROUP_NAME = "SHOW_TABLE_GROUP_NAME";

    public static final String MAX_PHYSICAL_PARTITION_COUNT = "MAX_PHYSICAL_PARTITION_COUNT";

    public static final String MAX_PARTITION_COLUMN_COUNT = "MAX_PARTITION_COLUMN_COUNT";

    public static final String ENABLE_BALANCER = "ENABLE_BALANCER";
    public static final String BALANCER_MAX_PARTITION_SIZE = "BALANCER_MAX_PARTITION_SIZE";
    public static final String BALANCER_WINDOW = "BALANCER_WINDOW";

    public static final String ENABLE_PARTITION_MANAGEMENT = "ENABLE_PARTITION_MANAGEMENT";

    public static final String ENABLE_PARTITION_PRUNING = "ENABLE_PARTITION_PRUNING";

    public static final String ENABLE_AUTO_MERGE_INTERVALS_IN_PRUNING = "ENABLE_AUTO_MERGE_INTERVALS_IN_PRUNING";

    public static final String ENABLE_INTERVAL_ENUMERATION_IN_PRUNING = "ENABLE_INTERVAL_ENUMERATION_IN_PRUNING";

    public static final String PARTITION_PRUNING_STEP_COUNT_LIMIT = "PARTITION_PRUNING_STEP_COUNT_LIMIT";

    public static final String USE_FAST_SINGLE_POINT_INTERVAL_MERGING = "USE_FAST_SINGLE_POINT_INTERVAL_MERGING";

    public static final String ENABLE_CONST_EXPR_EVAL_CACHE = "ENABLE_CONST_EXPR_EVAL_CACHE";

    public static final String MAX_ENUMERABLE_INTERVAL_LENGTH = "MAX_ENUMERABLE_INTERVAL_LENGTH";

    public static final String ENABLE_BRANCH_AND_BOUND_OPTIMIZATION = "ENABLE_BRANCH_AND_BOUND_OPTIMIZATION";

    public static final String ENABLE_BROADCAST_JOIN = "ENABLE_BROADCAST_JOIN";

    public static final String BROADCAST_SHUFFLE_COUNT = "BROADCAST_SHUFFLE_COUNT";

    public static final String BROADCAST_SHUFFLE_PARALLELISM = "BROADCAST_SHUFFLE_PARALLELISM";

    public static final String ENABLE_PASS_THROUGH_TRAIT = "ENABLE_PASS_THROUGH_TRAIT";

    public static final String ENABLE_DERIVE_TRAIT = "ENABLE_DERIVE_TRAIT";

    public static final String ENABLE_SHUFFLE_BY_PARTIAL_KEY = "ENABLE_SHUFFLE_BY_PARTIAL_KEY";

    public static final String ADVISE_TYPE = "ADVISE_TYPE";

    public static final String ENABLE_HLL = "ENABLE_HLL";

    public static final String MINOR_TOLERANCE_VALUE = "MINOR_TOLERANCE_VALUE";

    public static final String STORAGE_HA_TASK_PERIOD = "STORAGE_HA_TASK_PERIOD";

    public static final String STORAGE_HA_SOCKET_TIMEOUT = "STORAGE_HA_SOCKET_TIMEOUT";

    public static final String STORAGE_HA_CONNECT_TIMEOUT = "STORAGE_HA_CONNECT_TIMEOUT";

    public static final String ENABLE_HA_CHECK_TASK_LOG = "ENABLE_HA_CHECK_TASK_LOG";

    public static final String STATISTIC_NDV_SKETCH_EXPIRE_TIME = "STATISTIC_NDV_SKETCH_EXPIRE_TIME";

    public static final String STATISTIC_NDV_SKETCH_QUERY_TIMEOUT = "STATISTIC_NDV_SKETCH_QUERY_TIMEOUT";

    public static final String STATISTIC_NDV_SKETCH_MAX_DIFFERENT_VALUE = "STATISTIC_NDV_SKETCH_MAX_DIFFERENT_VALUE";

    public static final String STATISTIC_NDV_SKETCH_MAX_DIFFERENT_RATIO = "STATISTIC_NDV_SKETCH_MAX_DIFFERENT_RATIO";

    public static final String STATISTIC_NDV_SKETCH_SAMPLE_RATE = "STATISTIC_NDV_SKETCH_SAMPLE_RATE";

    public static final String AUTO_COLLECT_NDV_SKETCH = "AUTO_COLLECT_NDV_SKETCH";

    public static final String CDC_STARTUP_MODE = "CDC_STARTUP_MODE";

    public static final String SHARE_STORAGE_MODE = "SHARE_STORAGE_MODE";

    public static final String SHOW_ALL_PARAMS = "SHOW_ALL_PARAMS";

    public static final String ENABLE_SET_GLOBAL = "ENABLE_SET_GLOBAL";

    public static final String PREEMPTIVE_MDL_INITWAIT = "PREEMPTIVE_MDL_INITWAIT";
    public static final String PREEMPTIVE_MDL_INTERVAL = "PREEMPTIVE_MDL_INTERVAL";

    public static final String FORCE_READ_OUTSIDE_TX = "FORCE_READ_OUTSIDE_TX";
}
