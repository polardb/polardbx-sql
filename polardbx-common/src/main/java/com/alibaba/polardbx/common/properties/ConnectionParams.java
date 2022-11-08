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

import com.alibaba.polardbx.common.TddlConstants;
import com.alibaba.polardbx.common.constants.SequenceAttribute;
import com.alibaba.polardbx.common.constants.TransactionAttribute;
import com.alibaba.polardbx.common.ddl.Attribute;
import com.alibaba.polardbx.common.ddl.newengine.DdlConstants;

import java.util.HashMap;
import java.util.Map;

import static com.alibaba.polardbx.common.ddl.newengine.DdlLocalPartitionConstants.DEFAULT_SCHEDULE_CRON_EXPR;

/**
 * This class contains all definitions of connection param
 *
 * @author vettal
 */
public class ConnectionParams {

    public static final Map<String, ConfigParam> SUPPORTED_PARAMS = new HashMap<>();

    public static final BooleanConfigParam SHOW_TABLES_CACHE = new BooleanConfigParam(
        ConnectionProperties.SHOW_TABLES_CACHE,
        false,
        false);

    public static final BooleanConfigParam SHOW_TABLES_FROM_RULE_ONLY = new BooleanConfigParam(
        ConnectionProperties.SHOW_TABLES_FROM_RULE_ONLY,
        false,
        false);

    /**
     * rule 的兼容性配置开关,仅针对 sharding 表
     * 会影响分库分表键相同的表的 rule 计算过程
     * 5415 默认值由 false 改为 true
     */
    public static final BooleanConfigParam IS_CROSS_RULE = new BooleanConfigParam(
        ConnectionProperties.IS_CROSS_RULE,
        true,
        false);

    /**
     * Check if logical information_schema query is supported.
     */
    public static final BooleanConfigParam ENABLE_LOGICAL_INFO_SCHEMA_QUERY = new BooleanConfigParam(
        ConnectionProperties.ENABLE_LOGICAL_INFO_SCHEMA_QUERY,
        true,
        false);

    public static final BooleanConfigParam INFO_SCHEMA_QUERY_STAT_BY_GROUP = new BooleanConfigParam(
        ConnectionProperties.INFO_SCHEMA_QUERY_STAT_BY_GROUP,
        false,
        false);

    public static final BooleanConfigParam ENABLE_RANDOM_PHY_TABLE_NAME =
        new BooleanConfigParam(ConnectionProperties.ENABLE_RANDOM_PHY_TABLE_NAME,
            true,
            false);

    /**
     * Cache size for New Sequence
     */
    public static final LongConfigParam NEW_SEQ_CACHE_SIZE =
        new LongConfigParam(ConnectionProperties.NEW_SEQ_CACHE_SIZE,
            null,
            null,
            SequenceAttribute.NEW_SEQ_CACHE_SIZE,
            false);

    /**
     * Enable/Disable grouping for New Sequence
     */
    public static final BooleanConfigParam ENABLE_NEW_SEQ_GROUPING =
        new BooleanConfigParam(ConnectionProperties.ENABLE_NEW_SEQ_GROUPING,
            true,
            false);

    /**
     * Grouping timeout for New Sequence
     */
    public static final LongConfigParam NEW_SEQ_GROUPING_TIMEOUT =
        new LongConfigParam(ConnectionProperties.NEW_SEQ_GROUPING_TIMEOUT,
            null,
            null,
            SequenceAttribute.NEW_SEQ_GROUPING_TIMEOUT,
            false);

    /**
     * The number of task queues shared by all New Sequence objects in the same database
     */
    public static final IntConfigParam NEW_SEQ_TASK_QUEUE_NUM_PER_DB =
        new IntConfigParam(ConnectionProperties.NEW_SEQ_TASK_QUEUE_NUM_PER_DB,
            null,
            null,
            SequenceAttribute.NEW_SEQ_TASK_QUEUE_NUM_PER_DB,
            false);

    /**
     * Check if New Sequence value handler should merge requests from different sequences.
     */
    public static final BooleanConfigParam ENABLE_NEW_SEQ_REQUEST_MERGING =
        new BooleanConfigParam(ConnectionProperties.ENABLE_NEW_SEQ_REQUEST_MERGING,
            true,
            false);

    /**
     * Idle time before a value handler is terminated
     */
    public static final LongConfigParam NEW_SEQ_VALUE_HANDLER_KEEP_ALIVE_TIME =
        new LongConfigParam(ConnectionProperties.NEW_SEQ_VALUE_HANDLER_KEEP_ALIVE_TIME,
            null,
            null,
            SequenceAttribute.NEW_SEQ_VALUE_HANDLER_KEEP_ALIVE_TIME,
            false);

    /**
     * Check if simple sequence is allowed to be created. False by default.
     */
    public static final BooleanConfigParam ALLOW_SIMPLE_SEQUENCE =
        new BooleanConfigParam(ConnectionProperties.ALLOW_SIMPLE_SEQUENCE,
            false,
            false);

    /**
     * Step for Group Sequence
     */
    public static final IntConfigParam SEQUENCE_STEP =
        new IntConfigParam(ConnectionProperties.SEQUENCE_STEP,
            null,
            null,
            SequenceAttribute.DEFAULT_INNER_STEP,
            false);

    /**
     * Unit Count for Group Sequence
     */
    public static final IntConfigParam SEQUENCE_UNIT_COUNT =
        new IntConfigParam(ConnectionProperties.SEQUENCE_UNIT_COUNT,
            null,
            SequenceAttribute.UPPER_LIMIT_UNIT_COUNT,
            SequenceAttribute.DEFAULT_UNIT_COUNT,
            false);

    /**
     * Unit Index for Group Sequence
     */
    public static final IntConfigParam SEQUENCE_UNIT_INDEX =
        new IntConfigParam(ConnectionProperties.SEQUENCE_UNIT_INDEX,
            null,
            null,
            SequenceAttribute.DEFAULT_UNIT_INDEX,
            false);

    /**
     * Check if Group Sequence Catcher is enabled
     */
    public static final BooleanConfigParam ENABLE_GROUP_SEQ_CATCHER =
        new BooleanConfigParam(ConnectionProperties.ENABLE_GROUP_SEQ_CATCHER,
            true,
            false);

    /**
     * Check Interval for Group Sequence Catcher
     */
    public static final LongConfigParam GROUP_SEQ_CHECK_INTERVAL =
        new LongConfigParam(ConnectionProperties.GROUP_SEQ_CHECK_INTERVAL,
            null,
            null,
            SequenceAttribute.GROUP_SEQ_UPDATE_INTERVAL,
            false);

    /**
     * Check if asynchronous DDL is supported.
     */
    public static final BooleanConfigParam ENABLE_ASYNC_DDL =
        new BooleanConfigParam(ConnectionProperties.ENABLE_ASYNC_DDL,
            Attribute.DEFAULT_ENABLE_ASYNC_DDL,
            false);

    /**
     * Enable operate subjob
     */
    public static final BooleanConfigParam ENABLE_OPERATE_SUBJOB =
        new BooleanConfigParam(
            ConnectionProperties.ENABLE_OPERATE_SUBJOB,
            false,
            false);

    /**
     * Debug the DDL execution flow.
     */
    public static final StringConfigParam DDL_ENGINE_DEBUG =
        new StringConfigParam(ConnectionProperties.DDL_ENGINE_DEBUG,
            null,
            false);

    /**
     * Print detail information of physical shards during DDL execution for debug.
     */
    public static final BooleanConfigParam DDL_SHARD_CHANGE_DEBUG =
        new BooleanConfigParam(ConnectionProperties.DDL_SHARD_CHANGE_DEBUG,
            false,
            false);

    /**
     * Check if asynchronous DDL is pure.
     */
    public static final BooleanConfigParam PURE_ASYNC_DDL_MODE =
        new BooleanConfigParam(ConnectionProperties.PURE_ASYNC_DDL_MODE,
            Attribute.DEFAULT_PURE_ASYNC_DDL_MODE,
            false);

    /**
     * Check if the "INSTANT ADD COLUMN" feature is supported.
     */
    public static final BooleanConfigParam SUPPORT_INSTANT_ADD_COLUMN =
        new BooleanConfigParam(ConnectionProperties.SUPPORT_INSTANT_ADD_COLUMN,
            false,
            false);

    /**
     * DDL job request timeout.
     */
    public static final IntConfigParam DDL_JOB_REQUEST_TIMEOUT =
        new IntConfigParam(ConnectionProperties.DDL_JOB_REQUEST_TIMEOUT,
            Attribute.MIN_JOB_IDLE_WAITING_TIME,
            null,
            Integer.valueOf(Attribute.JOB_REQUEST_TIMEOUT),
            false);

    /**
     * Indicate that how many logical DDLs are allowed to execute concurrently.
     */
    public static final IntConfigParam LOGICAL_DDL_PARALLELISM =
        new IntConfigParam(ConnectionProperties.LOGICAL_DDL_PARALLELISM,
            Attribute.MIN_LOGICAL_DDL_PARALLELISM,
            Attribute.MAX_LOGICAL_DDL_PARALLELISM,
            Integer.valueOf(Attribute.DEFAULT_LOGICAL_DDL_PARALLELISM),
            false);

    public static final BooleanConfigParam ENABLE_ASYNC_PHY_OBJ_RECORDING =
        new BooleanConfigParam(ConnectionProperties.ENABLE_ASYNC_PHY_OBJ_RECORDING,
            true,
            false);

    /**
     * Physical DDL MDL WAITING TIMEOUT
     */

    public static final IntConfigParam PHYSICAL_DDL_MDL_WAITING_TIMEOUT =
        new IntConfigParam(ConnectionProperties.PHYSICAL_DDL_MDL_WAITING_TIMEOUT,
            DdlConstants.MIN_PHYSICAL_DDL_MDL_WAITING_TIMEOUT,
            DdlConstants.MAX_PHYSICAL_DDL_MDL_WAITING_TIMEOUT,
            Integer.valueOf(DdlConstants.PHYSICAL_DDL_MDL_WAITING_TIMEOUT),
            false);

    public static final IntConfigParam MAX_TABLE_PARTITIONS_PER_DB =
        new IntConfigParam(ConnectionProperties.MAX_TABLE_PARTITIONS_PER_DB,
            DdlConstants.MIN_ALLOWED_TABLE_SHARDS_PER_DB,
            DdlConstants.MAX_ALLOWED_TABLE_SHARDS_PER_DB,
            Integer.valueOf(DdlConstants.DEFAULT_ALLOWED_TABLE_SHARDS_PER_DB),
            false);

    public final static BooleanConfigParam GROUP_CONCURRENT_BLOCK = new BooleanConfigParam(
        ConnectionProperties.GROUP_CONCURRENT_BLOCK,
        true,
        false);

    public final static BooleanConfigParam OSS_FILE_CONCURRENT = new BooleanConfigParam(
        ConnectionProperties.OSS_FILE_CONCURRENT,
        false,
        false);

    /**
     * 是否强制串行执行，默认false
     */
    public final static BooleanConfigParam SEQUENTIAL_CONCURRENT_POLICY = new BooleanConfigParam(
        ConnectionProperties.SEQUENTIAL_CONCURRENT_POLICY,
        false,
        false);

    public final static BooleanConfigParam FIRST_THEN_CONCURRENT_POLICY = new BooleanConfigParam(
        ConnectionProperties.FIRST_THEN_CONCURRENT_POLICY,
        false,
        false);

    public final static StringConfigParam DML_EXECUTION_STRATEGY = new StringConfigParam(
        ConnectionProperties.DML_EXECUTION_STRATEGY,
        null,
        false);

    public final static BooleanConfigParam DML_PUSH_DUPLICATE_CHECK = new BooleanConfigParam(
        ConnectionProperties.DML_PUSH_DUPLICATE_CHECK,
        true,
        false);

    public final static BooleanConfigParam DML_SKIP_TRIVIAL_UPDATE = new BooleanConfigParam(
        ConnectionProperties.DML_SKIP_TRIVIAL_UPDATE,
        true,
        false);

    public final static BooleanConfigParam DML_SKIP_DUPLICATE_CHECK_FOR_PK = new BooleanConfigParam(
        ConnectionProperties.DML_SKIP_DUPLICATE_CHECK_FOR_PK,
        true,
        false);

    public final static BooleanConfigParam DML_SKIP_CRUCIAL_ERR_CHECK = new BooleanConfigParam(
        ConnectionProperties.DML_SKIP_CRUCIAL_ERR_CHECK,
        false,
        false);

    /**
     * 是否允许在 RC 的隔离级别下下推 REPLACE
     */
    public final static BooleanConfigParam DML_FORCE_PUSHDOWN_RC_REPLACE = new BooleanConfigParam(
        ConnectionProperties.DML_FORCE_PUSHDOWN_RC_REPLACE,
        false,
        false);

    /**
     * 是否使用 returning 优化
     */
    public final static BooleanConfigParam DML_USE_RETURNING = new BooleanConfigParam(
        ConnectionProperties.DML_USE_RETURNING,
        true,
        false);

    /**
     * 是否使用 GSI 检查冲突的插入值
     */
    public final static BooleanConfigParam DML_GET_DUP_USING_GSI = new BooleanConfigParam(
        ConnectionProperties.DML_GET_DUP_USING_GSI,
        true,
        false);

    /**
     * DML 检查冲突列时下发 DN 的一条 SQL 所能包含的最大 UNION 数量，<= 0 表示无限制
     */
    public final static IntConfigParam DML_GET_DUP_UNION_SIZE = new IntConfigParam(
        ConnectionProperties.DML_GET_DUP_UNION_SIZE,
        0,
        Integer.MAX_VALUE,
        300,
        true);

    /**
     * DML 检查冲突列时，在允许时是否使用 IN 来代替 UNION；会增加死锁概率
     */
    public final static BooleanConfigParam DML_GET_DUP_USING_IN = new BooleanConfigParam(
        ConnectionProperties.DML_GET_DUP_USING_IN,
        false,
        false);

    /**
     * DML 检查冲突列时下发 DN 的一条 SQL 所能包含的最大 IN 数量，<= 0 表示无限制
     */
    public final static IntConfigParam DML_GET_DUP_IN_SIZE = new IntConfigParam(
        ConnectionProperties.DML_GET_DUP_IN_SIZE,
        0,
        Integer.MAX_VALUE,
        10000,
        true);

    /**
     * 是否使用 duplicated row count 作为 INSERT IGNORE 的 affected rows
     */
    public final static BooleanConfigParam DML_RETURN_IGNORED_COUNT = new BooleanConfigParam(
        ConnectionProperties.DML_RETURN_IGNORED_COUNT,
        false,
        false);

    /**
     * 在 Logical Relocate 时是否跳过没有发生变化的行，不下发任何物理 SQL
     */
    public final static BooleanConfigParam DML_RELOCATE_SKIP_UNCHANGED_ROW = new BooleanConfigParam(
        ConnectionProperties.DML_RELOCATE_SKIP_UNCHANGED_ROW,
        false,
        false);

    /**
     * 在 RelocateWriter 中是否通过 PartitionField 判断拆分键是否变化
     */
    public final static BooleanConfigParam DML_USE_NEW_SK_CHECKER = new BooleanConfigParam(
        ConnectionProperties.DML_USE_NEW_SK_CHECKER,
        true,
        false);

    /**
     * 在 INSERT IGNORE、REPLACE、UPSERT 的时候使用 PartitionField 判断重复值
     */
    public final static BooleanConfigParam DML_USE_NEW_DUP_CHECKER = new BooleanConfigParam(
        ConnectionProperties.DML_USE_NEW_DUP_CHECKER,
        false,
        false);

    /**
     * 是否强制使用 Online Modify Column，即使列类型没有改变，或者不是支持的类型
     */
    public final static BooleanConfigParam OMC_FORCE_TYPE_CONVERSION = new BooleanConfigParam(
        ConnectionProperties.OMC_FORCE_TYPE_CONVERSION,
        false,
        false);

    /**
     * 是否在有 GSI 的表上使用 Online Modify Column
     */
    public final static BooleanConfigParam OMC_ALTER_TABLE_WITH_GSI = new BooleanConfigParam(
        ConnectionProperties.OMC_ALTER_TABLE_WITH_GSI,
        false,
        false);

    /**
     * Online Modify Column 回填时是否使用 returning 优化
     */
    public final static BooleanConfigParam OMC_BACK_FILL_USE_RETURNING = new BooleanConfigParam(
        ConnectionProperties.OMC_BACK_FILL_USE_RETURNING,
        true,
        false);

    /**
     * Online Modify Column 回填后是否进行检查
     */
    public final static BooleanConfigParam OMC_CHECK_AFTER_BACK_FILL = new BooleanConfigParam(
        ConnectionProperties.OMC_CHECK_AFTER_BACK_FILL,
        true,
        false);

    /**
     * Online Modify Column 检查是否使用 Simple Checker（只进行 NULL 值判断）
     */
    public final static BooleanConfigParam OMC_USE_SIMPLE_CHECKER = new BooleanConfigParam(
        ConnectionProperties.OMC_USE_SIMPLE_CHECKER,
        false,
        false);

    /**
     * Online Modify Column 是否跳过回填阶段（只用来 debug）
     */
    public final static BooleanConfigParam OMC_SKIP_BACK_FILL = new BooleanConfigParam(
        ConnectionProperties.OMC_SKIP_BACK_FILL,
        false,
        false);

    /**
     * Merge concurrently, default is false
     */
    public final static BooleanConfigParam MERGE_CONCURRENT = new BooleanConfigParam(
        ConnectionProperties.MERGE_CONCURRENT,
        false,
        false);

    public final static BooleanConfigParam MERGE_UNION = new BooleanConfigParam(ConnectionProperties.MERGE_UNION,
        true,
        false);

    public final static BooleanConfigParam MERGE_DDL_CONCURRENT = new BooleanConfigParam(
        ConnectionProperties.MERGE_DDL_CONCURRENT,
        false,
        false);

    public static final BooleanConfigParam ALLOW_FULL_TABLE_SCAN = new BooleanConfigParam(
        ConnectionProperties.ALLOW_FULL_TABLE_SCAN,
        false,
        false);

    public static final BooleanConfigParam CHOOSE_BROADCAST_WRITE = new BooleanConfigParam(
        ConnectionProperties.CHOOSE_BROADCAST_WRITE,
        true,
        true);

    public static final BooleanConfigParam FORBID_EXECUTE_DML_ALL = new BooleanConfigParam(
        ConnectionProperties.FORBID_EXECUTE_DML_ALL,
        true,
        true);

    public static final LongConfigParam FETCH_SIZE = new LongConfigParam(ConnectionProperties.FETCH_SIZE,
        null,
        null,
        0L,
        true);

    public static final StringConfigParam TRANSACTION_POLICY = new StringConfigParam(
        ConnectionProperties.TRANSACTION_POLICY,
        null,
        true);

    public static final BooleanConfigParam SHARE_READ_VIEW = new BooleanConfigParam(
        ConnectionProperties.SHARE_READ_VIEW,
        true,
        true);

    public static final BooleanConfigParam ENABLE_TRX_SINGLE_SHARD_OPTIMIZATION = new BooleanConfigParam(
        ConnectionProperties.ENABLE_TRX_SINGLE_SHARD_OPTIMIZATION,
        true,
        true);

    public static final BooleanConfigParam ENABLE_TRX_READ_CONN_REUSE = new BooleanConfigParam(
        ConnectionProperties.ENABLE_TRX_READ_CONN_REUSE,
        false,
        true);

    public static final LongConfigParam GET_TSO_TIMEOUT = new LongConfigParam(
        ConnectionProperties.GET_TSO_TIMEOUT,
        null,
        null,
        10000L,
        false);

    public static final LongConfigParam MAX_TRX_DURATION = new LongConfigParam(
        ConnectionProperties.MAX_TRX_DURATION,
        null,
        null,
        28800L,
        false);

    public static final BooleanConfigParam EXPLAIN_X_PLAN = new BooleanConfigParam(
        ConnectionProperties.EXPLAIN_X_PLAN,
        false,
        false);

    public static final LongConfigParam SOCKET_TIMEOUT = new LongConfigParam(ConnectionProperties.SOCKET_TIMEOUT,
        null,
        null,
        -1L,
        false);

    public static final BooleanConfigParam ENABLE_COMPATIBLE_DATETIME_ROUNDDOWN = new BooleanConfigParam(
        ConnectionProperties.ENABLE_COMPATIBLE_DATETIME_ROUNDDOWN,
        false,
        false);

    public static final BooleanConfigParam ENABLE_COMPATIBLE_TIMESTAMP_ROUNDDOWN = new BooleanConfigParam(
        ConnectionProperties.ENABLE_COMPATIBLE_TIMESTAMP_ROUNDDOWN,
        false,
        false);

    public static final LongConfigParam SLOW_SQL_TIME = new LongConfigParam(ConnectionProperties.SLOW_SQL_TIME,
        0L,
        null,
        1000L,
        false);

    public static final LongConfigParam LOAD_DATA_BATCH_INSERT_SIZE =
        new LongConfigParam(ConnectionProperties.LOAD_DATA_BATCH_INSERT_SIZE,
            0L,
            10 * 1024L,
            1 * 1024L,
            true);

    public static final LongConfigParam LOAD_DATA_CACHE_BUFFER_SIZE =
        new LongConfigParam(ConnectionProperties.LOAD_DATA_CACHE_BUFFER_SIZE,
            0L,
            200 * 1024 * 1024L,
            60 * 1024 * 1024L,
            true);

    public static final LongConfigParam SELECT_INTO_OUTFILE_BUFFER_SIZE =
        new LongConfigParam(ConnectionProperties.SELECT_INTO_OUTFILE_BUFFER_SIZE,
            0L,
            200 * 1024 * 1024L,
            1 * 1024 * 1024L,
            true);

    public static final BooleanConfigParam LOAD_DATA_USE_BATCH_MODE =
        new BooleanConfigParam(ConnectionProperties.LOAD_DATA_USE_BATCH_MODE,
            true,
            true);

    public static final StringConfigParam LOAD_DATA_HANDLE_EMPTY_CHAR =
        new StringConfigParam(ConnectionProperties.LOAD_DATA_HANDLE_EMPTY_CHAR,
            PropUtil.LOAD_NULL_MODE.DEFAULT_VALUE_MODE.toString(),
            true);

    public static final BooleanConfigParam LOAD_DATA_IGNORE_IS_SIMPLE_INSERT =
        new BooleanConfigParam(ConnectionProperties.LOAD_DATA_IGNORE_IS_SIMPLE_INSERT,
            true,
            false);

    public static final LongConfigParam MERGE_DDL_TIMEOUT = new LongConfigParam(ConnectionProperties.MERGE_DDL_TIMEOUT,
        0L,
        null,
        0L,
        false);

    public static final LongConfigParam DB_PRIV = new LongConfigParam(ConnectionProperties.DB_PRIV,
        -1L,
        null,
        -1L,
        false);

    public static final LongConfigParam MAX_ALLOWED_PACKET = new LongConfigParam(
        ConnectionProperties.MAX_ALLOWED_PACKET,
        0L,
        null,
        (long) (16 * 1024 * 1024),
        false);

    public static final BooleanConfigParam KILL_CLOSE_STREAM = new BooleanConfigParam(
        ConnectionProperties.KILL_CLOSE_STREAM,
        false,
        false);

    public static final BooleanConfigParam ENABLE_PARAMETERIZED_SQL_LOG = new BooleanConfigParam(
        ConnectionProperties.ENABLE_PARAMETERIZED_SQL_LOG,
        true,
        true);

    public static final LongConfigParam MAX_PARAMETERIZED_SQL_LOG_LENGTH = new LongConfigParam(
        ConnectionProperties.MAX_PARAMETERIZED_SQL_LOG_LENGTH,
        0L,
        null,
        5000L,
        false);

    public static final BooleanConfigParam COLLECT_SQL_ERROR_INFO = new BooleanConfigParam(
        ConnectionProperties.COLLECT_SQL_ERROR_INFO,
        false,
        false);

    public static final IntConfigParam XA_RECOVER_INTERVAL = new IntConfigParam(
        ConnectionProperties.XA_RECOVER_INTERVAL,
        1,
        null,
        TransactionAttribute.XA_RECOVER_INTERVAL,
        false);

    public static final IntConfigParam PURGE_TRANS_INTERVAL = new IntConfigParam(
        ConnectionProperties.PURGE_TRANS_INTERVAL,
        300,
        null,
        TransactionAttribute.TRANSACTION_PURGE_INTERVAL,
        false);

    public static final IntConfigParam PURGE_TRANS_BEFORE = new IntConfigParam(ConnectionProperties.PURGE_TRANS_BEFORE,
        1800,
        null,
        TransactionAttribute.TRANSACTION_PURGE_BEFORE,
        false);

    public static final BooleanConfigParam ENABLE_DEADLOCK_DETECTION = new BooleanConfigParam(
        ConnectionProperties.ENABLE_DEADLOCK_DETECTION,
        true,
        false);

    public static final IntConfigParam TSO_HEARTBEAT_INTERVAL = new IntConfigParam(
        ConnectionProperties.TSO_HEARTBEAT_INTERVAL,
        100,
        null,
        TransactionAttribute.DEFAULT_TSO_HEARTBEAT_INTERVAL,
        false);

    public static final StringConfigParam PURGE_TRANS_START_TIME = new StringConfigParam(
        ConnectionProperties.PURGE_TRANS_START_TIME,
        TransactionAttribute.PURGE_TRANS_START_TIME,
        false);

    public static final BooleanConfigParam ENABLE_RECYCLEBIN = new BooleanConfigParam(
        ConnectionProperties.ENABLE_RECYCLEBIN,
        false,
        true);

    public static void addSupportedParam(ConfigParam param) {
        SUPPORTED_PARAMS.put(param.getName(), param);
    }

    public static final BooleanConfigParam DDL_ON_GSI = new BooleanConfigParam(ConnectionProperties.DDL_ON_GSI,
        false,
        false);

    public static final BooleanConfigParam DML_ON_GSI = new BooleanConfigParam(ConnectionProperties.DML_ON_GSI,
        false,
        false);

    public static final BooleanConfigParam STORAGE_CHECK_ON_GSI = new BooleanConfigParam(
        ConnectionProperties.STORAGE_CHECK_ON_GSI,
        true,
        false);

    public static final BooleanConfigParam DISTRIBUTED_TRX_REQUIRED = new BooleanConfigParam(
        ConnectionProperties.DISTRIBUTED_TRX_REQUIRED,
        false,
        false);

    public static final StringConfigParam TRX_CLASS_REQUIRED = new StringConfigParam(
        ConnectionProperties.TRX_CLASS_REQUIRED,
        null,
        false);

    public static final BooleanConfigParam TSO_OMIT_GLOBAL_TX_LOG = new BooleanConfigParam(
        ConnectionProperties.TSO_OMIT_GLOBAL_TX_LOG,
        false,
        false);

    public static final BooleanConfigParam TRUNCATE_TABLE_WITH_GSI = new BooleanConfigParam(
        ConnectionProperties.TRUNCATE_TABLE_WITH_GSI,
        false,
        false);

    public static final BooleanConfigParam ALLOW_ADD_GSI = new BooleanConfigParam(ConnectionProperties.ALLOW_ADD_GSI,
        true,
        false);

    public static final StringConfigParam GSI_DEBUG = new StringConfigParam(ConnectionProperties.GSI_DEBUG,
        "",
        false);

    /**
     * debug mode on column, including hidden column, column multi-write, etc.
     */
    public static final StringConfigParam COLUMN_DEBUG = new StringConfigParam(ConnectionProperties.COLUMN_DEBUG,
        "",
        false);

    /**
     * debug mode on Global Secondary Index, which makes GSI status change slower etc.
     */
    public static final StringConfigParam GSI_FINAL_STATUS_DEBUG =
        new StringConfigParam(ConnectionProperties.GSI_FINAL_STATUS_DEBUG,
            "",
            false);

    public static final StringConfigParam REPARTITION_SKIP_CUTOVER =
        new StringConfigParam(ConnectionProperties.REPARTITION_SKIP_CUTOVER,
            "",
            false);

    public static final BooleanConfigParam REPARTITION_SKIP_CHECK =
        new BooleanConfigParam(ConnectionProperties.REPARTITION_SKIP_CHECK,
            false,
            false);

    public static final StringConfigParam REPARTITION_ENABLE_REBUILD_GSI =
        new StringConfigParam(ConnectionProperties.REPARTITION_ENABLE_REBUILD_GSI,
            "",
            false);

    public static final StringConfigParam REPARTITION_SKIP_CLEANUP =
        new StringConfigParam(ConnectionProperties.REPARTITION_SKIP_CLEANUP,
            "",
            false);

    public static final StringConfigParam REPARTITION_FORCE_GSI_NAME =
        new StringConfigParam(ConnectionProperties.REPARTITION_FORCE_GSI_NAME,
            "",
            false);

    public static final StringConfigParam SCALE_OUT_DEBUG = new StringConfigParam(ConnectionProperties.SCALE_OUT_DEBUG,
        "",
        false);

    public static final LongConfigParam SCALE_OUT_DEBUG_WAIT_TIME_IN_WO =
        new LongConfigParam(ConnectionProperties.SCALE_OUT_DEBUG_WAIT_TIME_IN_WO,
            0L,
            Long.MAX_VALUE,
            0L,
            false);

    public static final StringConfigParam SCALE_OUT_WRITE_DEBUG =
        new StringConfigParam(ConnectionProperties.SCALE_OUT_WRITE_DEBUG,
            "",
            false);

    public static final StringConfigParam SCALE_OUT_FINAL_TABLE_STATUS_DEBUG =
        new StringConfigParam(ConnectionProperties.SCALE_OUT_FINAL_TABLE_STATUS_DEBUG,
            "",
            false);

    public static final StringConfigParam SCALE_OUT_FINAL_DB_STATUS_DEBUG =
        new StringConfigParam(ConnectionProperties.SCALE_OUT_FINAL_DB_STATUS_DEBUG,
            "",
            false);

    public static final StringConfigParam SCALE_OUT_WRITE_PERFORMANCE_TEST =
        new StringConfigParam(ConnectionProperties.SCALE_OUT_WRITE_PERFORMANCE_TEST,
            "",
            false);

    public static final BooleanConfigParam SCALE_OUT_DROP_DATABASE_AFTER_SWITCH_DATASOURCE =
        new BooleanConfigParam(ConnectionProperties.SCALE_OUT_DROP_DATABASE_AFTER_SWITCH_DATASOURCE,
            true,
            false);

    /**
     * enable parallel physical table backfill
     */
    public static final BooleanConfigParam ENABLE_PHYSICAL_TABLE_PARALLEL_BACKFILL =
        new BooleanConfigParam(ConnectionProperties.ENABLE_PHYSICAL_TABLE_PARALLEL_BACKFILL,
            true,
            false);

    /**
     * enable backfill slide window for split backfill range
     */
    public static final BooleanConfigParam ENABLE_SLIDE_WINDOW_BACKFILL =
        new BooleanConfigParam(ConnectionProperties.ENABLE_SLIDE_WINDOW_BACKFILL,
            true,
            false);

    /**
     * the check time of backfill slide window
     */
    public static final LongConfigParam SLIDE_WINDOW_TIME_INTERVAL =
        new LongConfigParam(ConnectionProperties.SLIDE_WINDOW_TIME_INTERVAL,
            -1L, Long.MAX_VALUE, 60000L, false);

    public static final IntConfigParam SLIDE_WINDOW_SPLIT_SIZE =
        new IntConfigParam(ConnectionProperties.SLIDE_WINDOW_SPLIT_SIZE,
            -1, 64, 4, false);

    public static final IntConfigParam PHYSICAL_TABLE_BACKFILL_PARALLELISM =
        new IntConfigParam(ConnectionProperties.PHYSICAL_TABLE_BACKFILL_PARALLELISM,
            -1, 64, 4, false);

    /**
     * physcial table start split size for backfill
     */
    public static final LongConfigParam PHYSICAL_TABLE_START_SPLIT_SIZE =
        new LongConfigParam(ConnectionProperties.PHYSICAL_TABLE_START_SPLIT_SIZE,
            1L, Long.MAX_VALUE, 1000000L, false);

    /**
     * the parallelism for backfill
     */
    public static final IntConfigParam BACKFILL_PARALLELISM =
        new IntConfigParam(ConnectionProperties.BACKFILL_PARALLELISM,
            -1, 256, -1, false);

    /**
     * max physcial table sample size for backfill
     */
    public static final LongConfigParam BACKFILL_MAX_SAMPLE_SIZE =
        new LongConfigParam(ConnectionProperties.BACKFILL_MAX_SAMPLE_SIZE,
            10000L, Long.MAX_VALUE, 100000L, false);

    /**
     * check target table after scaleout's backfill
     */
    public static final BooleanConfigParam SCALEOUT_CHECK_AFTER_BACKFILL =
        new BooleanConfigParam(ConnectionProperties.SCALEOUT_CHECK_AFTER_BACKFILL,
            true,
            false);

    public static final BooleanConfigParam SCALEOUT_BACKFILL_USE_FASTCHECKER =
        new BooleanConfigParam(ConnectionProperties.SCALEOUT_BACKFILL_USE_FASTCHECKER,
            true,
            false);

    public static final BooleanConfigParam GSI_BACKFILL_USE_FASTCHECKER =
        new BooleanConfigParam(ConnectionProperties.GSI_BACKFILL_USE_FASTCHECKER,
            true,
            false);

    public static final IntConfigParam FASTCHECKER_RETRY_TIMES =
        new IntConfigParam(ConnectionProperties.FASTCHECKER_RETRY_TIMES,
            1, 5, 3, false);

    public static BooleanConfigParam CHECK_GLOBAL_INDEX_USE_FASTCHECKER =
        new BooleanConfigParam(ConnectionProperties.CHECK_GLOBAL_INDEX_USE_FASTCHECKER,
            true,
            false);

    public static final BooleanConfigParam ENABLE_SCALE_OUT_FEATURE =
        new BooleanConfigParam(ConnectionProperties.ENABLE_SCALE_OUT_FEATURE,
            true,
            false);

    public static final BooleanConfigParam ENABLE_SCALE_OUT_ALL_PHY_DML_LOG =
        new BooleanConfigParam(ConnectionProperties.ENABLE_SCALE_OUT_ALL_PHY_DML_LOG,
            false,
            false);

    public static final BooleanConfigParam ENABLE_SCALE_OUT_GROUP_PHY_DML_LOG =
        new BooleanConfigParam(ConnectionProperties.ENABLE_SCALE_OUT_GROUP_PHY_DML_LOG,
            true,
            false);

    public static final LongConfigParam SCALEOUT_BACKFILL_BATCH_SIZE = new LongConfigParam(
        ConnectionProperties.SCALEOUT_BACKFILL_BATCH_SIZE,
        16L,
        4096L,
        1024L,
        false);

    public static final BooleanConfigParam SCALEOUT_DML_PUSHDOWN_OPTIMIZATION =
        new BooleanConfigParam(ConnectionProperties.SCALEOUT_DML_PUSHDOWN_OPTIMIZATION,
            true,
            false);

    public static final IntConfigParam SCALEOUT_DML_PUSHDOWN_BATCH_LIMIT = new IntConfigParam(
        ConnectionProperties.SCALEOUT_DML_PUSHDOWN_BATCH_LIMIT, 0, Integer.MAX_VALUE, 32, true);

    public static final LongConfigParam SCALEOUT_BACKFILL_SPEED_LIMITATION = new LongConfigParam(
        ConnectionProperties.SCALEOUT_BACKFILL_SPEED_LIMITATION,
        -1L,
        Long.MAX_VALUE,
        300000L,
        false);

    public static final LongConfigParam SCALEOUT_BACKFILL_SPEED_MIN = new LongConfigParam(
        ConnectionProperties.SCALEOUT_BACKFILL_SPEED_MIN,
        -1L,
        Long.MAX_VALUE,
        100000L,
        false);

    public static final LongConfigParam SCALEOUT_BACKFILL_PARALLELISM = new LongConfigParam(
        ConnectionProperties.SCALEOUT_BACKFILL_PARALLELISM,
        -1L,
        Long.MAX_VALUE,
        -1L,
        false);

    /**
     * Parallelism of tasks for scale-out
     * Default: Min(NumCpuCores, Max(4, NumStorageNodes * 2))
     * Use automatic calculated value if the parameter is 0,
     * else use specified value from command.
     */
    public static final LongConfigParam SCALEOUT_TASK_PARALLELISM = new LongConfigParam(
        ConnectionProperties.SCALEOUT_TASK_PARALLELISM,
        -1L,
        Long.MAX_VALUE,
        0L,
        false);

    public static final LongConfigParam TABLEGROUP_TASK_PARALLELISM = new LongConfigParam(
        ConnectionProperties.TABLEGROUP_TASK_PARALLELISM,
        -1L, 1024L, 0L, false);

    /**
     * batch size for scaleout check procedure
     */
    public static final LongConfigParam SCALEOUT_CHECK_BATCH_SIZE = new LongConfigParam(
        ConnectionProperties.SCALEOUT_CHECK_BATCH_SIZE,
        16L,
        4096L,
        1024L,
        false);

    public static final LongConfigParam SCALEOUT_CHECK_SPEED_LIMITATION = new LongConfigParam(
        ConnectionProperties.SCALEOUT_CHECK_SPEED_LIMITATION,
        -1L,
        Long.MAX_VALUE,
        150000L,
        false);

    public static final LongConfigParam SCALEOUT_CHECK_SPEED_MIN = new LongConfigParam(
        ConnectionProperties.SCALEOUT_CHECK_SPEED_MIN,
        -1L,
        Long.MAX_VALUE,
        100000L,
        false);

    public static final LongConfigParam SCALEOUT_CHECK_PARALLELISM = new LongConfigParam(
        ConnectionProperties.SCALEOUT_CHECK_PARALLELISM,
        -1L,
        Long.MAX_VALUE,
        -1L,
        false);

    public static final IntConfigParam SCALEOUT_FASTCHECKER_PARALLELISM = new IntConfigParam(
        ConnectionProperties.SCALEOUT_FASTCHECKER_PARALLELISM,
        -1,
        128,
        4,
        false);

    public static final LongConfigParam SCALEOUT_EARLY_FAIL_NUMBER = new LongConfigParam(
        ConnectionProperties.SCALEOUT_EARLY_FAIL_NUMBER,
        100L,
        Long.MAX_VALUE,
        1024L,
        false);

    public static final StringConfigParam SCALEOUT_BACKFILL_POSITION_MARK = new StringConfigParam(
        ConnectionProperties.SCALEOUT_BACKFILL_POSITION_MARK,
        "",
        false);

    public static final BooleanConfigParam ALLOW_DROP_DATABASE_IN_SCALEOUT_PHASE =
        new BooleanConfigParam(ConnectionProperties.ALLOW_DROP_DATABASE_IN_SCALEOUT_PHASE,
            false,
            false);

    /**
     * force execute the drop database when the drop lock of the database is already fetched
     */
    public static final BooleanConfigParam ALLOW_DROP_DATABASE_FORCE =
        new BooleanConfigParam(ConnectionProperties.ALLOW_DROP_DATABASE_FORCE,
            false,
            false);
    /**
     * reload the database/tables status from metadb for debug purpose.
     */
    public static final BooleanConfigParam RELOAD_SCALE_OUT_STATUS_DEBUG =
        new BooleanConfigParam(ConnectionProperties.RELOAD_SCALE_OUT_STATUS_DEBUG,
            false,
            false);

    public static final LongConfigParam SCALEOUT_TASK_RETRY_TIME = new LongConfigParam(
        ConnectionProperties.SCALEOUT_TASK_RETRY_TIME,
        0L,
        Long.MAX_VALUE,
        3L,
        false);

    public static final BooleanConfigParam ALLOW_ALTER_GSI_INDIRECTLY = new BooleanConfigParam(
        ConnectionProperties.ALLOW_ALTER_GSI_INDIRECTLY,
        false,
        false);

    /**
     * allow drop part unique constrain(drop some not all columns in composite unique constrain) in primary table or UGSI.
     */
    public static final BooleanConfigParam ALLOW_DROP_OR_MODIFY_PART_UNIQUE_WITH_GSI = new BooleanConfigParam(
        ConnectionProperties.ALLOW_DROP_OR_MODIFY_PART_UNIQUE_WITH_GSI,
        false,
        false);

    public static final BooleanConfigParam ALLOW_LOOSE_ALTER_COLUMN_WITH_GSI = new BooleanConfigParam(
        ConnectionProperties.ALLOW_LOOSE_ALTER_COLUMN_WITH_GSI,
        false,
        false);

    public static final BooleanConfigParam AUTO_PARTITION = new BooleanConfigParam(
        ConnectionProperties.AUTO_PARTITION,
        false,
        false);

    public static final LongConfigParam AUTO_PARTITION_PARTITIONS = new LongConfigParam(
        ConnectionProperties.AUTO_PARTITION_PARTITIONS,
        2L,
        16384L,
        64L,
        false);

    public static final BooleanConfigParam GSI_DEFAULT_CURRENT_TIMESTAMP = new BooleanConfigParam(
        ConnectionProperties.GSI_DEFAULT_CURRENT_TIMESTAMP,
        true,
        false);

    public static final BooleanConfigParam GSI_ON_UPDATE_CURRENT_TIMESTAMP = new BooleanConfigParam(
        ConnectionProperties.GSI_ON_UPDATE_CURRENT_TIMESTAMP,
        true,
        false);

    public static final BooleanConfigParam GSI_IGNORE_RESTRICTION = new BooleanConfigParam(
        ConnectionProperties.GSI_IGNORE_RESTRICTION,
        false,
        false);

    public static final BooleanConfigParam GSI_CHECK_AFTER_CREATION =
        new BooleanConfigParam(ConnectionProperties.GSI_CHECK_AFTER_CREATION,
            true,
            false);

    public static final LongConfigParam GENERAL_DYNAMIC_SPEED_LIMITATION = new LongConfigParam(
        ConnectionProperties.GENERAL_DYNAMIC_SPEED_LIMITATION,
        -1L,
        Long.MAX_VALUE,
        -1L,
        false);

    /**
     * batch size for oss check data procedure
     */
    public static final LongConfigParam CHECK_OSS_BATCH_SIZE = new LongConfigParam(
        ConnectionProperties.CHECK_OSS_BATCH_SIZE,
        1L,
        8192L,
        4096L,
        false);

    public static final LongConfigParam GSI_BACKFILL_BATCH_SIZE = new LongConfigParam(
        ConnectionProperties.GSI_BACKFILL_BATCH_SIZE,
        16L,
        4096L,
        1024L,
        false);

    public static final LongConfigParam GSI_BACKFILL_SPEED_LIMITATION = new LongConfigParam(
        ConnectionProperties.GSI_BACKFILL_SPEED_LIMITATION,
        -1L,
        Long.MAX_VALUE,
        150000L,
        false);

    public static final LongConfigParam GSI_BACKFILL_SPEED_MIN = new LongConfigParam(
        ConnectionProperties.GSI_BACKFILL_SPEED_MIN,
        -1L,
        Long.MAX_VALUE,
        10000L,
        false);

    public static final LongConfigParam GSI_BACKFILL_PARALLELISM = new LongConfigParam(
        ConnectionProperties.GSI_BACKFILL_PARALLELISM,
        -1L,
        Long.MAX_VALUE,
        -1L,
        false);

    public static final LongConfigParam GSI_CHECK_BATCH_SIZE = new LongConfigParam(
        ConnectionProperties.GSI_CHECK_BATCH_SIZE,
        16L,
        4096L,
        1024L,
        false);

    public static final LongConfigParam GSI_CHECK_SPEED_LIMITATION = new LongConfigParam(
        ConnectionProperties.GSI_CHECK_SPEED_LIMITATION,
        -1L,
        Long.MAX_VALUE,
        150000L,
        false);

    public static final LongConfigParam GSI_CHECK_SPEED_MIN = new LongConfigParam(
        ConnectionProperties.GSI_CHECK_SPEED_MIN,
        -1L,
        Long.MAX_VALUE,
        10000L,
        false);

    public static final LongConfigParam GSI_CHECK_PARALLELISM = new LongConfigParam(
        ConnectionProperties.GSI_CHECK_PARALLELISM,
        -1L,
        Long.MAX_VALUE,
        -1L,
        false);

    public static final LongConfigParam GSI_EARLY_FAIL_NUMBER = new LongConfigParam(
        ConnectionProperties.GSI_EARLY_FAIL_NUMBER,
        100L,
        Long.MAX_VALUE,
        1024L,
        false);

    /**
     * phy table lock timeout for fastchecker, unit: second
     */
    public static final IntConfigParam FASTCHECKER_LOCK_TIMEOUT = new IntConfigParam(
        ConnectionProperties.FASTCHECKER_LOCK_TIMEOUT,
        1,
        Integer.MAX_VALUE,
        10,
        false);

    public static final IntConfigParam GSI_FASTCHECKER_PARALLELISM = new IntConfigParam(
        ConnectionProperties.GSI_FASTCHECKER_PARALLELISM,
        -1,
        128,
        4,
        false);

    public static final StringConfigParam GSI_BACKFILL_POSITION_MARK = new StringConfigParam(
        ConnectionProperties.GSI_BACKFILL_POSITION_MARK,
        "",
        false);

    public static final BooleanConfigParam GSI_CONCURRENT_WRITE_OPTIMIZE =
        new BooleanConfigParam(ConnectionProperties.GSI_CONCURRENT_WRITE_OPTIMIZE,
            true,
            false);

    public static final BooleanConfigParam GSI_CONCURRENT_WRITE =
        new BooleanConfigParam(ConnectionProperties.GSI_CONCURRENT_WRITE,
            false,
            false);

    /**
     * The read/write parallelism of one phy group of auto-mode db
     */
    public static final LongConfigParam GROUP_PARALLELISM = new LongConfigParam(
        ConnectionProperties.GROUP_PARALLELISM,
        1L,
        32L,
        8L,
        false);

    /**
     * the switch of the read/write parallelism of one phy group
     */
    public static final BooleanConfigParam ENABLE_GROUP_PARALLELISM = new BooleanConfigParam(
        ConnectionProperties.ENABLE_GROUP_PARALLELISM,
        false,
        false);

    /**
     * the switch of the read/write parallelism of one phy group
     */
    public static final BooleanConfigParam ENABLE_LOG_GROUP_CONN_KEY = new BooleanConfigParam(
        ConnectionProperties.ENABLE_LOG_GROUP_CONN_KEY,
        false,
        false);

    /**
     * enable MDL
     */
    public static final BooleanConfigParam ENABLE_MDL = new BooleanConfigParam(ConnectionProperties.ENABLE_MDL,
        true,
        false);

    public static final BooleanConfigParam ALWAYS_REBUILD_PLAN =
        new BooleanConfigParam(ConnectionProperties.ALWAYS_REBUILD_PLAN,
            false,
            false);

    public static final BooleanConfigParam STATISTIC_COLLECTOR_FROM_RULE = new BooleanConfigParam(
        ConnectionProperties.STATISTIC_COLLECTOR_FROM_RULE,
        true,
        false);

    public static final BooleanConfigParam REPLICATE_FILTER_TO_PRIMARY = new BooleanConfigParam(
        ConnectionProperties.REPLICATE_FILTER_TO_PRIMARY,
        true,
        false);

    public static final BooleanConfigParam ENABLE_JOIN_CLUSTERING = new BooleanConfigParam(
        ConnectionProperties.ENABLE_JOIN_CLUSTERING, true, true);

    public static final IntConfigParam JOIN_CLUSTERING_CONDITION_PROPAGATION_LIMIT = new IntConfigParam(
        ConnectionProperties.JOIN_CLUSTERING_CONDITION_PROPAGATION_LIMIT, 3, Integer.MAX_VALUE, 7, true);

    public static final BooleanConfigParam ENABLE_JOIN_CLUSTERING_AVOID_CROSS_JOIN = new BooleanConfigParam(
        ConnectionProperties.ENABLE_JOIN_CLUSTERING_AVOID_CROSS_JOIN, true, true);

    public static final BooleanConfigParam ENABLE_BACKGROUND_STATISTIC_COLLECTION = new BooleanConfigParam(
        ConnectionProperties.ENABLE_BACKGROUND_STATISTIC_COLLECTION, true, true);

    public static final StringConfigParam BACKGROUND_STATISTIC_COLLECTION_START_TIME = new StringConfigParam(
        ConnectionProperties.BACKGROUND_STATISTIC_COLLECTION_START_TIME, "02:00", true);

    public static final StringConfigParam BACKGROUND_STATISTIC_COLLECTION_END_TIME = new StringConfigParam(
        ConnectionProperties.BACKGROUND_STATISTIC_COLLECTION_END_TIME, "05:00", true);

    public static final StringConfigParam BACKGROUND_TTL_EXPIRE_END_TIME = new StringConfigParam(
        ConnectionProperties.BACKGROUND_TTL_EXPIRE_END_TIME, "05:00", true);
    public static final IntConfigParam STATISTIC_VISIT_DN_TIMEOUT = new IntConfigParam(
        ConnectionProperties.STATISTIC_VISIT_DN_TIMEOUT, 1, null, 60000, true);

    public static final IntConfigParam BACKGROUND_STATISTIC_COLLECTION_EXPIRE_TIME = new IntConfigParam(
        ConnectionProperties.BACKGROUND_STATISTIC_COLLECTION_EXPIRE_TIME, 1, null, 604800, true);

    public static final FloatConfigParam SAMPLE_PERCENTAGE = new FloatConfigParam(
        ConnectionProperties.SAMPLE_PERCENTAGE, -1f, 100f, -1f, true);

    public static final FloatConfigParam BACKFILL_MAX_SAMPLE_PERCENTAGE = new FloatConfigParam(
        ConnectionProperties.BACKFILL_MAX_SAMPLE_PERCENTAGE, -1f, 100f, 10f, true);

    public static final BooleanConfigParam ENABLE_INNODB_BTREE_SAMPLING = new BooleanConfigParam(
        ConnectionProperties.ENABLE_INNODB_BTREE_SAMPLING, false, true);

    public static final IntConfigParam HISTOGRAM_MAX_SAMPLE_SIZE = new IntConfigParam(
        ConnectionProperties.HISTOGRAM_MAX_SAMPLE_SIZE, 1000, Integer.MAX_VALUE, 100000, true);

    public static final IntConfigParam HISTOGRAM_BUCKET_SIZE = new IntConfigParam(
        ConnectionProperties.HISTOGRAM_BUCKET_SIZE, 1, Integer.MAX_VALUE, 64, true);

    public static final BooleanConfigParam ENABLE_SORT_MERGE_JOIN = new BooleanConfigParam(
        ConnectionProperties.ENABLE_SORT_MERGE_JOIN, true, true);

    public static final BooleanConfigParam ENABLE_BKA_JOIN = new BooleanConfigParam(
        ConnectionProperties.ENABLE_BKA_JOIN, true, true);

    public static final BooleanConfigParam ENABLE_BKA_PRUNING = new BooleanConfigParam(
        ConnectionProperties.ENABLE_BKA_PRUNING, true, true);

    public static final BooleanConfigParam ENABLE_BKA_IN_VALUES_PRUNING = new BooleanConfigParam(
        ConnectionProperties.ENABLE_BKA_IN_VALUES_PRUNING, true, true);

    public static final BooleanConfigParam ENABLE_HASH_JOIN = new BooleanConfigParam(
        ConnectionProperties.ENABLE_HASH_JOIN, true, true);

    public static final BooleanConfigParam FORCE_OUTER_DRIVER_HASH_JOIN = new BooleanConfigParam(
        ConnectionProperties.FORCE_OUTER_DRIVER_HASH_JOIN, false, true);
    public static final BooleanConfigParam FORBID_OUTER_DRIVER_HASH_JOIN = new BooleanConfigParam(
        ConnectionProperties.FORBID_OUTER_DRIVER_HASH_JOIN, false, true);

    public static final BooleanConfigParam ENABLE_NL_JOIN = new BooleanConfigParam(
        ConnectionProperties.ENABLE_NL_JOIN, true, true);

    public static final BooleanConfigParam ENABLE_SEMI_NL_JOIN = new BooleanConfigParam(
        ConnectionProperties.ENABLE_SEMI_NL_JOIN, true, true);

    public static final BooleanConfigParam ENABLE_SEMI_HASH_JOIN = new BooleanConfigParam(
        ConnectionProperties.ENABLE_SEMI_HASH_JOIN, true, true);

    public static final BooleanConfigParam ENABLE_SEMI_BKA_JOIN = new BooleanConfigParam(
        ConnectionProperties.ENABLE_SEMI_BKA_JOIN, true, true);

    public static final BooleanConfigParam ENABLE_SEMI_SORT_MERGE_JOIN = new BooleanConfigParam(
        ConnectionProperties.ENABLE_SEMI_SORT_MERGE_JOIN, true, true);

    public static final BooleanConfigParam ENABLE_MATERIALIZED_SEMI_JOIN = new BooleanConfigParam(
        ConnectionProperties.ENABLE_MATERIALIZED_SEMI_JOIN, true, true);

    public static final BooleanConfigParam ENABLE_MYSQL_HASH_JOIN = new BooleanConfigParam(
        ConnectionProperties.ENABLE_MYSQL_HASH_JOIN, false, true);

    public static final BooleanConfigParam ENABLE_MYSQL_SEMI_HASH_JOIN = new BooleanConfigParam(
        ConnectionProperties.ENABLE_MYSQL_SEMI_HASH_JOIN, true, true);

    public static final IntConfigParam MATERIALIZED_ITEMS_LIMIT = new IntConfigParam(
        ConnectionProperties.MATERIALIZED_ITEMS_LIMIT, 0, Integer.MAX_VALUE, 20000, true);

    public static final BooleanConfigParam ENABLE_SEMI_JOIN_REORDER = new BooleanConfigParam(
        ConnectionProperties.ENABLE_SEMI_JOIN_REORDER, true, true);

    public static final BooleanConfigParam ENABLE_OUTER_JOIN_REORDER = new BooleanConfigParam(
        ConnectionProperties.ENABLE_OUTER_JOIN_REORDER, true, true);

    public static final IntConfigParam CBO_TOO_MANY_JOIN_LIMIT = new IntConfigParam(
        ConnectionProperties.CBO_TOO_MANY_JOIN_LIMIT, 0, null, 14, true);

    public static final IntConfigParam CBO_LEFT_DEEP_TREE_JOIN_LIMIT = new IntConfigParam(
        ConnectionProperties.CBO_LEFT_DEEP_TREE_JOIN_LIMIT, 0, null, 7, true);

    public static final IntConfigParam CBO_ZIG_ZAG_TREE_JOIN_LIMIT = new IntConfigParam(
        ConnectionProperties.CBO_ZIG_ZAG_TREE_JOIN_LIMIT, 0, null, 5, true);

    public static final IntConfigParam CBO_BUSHY_TREE_JOIN_LIMIT = new IntConfigParam(
        ConnectionProperties.CBO_BUSHY_TREE_JOIN_LIMIT, 0, null, 3, true);

    public static final IntConfigParam CBO_JOIN_TABLELOOKUP_TRANSPOSE_LIMIT = new IntConfigParam(
        ConnectionProperties.CBO_JOIN_TABLELOOKUP_TRANSPOSE_LIMIT, 0, null, 1, true);

    public static final IntConfigParam RBO_HEURISTIC_JOIN_REORDER_LIMIT = new IntConfigParam(
        ConnectionProperties.RBO_HEURISTIC_JOIN_REORDER_LIMIT, 0, null, 8, true);

    public static final IntConfigParam MYSQL_JOIN_REORDER_EXHAUSTIVE_DEPTH = new IntConfigParam(
        ConnectionProperties.MYSQL_JOIN_REORDER_EXHAUSTIVE_DEPTH, 0, null, 4, true);

    public static final BooleanConfigParam ENABLE_LV_SUBQUERY_UNWRAP = new BooleanConfigParam(
        ConnectionProperties.ENABLE_LV_SUBQUERY_UNWRAP, true, true);

    public static final BooleanConfigParam ENABLE_FILTER_REORDER = new BooleanConfigParam(
        ConnectionProperties.ENABLE_FILTER_REORDER, true, true);

    public static final BooleanConfigParam ENABLE_STATISTIC_FEEDBACK = new BooleanConfigParam(
        ConnectionProperties.ENABLE_STATISTIC_FEEDBACK, true, true);

    public static final BooleanConfigParam ENABLE_HASH_AGG = new BooleanConfigParam(
        ConnectionProperties.ENABLE_HASH_AGG, true, true);

    public static final BooleanConfigParam ENABLE_SORT_AGG = new BooleanConfigParam(
        ConnectionProperties.ENABLE_SORT_AGG, true, true);

    public static final BooleanConfigParam ENABLE_PARTIAL_AGG = new BooleanConfigParam(
        ConnectionProperties.ENABLE_PARTIAL_AGG, true, true);

    public static final FloatConfigParam PARTIAL_AGG_SELECTIVITY_THRESHOLD = new FloatConfigParam(
        ConnectionProperties.PARTIAL_AGG_SELECTIVITY_THRESHOLD, 0f, 1f, 0.2f, true);

    public static final IntConfigParam PARTIAL_AGG_BUCKET_THRESHOLD = new IntConfigParam(
        ConnectionProperties.PARTIAL_AGG_BUCKET_THRESHOLD, 0, Integer.MAX_VALUE, 64, true);

    public static final IntConfigParam PARALLELISM = new IntConfigParam(
        ConnectionProperties.PARALLELISM, -1, Integer.MAX_VALUE, -1, true);

    public static final IntConfigParam PREFETCH_SHARDS = new IntConfigParam(
        ConnectionProperties.PREFETCH_SHARDS, -1, Integer.MAX_VALUE, -1, true);

    public static final BooleanConfigParam ENABLE_PUSH_PROJECT = new BooleanConfigParam(
        ConnectionProperties.ENABLE_PUSH_PROJECT, true, true);

    public static final BooleanConfigParam ENABLE_PUSH_JOIN = new BooleanConfigParam(
        ConnectionProperties.ENABLE_PUSH_JOIN, true, true);

    public static final BooleanConfigParam ENABLE_CBO_PUSH_JOIN = new BooleanConfigParam(
        ConnectionProperties.ENABLE_CBO_PUSH_JOIN, true, true);

    public static final IntConfigParam CBO_RESTRICT_PUSH_JOIN_LIMIT = new IntConfigParam(
        ConnectionProperties.CBO_RESTRICT_PUSH_JOIN_LIMIT, Integer.MIN_VALUE, Integer.MAX_VALUE, 5, true);

    public static final IntConfigParam CBO_RESTRICT_PUSH_JOIN_COUNT = new IntConfigParam(
        ConnectionProperties.CBO_RESTRICT_PUSH_JOIN_COUNT, 0, Integer.MAX_VALUE, 80, true);

    public static final BooleanConfigParam ENABLE_PUSH_AGG = new BooleanConfigParam(
        ConnectionProperties.ENABLE_PUSH_AGG, true, true);

    public static final IntConfigParam PUSH_AGG_INPUT_ROW_COUNT_THRESHOLD = new IntConfigParam(
        ConnectionProperties.PUSH_AGG_INPUT_ROW_COUNT_THRESHOLD, 0, Integer.MAX_VALUE, 10000, true);

    public static final BooleanConfigParam ENABLE_CBO_PUSH_AGG = new BooleanConfigParam(
        ConnectionProperties.ENABLE_CBO_PUSH_AGG, true, true);

    public static final BooleanConfigParam ENABLE_PUSH_SORT = new BooleanConfigParam(
        ConnectionProperties.ENABLE_PUSH_SORT, true, true);

    public static final BooleanConfigParam ENABLE_CBO_GROUP_JOIN = new BooleanConfigParam(
        ConnectionProperties.ENABLE_CBO_GROUP_JOIN, true, true);

    public static final IntConfigParam CBO_AGG_JOIN_TRANSPOSE_LIMIT = new IntConfigParam(
        ConnectionProperties.CBO_AGG_JOIN_TRANSPOSE_LIMIT, 0, 10, 1, true);

    public static final BooleanConfigParam ENABLE_SORT_JOIN_TRANSPOSE = new BooleanConfigParam(
        ConnectionProperties.ENABLE_SORT_JOIN_TRANSPOSE, true, true);

    public static final BooleanConfigParam ENABLE_EXPAND_DISTINCTAGG = new BooleanConfigParam(
        ConnectionProperties.ENABLE_EXPAND_DISTINCTAGG, true, true);

    public static final BooleanConfigParam ENABLE_START_UP_COST = new BooleanConfigParam(
        ConnectionProperties.ENABLE_START_UP_COST, true, true);

    public static final IntConfigParam CBO_START_UP_COST_JOIN_LIMIT = new IntConfigParam(
        ConnectionProperties.CBO_START_UP_COST_JOIN_LIMIT, 0, null, 5, true);

    public static final IntConfigParam JOIN_BLOCK_SIZE = new IntConfigParam(
        ConnectionProperties.JOIN_BLOCK_SIZE, 10, Integer.MAX_VALUE, 300, true);

    public static final IntConfigParam LOOKUP_JOIN_MAX_BATCH_SIZE = new IntConfigParam(
        ConnectionProperties.LOOKUP_JOIN_MAX_BATCH_SIZE, 10, Integer.MAX_VALUE, 6400, true);

    public static final IntConfigParam LOOKUP_JOIN_MIN_BATCH_SIZE = new IntConfigParam(
        ConnectionProperties.LOOKUP_JOIN_MIN_BATCH_SIZE, 10, 300, 100, false);

    public static final IntConfigParam CHUNK_SIZE = new IntConfigParam(
        ConnectionProperties.CHUNK_SIZE, 10, 100000, 1000, true);

    // sharding advisor parameter
    public static final IntConfigParam SHARDING_ADVISOR_MAX_NODE_NUM = new IntConfigParam(
        ConnectionProperties.SHARDING_ADVISOR_MAX_NODE_NUM,
        0, Integer.MAX_VALUE, 150, true);

    public static final IntConfigParam SHARDING_ADVISOR_APPRO_THRESHOLD = new IntConfigParam(
        ConnectionProperties.SHARDING_ADVISOR_APPRO_THRESHOLD,
        10000, Integer.MAX_VALUE, (int) 5e6, true);

    public static final IntConfigParam SHARDING_ADVISOR_SHARD = new IntConfigParam(
        ConnectionProperties.SHARDING_ADVISOR_SHARD,
        1, Integer.MAX_VALUE, 64, true);

    // the threshold to transform a table to broadcast table
    public static final IntConfigParam SHARDING_ADVISOR_BROADCAST_THRESHOLD = new IntConfigParam(
        ConnectionProperties.SHARDING_ADVISOR_BROADCAST_THRESHOLD,
        -1, Integer.MAX_VALUE, (int) 1e5, true);

    // record all plans in statistics.log
    public static final BooleanConfigParam SHARDING_ADVISOR_RECORD_PLAN = new BooleanConfigParam(
        ConnectionProperties.SHARDING_ADVISOR_RECORD_PLAN,
        false, true);

    // always return the plan generated by sharding advisor
    public static final BooleanConfigParam SHARDING_ADVISOR_RETURN_ANSWER = new BooleanConfigParam(
        ConnectionProperties.SHARDING_ADVISOR_RETURN_ANSWER,
        false, true);

    // SPM Params
    public static final IntConfigParam INDEX_ADVISOR_BROADCAST_THRESHOLD = new IntConfigParam(
        ConnectionProperties.INDEX_ADVISOR_BROADCAST_THRESHOLD,
        0, Integer.MAX_VALUE, 100000, true);

    // SPM Params

    public static final BooleanConfigParam PLAN_EXTERNALIZE_TEST = new BooleanConfigParam(
        ConnectionProperties.PLAN_EXTERNALIZE_TEST,
        false,
        true);

    public static final BooleanConfigParam ENABLE_SPM = new BooleanConfigParam(ConnectionProperties.ENABLE_SPM,
        true,
        true);

    public static final BooleanConfigParam ENABLE_EXPRESSION_VECTORIZATION =
        new BooleanConfigParam(ConnectionProperties.ENABLE_EXPRESSION_VECTORIZATION,
            true,
            true);

    public static final BooleanConfigParam ENABLE_SPM_EVOLUTION_BY_TIME =
        new BooleanConfigParam(ConnectionProperties.ENABLE_SPM_EVOLUTION_BY_TIME,
            false,
            true);

    public static final BooleanConfigParam ENABLE_SPM_BACKGROUND_TASK = new BooleanConfigParam(
        ConnectionProperties.ENABLE_SPM_BACKGROUND_TASK,
        true,
        true);

    public static final EnumConfigParam EXPLAIN_OUTPUT_FORMAT = new EnumConfigParam(
        ConnectionProperties.EXPLAIN_OUTPUT_FORMAT, PropUtil.ExplainOutputFormat.class,
        PropUtil.ExplainOutputFormat.TEXT, true);

    public static final IntConfigParam SPM_MAX_BASELINE_SIZE = new IntConfigParam(
        ConnectionProperties.SPM_MAX_BASELINE_SIZE, 1, 1000, 500, true);

    public static final IntConfigParam SPM_DIFF_ESTIMATE_TIME = new IntConfigParam(
        ConnectionProperties.SPM_DIFF_ESTIMATE_TIME, 100, 100000, 1000, true);

    public static final IntConfigParam SPM_MAX_ACCEPTED_PLAN_SIZE_PER_BASELINE = new IntConfigParam(
        ConnectionProperties.SPM_MAX_ACCEPTED_PLAN_SIZE_PER_BASELINE, 1, 10, 8, true);

    public static final IntConfigParam SPM_MAX_UNACCEPTED_PLAN_SIZE_PER_BASELINE = new IntConfigParam(
        ConnectionProperties.SPM_MAX_UNACCEPTED_PLAN_SIZE_PER_BASELINE, 1, 10, 3, true);

    public static final IntConfigParam SPM_MAX_UNACCEPTED_PLAN_EVOLUTION_TIMES = new IntConfigParam(
        ConnectionProperties.SPM_MAX_UNACCEPTED_PLAN_EVOLUTION_TIMES, 1, 100, 5, true);

    public static final IntConfigParam SPM_MAX_BASELINE_INFO_SQL_LENGTH =
        new IntConfigParam(ConnectionProperties.SPM_MAX_BASELINE_INFO_SQL_LENGTH,
            1,
            100 * 1024 * 1024,
            1024 * 1024,
            true);

    public static final IntConfigParam SPM_MAX_PLAN_INFO_PLAN_LENGTH = new IntConfigParam(
        ConnectionProperties.SPM_MAX_PLAN_INFO_PLAN_LENGTH, 1, 100 * 1024 * 1024, 1024 * 1024, true);

    public static final IntConfigParam SPM_MAX_PLAN_INFO_ERROR_COUNT = new IntConfigParam(
        ConnectionProperties.SPM_MAX_PLAN_INFO_ERROR_COUNT, 1, 1000, 16, true);

    public static final LongConfigParam SPM_RECENTLY_EXECUTED_PERIOD =
        new LongConfigParam(ConnectionProperties.SPM_RECENTLY_EXECUTED_PERIOD,
            0L,
            Long.MAX_VALUE,
            7 * 24 * 60 * 60 * 1000L,
            true);

    /**
     * SPM: max params size for one SQL
     * should being put into the pqo
     */
    public static final IntConfigParam SPM_MAX_PQO_PARAMS_SIZE = new IntConfigParam(
        ConnectionProperties.SPM_MAX_PQO_PARAMS_SIZE, 2, 1000, 10, true);

    public static final BooleanConfigParam SPM_ENABLE_PQO = new BooleanConfigParam(
        ConnectionProperties.SPM_ENABLE_PQO,
        false,
        true);
    // SPM END

    public static final BooleanConfigParam ENABLE_ALTER_SHARD_KEY = new BooleanConfigParam(
        ConnectionProperties.ENABLE_ALTER_SHARD_KEY, false, true);

    public static final LongConfigParam MAX_EXECUTE_MEMORY = new LongConfigParam(
        ConnectionProperties.MAX_EXECUTE_MEMORY,
        1L,
        null,
        TddlConstants.MAX_EXECUTE_MEMORY,
        false);

    public static final BooleanConfigParam FORBID_APPLY_CACHE = new BooleanConfigParam(
        ConnectionProperties.FORBID_APPLY_CACHE, false, true);

    public static final BooleanConfigParam FORCE_APPLY_CACHE = new BooleanConfigParam(
        ConnectionProperties.FORCE_APPLY_CACHE, false, true);

    public static final StringConfigParam BATCH_INSERT_POLICY = new StringConfigParam(
        ConnectionProperties.BATCH_INSERT_POLICY, "SPLIT", true);

    public final static IntConfigParam MERGE_UNION_SIZE = new IntConfigParam(
        ConnectionProperties.MERGE_UNION_SIZE, -1, Integer.MAX_VALUE, -1, true);

    /**
     * The minimum count of union physical sqls in a query.
     * Default value 4 is an empirical value.
     * For mpp, UNION will not be used.
     */
    public final static IntConfigParam MIN_MERGE_UNION_SIZE = new IntConfigParam(
        ConnectionProperties.MIN_MERGE_UNION_SIZE, 1, Integer.MAX_VALUE, 4, true);

    public final static IntConfigParam MAX_MERGE_UNION_SIZE = new IntConfigParam(
        ConnectionProperties.MIN_MERGE_UNION_SIZE, 2, Integer.MAX_VALUE, 8, true);

    /**
     * Param which decide stream mode enabled, default is false.
     */
    public static final BooleanConfigParam CHOOSE_STREAMING = new BooleanConfigParam(
        ConnectionProperties.CHOOSE_STREAMING, false, true);

    public static final BooleanConfigParam ENABLE_MODULE_CHECK =
        new BooleanConfigParam(ConnectionProperties.ENABLE_MODULE_CHECK,
            true,
            true);

    /**
     * The minimum affect rows for hot data for COLD_HOT mode. If less than this value, the sql will be executed
     * again in cold data.
     */
    public static final LongConfigParam COLD_HOT_LIMIT_COUNT = new LongConfigParam(
        ConnectionProperties.COLD_HOT_LIMIT_COUNT, 0L, Long.MAX_VALUE, 0L, true);

    public static final LongConfigParam MAX_UPDATE_NUM_IN_GSI = new LongConfigParam(
        ConnectionProperties.MAX_UPDATE_NUM_IN_GSI, 0L, Long.MAX_VALUE, 10000L, true);

    public static final LongConfigParam MAX_BATCH_INSERT_SQL_LENGTH = new LongConfigParam(
        ConnectionProperties.MAX_BATCH_INSERT_SQL_LENGTH, 0L, Long.MAX_VALUE, 256L, true);

    public static final LongConfigParam BATCH_INSERT_CHUNK_SIZE = new LongConfigParam(
        ConnectionProperties.BATCH_INSERT_CHUNK_SIZE, 0L, Long.MAX_VALUE, 200L, true);

    public static final LongConfigParam INSERT_SELECT_LIMIT =
        new LongConfigParam(ConnectionProperties.INSERT_SELECT_LIMIT, 0L, Long.MAX_VALUE,
            TddlConstants.DML_SELECT_LIMIT_DEFAULT, true);

    public static final LongConfigParam INSERT_SELECT_BATCH_SIZE =
        new LongConfigParam(ConnectionProperties.INSERT_SELECT_BATCH_SIZE, 0L, Long.MAX_VALUE,
            TddlConstants.DML_SELECT_BATCH_SIZE_DEFAULT, true);

    /**
     * Insert/update/delete select执行策略为Insert/update/delete多线程执行
     */
    public static final BooleanConfigParam MODIFY_SELECT_MULTI = new BooleanConfigParam(
        ConnectionProperties.MODIFY_SELECT_MULTI, true, true);

    /**
     * Insert/update/delete select执行策略为select 和 Insert/update/delete 并行
     */
    public static final BooleanConfigParam MODIFY_WHILE_SELECT = new BooleanConfigParam(
        ConnectionProperties.MODIFY_WHILE_SELECT, false, true);

    /**
     * Insert select执行策略为MPP执行
     */
    public static final BooleanConfigParam INSERT_SELECT_MPP = new BooleanConfigParam(
        ConnectionProperties.INSERT_SELECT_MPP, false, true);

    /**
     * Insert select self_table; insert 和 select 操作同一个表时,非事务下可能会导致数据 > 2倍，默认自身表时，先select 再insert
     */
    public static final BooleanConfigParam INSERT_SELECT_SELF_BY_PARALLEL = new BooleanConfigParam(
        ConnectionProperties.INSERT_SELECT_SELF_BY_PARALLEL, false, true);

    /**
     * MODIFY_SELECT_MULTI策略时 逻辑任务执行 的线程个数
     */
    public static final IntConfigParam MODIFY_SELECT_LOGICAL_THREADS = new IntConfigParam(
        ConnectionProperties.MODIFY_SELECT_LOGICAL_THREADS, 0, Integer.MAX_VALUE, 0, true);

    /**
     * MODIFY_SELECT_MULTI策略时 物理任务执行 的线程个数
     */
    public static final IntConfigParam MODIFY_SELECT_PHYSICAL_THREADS = new IntConfigParam(
        ConnectionProperties.MODIFY_SELECT_PHYSICAL_THREADS, 0, Integer.MAX_VALUE, 0, true);

    /**
     * MODIFY_SELECT_MULTI策略时 内存buffer大小，
     */
    public static final LongConfigParam MODIFY_SELECT_BUFFER_SIZE = new LongConfigParam(
        ConnectionProperties.MODIFY_SELECT_BUFFER_SIZE, 64 * 1024L, 256 * 1024 * 1024L,
        64 * 1024 * 1024L, true);

    public static final LongConfigParam MAX_CACHE_PARAMS = new LongConfigParam(ConnectionProperties.MAX_CACHE_PARAMS,
        0L, Long.MAX_VALUE, 10000L, true);

    public static final LongConfigParam PER_QUERY_MEMORY_LIMIT = new LongConfigParam(
        ConnectionProperties.PER_QUERY_MEMORY_LIMIT, 0L, Long.MAX_VALUE, -1L, true);

    public static final BooleanConfigParam BLOCK_CONCURRENT = new BooleanConfigParam(
        ConnectionProperties.BLOCK_CONCURRENT, false, true);

    public static final BooleanConfigParam PLAN_CACHE = new BooleanConfigParam(ConnectionProperties.PLAN_CACHE, true,
        true);

    /**
     * CoronaDB PlanCache
     */
    public static final StringConfigParam MAINTENANCE_TIME_START =
        new StringConfigParam(ConnectionProperties.MAINTENANCE_TIME_START, "02:00", true);

    /**
     * CoronaDB PlanCache
     */
    public static final StringConfigParam MAINTENANCE_TIME_END =
        new StringConfigParam(ConnectionProperties.MAINTENANCE_TIME_END, "05:00", true);

    /**
     * Physical sql template string cache for external sql
     */
    public static final BooleanConfigParam PHY_SQL_TEMPLATE_CACHE =
        new BooleanConfigParam(ConnectionProperties.PHY_SQL_TEMPLATE_CACHE, true,
            true);

    /**
     * Skip readonly check, Manager may do DDL(rename tables) after the servers
     * were set readonly. Default is false.
     */
    public static final BooleanConfigParam SKIP_READONLY_CHECK = new BooleanConfigParam(
        ConnectionProperties.SKIP_READONLY_CHECK, false, true);

    public static final BooleanConfigParam BROADCAST_DML = new BooleanConfigParam(ConnectionProperties.BROADCAST_DML,
        false, true);

    public static final BooleanConfigParam USING_RDS_RESULT_SKIP =
        new BooleanConfigParam(ConnectionProperties.USING_RDS_RESULT_SKIP, false, true);

    public static final BooleanConfigParam WINDOW_FUNC_OPTIMIZE =
        new BooleanConfigParam(ConnectionProperties.WINDOW_FUNC_OPTIMIZE,
            true,
            true);

    public static final BooleanConfigParam WINDOW_FUNC_SUBQUERY_CONDITION =
        new BooleanConfigParam(ConnectionProperties.WINDOW_FUNC_SUBQUERY_CONDITION,
            false,
            true);

    public static final IntConfigParam PUSH_CORRELATE_MATERIALIZED_LIMIT = new IntConfigParam(
        ConnectionProperties.PUSH_CORRELATE_MATERIALIZED_LIMIT, 1, 10000, 500, true);

    public static final BooleanConfigParam WINDOW_FUNC_REORDER_JOIN =
        new BooleanConfigParam(ConnectionProperties.WINDOW_FUNC_REORDER_JOIN,
            false,
            true);

    public static final BooleanConfigParam ENABLE_MPP = new BooleanConfigParam(
        ConnectionProperties.ENABLE_MPP, false, true);

    public static final BooleanConfigParam MPP_RPC_LOCAL_ENABLED = new BooleanConfigParam(
        ConnectionProperties.MPP_RPC_LOCAL_ENABLED, true, true);

    public static final BooleanConfigParam MPP_TASK_LOCAL_BUFFER_ENABLED = new BooleanConfigParam(
        ConnectionProperties.MPP_TASK_LOCAL_BUFFER_ENABLED, true, true);

    public static final BooleanConfigParam MPP_QUERY_PHASED_EXEC_SCHEDULE_ENABLE = new BooleanConfigParam(
        ConnectionProperties.MPP_QUERY_PHASED_EXEC_SCHEDULE_ENABLE, false, true);

    public static final BooleanConfigParam MPP_PARALLELISM_AUTO_ENABLE = new BooleanConfigParam(
        ConnectionProperties.MPP_PARALLELISM_AUTO_ENABLE, false, true);

    public static final BooleanConfigParam MPP_PRINT_ELAPSED_LONG_QUERY_ENABLED = new BooleanConfigParam(
        ConnectionProperties.MPP_PRINT_ELAPSED_LONG_QUERY_ENABLED, false, true);

    public static final LongConfigParam MPP_ELAPSED_QUERY_THRESHOLD_MILLS = new LongConfigParam(
        ConnectionProperties.MPP_ELAPSED_QUERY_THRESHOLD_MILLS, 1000L, Long.MAX_VALUE,
        600000L, true);

    public static final IntConfigParam MPP_MIN_PARALLELISM = new IntConfigParam(
        ConnectionProperties.MPP_MIN_PARALLELISM, 0, 128, -1, true);

    public static final IntConfigParam MPP_MAX_PARALLELISM = new IntConfigParam(
        ConnectionProperties.MPP_MAX_PARALLELISM, 0, 1024, -1, true);

    public static final IntConfigParam MPP_QUERY_ROWS_PER_PARTITION = new IntConfigParam(
        ConnectionProperties.MPP_QUERY_ROWS_PER_PARTITION, 1, Integer.MAX_VALUE, 150000, true);

    public static final IntConfigParam MPP_QUERY_IO_PER_PARTITION = new IntConfigParam(
        ConnectionProperties.MPP_QUERY_ROWS_PER_PARTITION, 1, Integer.MAX_VALUE, 5000, true);

    public static final IntConfigParam LOOKUP_JOIN_PARALLELISM_FACTOR = new IntConfigParam(
        ConnectionProperties.LOOKUP_JOIN_PARALLELISM_FACTOR, 1, 1024, 4, true);

    public static final IntConfigParam MPP_SCHEDULE_MAX_SPLITS_PER_NODE = new IntConfigParam(
        ConnectionProperties.MPP_SCHEDULE_MAX_SPLITS_PER_NODE, 0, Integer.MAX_VALUE, 0, true);

    public static final LongConfigParam MPP_JOIN_BROADCAST_NUM = new LongConfigParam(
        ConnectionProperties.MPP_JOIN_BROADCAST_NUM, -1L, Long.MAX_VALUE, 100L, true);

    public static final LongConfigParam MPP_QUERY_MAX_RUN_TIME = new LongConfigParam(
        ConnectionProperties.MPP_QUERY_MAX_RUN_TIME, 10000L, 7 * 24 * 3600 * 1000L,
        24 * 3600 * 1000L, true);

    public static final IntConfigParam MPP_PARALLELISM = new IntConfigParam(
        ConnectionProperties.MPP_PARALLELISM, 1, Integer.MAX_VALUE, -1, true);

    public static final IntConfigParam DATABASE_PARALLELISM = new IntConfigParam(
        ConnectionProperties.DATABASE_PARALLELISM, 0, 128, 0, true);

    public static final IntConfigParam POLARDBX_PARALLELISM = new IntConfigParam(
        ConnectionProperties.POLARDBX_PARALLELISM, 0, 128, -1, true);

    public static final BooleanConfigParam POLARDBX_SLAVE_INSTANCE_FIRST = new BooleanConfigParam(
        ConnectionProperties.POLARDBX_SLAVE_INSTANCE_FIRST, true, true);

    public static final IntConfigParam MPP_METRIC_LEVEL = new IntConfigParam(
        ConnectionProperties.MPP_METRIC_LEVEL, 0, 3, 3, true);

    public static final BooleanConfigParam MPP_QUERY_NEED_RESERVE = new BooleanConfigParam(
        ConnectionProperties.MPP_QUERY_NEED_RESERVE, false, true);

    public static final LongConfigParam MPP_TASK_LOCAL_MAX_BUFFER_SIZE = new LongConfigParam(
        ConnectionProperties.MPP_TASK_LOCAL_MAX_BUFFER_SIZE, 1000000L, Long.MAX_VALUE,
        8000000L, true);

    public static final LongConfigParam MPP_OUTPUT_MAX_BUFFER_SIZE = new LongConfigParam(
        ConnectionProperties.MPP_OUTPUT_MAX_BUFFER_SIZE, 1000000L, Long.MAX_VALUE,
        32000000L, true);

    public static final IntConfigParam MPP_TABLESCAN_CONNECTION_STRATEGY = new IntConfigParam(
        ConnectionProperties.MPP_TABLESCAN_CONNECTION_STRATEGY, 0, 4, 0, true);

    public static final BooleanConfigParam ENABLE_MODIFY_SHARDING_COLUMN = new BooleanConfigParam(
        ConnectionProperties.ENABLE_MODIFY_SHARDING_COLUMN,
        true,
        true);

    public static final BooleanConfigParam ENABLE_COMPLEX_DML_CROSS_DB = new BooleanConfigParam(
        ConnectionProperties.ENABLE_COMPLEX_DML_CROSS_DB,
        true,
        true);

    public static final BooleanConfigParam COMPLEX_DML_WITH_TRX = new BooleanConfigParam(
        ConnectionProperties.COMPLEX_DML_WITH_TRX,
        false,
        true);

    public static final BooleanConfigParam ENABLE_INDEX_SELECTION = new BooleanConfigParam(
        ConnectionProperties.ENABLE_INDEX_SELECTION,
        true,
        true);

    public static final BooleanConfigParam ENABLE_INDEX_SKYLINE = new BooleanConfigParam(
        ConnectionProperties.ENABLE_INDEX_SKYLINE,
        false,
        true);

    public static final BooleanConfigParam ENABLE_MERGE_INDEX = new BooleanConfigParam(
        ConnectionProperties.ENABLE_MERGE_INDEX,
        true,
        true);

    public static final LongConfigParam UPDATE_DELETE_SELECT_BATCH_SIZE = new LongConfigParam(
        ConnectionProperties.UPDATE_DELETE_SELECT_BATCH_SIZE,
        0L,
        null,
        TddlConstants.DML_SELECT_BATCH_SIZE_DEFAULT,
        true);

    public static final LongConfigParam UPDATE_DELETE_SELECT_LIMIT = new LongConfigParam(
        ConnectionProperties.UPDATE_DELETE_SELECT_LIMIT,
        0L,
        null,
        TddlConstants.DML_SELECT_LIMIT_DEFAULT,
        true);

    public static final LongConfigParam PL_MEMORY_LIMIT = new LongConfigParam(
        ConnectionProperties.PL_MEMORY_LIMIT,
        0L,
        null,
        (long) (1024 * 1024 * 1024),
        true
    );

    public static final LongConfigParam PL_CURSOR_MEMORY_LIMIT = new LongConfigParam(
        ConnectionProperties.PL_CURSOR_MEMORY_LIMIT,
        0L,
        null,
        (long) (10 * 1024 * 1024),
        true
    );

    public static final BooleanConfigParam ENABLE_UDF = new BooleanConfigParam(
        ConnectionProperties.ENABLE_UDF,
        true,
        true
    );

    public static final LongConfigParam PL_INTERNAL_CACHE_SIZE = new LongConfigParam(
        ConnectionProperties.PL_INTERNAL_CACHE_SIZE,
        0L,
        null,
        (long) 10,
        true
    );

    public static final LongConfigParam MAX_PL_DEPTH = new LongConfigParam(
        ConnectionProperties.MAX_PL_DEPTH,
        0L,
        null,
        (long) 4096,
        true
    );


    /**
     * switch groupKey between sourceGroupKey and targetGroupKey
     */
    public static final BooleanConfigParam SWITCH_GROUP_ONLY = new BooleanConfigParam(
        ConnectionProperties.SWITCH_GROUP_ONLY,
        false,
        false);

    public static final BooleanConfigParam ENABLE_POST_PLANNER = new BooleanConfigParam(
        ConnectionProperties.ENABLE_POST_PLANNER,
        true,
        true);

    public static final BooleanConfigParam ENABLE_BROADCAST_RANDOM_READ = new BooleanConfigParam(
        ConnectionProperties.ENABLE_BROADCAST_RANDOM_READ,
        true,
        true
    );

    public static final BooleanConfigParam ENABLE_DIRECT_PLAN = new BooleanConfigParam(
        ConnectionProperties.ENABLE_DIRECT_PLAN,
        true,
        true);

    public static final BooleanConfigParam ENABLE_SPILL = new BooleanConfigParam(
        ConnectionProperties.ENABLE_SPILL, false, true);

    public static final IntConfigParam HYBRID_HASH_JOIN_BUCKET_NUM = new IntConfigParam(
        ConnectionProperties.HYBRID_HASH_JOIN_BUCKET_NUM, 1, Integer.MAX_VALUE,
        4, true);

    public static final IntConfigParam HYBRID_HASH_JOIN_RECURSIVE_BUCKET_NUM = new IntConfigParam(
        ConnectionProperties.HYBRID_HASH_JOIN_RECURSIVE_BUCKET_NUM, 1, Integer.MAX_VALUE,
        4, true);

    public static final IntConfigParam HYBRID_HASH_JOIN_MAX_RECURSIVE_DEPTH = new IntConfigParam(
        ConnectionProperties.HYBRID_HASH_JOIN_MAX_RECURSIVE_DEPTH, 1, Integer.MAX_VALUE,
        3, true);

    public static final BooleanConfigParam ENABLE_PARAMETER_PLAN = new BooleanConfigParam(
        ConnectionProperties.ENABLE_PARAMETER_PLAN, true, true);

    public static final BooleanConfigParam ENABLE_CROSS_VIEW_OPTIMIZE = new BooleanConfigParam(
        ConnectionProperties.ENABLE_CROSS_VIEW_OPTIMIZE, true, true);

    public final static StringConfigParam CONN_POOL_PROPERTIES = new StringConfigParam(
        ConnectionProperties.CONN_POOL_PROPERTIES,
        "connectTimeout=5000;characterEncoding=utf8;autoReconnect=true;failOverReadOnly=false;socketTimeout=900000;rewriteBatchedStatements=true;useServerPrepStmts=false;useSSL=false;strictKeepAlive=true;",
        true);

    public static final IntConfigParam CONN_POOL_MIN_POOL_SIZE = new IntConfigParam(
        ConnectionProperties.CONN_POOL_MIN_POOL_SIZE, 0, Integer.MAX_VALUE,
        5, true);

    public static final IntConfigParam CONN_POOL_MAX_POOL_SIZE = new IntConfigParam(
        ConnectionProperties.CONN_POOL_MAX_POOL_SIZE, 1, Integer.MAX_VALUE,
        60, true);

    public static final IntConfigParam CONN_POOL_MAX_WAIT_THREAD_COUNT = new IntConfigParam(
        ConnectionProperties.CONN_POOL_MAX_WAIT_THREAD_COUNT, -1, Integer.MAX_VALUE,
        0, true);

    public static final IntConfigParam CONN_POOL_IDLE_TIMEOUT = new IntConfigParam(
        ConnectionProperties.CONN_POOL_IDLE_TIMEOUT, 1, Integer.MAX_VALUE,
        60, true);

    public static final IntConfigParam CONN_POOL_BLOCK_TIMEOUT = new IntConfigParam(
        ConnectionProperties.CONN_POOL_BLOCK_TIMEOUT, 1, Integer.MAX_VALUE,
        5000, true);

    public static final StringConfigParam CONN_POOL_XPROTO_CONFIG = new StringConfigParam(
        ConnectionProperties.CONN_POOL_XPROTO_CONFIG,
        "",
        true);

    public static final LongConfigParam CONN_POOL_XPROTO_FLAG = new LongConfigParam(
        ConnectionProperties.CONN_POOL_XPROTO_FLAG, 0L, Long.MAX_VALUE,
        0L, true);

    public static final IntConfigParam CONN_POOL_XPROTO_META_DB_PORT = new IntConfigParam(
        ConnectionProperties.CONN_POOL_XPROTO_META_DB_PORT, -1, Integer.MAX_VALUE,
        0, true);

    public static final IntConfigParam CONN_POOL_XPROTO_STORAGE_DB_PORT = new IntConfigParam(
        ConnectionProperties.CONN_POOL_XPROTO_STORAGE_DB_PORT, -1, Integer.MAX_VALUE,
        0, true);

    public static final IntConfigParam CONN_POOL_XPROTO_MAX_CLIENT_PER_INST = new IntConfigParam(
        ConnectionProperties.CONN_POOL_XPROTO_MAX_CLIENT_PER_INST, 1, Integer.MAX_VALUE,
        32, true);

    public static final IntConfigParam CONN_POOL_XPROTO_MAX_SESSION_PER_CLIENT = new IntConfigParam(
        ConnectionProperties.CONN_POOL_XPROTO_MAX_SESSION_PER_CLIENT, 1, Integer.MAX_VALUE,
        1024, true);

    public static final IntConfigParam CONN_POOL_XPROTO_MAX_POOLED_SESSION_PER_INST = new IntConfigParam(
        ConnectionProperties.CONN_POOL_XPROTO_MAX_POOLED_SESSION_PER_INST, 1, Integer.MAX_VALUE,
        512, true);

    public static final IntConfigParam CONN_POOL_XPROTO_MIN_POOLED_SESSION_PER_INST = new IntConfigParam(
        ConnectionProperties.CONN_POOL_XPROTO_MIN_POOLED_SESSION_PER_INST, 0, Integer.MAX_VALUE,
        32, true);

    public static final LongConfigParam CONN_POOL_XPROTO_SESSION_AGING_TIME = new LongConfigParam(
        ConnectionProperties.CONN_POOL_XPROTO_SESSION_AGING_TIME, 1L, Long.MAX_VALUE,
        600 * 1000L, true);

    public static final LongConfigParam CONN_POOL_XPROTO_SLOW_THRESH = new LongConfigParam(
        ConnectionProperties.CONN_POOL_XPROTO_SLOW_THRESH, 0L, Long.MAX_VALUE,
        1000L, true);

    public static final BooleanConfigParam CONN_POOL_XPROTO_AUTH = new BooleanConfigParam(
        ConnectionProperties.CONN_POOL_XPROTO_AUTH, true, true);

    public static final BooleanConfigParam CONN_POOL_XPROTO_AUTO_COMMIT_OPTIMIZE = new BooleanConfigParam(
        ConnectionProperties.CONN_POOL_XPROTO_AUTO_COMMIT_OPTIMIZE, true, true);

    public static final BooleanConfigParam CONN_POOL_XPROTO_XPLAN = new BooleanConfigParam(
        ConnectionProperties.CONN_POOL_XPROTO_XPLAN, true, true);

    public static final BooleanConfigParam CONN_POOL_XPROTO_XPLAN_EXPEND_STAR = new BooleanConfigParam(
        ConnectionProperties.CONN_POOL_XPROTO_XPLAN_EXPEND_STAR, true, true);

    public static final BooleanConfigParam CONN_POOL_XPROTO_XPLAN_TABLE_SCAN = new BooleanConfigParam(
        ConnectionProperties.CONN_POOL_XPROTO_XPLAN_TABLE_SCAN, false, true);

    public static final BooleanConfigParam CONN_POOL_XPROTO_TRX_LEAK_CHECK = new BooleanConfigParam(
        ConnectionProperties.CONN_POOL_XPROTO_TRX_LEAK_CHECK, false, true);

    public static final BooleanConfigParam CONN_POOL_XPROTO_MESSAGE_TIMESTAMP = new BooleanConfigParam(
        ConnectionProperties.CONN_POOL_XPROTO_MESSAGE_TIMESTAMP, true, true);

    public static final BooleanConfigParam CONN_POOL_XPROTO_PLAN_CACHE = new BooleanConfigParam(
        ConnectionProperties.CONN_POOL_XPROTO_PLAN_CACHE, true, true);

    public static final BooleanConfigParam CONN_POOL_XPROTO_CHUNK_RESULT = new BooleanConfigParam(
        ConnectionProperties.CONN_POOL_XPROTO_CHUNK_RESULT, true, true);

    public static final BooleanConfigParam CONN_POOL_XPROTO_PURE_ASYNC_MPP = new BooleanConfigParam(
        ConnectionProperties.CONN_POOL_XPROTO_PURE_ASYNC_MPP, true, true);

    public static final BooleanConfigParam CONN_POOL_XPROTO_CHECKER = new BooleanConfigParam(
        ConnectionProperties.CONN_POOL_XPROTO_CHECKER, true, true);

    public static final BooleanConfigParam CONN_POOL_XPROTO_DIRECT_WRITE = new BooleanConfigParam(
        ConnectionProperties.CONN_POOL_XPROTO_DIRECT_WRITE, false, true);

    public static final BooleanConfigParam CONN_POOL_XPROTO_FEEDBACK = new BooleanConfigParam(
        ConnectionProperties.CONN_POOL_XPROTO_FEEDBACK, true, true);

    public static final LongConfigParam CONN_POOL_XPROTO_MAX_PACKET_SIZE = new LongConfigParam(
        ConnectionProperties.CONN_POOL_XPROTO_MAX_PACKET_SIZE, 0L, Long.MAX_VALUE,
        67108864L, true);

    public static final IntConfigParam CONN_POOL_XPROTO_QUERY_TOKEN = new IntConfigParam(
        ConnectionProperties.CONN_POOL_XPROTO_QUERY_TOKEN, 1, Integer.MAX_VALUE,
        10000, true);

    public static final LongConfigParam CONN_POOL_XPROTO_PIPE_BUFFER_SIZE = new LongConfigParam(
        ConnectionProperties.CONN_POOL_XPROTO_PIPE_BUFFER_SIZE, 1L, Long.MAX_VALUE,
        256 * 1024 * 1024L, true);

    public static final LongConfigParam XPROTO_MAX_DN_CONCURRENT = new LongConfigParam(
        ConnectionProperties.XPROTO_MAX_DN_CONCURRENT, 1L, Long.MAX_VALUE,
        500L, true);

    public static final LongConfigParam XPROTO_MAX_DN_WAIT_CONNECTION = new LongConfigParam(
        ConnectionProperties.XPROTO_MAX_DN_WAIT_CONNECTION, 1L, Long.MAX_VALUE,
        32L, true);

    /**
     * X-Protocol always keep upper filter when use XPlan. (default true)
     */
    public static final BooleanConfigParam XPROTO_ALWAYS_KEEP_FILTER_ON_XPLAN_GET = new BooleanConfigParam(
        ConnectionProperties.XPROTO_ALWAYS_KEEP_FILTER_ON_XPLAN_GET, true, true);

    /**
     * x-protocol probe timeout(ms).
     */
    public static final IntConfigParam XPROTO_PROBE_TIMEOUT = new IntConfigParam(
        ConnectionProperties.XPROTO_PROBE_TIMEOUT, 0, Integer.MAX_VALUE,
        5000, true);

    /**
     * Galaxy prepare config. (default false)
     */
    public static final BooleanConfigParam XPROTO_GALAXY_PREPARE = new BooleanConfigParam(
        ConnectionProperties.XPROTO_GALAXY_PREPARE, false, true);

    public static final StringConfigParam PUSH_POLICY = new StringConfigParam(ConnectionProperties.PUSH_POLICY,
        null,
        false);

    public static final BooleanConfigParam ENABLE_AGG_PRUNING = new BooleanConfigParam(
        ConnectionProperties.ENABLE_AGG_PRUNING, true, true);

    public static class ConnectionParamValues {
        public final static String PUSH_POLICY_FULL = "FULL";
        public final static String PUSH_POLICY_BROADCAST = "BROADCAST";
        public final static String PUSH_POLICY_NO = "NOTHING";
    }

    public static final LongConfigParam VARIABLE_EXPIRE_TIME = new LongConfigParam(
        ConnectionProperties.VARIABLE_EXPIRE_TIME, 1L, null, 300L * 1000, true);

    public static final IntConfigParam DEADLOCK_DETECTION_INTERVAL = new IntConfigParam(
        ConnectionProperties.DEADLOCK_DETECTION_INTERVAL,
        1,
        null,
        TransactionAttribute.DEADLOCK_DETECTION_INTERVAL,
        false);

    public static final LongConfigParam MERGE_SORT_BUFFER_SIZE = new LongConfigParam(
        ConnectionProperties.MERGE_SORT_BUFFER_SIZE,
        0L,
        Long.MAX_VALUE,
        (long) (2 * 1024 * 1024),
        true);

    public static final LongConfigParam WORKLOAD_IO_THRESHOLD = new LongConfigParam(
        ConnectionProperties.WORKLOAD_IO_THRESHOLD, 0L, null, 15000L, true);

    public static final StringConfigParam WORKLOAD_TYPE = new StringConfigParam(
        ConnectionProperties.WORKLOAD_TYPE, null, true);

    public static final StringConfigParam EXECUTOR_MODE = new StringConfigParam(
        ConnectionProperties.EXECUTOR_MODE, "NONE", true);

    public static final BooleanConfigParam ENABLE_MASTER_MPP = new BooleanConfigParam(
        ConnectionProperties.ENABLE_MASTER_MPP, false, true);

    public static final BooleanConfigParam ENABLE_TEMP_TABLE_JOIN = new BooleanConfigParam(
        ConnectionProperties.ENABLE_TEMP_TABLE_JOIN, false, true);

    public static final IntConfigParam LOOKUP_IN_VALUE_LIMIT = new IntConfigParam(
        ConnectionProperties.LOOKUP_IN_VALUE_LIMIT, 300, Integer.MAX_VALUE,
        300, true);

    public static final IntConfigParam LOOKUP_JOIN_BLOCK_SIZE_PER_SHARD = new IntConfigParam(
        ConnectionProperties.LOOKUP_JOIN_BLOCK_SIZE_PER_SHARD, 1, Integer.MAX_VALUE,
        50, true);

    public static final BooleanConfigParam EXPLAIN_LOGICALVIEW = new BooleanConfigParam(
        ConnectionProperties.EXPLAIN_LOGICALVIEW, false, true);

    public static final BooleanConfigParam ENABLE_HTAP = new BooleanConfigParam(
        ConnectionProperties.ENABLE_HTAP, true, true);

    public static final BooleanConfigParam ENABLE_CONSISTENT_REPLICA_READ = new BooleanConfigParam(
        ConnectionProperties.ENABLE_CONSISTENT_REPLICA_READ,
        false,
        false);

    public static final IntConfigParam IN_SUB_QUERY_THRESHOLD = new IntConfigParam(
        ConnectionProperties.IN_SUB_QUERY_THRESHOLD, 2, Integer.MAX_VALUE,
        8, true);

    public static final BooleanConfigParam ENABLE_IN_SUB_QUERY_FOR_DML = new BooleanConfigParam(
        ConnectionProperties.ENABLE_IN_SUB_QUERY_FOR_DML, false, Boolean.TRUE);

    public static final BooleanConfigParam ENABLE_RUNTIME_FILTER = new BooleanConfigParam(
        ConnectionProperties.ENABLE_RUNTIME_FILTER, true, true);

    public static final LongConfigParam BLOOM_FILTER_BROADCAST_NUM = new LongConfigParam(
        ConnectionProperties.BLOOM_FILTER_BROADCAST_NUM, -1L, Long.MAX_VALUE, 20L, true);

    public static final LongConfigParam BLOOM_FILTER_MAX_SIZE = new LongConfigParam(
        ConnectionProperties.BLOOM_FILTER_MAX_SIZE, -1L, Long.MAX_VALUE, 2 * 1024 * 1024L, true);

    public static final FloatConfigParam BLOOM_FILTER_RATIO = new FloatConfigParam(
        ConnectionProperties.BLOOM_FILTER_RATIO, 0f, Float.MAX_VALUE, 0.5f, true);

    public static final LongConfigParam RUNTIME_FILTER_PROBE_MIN_ROW_COUNT = new LongConfigParam(
        ConnectionProperties.RUNTIME_FILTER_PROBE_MIN_ROW_COUNT, -1L, Long.MAX_VALUE, 10_000_000L, true);

    public static final LongConfigParam BLOOM_FILTER_GUESS_SIZE = new LongConfigParam(
        ConnectionProperties.BLOOM_FILTER_GUESS_SIZE, -1L, Long.MAX_VALUE, -1L, true);

    public static final LongConfigParam BLOOM_FILTER_MIN_SIZE = new LongConfigParam(
        ConnectionProperties.BLOOM_FILTER_MIN_SIZE, -1L, Long.MAX_VALUE, 1000L, true);

    public static final StringConfigParam FORCE_ENABLE_RUNTIME_FILTER_COLUMNS = new StringConfigParam(
        ConnectionProperties.FORCE_ENABLE_RUNTIME_FILTER_COLUMNS, "", false);

    public static final StringConfigParam FORCE_DISABLE_RUNTIME_FILTER_COLUMNS = new StringConfigParam(
        ConnectionProperties.FORCE_DISABLE_RUNTIME_FILTER_COLUMNS, "", false);

    public static final BooleanConfigParam ENABLE_PUSH_RUNTIME_FILTER_SCAN = new BooleanConfigParam(
        ConnectionProperties.ENABLE_PUSH_RUNTIME_FILTER_SCAN, true, false);

    public static final BooleanConfigParam WAIT_RUNTIME_FILTER_FOR_SCAN = new BooleanConfigParam(
        ConnectionProperties.WAIT_RUNTIME_FILTER_FOR_SCAN, true, false);

    public static final BooleanConfigParam ENABLE_RUNTIME_FILTER_INTO_BUILD_SIDE = new BooleanConfigParam(
        ConnectionProperties.ENABLE_RUNTIME_FILTER_INTO_BUILD_SIDE, true, false);

    public static final FloatConfigParam RUNTIME_FILTER_FPP = new FloatConfigParam(
        ConnectionProperties.RUNTIME_FILTER_FPP, 0.f, 0.99f, 0.03f, false);

    public static final BooleanConfigParam ENABLE_RUNTIME_FILTER_XXHASH = new BooleanConfigParam(
        ConnectionProperties.ENABLE_RUNTIME_FILTER_XXHASH, true, true);

    // Whether underlying mysqls all support bloom filter udf, can only be set by system, not by user
    public static final BooleanConfigParam STORAGE_SUPPORTS_BLOOM_FILTER = new BooleanConfigParam(
        ConnectionProperties.STORAGE_SUPPORTS_BLOOM_FILTER, false, false);

    public static final IntConfigParam WAIT_BLOOM_FILTER_TIMEOUT_MS = new IntConfigParam(
        ConnectionProperties.WAIT_BLOOM_FILTER_TIMEOUT_MS, 1, Integer.MAX_VALUE, 60000, true);

    public static final IntConfigParam RESUME_SCAN_STEP_SIZE = new IntConfigParam(
        ConnectionProperties.RESUME_SCAN_STEP_SIZE, 1, Integer.MAX_VALUE, 512, true);

    public static final BooleanConfigParam ENABLE_SPILL_OUTPUT = new BooleanConfigParam(
        ConnectionProperties.ENABLE_SPILL_OUTPUT, true, true);

    public static final LongConfigParam SPILL_OUTPUT_MAX_BUFFER_SIZE = new LongConfigParam(
        ConnectionProperties.SPILL_OUTPUT_MAX_BUFFER_SIZE, 1000000L, Long.MAX_VALUE,
        32000000L, true);

    public static final StringConfigParam SUPPORT_READ_FOLLOWER_STRATEGY = new StringConfigParam(
        ConnectionProperties.SUPPORT_READ_FOLLOWER_STRATEGY, "DEFAULT", true);

    public static final BooleanConfigParam ENABLE_LOGIN_AUDIT_CONFIG = new BooleanConfigParam(
        ConnectionProperties.ENABLE_LOGIN_AUDIT_CONFIG, false, false);

    public static final StringConfigParam TABLEGROUP_DEBUG =
        new StringConfigParam(ConnectionProperties.TABLEGROUP_DEBUG,
            "",
            false);

    public static final BooleanConfigParam ENABLE_DRIVING_STREAM_SCAN = new BooleanConfigParam(
        ConnectionProperties.ENABLE_DRIVING_STREAM_SCAN, true, true);

    public static final BooleanConfigParam ENABLE_SIMPLIFY_TRACE_SQL = new BooleanConfigParam(
        ConnectionProperties.ENABLE_SIMPLIFY_TRACE_SQL, false, true);

    public static final StringConfigParam PARAMETRIC_SIMILARITY_ALGO = new StringConfigParam(
        ConnectionProperties.PARAMETRIC_SIMILARITY_ALGO, "COSINE", true);

    public static final IntConfigParam TOPN_SIZE = new IntConfigParam(
        ConnectionProperties.TOPN_SIZE, 0, Integer.MAX_VALUE, 15, true);

    public static final IntConfigParam TOPN_MIN_NUM = new IntConfigParam(
        ConnectionProperties.TOPN_MIN_NUM, 1, Integer.MAX_VALUE, 3, true);

    public static final IntConfigParam FEEDBACK_WORKLOAD_TP_THRESHOLD = new IntConfigParam(
        ConnectionProperties.FEEDBACK_WORKLOAD_TP_THRESHOLD, 1, Integer.MAX_VALUE, -1, true);

    public static final IntConfigParam FEEDBACK_WORKLOAD_AP_THRESHOLD = new IntConfigParam(
        ConnectionProperties.FEEDBACK_WORKLOAD_AP_THRESHOLD, 1, Integer.MAX_VALUE, Integer.MAX_VALUE, true);

    //HTAP ROUTE

    public static final IntConfigParam MASTER_READ_WEIGHT = new IntConfigParam(
        ConnectionProperties.MASTER_READ_WEIGHT, -1, 100, -1, true);

    /**
     * set the operation strategy when the slave delay
     * <0 means nothing, =1 change master, =2 throw exception
     */
    public static final IntConfigParam DELAY_EXECUTION_STRATEGY = new IntConfigParam(
        ConnectionProperties.DELAY_EXECUTION_STRATEGY, 0, 2, 0, true);

    /**
     * inherit the DELAY_EXECUTION_STRATEGY from coordinator
     */
    public static final BooleanConfigParam KEEP_DELAY_EXECUTION_STRATEGY = new BooleanConfigParam(
        ConnectionProperties.KEEP_DELAY_EXECUTION_STRATEGY, true, false);

    /**
     * 是否开启 SELECT INTO OUTFILE 默认关闭
     */
    public static final BooleanConfigParam ENABLE_SELECT_INTO_OUTFILE =
        new BooleanConfigParam(ConnectionProperties.ENABLE_SELECT_INTO_OUTFILE, false, true);

    public static final BooleanConfigParam SHOW_HASH_PARTITIONS_BY_RANGE = new BooleanConfigParam(
        ConnectionProperties.SHOW_HASH_PARTITIONS_BY_RANGE,
        false,
        true);

    public static final BooleanConfigParam SHOW_TABLE_GROUP_NAME = new BooleanConfigParam(
        ConnectionProperties.SHOW_TABLE_GROUP_NAME,
        false,
        true);

    public static final IntConfigParam MAX_PHYSICAL_PARTITION_COUNT = new IntConfigParam(
        ConnectionProperties.MAX_PHYSICAL_PARTITION_COUNT, 1, Integer.MAX_VALUE, 8192, true);

    public static final IntConfigParam MAX_PARTITION_COLUMN_COUNT = new IntConfigParam(
        ConnectionProperties.MAX_PARTITION_COLUMN_COUNT, 1, Integer.MAX_VALUE, 5, true);

    public static final BooleanConfigParam CALCULATE_ACTUAL_SHARD_COUNT_FOR_COST = new BooleanConfigParam(
        ConnectionProperties.CALCULATE_ACTUAL_SHARD_COUNT_FOR_COST,
        true,
        true);

    public static final BooleanConfigParam ENABLE_BALANCER = new BooleanConfigParam(
        ConnectionProperties.ENABLE_BALANCER,
        false,
        true
    );

    public static final LongConfigParam BALANCER_MAX_PARTITION_SIZE = new LongConfigParam(
        ConnectionProperties.BALANCER_MAX_PARTITION_SIZE,
        1L, 32L << 30,
        512L << 20,
        true
    );

    public static final StringConfigParam BALANCER_WINDOW = new StringConfigParam(
        ConnectionProperties.BALANCER_WINDOW,
        "",
        true
    );

    public static final BooleanConfigParam ENABLE_PARTITION_MANAGEMENT = new BooleanConfigParam(
        ConnectionProperties.ENABLE_PARTITION_MANAGEMENT,
        false,
        true
    );

    /**
     * switch for partition pruning
     */
    public static final BooleanConfigParam ENABLE_PARTITION_PRUNING = new BooleanConfigParam(
        ConnectionProperties.ENABLE_PARTITION_PRUNING,
        true,
        true
    );

    public static final BooleanConfigParam ENABLE_AUTO_MERGE_INTERVALS_IN_PRUNING = new BooleanConfigParam(
        ConnectionProperties.ENABLE_AUTO_MERGE_INTERVALS_IN_PRUNING,
        true,
        true
    );

    public static final BooleanConfigParam ENABLE_INTERVAL_ENUMERATION_IN_PRUNING = new BooleanConfigParam(
        ConnectionProperties.ENABLE_INTERVAL_ENUMERATION_IN_PRUNING,
        true,
        true
    );

    public static final IntConfigParam PARTITION_PRUNING_STEP_COUNT_LIMIT = new IntConfigParam(
        ConnectionProperties.PARTITION_PRUNING_STEP_COUNT_LIMIT, 0, Integer.MAX_VALUE, 1024, true);

    public static final BooleanConfigParam ENABLE_CONST_EXPR_EVAL_CACHE = new BooleanConfigParam(
        ConnectionProperties.ENABLE_CONST_EXPR_EVAL_CACHE,
        true,
        true
    );

    public static final BooleanConfigParam USE_FAST_SINGLE_POINT_INTERVAL_MERGING = new BooleanConfigParam(
        ConnectionProperties.USE_FAST_SINGLE_POINT_INTERVAL_MERGING,
        false,
        true
    );

    public static final LongConfigParam MAX_ENUMERABLE_INTERVAL_LENGTH = new LongConfigParam(
        ConnectionProperties.MAX_ENUMERABLE_INTERVAL_LENGTH,
        1L,
        4096L, 32L, true
    );

    /**
     * The max length of the enumerable interval in pruning
     */
    public static final LongConfigParam MAX_IN_SUBQUERY_PRUNING_SIZE = new LongConfigParam(
        ConnectionProperties.MAX_IN_SUBQUERY_PRUNING_SIZE,
        1L,
        Long.MAX_VALUE, 8192L, true
    );

    /**
     * The max size of in value from the InSubQuery pruning
     */
    public static final BooleanConfigParam ENABLE_LOG_PART_PRUNING = new BooleanConfigParam(
        ConnectionProperties.ENABLE_LOG_PART_PRUNING,
        false,
        true
    );

    public static final BooleanConfigParam ENABLE_BRANCH_AND_BOUND_OPTIMIZATION = new BooleanConfigParam(
        ConnectionProperties.ENABLE_BRANCH_AND_BOUND_OPTIMIZATION, true, true);

    public static final BooleanConfigParam ENABLE_BROADCAST_JOIN = new BooleanConfigParam(
        ConnectionProperties.ENABLE_BROADCAST_JOIN, true, true);

    public static final IntConfigParam BROADCAST_SHUFFLE_PARALLELISM = new IntConfigParam(
        ConnectionProperties.BROADCAST_SHUFFLE_PARALLELISM, 1, Integer.MAX_VALUE, 64, true);

    public static final BooleanConfigParam ENABLE_PASS_THROUGH_TRAIT = new BooleanConfigParam(
        ConnectionProperties.ENABLE_PASS_THROUGH_TRAIT, true, true);

    public static final BooleanConfigParam ENABLE_DERIVE_TRAIT = new BooleanConfigParam(
        ConnectionProperties.ENABLE_DERIVE_TRAIT, true, true);

    public static final BooleanConfigParam ENABLE_SHUFFLE_BY_PARTIAL_KEY = new BooleanConfigParam(
        ConnectionProperties.ENABLE_SHUFFLE_BY_PARTIAL_KEY, true, true);

    public static final StringConfigParam ADVISE_TYPE = new StringConfigParam(
        ConnectionProperties.ADVISE_TYPE, null, true);

    public static final BooleanConfigParam ENABLE_HLL = new BooleanConfigParam(
        ConnectionProperties.ENABLE_HLL, false, true);

    public static final IntConfigParam MINOR_TOLERANCE_VALUE = new IntConfigParam(
        ConnectionProperties.MINOR_TOLERANCE_VALUE, -1, 100, 500, true);

    /**
     * upper bound for baseline sync
     */
    public static final LongConfigParam MAX_BASELINE_SYNC_BYTE_SIZE = new LongConfigParam(
        ConnectionProperties.MAX_BASELINE_SYNC_BYTE_SIZE,
        1 * 1024 * 1024L,
        Long.MAX_VALUE, 20 * 1024 * 1024L, true
    );

    public static final IntConfigParam MAX_BASELINE_SYNC_PLAN_SIZE = new IntConfigParam(
        ConnectionProperties.MAX_BASELINE_SYNC_PLAN_SIZE,
        1,
        Integer.MAX_VALUE, 500, true
    );

    public static final IntConfigParam SPM_OLD_PLAN_CHOOSE_COUNT_LEVEL = new IntConfigParam(
        ConnectionProperties.MAX_BASELINE_SYNC_PLAN_SIZE,
        1,
        Integer.MAX_VALUE, 100, true
    );

    public static final IntConfigParam STATISTIC_NDV_SKETCH_MAX_DIFFERENT_VALUE = new IntConfigParam(
        ConnectionProperties.STATISTIC_NDV_SKETCH_MAX_DIFFERENT_VALUE, 1, Integer.MAX_VALUE, 50000, true);

    public static final IntConfigParam AUTO_COLLECT_NDV_SKETCH = new IntConfigParam(
        ConnectionProperties.AUTO_COLLECT_NDV_SKETCH, 1, Integer.MAX_VALUE, 24, true);

    public static final IntConfigParam STATISTIC_NDV_SKETCH_EXPIRE_TIME = new IntConfigParam(
        ConnectionProperties.STATISTIC_NDV_SKETCH_EXPIRE_TIME, 60, Integer.MAX_VALUE, 1000 * 60 * 60 * 24 * 7, true);

    public static final IntConfigParam STATISTIC_NDV_SKETCH_QUERY_TIMEOUT = new IntConfigParam(
        ConnectionProperties.STATISTIC_NDV_SKETCH_QUERY_TIMEOUT, 60, Integer.MAX_VALUE, 60 * 1000, true);

    public static final StringConfigParam STATISTIC_NDV_SKETCH_SAMPLE_RATE = new StringConfigParam(
        ConnectionProperties.STATISTIC_NDV_SKETCH_SAMPLE_RATE, null, true);

    public static final IntConfigParam CDC_STARTUP_MODE = new IntConfigParam(
        ConnectionProperties.CDC_STARTUP_MODE, 0, 2, 1, true);

    public static final BooleanConfigParam SHARE_STORAGE_MODE = new BooleanConfigParam(
        ConnectionProperties.SHARE_STORAGE_MODE,
        false,
        true
    );

    public static final BooleanConfigParam SHOW_ALL_PARAMS = new BooleanConfigParam(
        ConnectionProperties.SHOW_ALL_PARAMS, false, true);

    public static final BooleanConfigParam ENABLE_SET_GLOBAL = new BooleanConfigParam(
        ConnectionProperties.ENABLE_SET_GLOBAL, false, true);

    public static final BooleanConfigParam ENABLE_PREEMPTIVE_MDL = new BooleanConfigParam(
        ConnectionProperties.ENABLE_PREEMPTIVE_MDL, true, true);

    public static final LongConfigParam PREEMPTIVE_MDL_INITWAIT = new LongConfigParam(
        ConnectionProperties.PREEMPTIVE_MDL_INITWAIT, 0L, 28800000L, 5000L, true);

    public static final LongConfigParam PREEMPTIVE_MDL_INTERVAL = new LongConfigParam(
        ConnectionProperties.PREEMPTIVE_MDL_INTERVAL, 0L, 28800000L, 5000L, true);

    public static final LongConfigParam SCHEDULER_SCAN_INTERVAL_SECONDS = new LongConfigParam(
        ConnectionProperties.SCHEDULER_SCAN_INTERVAL_SECONDS, 0L, 28800000L, 60L, true);

    public static final LongConfigParam SCHEDULER_CLEAN_UP_INTERVAL_HOURS = new LongConfigParam(
        ConnectionProperties.SCHEDULER_CLEAN_UP_INTERVAL_HOURS, 0L, 28800000L, 3L, true);

    public static final LongConfigParam SCHEDULER_RECORD_KEEP_HOURS = new LongConfigParam(
        ConnectionProperties.SCHEDULER_RECORD_KEEP_HOURS, 0L, 28800000L, 720L, true);

    public static final IntConfigParam SCHEDULER_MIN_WORKER_COUNT = new IntConfigParam(
        ConnectionProperties.SCHEDULER_MIN_WORKER_COUNT, 1, 512, 32, true);

    public static final IntConfigParam SCHEDULER_MAX_WORKER_COUNT = new IntConfigParam(
        ConnectionProperties.SCHEDULER_MAX_WORKER_COUNT, 1, 1024, 64, true);

    public static final StringConfigParam DEFAULT_LOCAL_PARTITION_SCHEDULE_CRON_EXPR = new StringConfigParam(
        ConnectionProperties.DEFAULT_LOCAL_PARTITION_SCHEDULE_CRON_EXPR, DEFAULT_SCHEDULE_CRON_EXPR, true);

    public static final BooleanConfigParam INTERRUPT_DDL_WHILE_LOSING_LEADER = new BooleanConfigParam(
        ConnectionProperties.INTERRUPT_DDL_WHILE_LOSING_LEADER, true, true);

    public static final BooleanConfigParam RECORD_SQL_COST = new BooleanConfigParam(
        ConnectionProperties.RECORD_SQL_COST, false, true);

    public static final BooleanConfigParam ENABLE_LOGICALVIEW_COST = new BooleanConfigParam(
        ConnectionProperties.ENABLE_LOGICALVIEW_COST, true, true);

    public static final BooleanConfigParam TABLEGROUP_REORG_CHECK_AFTER_BACKFILL = new BooleanConfigParam(
        ConnectionProperties.TABLEGROUP_REORG_CHECK_AFTER_BACKFILL, true, true);

    public static final BooleanConfigParam TABLEGROUP_REORG_BACKFILL_USE_FASTCHECKER = new BooleanConfigParam(
        ConnectionProperties.TABLEGROUP_REORG_BACKFILL_USE_FASTCHECKER, true, true);

    public static final LongConfigParam TABLEGROUP_REORG_CHECK_BATCH_SIZE = new LongConfigParam(
        ConnectionProperties.TABLEGROUP_REORG_CHECK_BATCH_SIZE,
        16L,
        4096L,
        1024L,
        false);
    public static final LongConfigParam TABLEGROUP_REORG_CHECK_SPEED_LIMITATION = new LongConfigParam(
        ConnectionProperties.TABLEGROUP_REORG_CHECK_SPEED_LIMITATION,
        -1L,
        Long.MAX_VALUE,
        150000L, // Default 150k rows/s.
        false);

    public static final LongConfigParam TABLEGROUP_REORG_CHECK_SPEED_MIN = new LongConfigParam(
        ConnectionProperties.TABLEGROUP_REORG_CHECK_SPEED_MIN,
        -1L,
        Long.MAX_VALUE,
        100000L, // Default 100k rows/s.
        false);

    public static final LongConfigParam TABLEGROUP_REORG_CHECK_PARALLELISM = new LongConfigParam(
        ConnectionProperties.TABLEGROUP_REORG_CHECK_PARALLELISM,
        -1L,
        Long.MAX_VALUE,
        -1L,
        false);

    public static final LongConfigParam TABLEGROUP_REORG_EARLY_FAIL_NUMBER = new LongConfigParam(
        ConnectionProperties.TABLEGROUP_REORG_EARLY_FAIL_NUMBER,
        100L,
        Long.MAX_VALUE,
        1024L,
        false);

    public static final IntConfigParam TABLEGROUP_REORG_FASTCHECKER_PARALLELISM = new IntConfigParam(
        ConnectionProperties.TABLEGROUP_REORG_FASTCHECKER_PARALLELISM,
        -1,
        128,
        4,
        false);

    public static final StringConfigParam TABLEGROUP_REORG_FINAL_TABLE_STATUS_DEBUG =
        new StringConfigParam(ConnectionProperties.TABLEGROUP_REORG_FINAL_TABLE_STATUS_DEBUG,
            "",
            false);

    public static final BooleanConfigParam ENABLE_LOGICAL_DB_WARMMING_UP = new BooleanConfigParam(
        ConnectionProperties.ENABLE_LOGICAL_DB_WARMMING_UP, true, true);

    public static final IntConfigParam LOGICAL_DB_WARMMING_UP_EXECUTOR_POOL_SIZE = new IntConfigParam(
        ConnectionProperties.LOGICAL_DB_WARMMING_UP_EXECUTOR_POOL_SIZE, 1, 1024, 4, false);

    public static final BooleanConfigParam ENABLE_OSS_INDEX_SELECTION = new BooleanConfigParam(
        ConnectionProperties.ENABLE_OSS_INDEX_SELECTION,
        true,
        true);

    public static final LongConfigParam WORKLOAD_OSS_NET_THRESHOLD = new LongConfigParam(
        ConnectionProperties.WORKLOAD_OSS_NET_THRESHOLD, 0L, null, 2L, true);

    /* ================ For OSS Table ORC File ================ */
    public static final IntConfigParam OSS_BACKFILL_PARALLELISM = new IntConfigParam(
        ConnectionProperties.OSS_BACKFILL_PARALLELISM, 1, 32, 32, true);
    public static final LongConfigParam OSS_ORC_INDEX_STRIDE = new LongConfigParam(
        ConnectionProperties.OSS_ORC_INDEX_STRIDE, 10L, 1000_000L, 1000L, true);
    public static final FloatConfigParam OSS_BLOOM_FILTER_FPP = new FloatConfigParam(
        ConnectionProperties.OSS_BLOOM_FILTER_FPP, .01f, .05f, .01f, true);
    public static final LongConfigParam OSS_MAX_ROWS_PER_FILE = new LongConfigParam(
        ConnectionProperties.OSS_MAX_ROWS_PER_FILE, 1_000L, 1000_000_000L, 5_000_000L, true);
    public static final BooleanConfigParam OSS_REMOVE_TMP_FILES = new BooleanConfigParam(
        ConnectionProperties.OSS_REMOVE_TMP_FILES, true, true);
    public static final StringConfigParam OSS_ORC_COMPRESSION = new StringConfigParam(
        ConnectionProperties.OSS_ORC_COMPRESSION, "LZ4", true);
    /* ================ For OSS Table File System (unused) ================ */
    /**
     * Rate limitation of oss data reading (Unit: in bytes)
     */
    public static final LongConfigParam OSS_FS_MAX_READ_RATE = new LongConfigParam(
        ConnectionProperties.OSS_FS_MAX_READ_RATE, -1L, Long.MAX_VALUE, -1L, true);

    /**
     * Rate limitation of oss data writing (Unit: in bytes)
     */
    public static final LongConfigParam OSS_FS_MAX_WRITE_RATE = new LongConfigParam(
        ConnectionProperties.OSS_FS_MAX_WRITE_RATE, -1L, Long.MAX_VALUE, -1L, true);

    public static final BooleanConfigParam OSS_FS_VALIDATION_ENABLE = new BooleanConfigParam(
        ConnectionProperties.OSS_FS_VALIDATION_ENABLE, false, true);

    /**
     * Period of Expiration (TimeUnit: Day)
     */
    public static final LongConfigParam OSS_FS_CACHE_TTL = new LongConfigParam(
        ConnectionProperties.OSS_FS_CACHE_TTL, 1L, Long.MAX_VALUE, 7L, true);

    /**
     * Max Entries in OSS Cache (An entry represents a remote file path)
     */
    public static final LongConfigParam OSS_FS_MAX_CACHED_ENTRIES = new LongConfigParam(
        ConnectionProperties.OSS_FS_MAX_CACHED_ENTRIES, 1L, Long.MAX_VALUE, 2048L, true);
    public static final LongConfigParam OSS_FS_HOT_CACHE_TTL = new LongConfigParam(
        ConnectionProperties.OSS_FS_HOT_CACHE_TTL, 1 * 1000L, 2 * 24 * 3600 * 1000L, 3 * 1000L, true);
    public static final LongConfigParam OSS_FS_MAX_HOT_CACHED_ENTRIES = new LongConfigParam(
        ConnectionProperties.OSS_FS_MAX_HOT_CACHED_ENTRIES, 1L, 4096L, 2048L, true);
    public static final LongConfigParam OSS_ORC_MAX_MERGE_DISTANCE =
        new LongConfigParam(ConnectionProperties.OSS_ORC_MAX_MERGE_DISTANCE, 0L, 2L * 1024 * 1024 * 1024, 64L * 1024,
            true);
    public static final BooleanConfigParam FLASHBACK_RENAME = new BooleanConfigParam(
        ConnectionProperties.FLASHBACK_RENAME, false, true);
    public static final BooleanConfigParam PURGE_FILE_STORAGE_TABLE = new BooleanConfigParam(
        ConnectionProperties.PURGE_FILE_STORAGE_TABLE, false, true);
    public static final StringConfigParam FILE_LIST = new StringConfigParam(
        ConnectionProperties.FILE_LIST, "ALL", true);
    public static final StringConfigParam FILE_PATTERN = new StringConfigParam(
        ConnectionProperties.FILE_PATTERN, "", true);

    public static final BooleanConfigParam ENABLE_EXPIRE_FILE_STORAGE_PAUSE = new BooleanConfigParam(
        ConnectionProperties.ENABLE_EXPIRE_FILE_STORAGE_PAUSE, true, true);

    /**
     * test the pause of backfill
     */
    public static final BooleanConfigParam ENABLE_EXPIRE_FILE_STORAGE_TEST_PAUSE = new BooleanConfigParam(
        ConnectionProperties.ENABLE_EXPIRE_FILE_STORAGE_TEST_PAUSE, false, true);

    public static final BooleanConfigParam ENABLE_FILE_STORE_CHECK_TABLE = new BooleanConfigParam(
        ConnectionProperties.ENABLE_FILE_STORE_CHECK_TABLE, false, true);
    public static final BooleanConfigParam ENABLE_OSS_BUFFER_POOL = new BooleanConfigParam(
        ConnectionProperties.ENABLE_OSS_BUFFER_POOL, false, true);
    public static final BooleanConfigParam ENABLE_OSS_DELAY_MATERIALIZATION = new BooleanConfigParam(
        ConnectionProperties.ENABLE_OSS_DELAY_MATERIALIZATION, false, true);
    public static final BooleanConfigParam ENABLE_OSS_ZERO_COPY = new BooleanConfigParam(
        ConnectionProperties.ENABLE_OSS_ZERO_COPY, false, true);
    public static final BooleanConfigParam ENABLE_OSS_COMPATIBLE = new BooleanConfigParam(
        ConnectionProperties.ENABLE_OSS_COMPATIBLE, true, true);
    public static final BooleanConfigParam ENABLE_OSS_DELAY_MATERIALIZATION_ON_EXCHANGE = new BooleanConfigParam(
        ConnectionProperties.ENABLE_OSS_DELAY_MATERIALIZATION_ON_EXCHANGE, false, true);
    public static final BooleanConfigParam ENABLE_OSS_FILE_CONCURRENT_SPLIT_ROUND_ROBIN = new BooleanConfigParam(
        ConnectionProperties.ENABLE_OSS_FILE_CONCURRENT_SPLIT_ROUND_ROBIN, false, true);
    public static final BooleanConfigParam ENABLE_REUSE_VECTOR = new BooleanConfigParam(
        ConnectionProperties.ENABLE_REUSE_VECTOR, false, true);
    public static final BooleanConfigParam ENABLE_DECIMAL_FAST_VEC = new BooleanConfigParam(
        ConnectionProperties.ENABLE_DECIMAL_FAST_VEC, false, true);
    public static final BooleanConfigParam ENABLE_UNIQUE_HASH_KEY = new BooleanConfigParam(
        ConnectionProperties.ENABLE_UNIQUE_HASH_KEY, false, true);
    public static final IntConfigParam BLOCK_BUILDER_CAPACITY = new IntConfigParam(
        ConnectionProperties.BLOCK_BUILDER_CAPACITY, 1, Integer.MAX_VALUE, 4, true);
    public static BooleanConfigParam ENABLE_HASH_TABLE_BLOOM_FILTER = new BooleanConfigParam(
        ConnectionProperties.ENABLE_HASH_TABLE_BLOOM_FILTER, true, true);
    public static final BooleanConfigParam ENABLE_COMMON_SUB_EXPRESSION_TREE_ELIMINATE = new BooleanConfigParam(
        ConnectionProperties.ENABLE_COMMON_SUB_EXPRESSION_TREE_ELIMINATE, false, true);
    public static final StringConfigParam OSS_FILE_ORDER = new StringConfigParam(ConnectionProperties.OSS_FILE_ORDER,
        null,
        true);

    public static final IntConfigParam MAX_SESSION_PREPARED_STMT_COUNT = new IntConfigParam(
        ConnectionProperties.MAX_SESSION_PREPARED_STMT_COUNT, 0, Integer.MAX_VALUE, 100, true);

    public static final BooleanConfigParam ENABLE_PARTITIONS_HEATMAP_COLLECTION = new BooleanConfigParam(
        ConnectionProperties.ENABLE_PARTITIONS_HEATMAP_COLLECTION, true, true);

    public static final StringConfigParam PARTITIONS_HEATMAP_COLLECTION_ONLY =
        new StringConfigParam(ConnectionProperties.PARTITIONS_HEATMAP_COLLECTION_ONLY,
            "",
            true);

    public static final IntConfigParam PARTITIONS_HEATMAP_COLLECTION_MAX_SCAN = new IntConfigParam(
        ConnectionProperties.PARTITIONS_HEATMAP_COLLECTION_MAX_SCAN, 0, Integer.MAX_VALUE, 8000, true);

    public static final IntConfigParam PARTITIONS_HEATMAP_COLLECTION_MAX_MERGE_NUM = new IntConfigParam(
        ConnectionProperties.PARTITIONS_HEATMAP_COLLECTION_MAX_MERGE_NUM, 0, Integer.MAX_VALUE, 1600, true);

    public static final IntConfigParam PARTITIONS_HEATMAP_COLLECTION_MAX_SINGLE_LOGIC_SCHEMA_COUNT = new IntConfigParam(
        ConnectionProperties.PARTITIONS_HEATMAP_COLLECTION_MAX_SINGLE_LOGIC_SCHEMA_COUNT, 0, Integer.MAX_VALUE, 8000,
        true);

    /**
     * the min size of IN expr that would be pruned
     */
    public static final IntConfigParam IN_PRUNE_SIZE = new IntConfigParam(
        ConnectionProperties.IN_PRUNE_SIZE, 0, Integer.MAX_VALUE, 150, true);

    /**
     * the batch size of IN expr being pruned
     */
    public static final IntConfigParam IN_PRUNE_STEP_SIZE = new IntConfigParam(
        ConnectionProperties.IN_PRUNE_STEP_SIZE, 1, Integer.MAX_VALUE, 10, true);

    public static final IntConfigParam IN_PRUNE_MAX_TIME = new IntConfigParam(
        ConnectionProperties.IN_PRUNE_MAX_TIME, 1, Integer.MAX_VALUE, 100, true);


    /**
     * Allow re-binding a new archive table to ttl table, replacing the old archive table.
     */
    public static final BooleanConfigParam ALLOW_REPLACE_ARCHIVE_TABLE = new BooleanConfigParam(
        ConnectionProperties.ALLOW_REPLACE_ARCHIVE_TABLE, false, true);
    /**
     * Allow create table like a file-store table.
     */
    public static final BooleanConfigParam ALLOW_CREATE_TABLE_LIKE_FILE_STORE = new BooleanConfigParam(
        ConnectionProperties.ALLOW_CREATE_TABLE_LIKE_FILE_STORE, false, true);

    public static final StringConfigParam PURGE_OSS_FILE_CRON_EXPR =  new StringConfigParam(
        ConnectionProperties.PURGE_OSS_FILE_CRON_EXPR, "0 0 1 ? * WED", true);
    public static final IntConfigParam PURGE_OSS_FILE_BEFORE_DAY = new IntConfigParam(
        ConnectionProperties.PURGE_OSS_FILE_BEFORE_DAY, 1, Integer.MAX_VALUE, 60, true);
    public static final IntConfigParam FILE_STORAGE_FILES_META_QUERY_PARALLELISM = new IntConfigParam(
        ConnectionProperties.FILE_STORAGE_FILES_META_QUERY_PARALLELISM, 1, Integer.MAX_VALUE, 8, true);

    public static final IntConfigParam REBALANCE_TASK_PARALISM = new IntConfigParam(
        ConnectionProperties.REBALANCE_TASK_PARALISM, 1, 64, 8, true);

    /**
     * statement summary
     */
    public static final BooleanConfigParam ENABLE_STATEMENTS_SUMMARY =
        new BooleanConfigParam(ConnectionProperties.ENABLE_STATEMENTS_SUMMARY, true, true);

    /**
     * only search local node without remote sync action
     */
    public static final BooleanConfigParam ENABLE_REMOTE_SYNC_ACTION =
        new BooleanConfigParam(ConnectionProperties.ENABLE_REMOTE_SYNC_ACTION, true, true);

    public static final BooleanConfigParam ENABLE_NODE_HINT_REPLACE = new BooleanConfigParam(
        ConnectionProperties.ENABLE_NODE_HINT_REPLACE, true, true);
    public static final BooleanConfigParam SKIP_MOVE_DATABASE_VALIDATOR = new BooleanConfigParam(
        ConnectionProperties.SKIP_MOVE_DATABASE_VALIDATOR, false, true);

    public static final StringConfigParam PHYSICAL_DDL_IGNORED_ERROR_CODE =
        new StringConfigParam(ConnectionProperties.PHYSICAL_DDL_IGNORED_ERROR_CODE,
            "",
            false);

    public static final BooleanConfigParam DDL_PAUSE_DURING_EXCEPTION =
        new BooleanConfigParam(ConnectionProperties.DDL_PAUSE_DURING_EXCEPTION,
            false,
            false);

    public static final BooleanConfigParam SKIP_TABLEGROUP_VALIDATOR =
        new BooleanConfigParam(ConnectionProperties.SKIP_TABLEGROUP_VALIDATOR, false, true);

    public static final IntConfigParam BACKUP_OSS_PERIOD = new IntConfigParam(
        ConnectionProperties.BACKUP_OSS_PERIOD, 1, Integer.MAX_VALUE, 30, true);

    public static final BooleanConfigParam FORBID_REMOTE_DDL_TASK = new BooleanConfigParam(
        ConnectionProperties.FORBID_REMOTE_DDL_TASK, true, true);

    /**
     * Whether enable auto savepoint.
     * If it is TRUE, failed DML statements will be rollbacked automatically.
     */
    public static final BooleanConfigParam ENABLE_AUTO_SAVEPOINT = new BooleanConfigParam(
        ConnectionProperties.ENABLE_AUTO_SAVEPOINT, false, true);

    /**
     * Whether enable cursor fetch.
     */
    public static final BooleanConfigParam ENABLE_CURSOR_FETCH = new BooleanConfigParam(
        ConnectionProperties.ENABLE_CURSOR_FETCH, false, true);

    /**
     * tablegroup / database mdl segment
     */
    public static final IntConfigParam TG_MDL_SEGMENT_SIZE = new IntConfigParam(
        ConnectionProperties.TG_MDL_SEGMENT_SIZE, 1, 1024, 15, true);

    public static final IntConfigParam DB_MDL_SEGMENT_SIZE = new IntConfigParam(
        ConnectionProperties.DB_MDL_SEGMENT_SIZE, 1, 1024, 63, true);

    public static final BooleanConfigParam FORCE_RESHARD = new BooleanConfigParam(
        ConnectionProperties.FORCE_RESHARD, false, true);

    public static final BooleanConfigParam REMOVE_DDL_JOB_REDUNDANCY_RELATIONS = new BooleanConfigParam(
        ConnectionProperties.REMOVE_DDL_JOB_REDUNDANCY_RELATIONS, true, true);

    public static final BooleanConfigParam ENABLE_TRIGGER_DIRECT_INFORMATION_SCHEMA_QUERY = new BooleanConfigParam(
        ConnectionProperties.ENABLE_TRIGGER_DIRECT_INFORMATION_SCHEMA_QUERY, false, true);

    public static final LongConfigParam DDL_PLAN_SCHEDULER_DELAY = new LongConfigParam(
        ConnectionProperties.DDL_PLAN_SCHEDULER_DELAY, 10L, 1800L, 60L, true);

    public static final BooleanConfigParam ENABLE_FAST_MOCK = new BooleanConfigParam(
        ConnectionProperties.ENABLE_FAST_MOCK, false, true);

    public static final IntConfigParam OPTIMIZE_TABLE_PARALLELISM = new IntConfigParam(
        ConnectionProperties.OPTIMIZE_TABLE_PARALLELISM, 1, 4096, 4096, true);

    public static final BooleanConfigParam OPTIMIZE_TABLE_USE_DAL = new BooleanConfigParam(
        ConnectionProperties.OPTIMIZE_TABLE_USE_DAL, false, true);

    public static final BooleanConfigParam ENABLE_AUTO_SPLIT_PARTITION = new BooleanConfigParam(
            ConnectionProperties.ENABLE_AUTO_SPLIT_PARTITION, true, true);

    public static final BooleanConfigParam ENABLE_MODULE_LOG = new BooleanConfigParam(
        ConnectionProperties.ENABLE_MODULE_LOG, true, true);

    public static final IntConfigParam MAX_MODULE_LOG_PARAMS_SIZE = new IntConfigParam(
        ConnectionProperties.MAX_MODULE_LOG_PARAMS_SIZE, 1, 64, 32, true);

    public static final IntConfigParam MAX_MODULE_LOG_PARAM_SIZE = new IntConfigParam(
        ConnectionProperties.MAX_MODULE_LOG_PARAM_SIZE, 1, Integer.MAX_VALUE, 1024, true);

    /**
     * max speed limit for oss backfill procedure
     */
    public static final LongConfigParam OSS_BACKFILL_SPEED_LIMITATION = new LongConfigParam(
        ConnectionProperties.OSS_BACKFILL_SPEED_LIMITATION,
        -1L,
        Long.MAX_VALUE,
        120000L, // max speed(rows/s)
        false);

    /**
     * min speed limit for oss backfill procedure
     */
    public static final LongConfigParam OSS_BACKFILL_SPEED_MIN = new LongConfigParam(
        ConnectionProperties.OSS_BACKFILL_SPEED_MIN,
        -1L,
        Long.MAX_VALUE,
        100000L, // min speed 100k rows/s.
        false);

    public static final BooleanConfigParam ONLY_MANUAL_TABLEGROUP_ALLOW =
        new BooleanConfigParam(ConnectionProperties.ONLY_MANUAL_TABLEGROUP_ALLOW,
            true,
            false);

    public static final BooleanConfigParam MANUAL_TABLEGROUP_NOT_ALLOW_AUTO_MATCH =
        new BooleanConfigParam(ConnectionProperties.MANUAL_TABLEGROUP_NOT_ALLOW_AUTO_MATCH,
            true,
            false);

    public static final BooleanConfigParam ACQUIRE_CREATE_TABLE_GROUP_LOCK = new BooleanConfigParam(
        ConnectionProperties.ACQUIRE_CREATE_TABLE_GROUP_LOCK, true, true);

    public static final BooleanConfigParam ENABLE_STORAGE_TRIGGER = new BooleanConfigParam(
        ConnectionProperties.ENABLE_STORAGE_TRIGGER,
        false,
        false);
}
