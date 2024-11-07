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

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.alibaba.fastjson.parser.Feature;
import com.alibaba.polardbx.common.TddlConstants;
import com.alibaba.polardbx.common.constants.IsolationLevel;
import com.alibaba.polardbx.common.statementsummary.StatementSummaryManager;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.version.InstanceVersion;
import com.alibaba.polardbx.config.ConfigDataMode;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * @version 1.0
 */
public class DynamicConfig {

    public static DynamicConfig getInstance() {
        return instance;
    }

    public void loadValue(Logger logger, String key, String value) {
        if (key != null && value != null) {
            switch (key.toUpperCase()) {
            case ConnectionProperties.GENERAL_DYNAMIC_SPEED_LIMITATION:
                generalDynamicSpeedLimitation = parseValue(value, Long.class, generalDynamicSpeedLimitationDefault);
                break;

            case ConnectionProperties.XPROTO_MAX_DN_CONCURRENT:
                xprotoMaxDnConcurrent = parseValue(value, Long.class, xprotoMaxDnConcurrentDefault);
                break;

            case ConnectionProperties.XPROTO_MAX_DN_WAIT_CONNECTION:
                xprotoMaxDnWaitConnection = parseValue(value, Long.class, xprotoMaxDnWaitConnectionDefault);
                break;

            case ConnectionProperties.XPROTO_ALWAYS_KEEP_FILTER_ON_XPLAN_GET:
                xprotoAlwaysKeepFilterOnXplanGet =
                    parseValue(value, Boolean.class, xprotoAlwaysKeepFilterOnXplanGetDefault);
                break;

            case ConnectionProperties.XPROTO_PROBE_TIMEOUT:
                xprotoProbeTimeout = parseValue(value, Integer.class, xprotoProbeTimeoutDefault);
                break;

            case ConnectionProperties.XPROTO_GALAXY_PREPARE:
                xprotoGalaxyPrepare = parseValue(value, Boolean.class, xprotoGalaxyPrepareDefault);
                break;

            case ConnectionProperties.XPROTO_FLOW_CONTROL_SIZE_KB:
                xprotoFlowControlSizeKb = parseValue(value, Integer.class, xprotoFlowControlSizeKbDefault);
                break;

            case ConnectionProperties.XPROTO_TCP_AGING:
                xprotoTcpAging = parseValue(value, Integer.class, xprotoTcpAgingDefault);
                break;

            case ConnectionProperties.AUTO_PARTITION_PARTITIONS:
                autoPartitionPartitions = parseValue(value, Long.class, autoPartitionPartitionsDefault);
                break;

            case ConnectionProperties.STORAGE_DELAY_THRESHOLD:
                delayThreshold = parseValue(value, Integer.class, 3);
                break;
            case ConnectionProperties.ENABLE_OPTIMIZER_ALERT:
                enableOptimizerAlert = parseValue(value, Boolean.class, true);
                break;
            case ConnectionProperties.ENABLE_OPTIMIZER_ALERT_LOG:
                enableOptimizerAlertLog = parseValue(value, Boolean.class, true);
                break;
            case ConnectionProperties.OPTIMIZER_ALERT_LOG_INTERVAL:
                optimizerAlertLogInterval = parseValue(value, Long.class, 600000L);
                break;
            case ConnectionProperties.ENABLE_HOT_GSI_EVOLUTION:
                enableHotGsiEvolution = parseValue(value, Boolean.class, true);
                break;
            case ConnectionProperties.ENABLE_TP_SLOW_ALERT_THRESHOLD:
                tpSlowAlertThreshold = parseValue(value, Integer.class, 10);
                break;
            case ConnectionProperties.STORAGE_BUSY_THRESHOLD:
                busyThreshold = parseValue(value, Integer.class, 100);
                break;
            case ConnectionProperties.USE_CDC_CON:
                isBasedCDC = parseValue(value, Boolean.class, true);
                break;
            case ConnectionProperties.GROUPING_LSN_THREAD_NUM:
                groupingThread = parseValue(value, Integer.class, 4);
                break;
            case ConnectionProperties.GROUPING_LSN_TIMEOUT:
                groupingTimeout = parseValue(value, Integer.class, 3000);
                break;
            case ConnectionProperties.FORCE_RECREATE_GROUP_DATASOURCE:
                enableCreateGroupDataSource = parseValue(value, Boolean.class, false);
                break;
            case ConnectionProperties.ENABLE_PLAN_TYPE_DIGEST:
                enablePlanTypeDigest = parseValue(value, Boolean.class, true);
                break;
            case ConnectionProperties.ENABLE_PLAN_TYPE_DIGEST_STRICT_MODE:
                enablePlanTypeDigestStrictMode = parseValue(value, Boolean.class, false);
                break;
            case ConnectionProperties.ENABLE_FOLLOWER_READ:
                supportFollowRead = parseValue(value, Boolean.class, false);
                break;
            case ConnectionProperties.ENABLE_SHARE_READVIEW_IN_RC:
                enableShareReadviewInRc = parseValue(value, Boolean.class, false);
                break;
            case ConnectionProperties.ENABLE_REMOTE_CONSUME_LOG:
                enableRemoteConsumeLog = parseValue(value, Boolean.class, false);
                break;
            case ConnectionProperties.REMOTE_CONSUME_LOG_BATCH_SIZE:
                consumeLogBatchSize = parseValue(value, Integer.class, 100);
                break;
            case ConnectionProperties.RECORD_SQL:
                enableRecordSql = parseValue(value, Boolean.class, true);
                break;
            case ConnectionProperties.LEARNER_LEVEL:
                ConfigDataMode.LearnerMode tempLearnerMode = parseValue(
                    value, ConfigDataMode.LearnerMode.class, ConfigDataMode.LearnerMode.ONLY_READ);
                if (tempLearnerMode != null) {
                    learnerMode = tempLearnerMode;
                }
                break;

            case ConnectionProperties.BLOCK_CACHE_MEMORY_SIZE_FACTOR:
                blockCacheMemoryFactor = parseValue(value, Float.class, 0.6f);
                break;
            case ConnectionProperties.CN_DIV_PRECISION_INCREMENT:
                cnDivPrecisionIncrement = parseValue(value, Integer.class, 4);
                break;

            case ConnectionProperties.PREHEATED_CACHE_MAX_ENTRIES:
                preheatedCacheMaxEntries = parseValue(value, Long.class, 4096L);
                break;

            case ConnectionProperties.MPP_QUERY_RESULT_MAX_WAIT_IN_MILLIS:
                mppQueryResultMaxWaitInMillis = parseValue(value, Long.class, 10L);
                break;

            case ConnectionProperties.PURGE_HISTORY_MS: {
                long tempPurgeHistoryMs = parseValue(value, Long.class, 600 * 1000L);
                if (tempPurgeHistoryMs > 0 && tempPurgeHistoryMs < purgeHistoryMs) {
                    purgeHistoryMs = tempPurgeHistoryMs;
                } else {
                    logger.warn("invalid values " + tempPurgeHistoryMs);
                }
                break;
            }

            case ConnectionProperties.MAX_PARTITION_COLUMN_COUNT:
                maxPartitionColumnCount = parseValue(value, Integer.class, maxPartitionColumnCountDefault);
                break;

            case ConnectionProperties.MAX_SESSION_PREPARED_STMT_COUNT:
                maxSessionPreparedStmtCount = parseValue(value, Integer.class, maxSessionPreparedStmtCountDefault);
                break;
            case ConnectionProperties.STATISTIC_IN_DEGRADATION_NUMBER:
                inDegradationNum = parseValue(value, Integer.class, 100);
                break;

            case ConnectionProperties.ENABLE_AUTO_USE_RANGE_FOR_TIME_INDEX:
                enableAutoUseRangeForTimeIndex = parseValue(value, Boolean.class, true);
                break;

            case ConnectionProperties.ENABLE_TRANS_LOG:
                enableTransLog = parseValue(value, Boolean.class, true);
                break;

            case ConnectionProperties.ENABLE_TRANSACTION_STATISTICS:
                enableTransactionStatistics = parseValue(value, Boolean.class, true);
                break;

            case ConnectionProperties.PLAN_CACHE_EXPIRE_TIME:
                planCacheExpireTime = parseValue(value, Integer.class, 12 * 3600 * 1000);   // 12h
                break;
            case ConnectionProperties.ENABLE_COLUMNAR_PLAN_CACHE:
                enableColumnarPlanCache = parseValue(value, Boolean.class, true);
                break;
            case ConnectionProperties.ENABLE_FLOATING_TYPE_PRECISION:
                enableFloatingTypePrecision = parseValue(value, Boolean.class, true);
                break;
            case ConnectionProperties.DEADLOCK_DETECTION_80_FETCH_TRX_ROWS:
                deadlockDetection80FetchTrxRows = parseValue(value, Long.class, 100_000L);
                break;
            case ConnectionProperties.DEADLOCK_DETECTION_DATA_LOCK_WAITS_THRESHOLD:
                deadlockDetectionDataLockWaitsThreshold = parseValue(value, Long.class, 50_000L);
                break;
            case ConnectionProperties.DEADLOCK_DETECTION_SKIP_ROUND:
                deadlockDetectionSkipRound = parseValue(value, Long.class, 10L);
                break;
            case ConnectionProperties.MAX_KEEP_DEADLOCK_LOGS:
                maxKeepDeadlockLogs = parseValue(value, Long.class, 100L);
                break;
            case ConnectionProperties.ENABLE_EXTREME_PERFORMANCE:
                enableExtremePerformance = parseValue(value, Boolean.class, true);
                break;
            case ConnectionProperties.ENBALE_BIND_PARAM_TYPE:
                enableBindType = parseValue(value, Boolean.class, true);
                break;
            case ConnectionProperties.ENBALE_BIND_COLLATE:
                enableBindCollate = parseValue(value, Boolean.class, true);
                break;
            case ConnectionProperties.ENABLE_CLEAN_FAILED_PLAN:
                enableClearFailedPlan = parseValue(value, Boolean.class, true);
                break;
            case ConnectionProperties.USE_PARAMETER_DELEGATE:
                useParameterDelegate = parseValue(value, Boolean.class, true);
                break;
            case ConnectionProperties.USE_JDK_DEFAULT_SER:
                useJdkDefaultSer = parseValue(value, Boolean.class, false);
                break;
            case ConnectionProperties.ENABLE_OR_OPT:
                enableOrOpt = parseValue(value, Boolean.class, true);
                break;
            case ConnectionProperties.TX_ISOLATION:
            case ConnectionProperties.TRANSACTION_ISOLATION:
                String ret = parseValue(value, String.class, "REPEATABLE-READ");
                try {
                    isolation = IsolationLevel.parse(ret).getCode();
                } catch (Throwable t) {
                    //ignore
                }
                break;
            case ConnectionProperties.FOREIGN_KEY_CHECKS:
                foreignKeyChecks = parseValue(value, Boolean.class, true);
                break;
            case ConnectionProperties.ENABLE_XPROTO_RESULT_DECIMAL64:
                enableXResultDecimal64 = parseValue(value, Boolean.class, false);
                break;
            case ConnectionProperties.ENABLE_COLUMNAR_DECIMAL64:
                enableColumnarDecimal64 = parseValue(value, Boolean.class, true);
                break;
            case ConnectionProperties.MAX_CONNECTIONS:
                maxConnections = parseValue(value, Integer.class, 20000);
                break;
            case ConnectionProperties.MAX_ALLOWED_PACKET:
                maxAllowedPacket = parseValue(value, Integer.class, 16 * 1024 * 1024);
                break;
            case ConnectionProperties.PHYSICAL_DDL_MDL_WAITING_TIMEOUT:
                phyiscalMdlWaitTimeout = parseValue(value, Integer.class, 15);
                break;
            case ConnectionProperties.ENABLE_STATEMENTS_SUMMARY:
                int enableStatementsSummary = parseValue(value, Boolean.class, true) ? 1 : 0;
                StatementSummaryManager.getInstance().getConfig().setEnableStmtSummary(enableStatementsSummary);
                break;
            case ConnectionProperties.STATEMENTS_SUMMARY_PERIOD_SEC:
                long stmtSummaryRefreshInterval = parseValue(value, Long.class,
                    (long) StatementSummaryManager.StatementSummaryConfig.USE_DEFAULT_VALUE);
                StatementSummaryManager.getInstance().getConfig()
                    .setStmtSummaryRefreshInterval(stmtSummaryRefreshInterval);
                break;
            case ConnectionProperties.STATEMENTS_SUMMARY_HISTORY_PERIOD_NUM:
                int stmtSummaryHistorySize =
                    parseValue(value, Integer.class, StatementSummaryManager.StatementSummaryConfig.USE_DEFAULT_VALUE);
                StatementSummaryManager.getInstance().getConfig().setStmtSummaryHistorySize(stmtSummaryHistorySize);
                break;
            case ConnectionProperties.STATEMENTS_SUMMARY_MAX_SQL_TEMPLATE_COUNT:
                int stmtSummaryMaxStmtCount =
                    parseValue(value, Integer.class, StatementSummaryManager.StatementSummaryConfig.USE_DEFAULT_VALUE);
                StatementSummaryManager.getInstance().getConfig().setStmtSummaryMaxStmtCount(stmtSummaryMaxStmtCount);
                break;
            case ConnectionProperties.STATEMENTS_SUMMARY_RECORD_INTERNAL:
                int recordIntervalStatement = parseValue(value, Boolean.class, true) ? 1 : 0;
                StatementSummaryManager.getInstance().getConfig().setRecordIntervalStatement(recordIntervalStatement);
                break;
            case ConnectionProperties.STATEMENTS_SUMMARY_MAX_SQL_LENGTH:
                int stmtSummaryMaxSqlLength =
                    parseValue(value, Integer.class, StatementSummaryManager.StatementSummaryConfig.USE_DEFAULT_VALUE);
                StatementSummaryManager.getInstance().getConfig().setStmtSummaryMaxSqlLength(stmtSummaryMaxSqlLength);
                break;
            case ConnectionProperties.STATEMENTS_SUMMARY_PERCENT:
                int stmtSummaryPercent = parseValue(value, Integer.class,
                    StatementSummaryManager.StatementSummaryConfig.DEFAULT_VALUE_PERCENT);
                StatementSummaryManager.getInstance().getConfig().setStmtSummaryPercent(stmtSummaryPercent);
                break;
            case ConnectionProperties.PASSWORD_CHECK_PATTERN:
                String patternStr = parseValue(value, String.class, DEFAULT_PASSWORD_CHECK_PATTERN_STR);
                if (StringUtils.isBlank(patternStr)) {
                    patternStr = DEFAULT_PASSWORD_CHECK_PATTERN_STR;
                }
                Pattern pattern;
                try {
                    pattern = Pattern.compile(patternStr);
                } catch (Throwable t) {
                    logger.error(t.getMessage());
                    pattern = DEFAULT_PASSWORD_CHECK_PATTERN;
                }
                this.passwordCheckPattern = pattern;
                break;
            case ConnectionProperties.DEPRECATE_EOF:
                deprecateEof = parseValue(value, Boolean.class, true);
                break;
            case ConnectionProperties.SLOW_TRANS_THRESHOLD:
                slowTransThreshold = parseValue(value, Integer.class, 3000);
                break;
            case ConnectionProperties.TRANSACTION_STATISTICS_TASK_INTERVAL:
                transactionStatisticsTaskInterval = parseValue(value, Integer.class, 5000);
                break;
            case ConnectionProperties.MAX_CACHED_SLOW_TRANS_STATS:
                maxCachedSlowTransStats = parseValue(value, Integer.class, 1024 * 1024 / 10);
                break;
            case ConnectionProperties.ENABLE_X_PROTO_OPT_FOR_AUTO_SP:
                xProtoOptForAutoSp = parseValue(value, Boolean.class, false);
                break;
            case ConnectionProperties.CHECK_CCI_TASK_CHECKPOINT_LIMIT:
                checkCciCheckpointLimit = parseValue(value, Long.class, 1L);
                break;
            case ConnectionProperties.SKIP_CHECK_CCI_SCHEDULE_JOB:
                skipCheckCciScheduleJob = parseValue(value, Boolean.class, false);
                break;
            case ConnectionProperties.ENABLE_AUTO_GEN_COLUMNAR_SNAPSHOT:
                enableAutoGenColumnarSnapshot = parseValue(value, Boolean.class, false);
                break;
            case ConnectionProperties.AUTO_GEN_COLUMNAR_SNAPSHOT_PARALLELISM:
                autoGenColumnarSnapshotParallelism = parseValue(value, Integer.class, 4);
                break;
            case ConnectionProperties.DATABASE_DEFAULT_SINGLE:
                databaseDefaultSingle = parseValue(value, Boolean.class, false);
                break;
            case ConnectionProperties.ENABLE_2PC_OPT:
                enable2pcOpt = parseValue(value, Boolean.class, false);
                break;
            case ConnectionProperties.COMPATIBLE_CHARSET_VARIABLES:
                compatibleCharsetVariables = parseValue(value, Boolean.class, false);
                break;
            case ConnectionProperties.VERSION_PREFIX:
                String versionPrefix = parseValue(value, String.class, null);
                InstanceVersion.reloadVersion(versionPrefix);
                break;
            case ConnectionProperties.TRX_LOG_METHOD:
                trxLogMethod = parseValue(value, Integer.class, 0);
                break;
            case ConnectionProperties.TRX_LOG_CLEAN_INTERVAL:
                trxLogCleanInterval = parseValue(value, Integer.class, 30);
                break;
            case ConnectionProperties.SKIP_LEGACY_LOG_TABLE_CLEAN:
                skipLegacyLogTableClean = parseValue(value, Boolean.class, false);
                break;
            case ConnectionProperties.WARM_UP_DB_PARALLELISM:
                warmUpDbParallelism = parseValue(value, Integer.class, 1);
                break;
            case ConnectionProperties.WARM_UP_DB_INTERVAL:
                warmUpDbInterval = parseValue(value, Long.class, 60L);
                break;
            case ConnectionProperties.MAX_PARTITION_NAME_LENGTH: {
                int newPartNameLength = parseValue(value, Integer.class,
                    Integer.valueOf(ConnectionParams.MAX_PARTITION_NAME_LENGTH.getDefault()));
                /**
                 * For protect partition meta. The max allowed length of (sub)partition name in metadb is 64
                 * , but subpartName = partName+subpartTempName, so the max allowed length of partition name
                 * should be 32.
                 */
                if (newPartNameLength > 32) {
                    newPartNameLength = 32;
                }
                if (newPartNameLength < 0) {
                    newPartNameLength = Integer.valueOf(ConnectionParams.MAX_PARTITION_NAME_LENGTH.getDefault());
                }
                maxPartitionNameLength = newPartNameLength;

            }
            break;
            case ConnectionProperties.ENABLE_HLL:
                enableHll = parseValue(value, Boolean.class, true);
                break;
            case ConnectionProperties.ENABLE_TRX_EVENT_LOG:
                enableTrxEventLog = parseValue(value, Boolean.class, true);
                break;
            case ConnectionProperties.ENABLE_TRX_DEBUG_MODE:
                enableTrxDebugMode = parseValue(value, Boolean.class, false);
                break;
            case ConnectionProperties.INSTANCE_READ_ONLY:
                instanceReadOnly = parseValue(value, Boolean.class, false);
                break;
            case ConnectionProperties.MAPPING_TO_MYSQL_ERROR_CODE:
                errorCodeMapping = initErrorCodeMapping(value);
                break;
            case ConnectionProperties.ENABLE_ACCURATE_INFO_SCHEMA_TABLES:
                enableAccurateInfoSchemaTables = parseValue(value, Boolean.class, false);
                break;
            case ConnectionProperties.ENABLE_SYNC_POINT:
                enableSyncPoint = parseValue(value, Boolean.class, false);
                break;
            case ConnectionProperties.SYNC_POINT_TASK_INTERVAL:
                syncPointTaskInterval = parseValue(value, Integer.class, 5000);
                break;
            case ConnectionProperties.DISABLE_LEGACY_VARIABLE:
                disableLegacyVariable = parseValue(value, Boolean.class, true);
                break;
            case ConnectionProperties.CCI_INCREMENTAL_CHECK_PARALLELISM:
                cciIncrementalCheckParallelism = parseValue(value, Integer.class, 8);
                break;
            case ConnectionProperties.CCI_INCREMENTAL_CHECK_BATCH_SIZE:
                cciIncrementalCheckBatchSize = parseValue(value, Integer.class, 128);
                break;
            case ConnectionProperties.ENABLE_COLUMNAR_DEBUG:
                enableColumnarDebug = parseValue(value, Boolean.class, false);
                break;
            case ConnectionProperties.PRUNING_TIME_WARNING_THRESHOLD:
                pruningTimeWarningThreshold = parseValue(value, Long.class, 500L);
                break;
            case ConnectionProperties.ENABLE_PRUNING_IN:
                enablePruningIn = parseValue(value, Boolean.class, true);
                break;
            case ConnectionProperties.ENABLE_PRUNING_IN_DML:
                enablePruningInDml = parseValue(value, Boolean.class, true);
                break;
            case ConnectionProperties.ENABLE_MQ_CACHE_COST_BY_THREAD:
                enableMQCacheByThread = parseValue(value, Boolean.class, true);
                break;
            case ConnectionProperties.NDV_ALIKE_PRECENTAGE_THRESHOLD:
                ndvAlikePercentageThreshold = parseValue(value, Long.class, 10L);
                break;
            case ConnectionProperties.ENABLE_USE_KEY_FOR_ALL_LOCAL_INDEX:
                enableUseKeyForAllLocalIndex = parseValue(value, Boolean.class, false);
                break;
            case ConnectionProperties.ENABLE_PARAM_TYPE_CHANGE:
                enableChangeParamTypeByMeta = parseValue(value, Boolean.class, false);
                break;
            case TddlConstants.BLACK_LIST_CONF:
                String blockLists = parseValue(value, String.class, "");
                List<String> tempBlackList = new ArrayList<>();
                if (StringUtils.isNotBlank(blockLists)) {
                    String[] blockListArr = blockLists.split(",");
                    for (String blockList : blockListArr) {
                        if (StringUtils.isNotBlank(blockList)) {
                            tempBlackList.add(blockList.toLowerCase(Locale.ROOT));
                        }
                    }
                }
                blackListConf = tempBlackList;
                break;
            case ConnectionProperties.ALLOW_COLUMNAR_BIND_MASTER:
                columnarBindMaster = parseValue(value, Boolean.class, false);
                break;
            case ConnectionProperties.SHOW_COLUMNAR_STATUS_USE_SUB_QUERY:
                showColumnarStatusUseSubQuery = parseValue(value, Boolean.class, false);
                break;

            case ConnectionProperties.OSS_STREAM_BUFFER_SIZE:
                ossStreamBufferSize = parseValue(value, Integer.class, 8192);
                break;

            case ConnectionProperties.TTL_GLOBAL_SELECT_WORKER_COUNT: {
                ttlGlobalSelectWorkerCount = parseValue(value, Integer.class,
                    Integer.valueOf(ConnectionParams.TTL_GLOBAL_SELECT_WORKER_COUNT.getDefault()));
            }
            break;

            case ConnectionProperties.TTL_GLOBAL_DELETE_WORKER_COUNT: {
                ttlGlobalDeleteWorkerCount = parseValue(value, Integer.class,
                    Integer.valueOf(ConnectionParams.TTL_GLOBAL_DELETE_WORKER_COUNT.getDefault()));
            }
            break;

            case ConnectionProperties.TTL_TMP_TBL_MAX_DATA_LENGTH: {
                ttlTmpTableMaxDataLength = parseValue(value, Long.class,
                    Long.valueOf(ConnectionParams.TTL_TMP_TBL_MAX_DATA_LENGTH.getDefault()));
            }
            break;

            case ConnectionProperties.TTL_TBL_MAX_DATA_FREE_PERCENT: {
                ttlTmpTableMaxDataFreePercent = parseValue(value, Integer.class,
                    Integer.valueOf(ConnectionParams.TTL_TBL_MAX_DATA_FREE_PERCENT.getDefault()));
            }
            break;

            case ConnectionProperties.TTL_INTRA_TASK_INTERRUPTION_MAX_WAIT_TIME: {
                ttlIntraTaskInterruptionMaxWaitTime = parseValue(value, Integer.class,
                    Integer.valueOf(ConnectionParams.TTL_INTRA_TASK_INTERRUPTION_MAX_WAIT_TIME.getDefault()));
            }
            break;

            case ConnectionProperties.TTL_INTRA_TASK_MONITOR_EACH_ROUTE_WAIT_TIME: {
                ttlIntraTaskMonitorEachRoundWaitTime = parseValue(value, Integer.class,
                    Integer.valueOf(ConnectionParams.TTL_INTRA_TASK_MONITOR_EACH_ROUTE_WAIT_TIME.getDefault()));
            }
            break;

            case ConnectionProperties.TTL_ENABLE_AUTO_OPTIMIZE_TABLE_IN_TTL_JOB: {
                ttlEnableAutoOptimizeTableInTtlJob = parseValue(value, Boolean.class,
                    Boolean.valueOf(ConnectionParams.TTL_ENABLE_AUTO_OPTIMIZE_TABLE_IN_TTL_JOB.getDefault()));
            }
            break;

            case ConnectionProperties.TTL_ENABLE_AUTO_EXEC_OPTIMIZE_TABLE_AFTER_ARCHIVING: {
                ttlEnableAutoExecOptimizeTableAfterArchiving = parseValue(value, Boolean.class,
                    Boolean.valueOf(ConnectionParams.TTL_ENABLE_AUTO_EXEC_OPTIMIZE_TABLE_AFTER_ARCHIVING.getDefault()));
            }
            break;

            case ConnectionProperties.TTL_SCHEDULED_JOB_MAX_PARALLELISM: {
                ttlScheduledJobMaxParallelism = parseValue(value, Integer.class,
                    Integer.valueOf(ConnectionParams.TTL_SCHEDULED_JOB_MAX_PARALLELISM.getDefault()));

            }
            break;

            case ConnectionProperties.TTL_JOB_DEFAULT_BATCH_SIZE: {
                ttlJobDefaultBatchSize = parseValue(value, Integer.class,
                    Integer.valueOf(ConnectionParams.TTL_JOB_DEFAULT_BATCH_SIZE.getDefault()));
            }
            break;

            case ConnectionProperties.TTL_CLEANUP_BOUND_INTERVAL_COUNT: {
                ttlCleanupBoundIntervalCount = parseValue(value, Integer.class,
                    Integer.valueOf(ConnectionParams.TTL_CLEANUP_BOUND_INTERVAL_COUNT.getDefault()));
            }
            break;

            case ConnectionProperties.TTL_STOP_ALL_JOB_SCHEDULING: {
                ttlStopAllJobScheduling = parseValue(value, Boolean.class,
                    Boolean.valueOf(ConnectionParams.TTL_STOP_ALL_JOB_SCHEDULING.getDefault()));
            }
            break;

            case ConnectionProperties.TTL_USE_ARCHIVE_TRANS_POLICY: {
                ttlUseArchiveTransPolicy = parseValue(value, Boolean.class,
                    Boolean.valueOf(ConnectionParams.TTL_USE_ARCHIVE_TRANS_POLICY.getDefault()));
            }
            break;

            case ConnectionProperties.TTL_SELECT_MERGE_UNION_SIZE: {
                ttlSelectMergeUnionSize = parseValue(value, Integer.class,
                    Integer.valueOf(ConnectionParams.TTL_SELECT_MERGE_UNION_SIZE.getDefault()));
            }
            break;

            case ConnectionProperties.TTL_SELECT_MERGE_CONCURRENT: {
                ttlSelectMergeConcurrent = parseValue(value, Boolean.class,
                    Boolean.valueOf(ConnectionParams.TTL_SELECT_MERGE_CONCURRENT.getDefault()));
            }
            break;

            case ConnectionProperties.TTL_SELECT_STMT_HINT: {
                ttlSelectStmtHint = parseValue(value, String.class,
                    String.valueOf(ConnectionParams.TTL_SELECT_STMT_HINT.getDefault()));
            }
            break;

            case ConnectionProperties.TTL_DELETE_STMT_HINT: {
                ttlDeleteStmtHint = parseValue(value, String.class,
                    String.valueOf(ConnectionParams.TTL_DELETE_STMT_HINT.getDefault()));
            }
            break;

            case ConnectionProperties.TTL_INSERT_STMT_HINT: {
                ttlInsertStmtHint = parseValue(value, String.class,
                    String.valueOf(ConnectionParams.TTL_INSERT_STMT_HINT.getDefault()));
            }
            break;

            case ConnectionProperties.TTL_OPTIMIZE_TABLE_STMT_HINT: {
                ttlOptimizeTableStmtHint = parseValue(value, String.class,
                    String.valueOf(ConnectionParams.TTL_OPTIMIZE_TABLE_STMT_HINT.getDefault()));
            }
            break;

            case ConnectionProperties.TTL_ALTER_ADD_PART_STMT_HINT: {
                ttlAlterTableAddPartsStmtHint = parseValue(value, String.class,
                    String.valueOf(ConnectionParams.TTL_ALTER_ADD_PART_STMT_HINT.getDefault()));
            }
            break;

            case ConnectionProperties.TTL_GROUP_PARALLELISM_ON_DQL_CONN: {
                ttlGroupParallelismOnDqlConn = parseValue(value, Long.class,
                    Long.valueOf(ConnectionParams.TTL_GROUP_PARALLELISM_ON_DQL_CONN.getDefault()));
            }
            break;

            case ConnectionProperties.TTL_GROUP_PARALLELISM_ON_DML_CONN: {
                ttlGroupParallelismOnDmlConn = parseValue(value, Long.class,
                    Long.valueOf(ConnectionParams.TTL_GROUP_PARALLELISM_ON_DML_CONN.getDefault()));
            }
            break;

            case ConnectionProperties.TTL_ADD_MAXVAL_PART_ON_CCI_CREATING: {
                ttlAddMaxValPartOnCciCreating = parseValue(value, Boolean.class,
                    Boolean.valueOf(ConnectionParams.TTL_ADD_MAXVAL_PART_ON_CCI_CREATING.getDefault()));
            }
            break;

            case ConnectionProperties.TTL_MAX_WAIT_ACQUIRE_RATE_PERMITS_PERIODS: {
                ttlMaxWaitAcquireRatePermitsPeriods = parseValue(value, Long.class,
                    Long.valueOf(ConnectionParams.TTL_MAX_WAIT_ACQUIRE_RATE_PERMITS_PERIODS.getDefault()));
            }
            break;

            case ConnectionProperties.TTL_ENABLE_CLEANUP_ROWS_SPEED_LIMIT: {
                ttlEnableCleanupRowsSpeedLimit = parseValue(value, Boolean.class,
                    Boolean.valueOf(ConnectionParams.TTL_ENABLE_CLEANUP_ROWS_SPEED_LIMIT.getDefault()));
            }
            break;

            case ConnectionProperties.TTL_CLEANUP_ROWS_SPEED_LIMIT_EACH_DN: {
                ttlCleanupRowsSpeedLimitEachDn = parseValue(value, Long.class,
                    Long.valueOf(ConnectionParams.TTL_CLEANUP_ROWS_SPEED_LIMIT_EACH_DN.getDefault()));
            }
            break;

            case ConnectionProperties.TTL_IGNORE_MAINTAIN_WINDOW_IN_DDL_JOB: {
                ttlIgnoreMaintainWindowInDdlJob = parseValue(value, Boolean.class,
                    Boolean.valueOf(ConnectionParams.TTL_IGNORE_MAINTAIN_WINDOW_IN_DDL_JOB.getDefault()));
            }
            break;

            case ConnectionProperties.TTL_GLOBAL_WORKER_DN_RATIO: {
                ttlGlobalWorkerDnRatio = parseValue(value, Integer.class,
                    Integer.valueOf(ConnectionParams.TTL_GLOBAL_WORKER_DN_RATIO.getDefault()));
            }
            break;

            case ConnectionProperties.TTL_DEFAULT_ARC_PRE_ALLOCATE_COUNT: {
                ttlDefaultArcPreAllocateCount = parseValue(value, Integer.class,
                    Integer.valueOf(ConnectionParams.TTL_DEFAULT_ARC_PRE_ALLOCATE_COUNT.getDefault()));
            }
            break;

            case ConnectionProperties.TTL_DEFAULT_ARC_POST_ALLOCATE_COUNT: {
                ttlDefaultArcPostAllocateCount = parseValue(value, Integer.class,
                    Integer.valueOf(ConnectionParams.TTL_DEFAULT_ARC_POST_ALLOCATE_COUNT.getDefault()));
            }
            break;

            case ConnectionProperties.TTL_ENABLE_AUTO_ADD_PARTS_FOR_ARC_CCI: {
                ttlEnableAutoAddPartsForArcCci = parseValue(value, Boolean.class,
                    Boolean.valueOf(ConnectionParams.TTL_ENABLE_AUTO_ADD_PARTS_FOR_ARC_CCI.getDefault()));
            }
            break;

            case ConnectionProperties.WAIT_FOR_COLUMNAR_COMMIT_MS:
                waitForColumnarCommitMS = parseValue(value, Long.class, 60000L);
            default:
                FileConfig.getInstance().loadValue(logger, key, value);
                break;
            }
        }
    }

    private static final long generalDynamicSpeedLimitationDefault =
        parseValue(ConnectionParams.GENERAL_DYNAMIC_SPEED_LIMITATION.getDefault(), Long.class, -1L);
    private volatile long generalDynamicSpeedLimitation = generalDynamicSpeedLimitationDefault;

    public long getGeneralDynamicSpeedLimitation() {
        return generalDynamicSpeedLimitation;
    }

    private static final long xprotoMaxDnConcurrentDefault =
        parseValue(ConnectionParams.XPROTO_MAX_DN_CONCURRENT.getDefault(), Long.class, 2000L);
    private volatile long xprotoMaxDnConcurrent = xprotoMaxDnConcurrentDefault;

    public long getXprotoMaxDnConcurrent() {
        return xprotoMaxDnConcurrent;
    }

    private static final long xprotoMaxDnWaitConnectionDefault =
        parseValue(ConnectionParams.XPROTO_MAX_DN_WAIT_CONNECTION.getDefault(), Long.class, 2000L);
    private volatile long xprotoMaxDnWaitConnection = xprotoMaxDnWaitConnectionDefault;

    public long getXprotoMaxDnWaitConnection() {
        return xprotoMaxDnWaitConnection;
    }

    // XPROTO_ALWAYS_KEEP_FILTER_ON_XPLAN_GET
    private static final boolean xprotoAlwaysKeepFilterOnXplanGetDefault =
        parseValue(ConnectionParams.XPROTO_ALWAYS_KEEP_FILTER_ON_XPLAN_GET.getDefault(), Boolean.class, true);
    private volatile boolean xprotoAlwaysKeepFilterOnXplanGet = xprotoAlwaysKeepFilterOnXplanGetDefault;

    public boolean getXprotoAlwaysKeepFilterOnXplanGet() {
        return xprotoAlwaysKeepFilterOnXplanGet;
    }

    // XPROTO_PROBE_TIMEOUT
    private static final int xprotoProbeTimeoutDefault =
        parseValue(ConnectionParams.XPROTO_PROBE_TIMEOUT.getDefault(), Integer.class, 5000);
    private volatile int xprotoProbeTimeout = xprotoProbeTimeoutDefault;

    public int getXprotoProbeTimeout() {
        return xprotoProbeTimeout;
    }

    private static final boolean xprotoGalaxyPrepareDefault =
        parseValue(ConnectionParams.XPROTO_GALAXY_PREPARE.getDefault(), Boolean.class, false);
    private volatile boolean xprotoGalaxyPrepare = xprotoGalaxyPrepareDefault;

    public boolean getXprotoGalaxyPrepare() {
        return xprotoGalaxyPrepare;
    }

    private static final int xprotoFlowControlSizeKbDefault =
        parseValue(ConnectionParams.XPROTO_FLOW_CONTROL_SIZE_KB.getDefault(), Integer.class, 10240);
    private volatile int xprotoFlowControlSizeKb = xprotoFlowControlSizeKbDefault;

    public int getXprotoFlowControlSizeKb() {
        return xprotoFlowControlSizeKb;
    }

    private static final int xprotoTcpAgingDefault =
        parseValue(ConnectionParams.XPROTO_TCP_AGING.getDefault(), Integer.class, 28800);
    private volatile int xprotoTcpAging = xprotoTcpAgingDefault;

    public int getXprotoTcpAging() {
        return xprotoTcpAging;
    }

    private static final long autoPartitionPartitionsDefault =
        parseValue(ConnectionParams.AUTO_PARTITION_PARTITIONS.getDefault(), Long.class, 64L);
    private volatile long autoPartitionPartitions = autoPartitionPartitionsDefault;

    private static final long autoPartitionCciPartitionsDefault =
        parseValue(ConnectionParams.COLUMNAR_DEFAULT_PARTITIONS.getDefault(), Long.class, 64L);
    private volatile long autoPartitionCciPartitions = autoPartitionCciPartitionsDefault;

    private static final float blockCacheMemoryFactorDefault =
        parseValue(ConnectionParams.BLOCK_CACHE_MEMORY_SIZE_FACTOR.getDefault(), Float.class, 0.6f);
    private volatile float blockCacheMemoryFactor = blockCacheMemoryFactorDefault;

    private static final long preheatedCacheMaxEntriesDefault =
        parseValue(ConnectionParams.PREHEATED_CACHE_MAX_ENTRIES.getDefault(), Long.class, 4096L);
    private volatile long preheatedCacheMaxEntries = preheatedCacheMaxEntriesDefault;

    private static final long mppQueryResultMaxWaitInMillisDefault =
        parseValue(ConnectionParams.MPP_QUERY_RESULT_MAX_WAIT_IN_MILLIS.getDefault(), Long.class, 10L);
    private volatile long mppQueryResultMaxWaitInMillis = mppQueryResultMaxWaitInMillisDefault;

    public long getMppQueryResultMaxWaitInMillis() {
        return mppQueryResultMaxWaitInMillis;
    }

    public long getPreheatedCacheMaxEntries() {
        return preheatedCacheMaxEntries;
    }

    private static final int cnDivPrecisionIncrementDefault =
        parseValue(ConnectionParams.CN_DIV_PRECISION_INCREMENT.getDefault(), Integer.class, 4);
    private volatile int cnDivPrecisionIncrement = cnDivPrecisionIncrementDefault;

    public int getCnDivPrecisionIncrement() {
        return cnDivPrecisionIncrement;
    }

    public float getBlockCacheMemoryFactor() {
        return blockCacheMemoryFactor;
    }

    public long getAutoPartitionPartitions(boolean isColumnar) {
        return isColumnar ? autoPartitionCciPartitions : autoPartitionPartitions;
    }

    public long getAutoPartitionCciPartitions() {
        return autoPartitionCciPartitions;
    }

    private volatile int delayThreshold = 3;

    public int getDelayThreshold() {
        return delayThreshold;
    }

    private volatile boolean enableOptimizerAlert = true;

    public boolean optimizerAlert() {
        return enableOptimizerAlert;
    }

    private volatile boolean enableOptimizerAlertLog = true;

    public boolean optimizerAlertLog() {
        return enableOptimizerAlertLog;
    }

    // default 10 min
    private volatile long optimizerAlertLogInterval = 10 * 60 * 1000;

    public long getOptimizerAlertLogInterval() {
        return optimizerAlertLogInterval;
    }

    private volatile boolean enableHotGsiEvolution = true;

    public boolean enableHotGsiEvolution() {
        return enableHotGsiEvolution;
    }

    private volatile int tpSlowAlertThreshold = 10;

    public int getTpSlowAlertThreshold() {
        return tpSlowAlertThreshold;
    }

    private volatile int busyThreshold = 100;

    public int getBusyThreshold() {
        return busyThreshold;
    }

    private volatile int groupingTimeout = 3000;

    public int getGroupingTimeout() {
        return groupingTimeout;
    }

    private volatile int groupingThread = 4;

    public int getGroupingThread() {
        return groupingThread;
    }

    private volatile boolean isBasedCDC = true;

    public boolean isBasedCDC() {
        return isBasedCDC;
    }

    private volatile boolean enableTransLog = true;

    private volatile boolean enableTransactionStatistics = true;

    public boolean isEnableTransLog() {
        return enableTransLog;
    }

    public boolean isEnableTransactionStatistics() {
        return enableTransactionStatistics;
    }

    private volatile boolean enableCreateGroupDataSource = false;

    public boolean forceCreateGroupDataSource() {
        return enableCreateGroupDataSource;
    }

    private volatile boolean enablePlanTypeDigest = true;

    public boolean enablePlanTypeDigest() {
        return enablePlanTypeDigest;
    }

    private volatile boolean enablePlanTypeDigestStrictMode = false;

    public boolean enablePlanTypeDigestStrictMode() {
        return enablePlanTypeDigestStrictMode;
    }

    private volatile long purgeHistoryMs = 10 * 60 * 1000L;

    public long getPurgeHistoryMs() {
        return purgeHistoryMs;
    }

    private volatile int planCacheExpireTime = 12 * 3600 * 1000; // 12h

    public int planCacheExpireTime() {
        return planCacheExpireTime;
    }

    private volatile boolean enableColumnarPlanCache = true;

    public boolean colPlanCache() {
        return enableColumnarPlanCache;
    }

    private volatile boolean enableFloatingTypePrecision = true;

    public boolean isEnableFloatingTypePrecision() {
        return enableFloatingTypePrecision;
    }

    private volatile long deadlockDetection80FetchTrxRows = 100_000L;

    public long getDeadlockDetection80FetchTrxRows() {
        return deadlockDetection80FetchTrxRows;
    }

    private volatile long deadlockDetectionDataLockWaitsThreshold = 50_000L;

    public long getDeadlockDetectionDataLockWaitsThreshold() {
        return deadlockDetectionDataLockWaitsThreshold;
    }

    private volatile long deadlockDetectionSkipRound = 10L;

    public long getDeadlockDetectionSkipRound() {
        return deadlockDetectionSkipRound;
    }

    private volatile long maxKeepDeadlockLogs = 100L;

    public long getMaxKeepDeadlockLogs() {
        return maxKeepDeadlockLogs;
    }

    private static final int maxPartitionColumnCountDefault =
        parseValue(ConnectionParams.MAX_PARTITION_COLUMN_COUNT.getDefault(), Integer.class, 3);
    private volatile int maxPartitionColumnCount = maxPartitionColumnCountDefault;

    public int getMaxPartitionColumnCount() {
        return maxPartitionColumnCount;
    }

    private volatile boolean enableExtremePerformance = false;

    public boolean enableExtremePerformance() {
        return enableExtremePerformance;
    }

    private volatile boolean enableBindType = true;

    public boolean enableBindType() {
        return enableBindType;
    }

    private volatile boolean enableBindCollate = false;

    public boolean enableBindCollate() {
        return enableBindCollate;
    }

    private volatile boolean enableClearFailedPlan = true;

    public boolean enableClearFailedPlan() {
        return enableClearFailedPlan;
    }

    private volatile boolean useParameterDelegate = true;

    public boolean useParameterDelegate() {
        return useParameterDelegate;
    }

    private volatile boolean useJdkDefaultSer = true;

    public boolean useJdkDefaultSer() {
        return useJdkDefaultSer;
    }

    private volatile boolean enableXResultDecimal64 = false;

    public boolean enableXResultDecimal64() {
        return enableXResultDecimal64;
    }

    private volatile boolean enableColumnarDecimal64 = true;

    public boolean enableColumnarDecimal64() {
        return enableColumnarDecimal64;
    }

    private volatile boolean enableOrOpt = true;

    public boolean useOrOpt() {
        return enableOrOpt;
    }

    private volatile boolean enableHll = true;

    public boolean enableHll() {
        return enableHll;
    }

    private volatile boolean enableMQCacheByThread = true;

    public boolean isEnableMQCacheByThread() {
        return enableMQCacheByThread;
    }

    private volatile int inDegradationNum =
        parseValue(ConnectionParams.STATISTIC_IN_DEGRADATION_NUMBER.getDefault(), Integer.class, 100);

    public int getInDegradationNum() {
        return inDegradationNum;
    }

    private static final int maxSessionPreparedStmtCountDefault =
        parseValue(ConnectionParams.MAX_SESSION_PREPARED_STMT_COUNT.getDefault(), Integer.class, 256);

    private volatile int maxSessionPreparedStmtCount = maxSessionPreparedStmtCountDefault;

    public int getMaxSessionPreparedStmtCount() {
        return maxSessionPreparedStmtCount;
    }

    private static final boolean enableAutoUseRangeForTimeIndexDefault =
        parseValue(ConnectionParams.ENABLE_AUTO_USE_RANGE_FOR_TIME_INDEX.getDefault(), Boolean.class, true);
    private volatile boolean enableAutoUseRangeForTimeIndex = enableAutoUseRangeForTimeIndexDefault;

    public boolean isEnableAutoUseRangeForTimeIndex() {
        return enableAutoUseRangeForTimeIndex;
    }

    private static final String DEFAULT_PASSWORD_CHECK_PATTERN_STR = "^[0-9A-Za-z!@#$%^&*()_+=-]{6,32}$";
    private static final Pattern DEFAULT_PASSWORD_CHECK_PATTERN =
        Pattern.compile(DEFAULT_PASSWORD_CHECK_PATTERN_STR);

    private volatile Pattern passwordCheckPattern = DEFAULT_PASSWORD_CHECK_PATTERN;

    public Pattern getPasswordCheckPattern() {
        return passwordCheckPattern;
    }

    public boolean isDefaultPasswordCheckPattern() {
        return DEFAULT_PASSWORD_CHECK_PATTERN_STR.equals(passwordCheckPattern.pattern());
    }

    private volatile boolean deprecateEof = true;

    public boolean enableDeprecateEof() {
        return deprecateEof;
    }

    private volatile boolean supportFollowRead = false;

    public boolean enableFollowReadForPolarDBX() {
        return supportFollowRead;
    }

    private volatile boolean enableShareReadviewInRc = false;

    public boolean isEnableShareReadviewInRc() {
        return enableShareReadviewInRc;
    }

    /**
     * Slow transaction threshold, unit: microsecond, default 3s.
     */
    private volatile long slowTransThreshold = 3000;

    public long getSlowTransThreshold() {
        return slowTransThreshold;
    }

    /**
     * Foreign key checks, default true.
     */
    private volatile boolean foreignKeyChecks = true;

    public boolean getForeignKeyChecks() {
        return foreignKeyChecks;
    }

    /**
     * Interval of task collecting slow transaction statistics, default 5s.
     */
    private volatile long transactionStatisticsTaskInterval = 5000;

    public long getTransactionStatisticsTaskInterval() {
        return transactionStatisticsTaskInterval;
    }

    /**
     * Consume at most 16 MB memory. (approximate 160 Bytes per object.)
     */
    private volatile long maxCachedSlowTransStats = 1024 * 1024 / 10;

    public long getMaxCachedSlowTransStats() {
        return maxCachedSlowTransStats;
    }

    private volatile int isolation = 4;

    public int getTxIsolation() {
        return isolation;
    }

    private volatile boolean xProtoOptForAutoSp = false;

    public boolean enableXProtoOptForAutoSp() {
        return xProtoOptForAutoSp;
    }

    private volatile long checkCciCheckpointLimit = 1;

    public long getCheckCciCheckpointLimit() {
        return checkCciCheckpointLimit;
    }

    private volatile boolean skipCheckCciScheduleJob = false;

    public boolean isSkipCheckCciScheduleJob() {
        return skipCheckCciScheduleJob;
    }

    private volatile boolean enableAutoGenColumnarSnapshot = true;

    public boolean isEnableAutoGenColumnarSnapshot() {
        return enableAutoGenColumnarSnapshot;
    }

    private volatile int autoGenColumnarSnapshotParallelism = 4;

    public int getAutoGenColumnarSnapshotParallelism() {
        return autoGenColumnarSnapshotParallelism;
    }

    private volatile boolean enableRemoteConsumeLog = false;

    public boolean enableRemoteConsumeLog() {
        return enableRemoteConsumeLog;
    }

    private volatile int consumeLogBatchSize = 1000;

    public int consumeLogBatchSize() {
        return consumeLogBatchSize;
    }

    private volatile boolean enableRecordSql = true;

    public boolean enableRecordSql() {
        return enableRecordSql;
    }

    private volatile boolean databaseDefaultSingle = false;

    public boolean isDatabaseDefaultSingle() {
        return databaseDefaultSingle;
    }

    private volatile boolean compatibleCharsetVariables = false;

    public boolean isCompatibleCharsetVariables() {
        return compatibleCharsetVariables;
    }

    private volatile ConfigDataMode.LearnerMode learnerMode = ConfigDataMode.LearnerMode.ONLY_READ;

    public ConfigDataMode.LearnerMode learnerMode() {
        return learnerMode;
    }

    private volatile boolean enable2pcOpt = false;

    public boolean isEnable2pcOpt() {
        return enable2pcOpt;
    }

    //---------------  the followed setting is for test -------------------
    private boolean supportSingleDbMultiTbs = false;
    private boolean supportRemoveDdl = false;
    private boolean supportDropAutoSeq = false;
    private boolean allowSimpleSequence = false;

    public boolean isSupportSingleDbMultiTbs() {
        return supportSingleDbMultiTbs;
    }

    public void setSupportSingleDbMultiTbs(boolean supportSingleDbMultiTbs) {
        this.supportSingleDbMultiTbs = supportSingleDbMultiTbs;
    }

    private volatile boolean enableChangeParamTypeByMeta = false;

    public boolean isEnableChangeParamTypeByMeta() {
        return enableChangeParamTypeByMeta;
    }

    public void setEnableChangeParamTypeByMeta(boolean enableChangeParamTypeByMeta) {
        this.enableChangeParamTypeByMeta = enableChangeParamTypeByMeta;
    }

    public boolean isSupportRemoveDdl() {
        return supportRemoveDdl;
    }

    public void setSupportRemoveDdl(boolean supportRemoveDdl) {
        this.supportRemoveDdl = supportRemoveDdl;
    }

    public boolean isSupportDropAutoSeq() {
        return supportDropAutoSeq;
    }

    public void setSupportDropAutoSeq(boolean supportDropAutoSeq) {
        this.supportDropAutoSeq = supportDropAutoSeq;
    }

    public boolean isAllowSimpleSequence() {
        return allowSimpleSequence;
    }

    public void setAllowSimpleSequence(boolean allowSimpleSequence) {
        this.allowSimpleSequence = allowSimpleSequence;
    }

    private volatile int trxLogMethod = 0;

    public int getTrxLogMethod() {
        return trxLogMethod;
    }

    private volatile long trxLogCleanInterval = 30;

    public long getTrxLogCleanInterval() {
        return trxLogCleanInterval;
    }

    private volatile boolean skipLegacyLogTableClean = false;

    public boolean isSkipLegacyLogTableClean() {
        return skipLegacyLogTableClean;
    }

    private volatile int warmUpDbParallelism = 1;

    public int getWarmUpDbParallelism() {
        return warmUpDbParallelism < 0 ? 1 : warmUpDbParallelism;
    }

    private String columnarOssDirectory;

    public String getColumnarOssDirectory() {
        return columnarOssDirectory;
    }

    public void setColumnarOssDirectory(String columnarOssDirectory) {
        this.columnarOssDirectory = columnarOssDirectory;
    }

    int ossStreamBufferSize = 8192;

    public int getOssStreamBufferSize() {
        return ossStreamBufferSize;
    }

    /**
     * Default 60s.
     */
    private volatile long warmUpDbInterval = 60;

    public long getWarmUpDbInterval() {
        return warmUpDbInterval;
    }

    public int maxPartitionNameLength = Integer.valueOf(ConnectionParams.MAX_PARTITION_NAME_LENGTH.getDefault());

    public int getMaxPartitionNameLength() {
        return maxPartitionNameLength;
    }

    private volatile int maxConnections = 20000;

    public int getMaxConnections() {
        return maxConnections;
    }

    private volatile int maxAllowedPacket = 16 * 1024 * 1024;

    public int getMaxAllowedPacket() {
        return maxAllowedPacket;
    }

    private volatile boolean enableTrxEventLog = true;

    public boolean isEnableTrxEventLog() {
        return enableTrxEventLog;
    }

    private volatile boolean enableTrxDebugMode = false;

    public boolean isEnableTrxDebugMode() {
        return enableTrxDebugMode;
    }

    private volatile int phyiscalMdlWaitTimeout = 15;

    public int getPhyiscalMdlWaitTimeout() {
        return phyiscalMdlWaitTimeout;
    }

    private volatile boolean instanceReadOnly = false;

    public boolean isInstanceReadOnly() {
        return instanceReadOnly;
    }

    private volatile Map<Integer, Integer> errorCodeMapping = new HashMap<>();

    public Map<Integer, Integer> getErrorCodeMapping() {
        return errorCodeMapping;
    }

    private Map<Integer, Integer> initErrorCodeMapping(String mapping) {
        if (TStringUtil.isNotBlank(mapping)) {
            try {
                return JSON.parseObject(mapping, new TypeReference<Map<Integer, Integer>>() {
                }, Feature.IgnoreAutoType);
            } catch (Exception ignored) {
            }
        }
        return new HashMap<>();
    }

    private volatile boolean enableAccurateInfoSchemaTables = false;

    public boolean isEnableAccurateInfoSchemaTables() {
        return enableAccurateInfoSchemaTables;
    }

    private boolean enableUseKeyForAllLocalIndex =
        Boolean.valueOf(ConnectionParams.ENABLE_USE_KEY_FOR_ALL_LOCAL_INDEX.getDefault());

    public boolean isEnableUseKeyForAllLocalIndex() {
        return enableUseKeyForAllLocalIndex;
    }

    private volatile boolean enableSyncPoint = false;

    public boolean isEnableSyncPoint() {
        return enableSyncPoint;
    }

    /**
     * Default 5 * 60 * 1000 ms.
     */
    private volatile long syncPointTaskInterval = 5 * 60 * 1000;

    public long getSyncPointTaskInterval() {
        return syncPointTaskInterval;
    }

    private volatile boolean disableLegacyVariable = true;

    public boolean isDisableLegacyVariable() {
        return disableLegacyVariable;
    }

    private volatile int cciIncrementalCheckParallelism = 8;

    public int getCciIncrementalCheckParallelism() {
        return cciIncrementalCheckParallelism;
    }

    private volatile int cciIncrementalCheckBatchSize = 128;

    public int getCciIncrementalCheckBatchSize() {
        return cciIncrementalCheckBatchSize;
    }

    private volatile boolean enableColumnarDebug = false;

    public boolean isEnableColumnarDebug() {
        return enableColumnarDebug;
    }

    // pruning warning threshold in microsecond
    private volatile long pruningTimeWarningThreshold = 500;

    public long getPruningTimeWarningThreshold() {
        return pruningTimeWarningThreshold;
    }

    // ndv alike percentage threshold
    private volatile long ndvAlikePercentageThreshold = 10L;

    public long getNdvAlikePercentageThreshold() {
        return ndvAlikePercentageThreshold;
    }

    private volatile boolean enablePruningIn = true;

    private volatile boolean enablePruningInDml = true;

    public boolean isEnablePruningIn() {
        return enablePruningIn;
    }

    public boolean isEnablePruningInDml() {
        return enablePruningInDml;
    }

    private volatile boolean columnarBindMaster = false;

    public boolean allowColumnarBindMaster() {
        return columnarBindMaster;
    }

    private volatile boolean existColumnarNodes = false;

    public void existColumnarNodes(boolean enable) {
        this.existColumnarNodes = enable;
    }

    public boolean existColumnarNodes() {
        return existColumnarNodes;
    }

    private volatile List<String> blackListConf = new ArrayList<>();

    public List<String> getBlacklistConf() {
        return blackListConf;
    }

    private volatile boolean showColumnarStatusUseSubQuery =
        Boolean.parseBoolean(ConnectionParams.SHOW_COLUMNAR_STATUS_USE_SUB_QUERY.getDefault());

    public boolean isShowColumnarStatusUseSubQuery() {
        return showColumnarStatusUseSubQuery;
    }

    public volatile int ttlGlobalSelectWorkerCount =
        Integer.valueOf(ConnectionParams.TTL_GLOBAL_SELECT_WORKER_COUNT.getDefault());

    public volatile int ttlGlobalDeleteWorkerCount =
        Integer.valueOf(ConnectionParams.TTL_GLOBAL_DELETE_WORKER_COUNT.getDefault());

    public volatile long ttlTmpTableMaxDataLength =
        Long.valueOf(ConnectionParams.TTL_TMP_TBL_MAX_DATA_LENGTH.getDefault());

    public volatile int ttlTmpTableMaxDataFreePercent =
        Integer.valueOf(ConnectionParams.TTL_TBL_MAX_DATA_FREE_PERCENT.getDefault());
    ;

    public volatile int ttlIntraTaskInterruptionMaxWaitTime =
        Integer.valueOf(ConnectionParams.TTL_INTRA_TASK_INTERRUPTION_MAX_WAIT_TIME.getDefault());

    public volatile int ttlIntraTaskMonitorEachRoundWaitTime =
        Integer.valueOf(ConnectionParams.TTL_INTRA_TASK_MONITOR_EACH_ROUTE_WAIT_TIME.getDefault());

    public volatile int ttlScheduledJobMaxParallelism =
        Integer.valueOf(ConnectionParams.TTL_SCHEDULED_JOB_MAX_PARALLELISM.getDefault());
    ;

    public volatile boolean ttlEnableAutoOptimizeTableInTtlJob =
        Boolean.valueOf(ConnectionParams.TTL_ENABLE_AUTO_OPTIMIZE_TABLE_IN_TTL_JOB.getDefault());

    public volatile boolean ttlEnableAutoExecOptimizeTableAfterArchiving =
        Boolean.valueOf(ConnectionParams.TTL_ENABLE_AUTO_EXEC_OPTIMIZE_TABLE_AFTER_ARCHIVING.getDefault());
    ;
    public volatile int ttlJobDefaultBatchSize =
        Integer.valueOf(ConnectionParams.TTL_JOB_DEFAULT_BATCH_SIZE.getDefault());

    public volatile int ttlCleanupBoundIntervalCount =
        Integer.valueOf(ConnectionParams.TTL_CLEANUP_BOUND_INTERVAL_COUNT.getDefault());

    public volatile boolean ttlStopAllJobScheduling =
        Boolean.valueOf(ConnectionParams.TTL_STOP_ALL_JOB_SCHEDULING.getDefault());

    public volatile boolean ttlUseArchiveTransPolicy =
        Boolean.valueOf(ConnectionParams.TTL_USE_ARCHIVE_TRANS_POLICY.getDefault());

    public volatile int ttlSelectMergeUnionSize =
        Integer.valueOf(ConnectionParams.TTL_SELECT_MERGE_UNION_SIZE.getDefault());

    public volatile boolean ttlSelectMergeConcurrent =
        Boolean.valueOf(ConnectionParams.TTL_SELECT_MERGE_CONCURRENT.getDefault());

    public volatile String ttlSelectStmtHint =
        String.valueOf(ConnectionParams.TTL_SELECT_STMT_HINT.getDefault());

    public volatile String ttlDeleteStmtHint =
        String.valueOf(ConnectionParams.TTL_DELETE_STMT_HINT.getDefault());

    public volatile String ttlInsertStmtHint =
        String.valueOf(ConnectionParams.TTL_INSERT_STMT_HINT.getDefault());

    public volatile String ttlOptimizeTableStmtHint =
        String.valueOf(ConnectionParams.TTL_OPTIMIZE_TABLE_STMT_HINT.getDefault());

    public volatile String ttlAlterTableAddPartsStmtHint =
        String.valueOf(ConnectionParams.TTL_ALTER_ADD_PART_STMT_HINT.getDefault());

    public volatile Long ttlGroupParallelismOnDqlConn =
        Long.valueOf(ConnectionParams.TTL_GROUP_PARALLELISM_ON_DQL_CONN.getDefault());

    public volatile Long ttlGroupParallelismOnDmlConn =
        Long.valueOf(ConnectionParams.TTL_GROUP_PARALLELISM_ON_DML_CONN.getDefault());

    public volatile Boolean ttlAddMaxValPartOnCciCreating =
        Boolean.valueOf(ConnectionParams.TTL_ADD_MAXVAL_PART_ON_CCI_CREATING.getDefault());

    public volatile Long ttlMaxWaitAcquireRatePermitsPeriods =
        Long.valueOf(ConnectionParams.TTL_MAX_WAIT_ACQUIRE_RATE_PERMITS_PERIODS.getDefault());

    public volatile Boolean ttlEnableCleanupRowsSpeedLimit =
        Boolean.valueOf(ConnectionParams.TTL_ENABLE_CLEANUP_ROWS_SPEED_LIMIT.getDefault());

    public volatile Long ttlCleanupRowsSpeedLimitEachDn =
        Long.valueOf(ConnectionParams.TTL_CLEANUP_ROWS_SPEED_LIMIT_EACH_DN.getDefault());

    public volatile Boolean ttlIgnoreMaintainWindowInDdlJob =
        Boolean.valueOf(ConnectionParams.TTL_IGNORE_MAINTAIN_WINDOW_IN_DDL_JOB.getDefault());

    public volatile int ttlGlobalWorkerDnRatio =
        Integer.valueOf(ConnectionParams.TTL_GLOBAL_WORKER_DN_RATIO.getDefault());

    public volatile int ttlDefaultArcPreAllocateCount =
        Integer.valueOf(ConnectionParams.TTL_DEFAULT_ARC_PRE_ALLOCATE_COUNT.getDefault());

    public volatile int ttlDefaultArcPostAllocateCount =
        Integer.valueOf(ConnectionParams.TTL_DEFAULT_ARC_POST_ALLOCATE_COUNT.getDefault());

    public volatile boolean ttlEnableAutoAddPartsForArcCci =
        Boolean.valueOf(ConnectionParams.TTL_ENABLE_AUTO_ADD_PARTS_FOR_ARC_CCI.getDefault());

    public int getTtlGlobalDeleteWorkerCount() {
        return ttlGlobalDeleteWorkerCount;
    }

    public long getTtlTmpTableMaxDataLength() {
        return ttlTmpTableMaxDataLength;
    }

    public int getTtlTmpTableMaxDataFreePercent() {
        return ttlTmpTableMaxDataFreePercent;
    }

    public int getTtlIntraTaskInterruptionMaxWaitTime() {
        return ttlIntraTaskInterruptionMaxWaitTime;
    }

    public int getTtlScheduledJobMaxParallelism() {
        return ttlScheduledJobMaxParallelism;
    }

    public boolean isTtlEnableAutoOptimizeTableInTtlJob() {
        return ttlEnableAutoOptimizeTableInTtlJob;
    }

    public boolean isTtlEnableAutoExecOptimizeTableAfterArchiving() {
        return ttlEnableAutoExecOptimizeTableAfterArchiving;
    }

    public int getTtlIntraTaskMonitorEachRoundWaitTime() {
        return ttlIntraTaskMonitorEachRoundWaitTime;
    }

    public int getTtlJobDefaultBatchSize() {
        return ttlJobDefaultBatchSize;
    }

    public int getTtlCleanupBoundIntervalCount() {
        return ttlCleanupBoundIntervalCount;
    }

    public boolean isTtlStopAllJobScheduling() {
        return ttlStopAllJobScheduling;
    }

    public boolean isTtlUseArchiveTransPolicy() {
        return ttlUseArchiveTransPolicy;
    }

    public int getTtlSelectMergeUnionSize() {
        return ttlSelectMergeUnionSize;
    }

    public boolean isTtlSelectMergeConcurrent() {
        return ttlSelectMergeConcurrent;
    }

    public String getTtlSelectStmtHint() {
        return ttlSelectStmtHint;
    }

    public String getTtlDeleteStmtHint() {
        return ttlDeleteStmtHint;
    }

    public String getTtlInsertStmtHint() {
        return ttlInsertStmtHint;
    }

    public Long getTtlGroupParallelismOnDmlConn() {
        return ttlGroupParallelismOnDmlConn;
    }

    public Long getTtlGroupParallelismOnDqlConn() {
        return ttlGroupParallelismOnDqlConn;
    }

    public String getTtlOptimizeTableStmtHint() {
        return ttlOptimizeTableStmtHint;
    }

    public String getTtlAlterTableAddPartsStmtHint() {
        return ttlAlterTableAddPartsStmtHint;
    }

    public Boolean getTtlAddMaxValPartOnCciCreating() {
        return ttlAddMaxValPartOnCciCreating;
    }

    public Long getTtlCleanupRowsSpeedLimitEachDn() {
        return ttlCleanupRowsSpeedLimitEachDn;
    }

    public Boolean getTtlEnableCleanupRowsSpeedLimit() {
        return ttlEnableCleanupRowsSpeedLimit;
    }

    public Long getTtlMaxWaitAcquireRatePermitsPeriods() {
        return ttlMaxWaitAcquireRatePermitsPeriods;
    }

    public Boolean getTtlIgnoreMaintainWindowInDdlJob() {
        return ttlIgnoreMaintainWindowInDdlJob;
    }

    public int getTtlGlobalSelectWorkerCount() {
        return ttlGlobalSelectWorkerCount;
    }

    public int getTtlGlobalWorkerDnRatio() {
        return ttlGlobalWorkerDnRatio;
    }

    public int getTtlDefaultArcPreAllocateCount() {
        return ttlDefaultArcPreAllocateCount;
    }

    public int getTtlDefaultArcPostAllocateCount() {
        return ttlDefaultArcPostAllocateCount;
    }

    public boolean getTtlEnableAutoAddPartsForArcCci() {
        return ttlEnableAutoAddPartsForArcCci;
    }

    private volatile long waitForColumnarCommitMS =
        Long.parseLong(ConnectionParams.WAIT_FOR_COLUMNAR_COMMIT_MS.getDefault());

    public long getWaitForColumnarCommitMS() {
        return waitForColumnarCommitMS;
    }

    public static <T> T parseValue(String value, Class<T> type, T defaultValue) {
        if (value == null) {
            return defaultValue;
        } else if (type == String.class) {
            return (T) value;
        } else if (type == Integer.class) {
            return (T) (Integer.valueOf(value));
        } else if (type == Long.class) {
            return (T) (Long.valueOf(value));
        } else if (type == Float.class) {
            return (T) (Float.valueOf(value));
        } else if (type == Double.class) {
            return (T) (Double.valueOf(value));
        } else if (type == Boolean.class) {
            return (T) (Boolean.valueOf(value));
        } else if (type == ConfigDataMode.LearnerMode.class) {
            return (T) (ConfigDataMode.LearnerMode.nameOf(value));
        } else {
            return defaultValue;
        }
    }

    private static final DynamicConfig instance = new DynamicConfig();

}
