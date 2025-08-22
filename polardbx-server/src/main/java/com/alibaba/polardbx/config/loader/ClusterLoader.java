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

package com.alibaba.polardbx.config.loader;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.polardbx.CobarServer;
import com.alibaba.polardbx.common.audit.AuditUtils;
import com.alibaba.polardbx.common.constants.IsolationLevel;
import com.alibaba.polardbx.common.privilege.PasswdRuleConfig;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.properties.MppConfig;
import com.alibaba.polardbx.common.properties.SystemPropertiesHelper;
import com.alibaba.polardbx.common.utils.version.InstanceVersion;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.config.SystemConfig;
import com.alibaba.polardbx.executor.common.GsiStatisticsManager;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.TtlDataCleanupRateLimiter;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.TtlIntraTaskExecutor;
import com.alibaba.polardbx.executor.ddl.workqueue.BackFillThreadPool;
import com.alibaba.polardbx.executor.ddl.workqueue.ChangeSetThreadPool;
import com.alibaba.polardbx.executor.ddl.workqueue.FastCheckerThreadPool;
import com.alibaba.polardbx.executor.ddl.workqueue.OmcThreadPoll;
import com.alibaba.polardbx.executor.gms.ColumnarManager;
import com.alibaba.polardbx.executor.gms.DynamicColumnarManager;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.executor.ddl.workqueue.PriorityWorkQueue;
import com.alibaba.polardbx.gms.ha.impl.StorageHaChecker;
import com.alibaba.polardbx.gms.ha.impl.StorageHaManager;
import com.alibaba.polardbx.gms.lbac.LBACSecurityManager;
import com.alibaba.polardbx.gms.privilege.PolarLoginErrConfig;
import com.alibaba.polardbx.gms.privilege.PolarPrivManager;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import com.alibaba.polardbx.gms.util.GmsJdbcUtil;
import com.alibaba.polardbx.optimizer.core.planner.PlanCache;
import com.alibaba.polardbx.optimizer.core.profiler.RuntimeStat;
import com.alibaba.polardbx.optimizer.hint.util.HintUtil;
import com.alibaba.polardbx.optimizer.memory.MemoryManager;
import com.alibaba.polardbx.optimizer.memory.MemorySetting;
import com.alibaba.polardbx.optimizer.partition.FullScanTableBlackListManager;
import com.alibaba.polardbx.optimizer.planmanager.PlanManager;
import com.alibaba.polardbx.optimizer.ttl.TtlConfigUtil;
import com.alibaba.polardbx.optimizer.view.InformationSchemaViewManager;
import com.alibaba.polardbx.server.util.LogUtils;
import com.alibaba.polardbx.transaction.ColumnarTsoManager;
import com.alibaba.polardbx.util.RexMemoryLimitHelper;
import org.apache.calcite.rex.RexUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.Properties;

import static com.alibaba.polardbx.common.properties.ConnectionParams.PLAN_CACHE_SIZE;

/**
 * 一个cluster的加载
 *
 * @author jianghang 2014-5-28 下午5:32:40
 * @since 5.1.0
 */
public abstract class ClusterLoader extends BaseClusterLoader {

    protected static final Logger logger = LoggerFactory.getLogger(ClusterLoader.class);
    protected String cluster;
    protected String unitName;
    protected BaseAppLoader appLoader;
    protected boolean isLock = false;

    public ClusterLoader(String cluster, String unitName) {
        this.cluster = cluster;
        this.unitName = unitName;
    }

    protected void applyProperties(Properties p) {

        if (p.isEmpty()) {
            return;
        }

        if (p.containsKey(ConnectionProperties.ALLOW_CROSS_DB_QUERY)) {
            CobarServer.getInstance()
                .getConfig()
                .getSystem()
                .setAllowCrossDbQuery((Boolean.valueOf(p.getProperty(ConnectionProperties.ALLOW_CROSS_DB_QUERY))));
        }

        if (p.containsKey(ConnectionProperties.RETRY_ERROR_SQL_ON_OLD_SERVER)) {
            CobarServer.getInstance()
                .getConfig()
                .getSystem()
                .setRetryErrorSqlOnOldServer(
                    Boolean.valueOf(p.getProperty(ConnectionProperties.RETRY_ERROR_SQL_ON_OLD_SERVER)));
        }

        if (p.containsKey(ConnectionProperties.VERSION_PREFIX)) {
            final String property = p.getProperty(ConnectionProperties.VERSION_PREFIX, InstanceVersion.getVersion());
            CobarServer.getInstance().getConfig().getSystem().setVersionPrefix(
                property);
            InstanceVersion.reloadVersion(property);

        }

        if (p.containsKey(ConnectionProperties.SQL_SIMPLE_MAX_LENGTH)) {
            CobarServer.getInstance()
                .getConfig()
                .getSystem()
                .setSqlSimpleMaxLen(Integer.valueOf(p.getProperty(ConnectionProperties.SQL_SIMPLE_MAX_LENGTH)));
        }

        if (p.containsKey(ConnectionProperties.SQL_LOG_MAX_LENGTH)) {
            int sqlLogMaxLen = Integer.valueOf(p.getProperty(ConnectionProperties.SQL_LOG_MAX_LENGTH));
            LogUtils.resetMaxSqlLen(sqlLogMaxLen);
        }

        if (p.containsKey(ConnectionProperties.DNF_REX_NODE_LIMIT)) {
            int dnfRexLimit = Integer.valueOf(p.getProperty(ConnectionProperties.DNF_REX_NODE_LIMIT));
            RexUtil.DNF_REX_NODE_LIMIT = dnfRexLimit;
        }

        if (p.containsKey(ConnectionProperties.CNF_REX_NODE_LIMIT)) {
            int cnfRexLimit = Integer.valueOf(p.getProperty(ConnectionProperties.CNF_REX_NODE_LIMIT));
            RexUtil.CNF_REX_NODE_LIMIT = cnfRexLimit;
        }

        if (p.containsKey(ConnectionProperties.ENABLE_SQL_PROFILE_LOG)) {
            boolean enableSqlProfileLog = Boolean.valueOf(p.getProperty(ConnectionProperties.ENABLE_SQL_PROFILE_LOG));
            LogUtils.resetEnableSqlProfileLog(enableSqlProfileLog);
        }

        if (p.containsKey(ConnectionProperties.ENABLE_CPU_PROFILE)) {
            boolean enableSqlProfile = Boolean.valueOf(p.getProperty(ConnectionProperties.ENABLE_CPU_PROFILE));
            RuntimeStat.setEnableCpuProfile(enableSqlProfile);
        }

        if (p.containsKey(ConnectionProperties.ENABLE_MEMORY_POOL)) {
            boolean enableMemoryPool = Boolean.valueOf(p.getProperty(ConnectionProperties.ENABLE_MEMORY_POOL));
            MemorySetting.ENABLE_MEMORY_POOL = enableMemoryPool;
        }

        if (p.containsKey(ConnectionProperties.ENABLE_MEMORY_LIMITATION)) {
            boolean enableMemoryLimitation =
                Boolean.valueOf(p.getProperty(ConnectionProperties.ENABLE_MEMORY_LIMITATION));
            MemorySetting.ENABLE_MEMORY_LIMITATION = enableMemoryLimitation;
        }

        if (p.containsKey(ConnectionProperties.TP_LOW_MEMORY_PROPORTION)) {
            MemorySetting.TP_LOW_MEMORY_PROPORTION =
                Double.valueOf(p.getProperty(ConnectionProperties.TP_LOW_MEMORY_PROPORTION));
        }

        if (p.containsKey(ConnectionProperties.TP_HIGH_MEMORY_PROPORTION)) {
            MemorySetting.TP_HIGH_MEMORY_PROPORTION =
                Double.valueOf(p.getProperty(ConnectionProperties.TP_HIGH_MEMORY_PROPORTION));
        }

        if (p.containsKey(ConnectionProperties.AP_LOW_MEMORY_PROPORTION)) {
            MemorySetting.AP_LOW_MEMORY_PROPORTION =
                Double.valueOf(p.getProperty(ConnectionProperties.AP_LOW_MEMORY_PROPORTION));
        }

        if (p.containsKey(ConnectionProperties.AP_HIGH_MEMORY_PROPORTION)) {
            MemorySetting.AP_HIGH_MEMORY_PROPORTION =
                Double.valueOf(p.getProperty(ConnectionProperties.AP_HIGH_MEMORY_PROPORTION));
        }

        if (p.containsKey(ConnectionProperties.SINGLE_GROUP_STORAGE_INST_LIST)) {
            DbTopologyManager.refreshSingleGroupStorageInsts();
        }

        if (p.containsKey(ConnectionProperties.STORAGE_HA_TASK_PERIOD)) {
            Integer newHaTaskPeriod = Integer.valueOf(p.getProperty(ConnectionProperties.STORAGE_HA_TASK_PERIOD));
            if (newHaTaskPeriod > 0) {
                DynamicConfig.getInstance().loadValue(logger, ConnectionProperties.STORAGE_HA_TASK_PERIOD,
                    p.getProperty(ConnectionProperties.STORAGE_HA_TASK_PERIOD));
                StorageHaManager.getInstance().adjustStorageHaTaskPeriod(newHaTaskPeriod);
            }
        }

        if (p.containsKey(ConnectionProperties.STORAGE_HA_SOCKET_TIMEOUT)) {
            Integer haSocketTimeout = Integer.valueOf(p.getProperty(ConnectionProperties.STORAGE_HA_SOCKET_TIMEOUT));
            if (haSocketTimeout > 0) {
                StorageHaChecker
                    .adjustStorageHaTaskConnProps(GmsJdbcUtil.JDBC_SOCKET_TIMEOUT, String.valueOf(haSocketTimeout));
            }
        }

        if (p.containsKey(ConnectionProperties.STORAGE_HA_CONNECT_TIMEOUT)) {
            Integer haConnectTimeout = Integer.valueOf(p.getProperty(ConnectionProperties.STORAGE_HA_CONNECT_TIMEOUT));
            if (haConnectTimeout > 0) {
                StorageHaChecker
                    .adjustStorageHaTaskConnProps(GmsJdbcUtil.JDBC_CONNECT_TIMEOUT, String.valueOf(haConnectTimeout));
            }
        }

        if (p.containsKey(ConnectionProperties.ENABLE_HA_CHECK_TASK_LOG)) {
            Boolean enableTaskLog = Boolean.valueOf(p.getProperty(ConnectionProperties.ENABLE_HA_CHECK_TASK_LOG));
            StorageHaChecker.setPrintHaCheckTaskLog(enableTaskLog);
        }

        if (p.containsKey(ConnectionProperties.BACKFILL_PARALLELISM)) {
            int backfillParallelism = Integer.parseInt(p.getProperty(ConnectionProperties.BACKFILL_PARALLELISM));
            if (backfillParallelism > 0) {
                if (backfillParallelism > PriorityWorkQueue.getInstance().getMaximumPoolSize()) {
                    PriorityWorkQueue.getInstance().setMaximumPoolSize(backfillParallelism);
                    PriorityWorkQueue.getInstance().setCorePoolSize(backfillParallelism);
                } else {
                    PriorityWorkQueue.getInstance().setCorePoolSize(backfillParallelism);
                    PriorityWorkQueue.getInstance().setMaximumPoolSize(backfillParallelism);
                }
            }
        }

        if (p.containsKey(ConnectionProperties.CHANGE_SET_APPLY_PARALLELISM)) {
            int parallelism = Integer.parseInt(p.getProperty(ConnectionProperties.CHANGE_SET_APPLY_PARALLELISM));
            if (parallelism > 0) {
                if (parallelism > ChangeSetThreadPool.getInstance().getMaximumPoolSize()) {
                    ChangeSetThreadPool.getInstance().setMaximumPoolSize(parallelism);
                    ChangeSetThreadPool.getInstance().setCorePoolSize(parallelism);
                } else {
                    ChangeSetThreadPool.getInstance().setCorePoolSize(parallelism);
                    ChangeSetThreadPool.getInstance().setMaximumPoolSize(parallelism);
                }
            }
        }

        if (p.containsKey(ConnectionProperties.FASTCHECKER_THREAD_POOL_SIZE)) {
            int parallelism = Integer.parseInt(p.getProperty(ConnectionProperties.FASTCHECKER_THREAD_POOL_SIZE));
            FastCheckerThreadPool.getInstance().setParallelism(parallelism);
        }

        if (p.containsKey(ConnectionProperties.OMC_THREAD_POOL_SIZE)) {
            int parallelism = Integer.parseInt(p.getProperty(ConnectionProperties.OMC_THREAD_POOL_SIZE));
            OmcThreadPoll.getInstance().setParallelism(parallelism);
        }

        if (p.containsKey(ConnectionProperties.GLOBAL_MEMORY_LIMIT)) {
            String memoryLimitString = p.getProperty(ConnectionProperties.GLOBAL_MEMORY_LIMIT);
            try {
                long memoryLimit = Long.parseLong(memoryLimitString);
                if (memoryLimit != MemorySetting.USE_DEFAULT_MEMORY_LIMIT_VALUE) {
                    // does not use the default val that is set by drds

                    // provide at least 1GB
                    final long minMemoryLimit = 1L << 30;
                    if (memoryLimit >= minMemoryLimit) {
                        long maxAllowMemoryLimit = SystemConfig.getMaxMemoryLimit();
                        long finalMemoryLimit = memoryLimit;
                        if (memoryLimit >= maxAllowMemoryLimit) {
                            finalMemoryLimit = maxAllowMemoryLimit;
                        }
                        CobarServer.getInstance().getConfig().getSystem().setGlobalMemoryLimit(finalMemoryLimit);
                        MemoryManager.getInstance().adjustMemoryLimit(finalMemoryLimit);
                        logger.info(String.format(
                            "GLOBAL_MEMORY_LIMIT is set to %s, maxAllowMemoryLimit is %s, memoryLimit of ConnectionProperties is %s",
                            finalMemoryLimit,
                            maxAllowMemoryLimit,
                            memoryLimit));

                    } else {
                        logger.warn("GLOBAL_MEMORY_LIMIT is less than [" + minMemoryLimit + "], so ignore the adjust");
                    }
                } else {
                    // use the default val that is set by drds
                    memoryLimit = SystemConfig.getDefaultMemoryLimit();
                    CobarServer.getInstance().getConfig().getSystem().setGlobalMemoryLimit(memoryLimit);
                    MemoryManager.getInstance().adjustMemoryLimit(memoryLimit);
                    logger.info("GLOBAL_MEMORY_LIMIT set to " + memoryLimit);
                }

            } catch (NumberFormatException ex) {
                logger.warn("Bad value for GLOBAL_MEMORY_LIMIT: " + memoryLimitString);
            }
        }

        //set DRDS instance prop
        if (p.containsKey(ConnectionProperties.REX_MEMORY_LIMIT)) {
            boolean enableRexMemoryLimit = Boolean.valueOf(p.getProperty(ConnectionProperties.REX_MEMORY_LIMIT));
            RexMemoryLimitHelper.ENABLE_REX_MEMORY_LIMIT = enableRexMemoryLimit;
        }

        //set DRDS instance prop
        if (p.containsKey(ConnectionProperties.PASSWORD_RULE_CONFIG)) {
            String passwordRule = p.getProperty(ConnectionProperties.PASSWORD_RULE_CONFIG);
            try {
                PasswdRuleConfig config = null;
                final JSONObject jsonObject = JSON.parseObject(passwordRule);
                if (jsonObject != null) {
                    config = PasswdRuleConfig.parse(jsonObject);
                }
                CobarServer.getInstance().getConfig().getSystem().setPasswordRuleConfig(config);
            } catch (Exception e) {
                logger.warn("Loading PASSWORD_RULE_CONFIG", e);
            }
        }
        if (p.containsKey(ConnectionProperties.ENABLE_LOGIN_AUDIT_CONFIG)) {
            AuditUtils
                .setEnableLogAudit(Boolean.parseBoolean(p.getProperty(ConnectionProperties.ENABLE_LOGIN_AUDIT_CONFIG)));
        } else {
            AuditUtils.setEnableLogAudit(Boolean.parseBoolean(ConnectionParams.ENABLE_LOGIN_AUDIT_CONFIG.getDefault()));
        }
        // only set PolarDb  instance prop
            PolarPrivManager.getInstance().getConfig().config(p);
            PolarLoginErrConfig config = null;
            if (p.containsKey(ConnectionProperties.LOGIN_ERROR_MAX_COUNT_CONFIG)) {
                String loginErrorRule = p.getProperty(ConnectionProperties.LOGIN_ERROR_MAX_COUNT_CONFIG);
                try {
                    final JSONObject jsonObject = JSON.parseObject(loginErrorRule);
                    config = PolarLoginErrConfig.parse(jsonObject);
                } catch (Exception e) {
                    logger.warn("Loading LOGIN_ERROR_RULE_CONFIG error: " + loginErrorRule, e);
                }
            }
            if (config == null) {
                config = new PolarLoginErrConfig();
            }
            PolarPrivManager.getInstance().setPolarLoginErrConfig(config);

        // for scaleout config
        if (p.containsKey(ConnectionProperties.ENABLE_SCALE_OUT_FEATURE)) {
            System.setProperty(ConnectionProperties.ENABLE_SCALE_OUT_FEATURE,
                p.getProperty(ConnectionProperties.ENABLE_SCALE_OUT_FEATURE));
        }

        if (p.containsKey(ConnectionProperties.SCALE_OUT_DROP_DATABASE_AFTER_SWITCH_DATASOURCE)) {
            System.setProperty(ConnectionProperties.SCALE_OUT_DROP_DATABASE_AFTER_SWITCH_DATASOURCE,
                p.getProperty(ConnectionProperties.SCALE_OUT_DROP_DATABASE_AFTER_SWITCH_DATASOURCE));
        }

        if (p.containsKey(ConnectionProperties.SCALEOUT_BACKFILL_SPEED_LIMITATION)) {
            System.setProperty(ConnectionProperties.SCALEOUT_BACKFILL_SPEED_LIMITATION,
                p.getProperty(ConnectionProperties.SCALEOUT_BACKFILL_SPEED_LIMITATION));
        }

        if (p.containsKey(ConnectionProperties.SCALEOUT_CHECK_SPEED_LIMITATION)) {
            System.setProperty(ConnectionProperties.SCALEOUT_CHECK_SPEED_LIMITATION,
                p.getProperty(ConnectionProperties.SCALEOUT_CHECK_SPEED_LIMITATION));
        }

        CobarServer.getInstance().getConfig().getSystem().setInstanceProp(p);

        // 统一设置实例级备库的socketTimeout, 当连接参数中socketTimeout小于此值时设置
        if (p.containsKey(ConnectionProperties.SLAVE_SOCKET_TIMEOUT)) {
            System.setProperty(ConnectionProperties.SLAVE_SOCKET_TIMEOUT,
                p.getProperty(ConnectionProperties.SLAVE_SOCKET_TIMEOUT));
        }
        /* ========mpp system engine config======== */

        for (String key : SystemPropertiesHelper.getConnectionProperties()) {
            try {
                if (key.startsWith("MPP_")) {
                    MppConfig.getInstance().loadValue(logger, key, p.getProperty(key));
                }
                DynamicConfig.getInstance().loadValue(logger, key, p.getProperty(key));
            } catch (Exception e) {
                logger.warn("ClusterLoader error", e);
            }
        }

        if (p.containsKey(ConnectionProperties.ENABLE_FORBID_PUSH_DML_WITH_HINT)) {
            boolean enableForbidPushDmlWithHint =
                Boolean.valueOf(p.getProperty(ConnectionProperties.ENABLE_FORBID_PUSH_DML_WITH_HINT));
            CobarServer.getInstance().getConfig().getSystem()
                .setEnableForbidPushDmlWithHint(enableForbidPushDmlWithHint);
            HintUtil.enableForbidPushDmlWithHint = enableForbidPushDmlWithHint;
        }

        // for create db
        if (p.containsKey(ConnectionProperties.SHARD_DB_COUNT_EACH_STORAGE_INST)) {
            int shardDbCountEachStorageInst =
                Integer.valueOf(p.getProperty(ConnectionProperties.SHARD_DB_COUNT_EACH_STORAGE_INST));
            CobarServer.getInstance().getConfig().getSystem()
                .setShardDbCountEachStorageInst(shardDbCountEachStorageInst);
            DbTopologyManager.resetShardDbCountEachStorageInst(shardDbCountEachStorageInst);
        }

        if (p.containsKey(ConnectionProperties.SINGLE_GROUP_STORAGE_INST_LIST)) {
            DbTopologyManager.refreshSingleGroupStorageInsts();
        }

        if (p.containsKey(ConnectionProperties.MAX_LOGICAL_DB_COUNT)) {
            int maxLogicalDbCount =
                Integer.valueOf(p.getProperty(ConnectionProperties.MAX_LOGICAL_DB_COUNT));
            DbTopologyManager.maxLogicalDbCount = maxLogicalDbCount;
        }

        if (p.containsKey(ConnectionProperties.DEFAULT_PARTITION_MODE)) {
            String defaultPartMode =
                String.valueOf(p.getProperty(ConnectionProperties.DEFAULT_PARTITION_MODE));
            CobarServer.getInstance().getConfig().getSystem()
                .setDefaultPartitionMode(defaultPartMode);
            DbTopologyManager.setDefaultPartitionMode(defaultPartMode);
        }

        if (p.containsKey(ConnectionProperties.COLLATION_SERVER)) {
            String defaultServerCollation =
                String.valueOf(p.getProperty(ConnectionProperties.COLLATION_SERVER));
            DbTopologyManager.setDefaultCollationForCreatingDb(defaultServerCollation);
        }

        if (p.containsKey(ConnectionProperties.ENABLE_LOGICAL_DB_WARMMING_UP)) {
            boolean enableLogicalDbWarmmingUp =
                Boolean.valueOf(p.getProperty(ConnectionProperties.ENABLE_LOGICAL_DB_WARMMING_UP));
            CobarServer.getInstance().getConfig().getSystem().setEnableLogicalDbWarmmingUp(enableLogicalDbWarmmingUp);
        }

        if (p.containsKey(ConnectionProperties.LOGICAL_DB_WARMMING_UP_EXECUTOR_POOL_SIZE)) {
            int logicalDbWarmmingUpPoolSize =
                Integer.valueOf(p.getProperty(ConnectionProperties.LOGICAL_DB_WARMMING_UP_EXECUTOR_POOL_SIZE));
            CobarServer.getInstance().getConfig().getSystem()
                .setLogicalDbWarmmingUpExecutorPoolSize(logicalDbWarmmingUpPoolSize);
        }

        if (p.containsKey(ConnectionProperties.GSI_STATISTICS_COLLECTION)) {
            try {
                boolean enableCollect =
                    Boolean.valueOf(p.getProperty(ConnectionProperties.GSI_STATISTICS_COLLECTION));
                GsiStatisticsManager manager = GsiStatisticsManager.getInstance();
                if (enableCollect && manager.cacheIsEmpty()) {
                    manager.loadFromMetaDb();
                } else if (!enableCollect && !manager.cacheIsEmpty()) {
                    GsiStatisticsManager.getInstance().resetStatistics();
                }
            } catch (Throwable t) {
                logger.warn("load gsi statictics error", t);
            }
        }

        /* ========ttl job config======== */
        if (p.containsKey(ConnectionProperties.TTL_GLOBAL_SELECT_WORKER_COUNT)) {
            String valStr = p.getProperty(ConnectionProperties.TTL_GLOBAL_SELECT_WORKER_COUNT);
            DynamicConfig.getInstance().loadValue(logger, ConnectionProperties.TTL_GLOBAL_SELECT_WORKER_COUNT, valStr);
            TtlIntraTaskExecutor.getInstance()
                .adjustTaskExecutorWorkerCount(TtlIntraTaskExecutor.SELECT_TASK_EXECUTOR_TYPE,
                    DynamicConfig.getInstance().getTtlGlobalSelectWorkerCount());
        }

        if (p.containsKey(ConnectionProperties.TTL_GLOBAL_DELETE_WORKER_COUNT)) {
            String valStr = p.getProperty(ConnectionProperties.TTL_GLOBAL_DELETE_WORKER_COUNT);
            DynamicConfig.getInstance().loadValue(logger, ConnectionProperties.TTL_GLOBAL_DELETE_WORKER_COUNT, valStr);
            TtlIntraTaskExecutor.getInstance()
                .adjustTaskExecutorWorkerCount(TtlIntraTaskExecutor.DELETE_TASK_EXECUTOR_TYPE,
                    DynamicConfig.getInstance().getTtlGlobalDeleteWorkerCount());
        }

        if (p.containsKey(ConnectionProperties.TTL_TMP_TBL_MAX_DATA_LENGTH)) {
            String valStr = p.getProperty(ConnectionProperties.TTL_TMP_TBL_MAX_DATA_LENGTH);
            DynamicConfig.getInstance().loadValue(logger, ConnectionProperties.TTL_TMP_TBL_MAX_DATA_LENGTH, valStr);
            TtlConfigUtil.setMaxTtlTmpTableDataLength(DynamicConfig.getInstance().getTtlTmpTableMaxDataLength());
        }

        if (p.containsKey(ConnectionProperties.TTL_TBL_MAX_DATA_FREE_PERCENT)) {
            String valStr = p.getProperty(ConnectionProperties.TTL_TBL_MAX_DATA_FREE_PERCENT);
            DynamicConfig.getInstance().loadValue(logger, ConnectionProperties.TTL_TBL_MAX_DATA_FREE_PERCENT, valStr);
            TtlConfigUtil.setMaxDataFreePercentOfTtlTable(
                DynamicConfig.getInstance().getTtlTmpTableMaxDataFreePercent());
        }

        if (p.containsKey(ConnectionProperties.TTL_INTRA_TASK_INTERRUPTION_MAX_WAIT_TIME)) {
            String valStr = p.getProperty(ConnectionProperties.TTL_INTRA_TASK_INTERRUPTION_MAX_WAIT_TIME);
            DynamicConfig.getInstance()
                .loadValue(logger, ConnectionProperties.TTL_INTRA_TASK_INTERRUPTION_MAX_WAIT_TIME, valStr);
            TtlConfigUtil.setIntraTaskInterruptionMaxWaitTime(
                DynamicConfig.getInstance().getTtlIntraTaskInterruptionMaxWaitTime());
        }

        if (p.containsKey(ConnectionProperties.TTL_INTRA_TASK_MONITOR_EACH_ROUTE_WAIT_TIME)) {
            String valStr = p.getProperty(ConnectionProperties.TTL_INTRA_TASK_MONITOR_EACH_ROUTE_WAIT_TIME);
            DynamicConfig.getInstance()
                .loadValue(logger, ConnectionProperties.TTL_INTRA_TASK_MONITOR_EACH_ROUTE_WAIT_TIME, valStr);
            TtlConfigUtil.setIntraTaskDelegateEachRoundWaitTime(
                DynamicConfig.getInstance().getTtlIntraTaskMonitorEachRoundWaitTime());
        }

        if (p.containsKey(ConnectionProperties.TTL_ENABLE_AUTO_OPTIMIZE_TABLE_IN_TTL_JOB)) {
            String valStr = p.getProperty(ConnectionProperties.TTL_ENABLE_AUTO_OPTIMIZE_TABLE_IN_TTL_JOB);
            DynamicConfig.getInstance()
                .loadValue(logger, ConnectionProperties.TTL_ENABLE_AUTO_OPTIMIZE_TABLE_IN_TTL_JOB, valStr);
            TtlConfigUtil.setEnableAutoControlOptiTblByTtlJob(
                DynamicConfig.getInstance().isTtlEnableAutoOptimizeTableInTtlJob());
        }

        if (p.containsKey(ConnectionProperties.TTL_ENABLE_AUTO_EXEC_OPTIMIZE_TABLE_AFTER_ARCHIVING)) {
            String valStr = p.getProperty(ConnectionProperties.TTL_ENABLE_AUTO_EXEC_OPTIMIZE_TABLE_AFTER_ARCHIVING);
            DynamicConfig.getInstance()
                .loadValue(logger, ConnectionProperties.TTL_ENABLE_AUTO_EXEC_OPTIMIZE_TABLE_AFTER_ARCHIVING, valStr);
            TtlConfigUtil.setEnablePerformAutoOptiTtlTableAfterArchiving(
                DynamicConfig.getInstance().isTtlEnableAutoExecOptimizeTableAfterArchiving());
        }

        if (p.containsKey(ConnectionProperties.TTL_SCHEDULED_JOB_MAX_PARALLELISM)) {
            String valStr = p.getProperty(ConnectionProperties.TTL_SCHEDULED_JOB_MAX_PARALLELISM);
            DynamicConfig.getInstance()
                .loadValue(logger, ConnectionProperties.TTL_SCHEDULED_JOB_MAX_PARALLELISM, valStr);
            TtlConfigUtil.setTtlScheduledJobMaxParallelism(
                DynamicConfig.getInstance().getTtlScheduledJobMaxParallelism());
        }

        if (p.containsKey(ConnectionProperties.TTL_DEBUG_USE_GSI_FOR_COLUMNAR_ARC_TBL)) {
            String valStr = p.getProperty(ConnectionProperties.TTL_DEBUG_USE_GSI_FOR_COLUMNAR_ARC_TBL);
            DynamicConfig.getInstance()
                .loadValue(logger, ConnectionProperties.TTL_DEBUG_USE_GSI_FOR_COLUMNAR_ARC_TBL, valStr);
        }

        if (p.containsKey(ConnectionProperties.TTL_JOB_DEFAULT_BATCH_SIZE)) {
            String valStr = p.getProperty(ConnectionProperties.TTL_JOB_DEFAULT_BATCH_SIZE);
            DynamicConfig.getInstance()
                .loadValue(logger, ConnectionProperties.TTL_JOB_DEFAULT_BATCH_SIZE, valStr);
            TtlConfigUtil.setTtlJobDefaultBatchSize(DynamicConfig.getInstance().getTtlJobDefaultBatchSize());
        }

        if (p.containsKey(ConnectionProperties.TTL_CLEANUP_BOUND_INTERVAL_COUNT)) {
            String valStr = p.getProperty(ConnectionProperties.TTL_CLEANUP_BOUND_INTERVAL_COUNT);
            DynamicConfig.getInstance()
                .loadValue(logger, ConnectionProperties.TTL_CLEANUP_BOUND_INTERVAL_COUNT, valStr);
            TtlConfigUtil.setTtlCleanupBoundIntervalCount(
                DynamicConfig.getInstance().getTtlCleanupBoundIntervalCount());
        }

        if (p.containsKey(ConnectionProperties.TTL_STOP_ALL_JOB_SCHEDULING)) {
            String valStr = p.getProperty(ConnectionProperties.TTL_STOP_ALL_JOB_SCHEDULING);
            DynamicConfig.getInstance()
                .loadValue(logger, ConnectionProperties.TTL_STOP_ALL_JOB_SCHEDULING, valStr);
            TtlConfigUtil.setStopAllTtlTableJobScheduling(DynamicConfig.getInstance().isTtlStopAllJobScheduling());
        }

        if (p.containsKey(ConnectionProperties.TTL_USE_ARCHIVE_TRANS_POLICY)) {
            String valStr = p.getProperty(ConnectionProperties.TTL_USE_ARCHIVE_TRANS_POLICY);
            DynamicConfig.getInstance()
                .loadValue(logger, ConnectionProperties.TTL_USE_ARCHIVE_TRANS_POLICY, valStr);
            TtlConfigUtil.setUseArchiveTransPolicy(DynamicConfig.getInstance().isTtlUseArchiveTransPolicy());
        }

        if (p.containsKey(ConnectionProperties.TTL_SELECT_MERGE_UNION_SIZE)) {
            String valStr = p.getProperty(ConnectionProperties.TTL_SELECT_MERGE_UNION_SIZE);
            DynamicConfig.getInstance()
                .loadValue(logger, ConnectionProperties.TTL_SELECT_MERGE_UNION_SIZE, valStr);
            TtlConfigUtil.setMergeUnionSizeForSelectLowerBound(
                DynamicConfig.getInstance().getTtlSelectMergeUnionSize());
        }

        if (p.containsKey(ConnectionProperties.TTL_SELECT_MERGE_CONCURRENT)) {
            String valStr = p.getProperty(ConnectionProperties.TTL_SELECT_MERGE_CONCURRENT);
            DynamicConfig.getInstance()
                .loadValue(logger, ConnectionProperties.TTL_SELECT_MERGE_CONCURRENT, valStr);
            TtlConfigUtil.setUseMergeConcurrentForSelectLowerBound(
                DynamicConfig.getInstance().isTtlSelectMergeConcurrent());
        }

        if (p.containsKey(ConnectionProperties.TTL_SELECT_STMT_HINT)) {
            String valStr = p.getProperty(ConnectionProperties.TTL_SELECT_STMT_HINT);
            DynamicConfig.getInstance()
                .loadValue(logger, ConnectionProperties.TTL_SELECT_STMT_HINT, valStr);
            TtlConfigUtil.setQueryHintForSelectLowerBound(DynamicConfig.getInstance().getTtlSelectStmtHint());
        }

        if (p.containsKey(ConnectionProperties.TTL_DELETE_STMT_HINT)) {
            String valStr = p.getProperty(ConnectionProperties.TTL_DELETE_STMT_HINT);
            DynamicConfig.getInstance()
                .loadValue(logger, ConnectionProperties.TTL_DELETE_STMT_HINT, valStr);
            TtlConfigUtil.setQueryHintForDeleteExpiredData(DynamicConfig.getInstance().getTtlDeleteStmtHint());
        }

        if (p.containsKey(ConnectionProperties.TTL_INSERT_STMT_HINT)) {
            String valStr = p.getProperty(ConnectionProperties.TTL_INSERT_STMT_HINT);
            DynamicConfig.getInstance()
                .loadValue(logger, ConnectionProperties.TTL_INSERT_STMT_HINT, valStr);
            TtlConfigUtil.setQueryHintForInsertExpiredData(DynamicConfig.getInstance().getTtlInsertStmtHint());
        }

        if (p.containsKey(ConnectionProperties.TTL_OPTIMIZE_TABLE_STMT_HINT)) {
            String valStr = p.getProperty(ConnectionProperties.TTL_OPTIMIZE_TABLE_STMT_HINT);
            DynamicConfig.getInstance()
                .loadValue(logger, ConnectionProperties.TTL_OPTIMIZE_TABLE_STMT_HINT, valStr);
            TtlConfigUtil.setQueryHintForOptimizeTable(DynamicConfig.getInstance().getTtlOptimizeTableStmtHint());
        }

        if (p.containsKey(ConnectionProperties.TTL_ALTER_ADD_PART_STMT_HINT)) {
            String valStr = p.getProperty(ConnectionProperties.TTL_ALTER_ADD_PART_STMT_HINT);
            DynamicConfig.getInstance()
                .loadValue(logger, ConnectionProperties.TTL_ALTER_ADD_PART_STMT_HINT, valStr);
            TtlConfigUtil.setQueryHintForAutoAddParts(DynamicConfig.getInstance().getTtlAlterTableAddPartsStmtHint());
        }

        if (p.containsKey(ConnectionProperties.TTL_GROUP_PARALLELISM_ON_DQL_CONN)) {
            String valStr = p.getProperty(ConnectionProperties.TTL_GROUP_PARALLELISM_ON_DQL_CONN);
            DynamicConfig.getInstance()
                .loadValue(logger, ConnectionProperties.TTL_GROUP_PARALLELISM_ON_DQL_CONN, valStr);
            TtlConfigUtil.setDefaultGroupParallelismOnDqlConn(
                DynamicConfig.getInstance().getTtlGroupParallelismOnDqlConn());
        }

        if (p.containsKey(ConnectionProperties.TTL_GROUP_PARALLELISM_ON_DML_CONN)) {
            String valStr = p.getProperty(ConnectionProperties.TTL_GROUP_PARALLELISM_ON_DML_CONN);
            DynamicConfig.getInstance()
                .loadValue(logger, ConnectionProperties.TTL_GROUP_PARALLELISM_ON_DML_CONN, valStr);
            TtlConfigUtil.setDefaultGroupParallelismOnDmlConn(
                DynamicConfig.getInstance().getTtlGroupParallelismOnDmlConn());
        }

        if (p.containsKey(ConnectionProperties.TTL_ADD_MAXVAL_PART_ON_CCI_CREATING)) {
            String valStr = p.getProperty(ConnectionProperties.TTL_ADD_MAXVAL_PART_ON_CCI_CREATING);
            DynamicConfig.getInstance()
                .loadValue(logger, ConnectionProperties.TTL_ADD_MAXVAL_PART_ON_CCI_CREATING, valStr);
            TtlConfigUtil.setAutoAddMaxValuePartForCci(DynamicConfig.getInstance().getTtlAddMaxValPartOnCciCreating());
        }

        if (p.containsKey(ConnectionProperties.TTL_ADD_MAXVAL_PART_ON_CCI_CREATING)) {
            String valStr = p.getProperty(ConnectionProperties.TTL_ADD_MAXVAL_PART_ON_CCI_CREATING);
            DynamicConfig.getInstance()
                .loadValue(logger, ConnectionProperties.TTL_ADD_MAXVAL_PART_ON_CCI_CREATING, valStr);
            TtlConfigUtil.setAutoAddMaxValuePartForCci(DynamicConfig.getInstance().getTtlAddMaxValPartOnCciCreating());
        }

        if (p.containsKey(ConnectionProperties.TTL_MAX_WAIT_ACQUIRE_RATE_PERMITS_PERIODS)) {
            String valStr = p.getProperty(ConnectionProperties.TTL_MAX_WAIT_ACQUIRE_RATE_PERMITS_PERIODS);
            DynamicConfig.getInstance()
                .loadValue(logger, ConnectionProperties.TTL_MAX_WAIT_ACQUIRE_RATE_PERMITS_PERIODS, valStr);
            TtlConfigUtil.setMaxWaitAcquireRatePermitsPeriods(
                DynamicConfig.getInstance().getTtlMaxWaitAcquireRatePermitsPeriods());
        }

        if (p.containsKey(ConnectionProperties.TTL_ENABLE_CLEANUP_ROWS_SPEED_LIMIT)) {
            String valStr = p.getProperty(ConnectionProperties.TTL_ENABLE_CLEANUP_ROWS_SPEED_LIMIT);
            DynamicConfig.getInstance()
                .loadValue(logger, ConnectionProperties.TTL_ENABLE_CLEANUP_ROWS_SPEED_LIMIT, valStr);
            TtlConfigUtil.setEnableTtlCleanupRowsSpeedLimit(
                DynamicConfig.getInstance().getTtlEnableCleanupRowsSpeedLimit());
        }

        if (p.containsKey(ConnectionProperties.TTL_CLEANUP_ROWS_SPEED_LIMIT_EACH_DN)) {
            String valStr = p.getProperty(ConnectionProperties.TTL_CLEANUP_ROWS_SPEED_LIMIT_EACH_DN);
            DynamicConfig.getInstance()
                .loadValue(logger, ConnectionProperties.TTL_CLEANUP_ROWS_SPEED_LIMIT_EACH_DN, valStr);
            TtlConfigUtil.setCleanupRowsSpeedLimitEachDn(
                DynamicConfig.getInstance().getTtlCleanupRowsSpeedLimitEachDn());
            TtlDataCleanupRateLimiter.getInstance().adjustRate(TtlConfigUtil.getCleanupRowsSpeedLimitEachDn());

        }

        if (p.containsKey(ConnectionProperties.TTL_IGNORE_MAINTAIN_WINDOW_IN_DDL_JOB)) {
            String valStr = p.getProperty(ConnectionProperties.TTL_IGNORE_MAINTAIN_WINDOW_IN_DDL_JOB);
            DynamicConfig.getInstance()
                .loadValue(logger, ConnectionProperties.TTL_IGNORE_MAINTAIN_WINDOW_IN_DDL_JOB, valStr);
            TtlConfigUtil.setIgnoreMaintainWindowInTtlJob(
                DynamicConfig.getInstance().getTtlIgnoreMaintainWindowInDdlJob());
        }

        if (p.containsKey(ConnectionProperties.TTL_JOB_MAINTENANCE_ENABLE)) {
            String valStr = p.getProperty(ConnectionProperties.TTL_JOB_MAINTENANCE_ENABLE);
            DynamicConfig.getInstance()
                .loadValue(logger, ConnectionProperties.TTL_JOB_MAINTENANCE_ENABLE, valStr);
            TtlConfigUtil.setTtlJobMaintenanceEnable(
                DynamicConfig.getInstance().getTtlJobMaintenanceEnable());
        }

        if (p.containsKey(ConnectionProperties.TTL_JOB_MAINTENANCE_TIME_START)) {
            String valStr = p.getProperty(ConnectionProperties.TTL_JOB_MAINTENANCE_TIME_START);
            DynamicConfig.getInstance()
                .loadValue(logger, ConnectionProperties.TTL_JOB_MAINTENANCE_TIME_START, valStr);
            TtlConfigUtil.setTtlJobMaintenanceTimeStart(
                DynamicConfig.getInstance().getTtlJobMaintenanceTimeStart());
        }

        if (p.containsKey(ConnectionProperties.TTL_JOB_MAINTENANCE_TIME_END)) {
            String valStr = p.getProperty(ConnectionProperties.TTL_JOB_MAINTENANCE_TIME_END);
            DynamicConfig.getInstance()
                .loadValue(logger, ConnectionProperties.TTL_JOB_MAINTENANCE_TIME_END, valStr);
            TtlConfigUtil.setTtlJobMaintenanceTimeEnd(
                DynamicConfig.getInstance().getTtlJobMaintenanceTimeEnd());
        }

        if (p.containsKey(ConnectionProperties.TTL_GLOBAL_WORKER_DN_RATIO)) {
            String valStr = p.getProperty(ConnectionProperties.TTL_GLOBAL_WORKER_DN_RATIO);
            DynamicConfig.getInstance()
                .loadValue(logger, ConnectionProperties.TTL_GLOBAL_WORKER_DN_RATIO, valStr);
            TtlConfigUtil.setTtlGlobalWorkerDnRatio(DynamicConfig.getInstance().getTtlGlobalWorkerDnRatio());
        }

        if (p.containsKey(ConnectionProperties.TTL_DEFAULT_ARC_PRE_ALLOCATE_COUNT)) {
            String valStr = p.getProperty(ConnectionProperties.TTL_DEFAULT_ARC_PRE_ALLOCATE_COUNT);
            DynamicConfig.getInstance()
                .loadValue(logger, ConnectionProperties.TTL_DEFAULT_ARC_PRE_ALLOCATE_COUNT, valStr);
            TtlConfigUtil.setPreBuiltPartCntForCreatColumnarIndex(
                DynamicConfig.getInstance().getTtlDefaultArcPreAllocateCount());
        }

        if (p.containsKey(ConnectionProperties.TTL_DEFAULT_ARC_POST_ALLOCATE_COUNT)) {
            String valStr = p.getProperty(ConnectionProperties.TTL_DEFAULT_ARC_POST_ALLOCATE_COUNT);
            DynamicConfig.getInstance()
                .loadValue(logger, ConnectionProperties.TTL_DEFAULT_ARC_POST_ALLOCATE_COUNT, valStr);
            TtlConfigUtil.setPostBuiltPartCntForCreateColumnarIndex(
                DynamicConfig.getInstance().getTtlDefaultArcPostAllocateCount());
        }

        if (p.containsKey(ConnectionProperties.TTL_ENABLE_AUTO_ADD_PARTS_FOR_ARC_CCI)) {
            String valStr = p.getProperty(ConnectionProperties.TTL_ENABLE_AUTO_ADD_PARTS_FOR_ARC_CCI);
            DynamicConfig.getInstance()
                .loadValue(logger, ConnectionProperties.TTL_ENABLE_AUTO_ADD_PARTS_FOR_ARC_CCI, valStr);
            TtlConfigUtil.setEnableAutoAddPartsForArcCci(
                DynamicConfig.getInstance().getTtlEnableAutoAddPartsForArcCci());
        }

        if (p.containsKey(ConnectionProperties.FULL_SCAN_TABLE_BLACK_LIST)) {
            String valStr = p.getProperty(ConnectionProperties.FULL_SCAN_TABLE_BLACK_LIST);
            DynamicConfig.getInstance()
                .loadValue(logger, ConnectionProperties.FULL_SCAN_TABLE_BLACK_LIST, valStr);
            FullScanTableBlackListManager.getInstance().reload(DynamicConfig.getInstance().getFullScanTableBlackList());
        }

        if (CobarServer.getInstance().isInited()) {
            CobarServer.getInstance().reloadSystemConfig();
        }

        if (p.containsKey(ConnectionProperties.ENABLE_LOWER_CASE_TABLE_NAMES)) {
            boolean enableLowerCase =
                Boolean.parseBoolean(p.getProperty(ConnectionProperties.ENABLE_LOWER_CASE_TABLE_NAMES));
            InformationSchemaViewManager.getInstance().defineCaseSensitiveView(enableLowerCase);
        }

        if (p.containsKey(ConnectionProperties.MAPPING_TO_MYSQL_ERROR_CODE)) {
            DynamicConfig.getInstance().loadValue(logger,
                ConnectionProperties.MAPPING_TO_MYSQL_ERROR_CODE,
                p.getProperty(ConnectionProperties.MAPPING_TO_MYSQL_ERROR_CODE));
        }

        if (p.containsKey(ConnectionProperties.COLUMNAR_TSO_UPDATE_INTERVAL)) {
            int columnarTsoUpdateInterval =
                Integer.parseInt(p.getProperty(ConnectionProperties.COLUMNAR_TSO_UPDATE_INTERVAL));
            ColumnarTsoManager.getInstance().resetColumnarTsoUpdateInterval(columnarTsoUpdateInterval);
        }

        if (p.containsKey(ConnectionProperties.CSV_CACHE_SIZE)) {
            int newSize = Integer.parseInt(p.getProperty(ConnectionProperties.CSV_CACHE_SIZE));
            ((DynamicColumnarManager) ColumnarManager.getInstance()).resetCsvCacheSize(newSize);
        }

        if (p.containsKey(ConnectionProperties.COLUMNAR_TSO_PURGE_INTERVAL)) {
            int columnarTsoPurgeInterval =
                Integer.parseInt(p.getProperty(ConnectionProperties.COLUMNAR_TSO_PURGE_INTERVAL));
            ColumnarTsoManager.getInstance().resetColumnarTsoPurgeInterval(columnarTsoPurgeInterval);
        }

        if (p.containsKey(ConnectionProperties.COLUMNAR_SNAPSHOT_CACHE_TTL_MS)) {
            int cacheTtlMs = Integer.parseInt(p.getProperty(ConnectionProperties.COLUMNAR_SNAPSHOT_CACHE_TTL_MS));
            ((DynamicColumnarManager) ColumnarManager.getInstance()).resetSnapshotCacheTtlMs(cacheTtlMs);
        }

        if (p.containsKey(ConnectionProperties.PLAN_CACHE_SIZE)) {
            int newSize = InstConfUtil.getInt(PLAN_CACHE_SIZE);
            PlanCache.getInstance().resize(newSize);
        }

        if (p.containsKey(ConnectionProperties.ENABLE_LBAC)) {
            for (com.alibaba.polardbx.common.utils.Pair<String, String> pair :
                LBACSecurityManager.getInstance().getAllTableWithPolicy()) {
                PlanManager.getInstance().invalidateTable(pair.getKey(), pair.getValue(), true);
            }
        }

        logger.info("load instance properties ok");
        logger.info(String.valueOf(p.toString()));
    }

    @Override
    protected void doDestroy() {
        appLoader.doDestroy();
    }

    public BaseAppLoader getAppLoader() {
        return appLoader;
    }

    public boolean isLock() {
        return isLock;
    }

}
