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
import com.alibaba.polardbx.common.utils.version.InstanceVersion;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.config.SystemConfig;
import com.alibaba.polardbx.executor.common.GsiStatisticsManager;
import com.alibaba.polardbx.executor.ddl.workqueue.ChangeSetThreadPool;
import com.alibaba.polardbx.executor.ddl.workqueue.PriorityWorkQueue;
import com.alibaba.polardbx.gms.ha.impl.StorageHaChecker;
import com.alibaba.polardbx.gms.ha.impl.StorageHaManager;
import com.alibaba.polardbx.gms.privilege.PolarLoginErrConfig;
import com.alibaba.polardbx.gms.privilege.PolarPrivManager;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import com.alibaba.polardbx.gms.util.GmsJdbcUtil;
import com.alibaba.polardbx.optimizer.core.profiler.RuntimeStat;
import com.alibaba.polardbx.optimizer.hint.util.HintUtil;
import com.alibaba.polardbx.optimizer.memory.MemoryManager;
import com.alibaba.polardbx.optimizer.memory.MemorySetting;
import com.alibaba.polardbx.optimizer.view.InformationSchemaViewManager;
import com.alibaba.polardbx.server.util.LogUtils;
import com.alibaba.polardbx.util.RexMemoryLimitHelper;
import org.apache.calcite.rex.RexUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.Properties;

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

        if (p.containsKey(ConnectionProperties.STORAGE_HA_TASK_PERIOD)) {
            Integer newHaTaskPeriod = Integer.valueOf(p.getProperty(ConnectionProperties.STORAGE_HA_TASK_PERIOD));
            if (newHaTaskPeriod > 0) {
                StorageHaManager.getInstance().adjustStorageHaTaskPeriod(newHaTaskPeriod);
            }
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

        if (p.containsKey(ConnectionProperties.TRANSACTION_ISOLATION)) {
            String str = p.getProperty(ConnectionProperties.TRANSACTION_ISOLATION);
            // try to parse string
            IsolationLevel level = IsolationLevel.parse(str);
            if (level == null) {
                throw new IllegalArgumentException("unknown isolation level setting : " + str);
            }

            ConfigDataMode.setTxIsolation(level.getCode());
            logger.info("Transaction isolation level set to " + level.nameWithHyphen());
        }

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

        for (Field field : ConnectionProperties.class.getDeclaredFields()) {
            try {
                String key = field.get(ConnectionProperties.class).toString();
                if (key != null && key.toUpperCase().startsWith("MPP_")) {
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

        if (CobarServer.getInstance().isInited()) {
            CobarServer.getInstance().reloadSystemConfig();
        }

        if (p.containsKey(ConnectionProperties.ENABLE_LOWER_CASE_TABLE_NAMES)) {
            boolean enableLowerCase =
                Boolean.parseBoolean(p.getProperty(ConnectionProperties.ENABLE_LOWER_CASE_TABLE_NAMES));
            InformationSchemaViewManager.getInstance().defineCaseSensitiveView(enableLowerCase);
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
