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

package com.alibaba.polardbx.gms.config.impl;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.timezone.InternalTimeZone;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.gms.topology.DbInfoRecord;
import com.alibaba.polardbx.gms.topology.StorageInfoRecord;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import com.alibaba.polardbx.gms.topology.InstConfigAccessor;
import com.alibaba.polardbx.gms.topology.StorageInfoAccessor;
import com.alibaba.polardbx.gms.util.InstIdUtil;
import com.alibaba.polardbx.gms.util.MetaDbLogUtil;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * Helper to prepare default properties and put them into metadb.inst_config
 *
 * @author chenghui.lch
 */
public class SystemDefaultPropertyHelper {
    private final static Logger logger = LoggerFactory.getLogger(SystemDefaultPropertyHelper.class);

    private final static String countContainShardingTypeLogDbSql =
        String.format("select count(1) from db_info where db_type=%s",
            DbInfoRecord.DB_TYPE_PART_DB);

    public static void initDefaultInstConfig() {

        Properties sysDefaultPropertyMap = prepareSystemDefaultProperties();

        // For each default property
        Properties propsToBeInsert = new Properties();
        for (String propName : sysDefaultPropertyMap.stringPropertyNames()) {
            Object val = sysDefaultPropertyMap.get(propName);
            if (val == null) {
                continue;
            }
            propsToBeInsert.put(propName, String.valueOf(val));
        }

        // Add (by using insert ignore) the default val of conn props to meta db
        // if the prop key does not exist in the table inst_config of meta db
        registerProperties(propsToBeInsert);

    }

    protected static void registerProperties(Properties properties) {
        try (Connection conn = MetaDbDataSource.getInstance().getConnection()) {
            conn.setAutoCommit(false);
            InstConfigAccessor instConfigAcc = new InstConfigAccessor();
            instConfigAcc.setConnection(conn);
            instConfigAcc.addInstConfigs(InstIdUtil.getInstId(), properties);
            conn.commit();
        } catch (Throwable ex) {
            logger.warn("Failed to register to system default properties into metadb, err is" + ex.getMessage(), ex);
            MetaDbLogUtil.META_DB_LOG
                .warn("Failed to register to system default properties into metadb, err is" + ex.getMessage(), ex);
        }
    }

    protected static Properties prepareSystemDefaultProperties() {

        Properties sysDefaultProperties = new Properties();

        // Prepare the default storageInstId list for single groups
        prepareDefaultSingleGroupStorageInstIdSet(sysDefaultProperties);

        // Prepare the default conn pool for druid datasource of all groups
        prepareDefaultConnPoolConfingProperties(sysDefaultProperties);

        // Prepare the default logical db count
        int logicalDbCnt = DbTopologyManager.DEFAULT_MAX_LOGICAL_DB_COUNT;
        sysDefaultProperties.put(ConnectionProperties.MAX_LOGICAL_DB_COUNT, String.valueOf(logicalDbCnt));

        // whether to enable the scaleOut feature
        sysDefaultProperties.put(ConnectionProperties.ENABLE_SCALE_OUT_FEATURE,
            ConnectionParams.ENABLE_SCALE_OUT_FEATURE.getDefault());

        sysDefaultProperties.put(ConnectionProperties.SCALE_OUT_DROP_DATABASE_AFTER_SWITCH_DATASOURCE,
            ConnectionParams.SCALE_OUT_DROP_DATABASE_AFTER_SWITCH_DATASOURCE.getDefault());

        sysDefaultProperties.put(ConnectionProperties.ALLOW_SIMPLE_SEQUENCE,
            ConnectionParams.ALLOW_SIMPLE_SEQUENCE.getDefault());

        sysDefaultProperties.put(ConnectionProperties.CDC_STARTUP_MODE,
            ConnectionParams.CDC_STARTUP_MODE.getDefault());

        sysDefaultProperties.put(ConnectionProperties.ENABLE_CDC_META_BUILD_SNAPSHOT,
            ConnectionParams.ENABLE_CDC_META_BUILD_SNAPSHOT.getDefault());

        // Prepare the default logical time zone
        sysDefaultProperties.put(ConnectionProperties.LOGICAL_DB_TIME_ZONE, String.valueOf(
            InternalTimeZone.defaultTimeZone.getMySqlTimeZoneName()));

        //  The read/write parallelism of one phy group of auto-mode db
        sysDefaultProperties.put(ConnectionProperties.GROUP_PARALLELISM, String.valueOf(
            ConnectionParams.GROUP_PARALLELISM.getDefault()));

        // Prepare some default properties for partition management
        prepareDefaultPropertiesForPartitionManagementProperties(sysDefaultProperties);

        return sysDefaultProperties;
    }

    private static void prepareDefaultSingleGroupStorageInstIdSet(Properties sysDefaultProperties) {

        if (!ConfigDataMode.isMasterMode()) {
            return;
        }

        List<String> allStorageInstSet = new ArrayList<>();
        try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {
            StorageInfoAccessor storageInfoAccessor = new StorageInfoAccessor();
            storageInfoAccessor.setConnection(metaDbConn);
            allStorageInstSet = storageInfoAccessor.getStorageIdListByInstIdAndInstKind(InstIdUtil.getInstId(),
                StorageInfoRecord.INST_KIND_MASTER);
        } catch (Throwable ex) {
            MetaDbLogUtil.META_DB_LOG.error(ex);
        }
        String storageInstId = "";
        if (allStorageInstSet.size() >= 1) {
            Collections.sort(allStorageInstSet);
            storageInstId = allStorageInstSet.get(0);
            sysDefaultProperties.put(ConnectionProperties.SINGLE_GROUP_STORAGE_INST_LIST, storageInstId);
        }
    }

    private static void prepareDefaultConnPoolConfingProperties(Properties sysDefaultProperties) {
        ConnPoolConfig defaultConnPoolConfig = ConnPoolConfigManager.buildDefaultConnPoolConfig();
        Properties properties = new Properties();
        properties.setProperty(ConnectionProperties.CONN_POOL_PROPERTIES, defaultConnPoolConfig.connProps);
        properties.setProperty(ConnectionProperties.CONN_POOL_MIN_POOL_SIZE,
            String.valueOf(defaultConnPoolConfig.minPoolSize));
        properties.setProperty(ConnectionProperties.CONN_POOL_MAX_POOL_SIZE,
            String.valueOf(defaultConnPoolConfig.maxPoolSize));
        properties.setProperty(ConnectionProperties.CONN_POOL_MAX_WAIT_THREAD_COUNT,
            String.valueOf(defaultConnPoolConfig.maxWaitThreadCount));
        properties.setProperty(ConnectionProperties.CONN_POOL_IDLE_TIMEOUT,
            String.valueOf(defaultConnPoolConfig.idleTimeout));
        properties.setProperty(ConnectionProperties.CONN_POOL_BLOCK_TIMEOUT,
            String.valueOf(defaultConnPoolConfig.blockTimeout));
        properties.setProperty(ConnectionProperties.CONN_POOL_XPROTO_CONFIG, defaultConnPoolConfig.xprotoConfig);
        properties
            .setProperty(ConnectionProperties.CONN_POOL_XPROTO_FLAG, String.valueOf(defaultConnPoolConfig.xprotoFlag));
        // CONN_POOL_XPROTO_META_DB_PORT is always set manually.
        properties.setProperty(ConnectionProperties.CONN_POOL_XPROTO_STORAGE_DB_PORT,
            String.valueOf(defaultConnPoolConfig.xprotoStorageDbPort));
        properties.setProperty(ConnectionProperties.CONN_POOL_XPROTO_MAX_CLIENT_PER_INST,
            String.valueOf(defaultConnPoolConfig.xprotoMaxClientPerInstance));
        properties.setProperty(ConnectionProperties.CONN_POOL_XPROTO_MAX_SESSION_PER_CLIENT,
            String.valueOf(defaultConnPoolConfig.xprotoMaxSessionPerClient));
        properties.setProperty(ConnectionProperties.CONN_POOL_XPROTO_MAX_POOLED_SESSION_PER_INST,
            String.valueOf(defaultConnPoolConfig.xprotoMaxPooledSessionPerInstance));
        properties.setProperty(ConnectionProperties.CONN_POOL_XPROTO_MIN_POOLED_SESSION_PER_INST,
            String.valueOf(defaultConnPoolConfig.xprotoMinPooledSessionPerInstance));
        properties.setProperty(ConnectionProperties.CONN_POOL_XPROTO_SESSION_AGING_TIME,
            String.valueOf(defaultConnPoolConfig.xprotoSessionAgingTime));
        properties.setProperty(ConnectionProperties.CONN_POOL_XPROTO_SLOW_THRESH,
            String.valueOf(defaultConnPoolConfig.xprotoSlowThreshold));
        properties.setProperty(ConnectionProperties.CONN_POOL_XPROTO_AUTH,
            String.valueOf(defaultConnPoolConfig.xprotoAuth));
        properties.setProperty(ConnectionProperties.CONN_POOL_XPROTO_AUTO_COMMIT_OPTIMIZE,
            String.valueOf(defaultConnPoolConfig.xprotoAutoCommitOptimize));
        properties.setProperty(ConnectionProperties.CONN_POOL_XPROTO_XPLAN,
            String.valueOf(defaultConnPoolConfig.xprotoXplan));
        properties.setProperty(ConnectionProperties.CONN_POOL_XPROTO_XPLAN_EXPEND_STAR,
            String.valueOf(defaultConnPoolConfig.xprotoXplanExpendStar));
        properties.setProperty(ConnectionProperties.CONN_POOL_XPROTO_XPLAN_TABLE_SCAN,
            String.valueOf(defaultConnPoolConfig.xprotoXplanTableScan));
        properties.setProperty(ConnectionProperties.CONN_POOL_XPROTO_TRX_LEAK_CHECK,
            String.valueOf(defaultConnPoolConfig.xprotoTrxLeakCheck));
        properties.setProperty(ConnectionProperties.CONN_POOL_XPROTO_MESSAGE_TIMESTAMP,
            String.valueOf(defaultConnPoolConfig.xprotoMessageTimestamp));
        properties.setProperty(ConnectionProperties.CONN_POOL_XPROTO_PLAN_CACHE,
            String.valueOf(defaultConnPoolConfig.xprotoPlanCache));
        properties.setProperty(ConnectionProperties.CONN_POOL_XPROTO_CHUNK_RESULT,
            String.valueOf(defaultConnPoolConfig.xprotoChunkResult));
        properties.setProperty(ConnectionProperties.CONN_POOL_XPROTO_PURE_ASYNC_MPP,
            String.valueOf(defaultConnPoolConfig.xprotoPureAsyncMpp));
        properties.setProperty(ConnectionProperties.CONN_POOL_XPROTO_DIRECT_WRITE,
            String.valueOf(defaultConnPoolConfig.xprotoDirectWrite));
        properties.setProperty(ConnectionProperties.CONN_POOL_XPROTO_FEEDBACK,
            String.valueOf(defaultConnPoolConfig.xprotoFeedback));
        properties.setProperty(ConnectionProperties.CONN_POOL_XPROTO_CHECKER,
            String.valueOf(defaultConnPoolConfig.xprotoChecker));
        properties.setProperty(ConnectionProperties.CONN_POOL_XPROTO_MAX_PACKET_SIZE,
            String.valueOf(defaultConnPoolConfig.xprotoMaxPacketSize));
        properties.setProperty(ConnectionProperties.CONN_POOL_XPROTO_QUERY_TOKEN,
            String.valueOf(defaultConnPoolConfig.xprotoQueryToken));
        properties.setProperty(ConnectionProperties.CONN_POOL_XPROTO_PIPE_BUFFER_SIZE,
            String.valueOf(defaultConnPoolConfig.xprotoPipeBufferSize));
        sysDefaultProperties.putAll(properties);
    }

    private static void prepareDefaultPropertiesForPartitionManagementProperties(Properties sysDefaultProperties) {

        boolean enableAutoPartModeAsDefault = checkNeedEnableAutoPartitionMode();
        if (enableAutoPartModeAsDefault) {
//            sysDefaultProperties.put(ConnectionProperties.DEFAULT_PARTITION_MODE,
//                "auto");
            /**
             * Use drds as the default partition mode even if there are no any logigcal db with mode='auto'
             */
            sysDefaultProperties.put(ConnectionProperties.DEFAULT_PARTITION_MODE,
                "drds");
        } else {
            sysDefaultProperties.put(ConnectionProperties.DEFAULT_PARTITION_MODE,
                "auto");
        }

        sysDefaultProperties.put(ConnectionProperties.AUTO_PARTITION_PARTITIONS,
            String.valueOf(DbTopologyManager.decideAutoPartitionCount()));
        sysDefaultProperties.put(ConnectionProperties.SHOW_HASH_PARTITIONS_BY_RANGE,
            ConnectionParams.SHOW_HASH_PARTITIONS_BY_RANGE.getDefault());
        sysDefaultProperties
            .put(ConnectionProperties.SHOW_TABLE_GROUP_NAME, ConnectionParams.SHOW_TABLE_GROUP_NAME.getDefault());
        sysDefaultProperties.put(ConnectionProperties.MAX_PHYSICAL_PARTITION_COUNT,
            ConnectionParams.MAX_PHYSICAL_PARTITION_COUNT.getDefault());
        sysDefaultProperties.put(ConnectionProperties.MAX_PARTITION_COLUMN_COUNT,
            ConnectionParams.MAX_PARTITION_COLUMN_COUNT.getDefault());
        sysDefaultProperties.put(ConnectionProperties.PARTITION_PRUNING_STEP_COUNT_LIMIT,
            ConnectionParams.PARTITION_PRUNING_STEP_COUNT_LIMIT.getDefault());
        sysDefaultProperties.put(ConnectionProperties.ENABLE_AUTO_MERGE_INTERVALS_IN_PRUNING,
            ConnectionParams.ENABLE_AUTO_MERGE_INTERVALS_IN_PRUNING.getDefault());
        sysDefaultProperties.put(ConnectionProperties.ENABLE_INTERVAL_ENUMERATION_IN_PRUNING,
            ConnectionParams.ENABLE_INTERVAL_ENUMERATION_IN_PRUNING.getDefault());
        sysDefaultProperties.put(ConnectionProperties.MAX_ENUMERABLE_INTERVAL_LENGTH,
            ConnectionParams.MAX_ENUMERABLE_INTERVAL_LENGTH.getDefault());
        sysDefaultProperties.put(ConnectionProperties.MAX_IN_SUBQUERY_PRUNING_SIZE,
            ConnectionParams.MAX_IN_SUBQUERY_PRUNING_SIZE.getDefault());
        sysDefaultProperties.put(ConnectionProperties.USE_FAST_SINGLE_POINT_INTERVAL_MERGING,
            ConnectionParams.USE_FAST_SINGLE_POINT_INTERVAL_MERGING.getDefault());
        sysDefaultProperties.put(ConnectionProperties.ENABLE_CONST_EXPR_EVAL_CACHE,
            ConnectionParams.ENABLE_CONST_EXPR_EVAL_CACHE.getDefault());

    }

    protected static Boolean checkNeedEnableAutoPartitionMode() {
        int shardingDbCnt = 0;
        try (Connection conn = MetaDbDataSource.getInstance().getConnection(); Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(countContainShardingTypeLogDbSql)) {
            if (rs != null) {
                rs.next();
                shardingDbCnt = rs.getInt(1);
            }
        } catch (Throwable ex) {
            logger.warn("Failed to count the sharding type logical db", ex);
        }
        return shardingDbCnt == 0;
    }
}
