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

package com.alibaba.polardbx.server.util;

import com.alibaba.polardbx.CobarServer;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.config.SchemaConfig;
import com.alibaba.polardbx.config.ServerConfigManager;
import com.alibaba.polardbx.config.SystemConfig;
import com.alibaba.polardbx.matrix.jdbc.TConnection;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.alibaba.polardbx.common.TrxIdGenerator;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.MppConfig;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.config.SchemaConfig;
import com.alibaba.polardbx.config.ServerConfigManager;
import com.alibaba.polardbx.config.SystemConfig;
import com.alibaba.polardbx.matrix.jdbc.TDataSource;
import com.alibaba.polardbx.optimizer.statis.SQLRecorder;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import java.util.Map;
import java.util.concurrent.ExecutionException;

public class MockUtil {
    protected final static Logger logger = LoggerFactory.getLogger(MockUtil.class);

    private static final LoadingCache<String, SchemaConfig> schemas = CacheBuilder.newBuilder().build(
        new CacheLoader<String, SchemaConfig>() {
            @Override
            public SchemaConfig load(String schemaName) {
                return initSchema(schemaName);
            }
        });

    static {
        if (ConfigDataMode.isFastMock()) {
            try {
                mockSchema("information_schema");
            } catch (Exception e) {
                logger.error("mock information_schema failed", e);
            }
        }
    }

    public static Map<String, SchemaConfig> schemas() {
        return schemas.asMap();
    }

    public static SchemaConfig mockSchema(String schemaName) {
        try {
            return schemas.get(schemaName);
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        return null;
    }

    private static SchemaConfig initSchema(String schemaName) {
        SchemaConfig schemaConfig = new SchemaConfig(schemaName);

        SystemConfig system = CobarServer.getInstance().getConfig().getSystem();
        TDataSource ds = new TDataSource();
        ds.setServerConfigManager(new ServerConfigManager(CobarServer.getInstance()));
        ds.putConnectionProperties(ConnectionProperties.CHOOSE_STREAMING, true);
        ds.putConnectionProperties(ConnectionProperties.PROCESS_AUTO_INCREMENT_BY_SEQUENCE, true);
        ds.putConnectionProperties(ConnectionProperties.INIT_CONCURRENT_POOL_EVERY_CONNECTION, false);
        ds.putConnectionProperties(ConnectionProperties.ENABLE_SELF_CROSS_JOIN, true);
        ds.putConnectionProperties(ConnectionProperties.NET_WRITE_TIMEOUT, 28800);
        ds.putConnectionProperties(ConnectionProperties.RETRY_ERROR_SQL_ON_OLD_SERVER,
            system.isRetryErrorSqlOnOldServer());
        ds.putConnectionProperties(ConnectionProperties.ENABLE_MPP, system.isEnableMpp());
        ds.putConnectionProperties(ConnectionProperties.MPP_TABLESCAN_CONNECTION_STRATEGY,
            MppConfig.getInstance().getTableScanConnectionStrategy());
        ds.putConnectionProperties(ConnectionProperties.STATISTIC_COLLECTOR_FROM_RULE,
            !system.isEnableCollectorAllTables());

        if (system.getCoronaMode() == 1) {
            //corona模式下rule不全，也会从0库加载表
            ds.putConnectionProperties(ConnectionProperties.SHOW_TABLES_FROM_RULE_ONLY, false);
        }

        // 共享一个线程池
        ds.setGlobalExecutorService(CobarServer.getInstance().getServerExecutor());
        ds.setSharding(false);// 允许非sharding启动
        ds.setAppName(schemaName);
        ds.setSchemaName(schemaName);
        ds.setUnitName(null);
        ds.setTraceIdGenerator(TrxIdGenerator.getInstance().getIdGenerator());

        ds.setRecorder(
            new SQLRecorder(system.getSqlRecordCount(), system.getSlowSqlSizeThresold(), system.getSlowSqlTime()));
        ds.setPhysicalRecorder(
            new SQLRecorder(system.getSqlRecordCount(), system.getSlowSqlSizeThresold(), system.getSlowSqlTime()));

        ds.init();
        schemaConfig.setDataSource(ds);
        return schemaConfig;
    }

    public static void destroySchema(String dbName) {
        try {
            schemas.get(dbName).getDataSource().doDestroy();
            schemas.invalidate(dbName);
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }
}
