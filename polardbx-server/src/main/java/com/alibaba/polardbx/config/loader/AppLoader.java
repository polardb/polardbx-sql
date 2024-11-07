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

import com.alibaba.polardbx.CobarServer;
import com.alibaba.polardbx.common.TrxIdGenerator;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.MppConfig;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.config.SchemaConfig;
import com.alibaba.polardbx.config.ServerConfigManager;
import com.alibaba.polardbx.config.SystemConfig;
import com.alibaba.polardbx.executor.mdl.MdlManager;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.matrix.jdbc.TDataSource;
import com.alibaba.polardbx.matrix.jdbc.utils.TDataSourceInitUtils;
import com.alibaba.polardbx.optimizer.config.schema.InformationSchema;
import com.alibaba.polardbx.optimizer.statis.SQLRecorder;

import java.util.Set;
import java.util.TreeSet;

/**
 * 加载一个appname对应的资源,比如用户密码/tddl数据源等
 *
 * @author jianghang 2014-5-28 下午5:09:41
 * @since 5.1.0
 */
public abstract class AppLoader extends BaseAppLoader {

    protected static final Logger logger = LoggerFactory.getLogger(AppLoader.class);

    public AppLoader(String cluster, String unitName) {
        super(cluster, unitName);
    }

    @Override
    protected void loadMdlManager(String dbName) {
        // lazy load
    }

    @Override
    protected void unloadMdlManager(String dbName) {
        MdlManager.removeInstance(dbName);
    }

    @Override
    protected synchronized void loadSchema(final String dbName, final String appName) {
        SchemaConfig schema = schemas.get(dbName);
        if (schema != null) {
            return;
        }

        SystemConfig system = CobarServer.getInstance().getConfig().getSystem();
        TDataSource ds = new TDataSource();
        ds.setServerConfigManager(new ServerConfigManager(CobarServer.getInstance()));
        ds.putConnectionProperties(ConnectionProperties.CHOOSE_STREAMING, true);
        ds.putConnectionProperties(ConnectionProperties.PROCESS_AUTO_INCREMENT_BY_SEQUENCE, true);
        ds.putConnectionProperties(ConnectionProperties.INIT_CONCURRENT_POOL_EVERY_CONNECTION, false);
        ds.putConnectionProperties(ConnectionProperties.ENABLE_SELF_CROSS_JOIN, true);
        ds.putConnectionProperties(ConnectionProperties.ENABLE_ALTER_DDL, true);
        ds.putConnectionProperties(ConnectionProperties.ENABLE_DDL, ConfigDataMode.needInitMasterModeResource());
        ds.putConnectionProperties(ConnectionProperties.NET_WRITE_TIMEOUT, 28800);
        ds.putConnectionProperties(ConnectionProperties.RETRY_ERROR_SQL_ON_OLD_SERVER,
            system.isRetryErrorSqlOnOldServer());
        ds.putConnectionProperties(ConnectionProperties.ENABLE_MPP, system.isEnableMpp());
        ds.putConnectionProperties(ConnectionProperties.MPP_TABLESCAN_CONNECTION_STRATEGY,
            MppConfig.getInstance().getTableScanConnectionStrategy());
        ds.putConnectionProperties(ConnectionProperties.STATISTIC_COLLECTOR_FROM_RULE,
            !system.isEnableCollectorAllTables());
        ds.putConnectionProperties(ConnectionProperties.ENABLE_SCALE_OUT_FEATURE, system.getEnableScaleout());
        ds.putConnectionProperties(ConnectionProperties.SCALE_OUT_DROP_DATABASE_AFTER_SWITCH_DATASOURCE,
            system.getDropOldDataBaseAfterSwitchDataSource());

        if (ConfigDataMode.isColumnarMode()) {
            ds.putConnectionProperties(ConnectionProperties.ENABLE_COLUMNAR_OPTIMIZER, true);
        }

        if (system.getWorkloadType() != null) {
            ds.putConnectionProperties(ConnectionProperties.WORKLOAD_TYPE, system.getWorkloadType());
        }

        if (system.isEnableRemoteRPC()) {
            ds.putConnectionProperties(ConnectionProperties.MPP_RPC_LOCAL_ENABLED, false);
        }

        if (system.isEnableMasterMpp()) {
            ds.putConnectionProperties(ConnectionProperties.ENABLE_MASTER_MPP, true);
        }

        // 共享一个线程池
        ds.setGlobalExecutorService(CobarServer.getInstance().getServerExecutor());
        ds.setSharding(false);// 允许非sharding启动
        ds.setAppName(appName);
        ds.setSchemaName(dbName);
        ds.setUnitName(unitName);
        ds.setTraceIdGenerator(TrxIdGenerator.getInstance().getIdGenerator());
        ds.setDefaultDb(dbName.equals(SystemDbHelper.DEFAULT_DB_NAME)
            || dbName.equalsIgnoreCase(InformationSchema.NAME));

        schema = new SchemaConfig(dbName);
        schema.setDataSource(ds);
        ds.setRecorder(
            new SQLRecorder(system.getSqlRecordCount(), system.getSlowSqlSizeThresold()));
        ds.setPhysicalRecorder(
            new SQLRecorder(system.getSqlRecordCount(), system.getSlowSqlSizeThresold()));
        if (ds.isDefaultDb()) {
            TDataSourceInitUtils.initDataSource(ds);
        }
        schemas.put(dbName, schema);
    }

    @Override
    protected synchronized void unLoadSchema(final String dbName, final String appName) {
        SchemaConfig schema = schemas.remove(dbName);
        if (schema != null) {
            schema.setDropped(true);
            TDataSource dataSource = schema.getDataSource();
            if (dataSource != null) {
                dataSource.destroy();
            }
        }
    }

}
