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

import com.alibaba.polardbx.ClusterSyncManager;
import com.alibaba.polardbx.CobarServer;
import com.alibaba.polardbx.common.IdGenerator;
import com.alibaba.polardbx.common.TrxIdGenerator;
import com.alibaba.polardbx.common.properties.SystemPropertiesHelper;
import com.alibaba.polardbx.common.utils.AsyncUtils;
import com.alibaba.polardbx.common.utils.CaseInsensitive;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.InstanceRole;
import com.alibaba.polardbx.common.utils.thread.ExecutorUtil;
import com.alibaba.polardbx.common.utils.version.InstanceVersion;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.config.InstanceRoleManager;
import com.alibaba.polardbx.config.SchemaConfig;
import com.alibaba.polardbx.config.SystemConfig;
import com.alibaba.polardbx.executor.ddl.job.task.basic.pl.accessor.FunctionAccessor;
import com.alibaba.polardbx.executor.pl.PLUtils;
import com.alibaba.polardbx.executor.pl.UdfUtils;
import com.alibaba.polardbx.gms.config.InstConfigReceiver;
import com.alibaba.polardbx.gms.config.impl.MetaDbInstConfigManager;
import com.alibaba.polardbx.gms.config.impl.MetaDbVariableConfigManager;
import com.alibaba.polardbx.gms.ha.impl.StorageHaManager;
import com.alibaba.polardbx.gms.listener.ConfigListener;
import com.alibaba.polardbx.gms.listener.impl.MetaDbConfigManager;
import com.alibaba.polardbx.gms.listener.impl.MetaDbDataIdBuilder;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.metadb.pl.function.FunctionMetaRecord;
import com.alibaba.polardbx.gms.node.GmsNodeManager;
import com.alibaba.polardbx.gms.privilege.PolarPrivManager;
import com.alibaba.polardbx.gms.sync.GmsSyncManagerHelper;
import com.alibaba.polardbx.gms.topology.DbGroupInfoManager;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.gms.topology.DbInfoRecord;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import com.alibaba.polardbx.gms.topology.InstLockAccessor;
import com.alibaba.polardbx.gms.topology.InstLockRecord;
import com.alibaba.polardbx.gms.topology.ServerInstIdManager;
import com.alibaba.polardbx.gms.util.InstIdUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.matrix.jdbc.TDataSource;
import com.alibaba.polardbx.matrix.jdbc.utils.TDataSourceInitUtils;
import com.alibaba.polardbx.optimizer.ccl.CclManager;
import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

public class GmsClusterLoader extends ClusterLoader {

    private final SystemConfig systemConfig;
    protected String instanceId;
    protected String instanceName;
    protected String instanceType;
    protected boolean isUsing = false;
    protected Long opVersion = -1L;

    protected static class InstInfoListener implements ConfigListener {

        protected final GmsClusterLoader gmsClusterLoader;

        public InstInfoListener(GmsClusterLoader gmsClusterLoader) {
            this.gmsClusterLoader = gmsClusterLoader;
        }

        @Override
        public void onHandleConfig(String dataId, long newOpVersion) {

            // reload all read-only server inst ids
            ServerInstIdManager.getInstance().reload();

            // Reload all nodes including master and read-only.
            this.gmsClusterLoader.loadNodeInfos();

            // Init group configs for all read-only storage inst
            DbTopologyManager.initGroupConfigsForReadOnlyInstIfNeed();
        }
    }

    protected static class ServerInfoListener implements ConfigListener {
        protected GmsClusterLoader gmsClusterLoader;

        public ServerInfoListener(GmsClusterLoader clusterLoader) {
            this.gmsClusterLoader = clusterLoader;
        }

        @Override
        public void onHandleConfig(String dataId, long newOpVersion) {
            this.gmsClusterLoader.loadNodeInfos();
        }
    }

    protected static class InstLockConfigListener implements ConfigListener {

        @Override
        public void onHandleConfig(String dataId, long newOpVersion) {
            String instId = InstIdUtil.getInstId();
            processInstLockByGms(instId);
        }
    }

    protected static class InstPropertiesConfigListener implements ConfigListener {

        @Override
        public void onHandleConfig(String dataId, long newOpVersion) {
            MetaDbInstConfigManager.getInstance().reloadInstConfig();
        }
    }

    protected static class DbInfoConfigListener implements ConfigListener {

        protected GmsClusterLoader gmsClusterLoader;

        public DbInfoConfigListener(GmsClusterLoader gmsClusterLoader) {
            this.gmsClusterLoader = gmsClusterLoader;
        }

        @Override
        public void onHandleConfig(String dataId, long newOpVersion) {
            this.gmsClusterLoader.reloadDbInfoFromMetaDB();
        }
    }

    public GmsClusterLoader(SystemConfig systemConfig) {
        super(systemConfig.getClusterName(), systemConfig.getUnitName());
        this.systemConfig = systemConfig;
    }

    @Override
    public void doInit() {
        this.appLoader = new GmsAppLoader(this.cluster, this.unitName);
        if (StringUtils.isNotEmpty(cluster)) {
            this.appLoader.init();
        }
        this.loadCluster();
        this.loadLock();
    }

    protected void initDbInfoListener() {
        MetaDbConfigManager.getInstance().register(MetaDbDataIdBuilder.getDbInfoDataId(), null);
        MetaDbConfigManager.getInstance()
            .bindListener(MetaDbDataIdBuilder.getDbInfoDataId(), new DbInfoConfigListener(this));
    }

    protected void loadLock() {
        String instId = InstIdUtil.getInstId();
        processInstLockByGms(instId);
        MetaDbConfigManager.getInstance().register(MetaDbDataIdBuilder.getInstLockDataId(instId), null);
        MetaDbConfigManager.getInstance().bindListener(MetaDbDataIdBuilder.getInstLockDataId(instId),
            new InstLockConfigListener());
    }

    protected static void processInstLockByGms(String instId) {
        try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {
            InstLockAccessor instLockAccessor = new InstLockAccessor();
            instLockAccessor.setConnection(metaDbConn);
            InstLockRecord instLockRecord =
                instLockAccessor.getInstLockByInstId(instId);
            CobarServer.getInstance().getConfig().getClusterLoader().isLock =
                (instLockRecord != null && instLockRecord.locked == InstLockRecord.INST_LOCKED);
        } catch (Throwable ex) {
            throw GeneralUtil.nestedException(ex);
        }
    }

    @Override
    public void loadCluster() {
        logger.info("loadCluster:" + cluster + ",ConfigDataMode=" + ConfigDataMode.getConfigServerMode());
        try {
            loadPolarDbXCluster();
        } catch (Throwable ex) {
            throw GeneralUtil.nestedException(ex);
        }
    }

    protected void loadPolarDbXCluster() {

        // Set Sync Manager used by GMS.
        GmsSyncManagerHelper.setSyncManager(new ClusterSyncManager());

        // load node infos from server_info in metaDb
        loadServerNodeInfos();

        // load properties from inst_config in metaDb
        this.loadProperties(instanceId);

        // register listener for db_info
        initDbInfoListener();

        // load dbInfo/userPriv/dbPriv from metaDb
        initClusterAppInfo();

        // register stored function
        registerStoredFunction();

        //init ccl
        CclManager.getService();
    }

    private void registerStoredFunction() {
        try (Connection connection = MetaDbUtil.getConnection();) {
            FunctionAccessor accessor = new FunctionAccessor();
            accessor.setConnection(connection);
            List<FunctionMetaRecord> records = accessor.loadFunctionMetas();
            for(FunctionMetaRecord record: records) {
                UdfUtils.registerSqlUdf(record.routineMeta, record.canPush);
            }
        } catch (Exception ex) {
            logger.warn("Load function failed: " + ex.getCause());
        }
    }

    protected void loadServerInstIdInfos() {

        // register the listener to server_info
        String instInfoDataId = MetaDbDataIdBuilder.getInstInfoDataId();
        MetaDbConfigManager.getInstance()
            .register(instInfoDataId, null);
        MetaDbConfigManager.getInstance()
            .bindListener(instInfoDataId, new InstInfoListener(this));
    }

    protected void loadServerNodeInfos() {

        // load all server instId info (included server master inst and server read-only inst)
        // and their change  from metaDb
        loadServerInstIdInfos();

        // Load server/nodes info.
        loadNodeInfos();

        // register the listener to server_info
        String serverInfoDataId = MetaDbDataIdBuilder.getServerInfoDataId(InstIdUtil.getInstId());
        MetaDbConfigManager.getInstance()
            .register(serverInfoDataId, null);
        MetaDbConfigManager.getInstance()
            .bindListener(serverInfoDataId, new ServerInfoListener(this));

        // load instId
        this.instanceId = CobarServer.getInstance().getConfig().getInstanceId();

        // load inst version
        String instanceVersion = Optional.ofNullable(System.getProperty(InstanceVersion.systemVersion)).orElse("5");
        InstanceVersion.reloadVersion(instanceVersion);

        // load instRole and instType
        InstanceRole instRole =
            ServerInstIdManager.getInstance().isMasterInst() ? InstanceRole.MASTER : InstanceRole.LEARNER;
        initInstanceRoleConfig(instRole);

    }

    protected void initInstanceRoleConfig(InstanceRole instanceRole) {
        /*
         * no found any instRole from manager, use default instRole from
         * ReadOnlyInstManager，
         */
        if (instanceRole == null) {
            // the default role is master
            instanceRole = InstanceRoleManager.INSTANCE.getInstanceRole();
        }

        /*
         * If found instInfo is specified by local system.properties, use it
         * instead of instRole from manager.
         */
        String instInfoOfSystemProperties =
            (String) SystemPropertiesHelper.getPropertyValue(SystemPropertiesHelper.INST_ROLE);
        if (instInfoOfSystemProperties != null) {
            instanceRole = InstanceRole.valueOf(instInfoOfSystemProperties);
        }

        InstanceRoleManager.INSTANCE.setInstanceRole(instanceRole);

        /*
         * Must put instRole into System.properties because it is used by
         * Master-Slave routing in GroupDataSource
         */
        if (instanceRole != null) {
            SystemPropertiesHelper.setPropertyValue(SystemPropertiesHelper.INST_ROLE, instanceRole.toString());
        }
    }

    protected void resetIdGenerator() {
        // IdGenerator must be reset after NodeId is allocated.
        IdGenerator.remove(TrxIdGenerator.getInstance().getIdGenerator());
        TrxIdGenerator.getInstance().setIdGenerator(IdGenerator.getIdGenerator());

        // Rebind new NodeId to all IdGenerators
        IdGenerator.rebindAll();
    }

    protected void loadNodeInfos() {
        synchronized (this) {
            GmsNodeManager.getInstance().reloadNodes(systemConfig.getServerPort());
            resetIdGenerator();
        }
    }

    protected void initClusterAppInfo() {
        if (StringUtils.isEmpty(instanceId)) {
            return;
        }

        // To be load by MetaDB
        if (!appLoader.isInited()) {
            appLoader.init();
        }

        CobarServer.getInstance().getConfig().setInstanceId(this.instanceId);
        CobarServer.getInstance().getConfig().getSystem().setInstanceId(this.instanceId);
        CobarServer.getInstance().getConfig().getSystem().setInstanceType(this.instanceType);

        // CobarServer should set instance id before initializing
        // this so that we can get correct instance id in subsequent
        // MatrixConfigHolder.doInit().
        ((GmsAppLoader) appLoader).initDbUserPrivsInfo();

        warmingLogicalDb();

        // open the server port and accept query now!
        CobarServer.getInstance().online();
    }

    protected void warmingLogicalDb() {
        if (systemConfig.getEnableLogicalDbWarmmingUp()) {
            // Auto load all schemas here
            ThreadPoolExecutor threadPool = null;
            try {
                int poolSize = systemConfig.getLogicalDbWarmmingUpExecutorPoolSize();
                threadPool = ExecutorUtil.createExecutor("LogicalDb-Warmming-Up-Executor", poolSize);
                List<Future> futures = new ArrayList<>();

                for (SchemaConfig schema : appLoader.getSchemas().values()) {
                    final TDataSource ds = schema.getDataSource();
                    futures.add(threadPool.submit(() -> {
                        long startTime = System.nanoTime();
                        Throwable ex = TDataSourceInitUtils.initDataSource(ds);
                        if (ex == null) {
                            logger.info("Init schema '{}' costs {} secs", schema.getName(),
                                (System.nanoTime() - startTime) / 1e9);
                        } else {
                            logger.warn(
                                    "Failed to init schema " + schema.getName() + ", cause is " + ex.getMessage(),
                                ex);
                        }
                    }));
                }
                AsyncUtils.waitAll(futures);
            } finally {
                threadPool.shutdown();
            }

        }
    }

    public void loadProperties(String instId) {
        MetaDbInstConfigManager.getInstance().registerInstReceiver(new InstConfigReceiver() {
            @Override
            public void apply(Properties props) {
                applyProperties(props);
            }
        });
        String dataId = MetaDbDataIdBuilder.getInstConfigDataId(InstIdUtil.getInstId());
        MetaDbConfigManager.getInstance().register(dataId, null);
        MetaDbConfigManager.getInstance().bindListener(dataId, new InstPropertiesConfigListener());
        MetaDbConfigManager.getInstance().register(MetaDbDataIdBuilder.getVariableConfigDataId(instId), null);
        MetaDbConfigManager.getInstance()
            .bindListener(MetaDbDataIdBuilder.getVariableConfigDataId(instId),
                new MetaDbVariableConfigManager.MetaDbVariableConfigListener());
    }

    protected void reloadDbInfoFromMetaDB() {

        // Fetch all SchemaConfigs that are load in memory
        Map<String, SchemaConfig> allSchemaConfigMap = CobarServer.getInstance().getConfig().getSchemas();

        // Get dbInfos from metaDB
        Map<String, DbInfoRecord> newAddedDbInfoMap = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
        Map<String, DbInfoRecord> newRemovedDbInfoMap = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
        DbInfoManager.getInstance().loadDbInfoFromMetaDb(newAddedDbInfoMap, newRemovedDbInfoMap);

        // reload DbGroupManager
        DbGroupInfoManager.getInstance().onDbInfoChange(newAddedDbInfoMap, newRemovedDbInfoMap);

        // Find all db that are added
        Map<String, DbInfoRecord> dbInfoToBeLoadMap = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
        for (Map.Entry<String, DbInfoRecord> dbAppItem : newAddedDbInfoMap.entrySet()) {
            String dbName = dbAppItem.getKey();
            if (!allSchemaConfigMap.containsKey(dbName)) {
                dbInfoToBeLoadMap.putIfAbsent(dbName, dbAppItem.getValue());
            }
        }

        // Find all db that are removed
        Map<String, DbInfoRecord> dbInfoToUnLoadMap = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
        for (Map.Entry<String, DbInfoRecord> dbAppItem : newRemovedDbInfoMap.entrySet()) {
            String dbName = dbAppItem.getKey();
            if (allSchemaConfigMap.containsKey(dbName)) {
                dbInfoToUnLoadMap.putIfAbsent(dbName, dbAppItem.getValue());
            }
        }
        if (!ConfigDataMode.isMasterMode()) {
            // Find all db that are removed and is realdy load in schemaConfig
            for (String schemaName : allSchemaConfigMap.keySet()) {
                if (!newAddedDbInfoMap.containsKey(schemaName)) {
                    dbInfoToUnLoadMap.putIfAbsent(schemaName, null);
                }
            }
        }

        // Reload priv info
        PolarPrivManager.getInstance().reloadPriv();

        // alloc resource for new created db
        for (Map.Entry<String, DbInfoRecord> dbAppItem : dbInfoToBeLoadMap.entrySet()) {
            allocResourceForLogicalDb(dbAppItem.getKey());
        }

        // clean resource and configs for removed db
        for (Map.Entry<String, DbInfoRecord> dbAppItem : dbInfoToUnLoadMap.entrySet()) {
            releaseResourceForLogicalDb(dbAppItem.getKey());
        }

    }

    protected void allocResourceForLogicalDb(String dbName) {
        this.appLoader.loadApp(dbName);
    }

    protected void releaseResourceForLogicalDb(String dbName) {
        this.appLoader.unLoadApp(dbName);

        // ---- clear db config receiver
        MetaDbInstConfigManager.getInstance().unRegisterDbReceiver(dbName);

        // ---- clear all group HaSwitcher for db
        StorageHaManager.getInstance().clearHaSwitcher(dbName);
    }

    public String getInstanceId() {
        return instanceId;
    }

    public void setInstanceId(String instanceId) {
        this.instanceId = instanceId;
    }

    public String getInstanceName() {
        return instanceName;
    }

    public void setInstanceName(String instanceName) {
        this.instanceName = instanceName;
    }

    public String getInstanceType() {
        return instanceType;
    }

    public void setInstanceType(String instanceType) {
        this.instanceType = instanceType;
    }

    public boolean isUsing() {
        return isUsing;
    }

    public void setUsing(boolean using) {
        isUsing = using;
    }

    public Long getOpVersion() {
        return opVersion;
    }

    public void setOpVersion(Long opVersion) {
        this.opVersion = opVersion;
    }
}
