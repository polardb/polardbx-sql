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
import com.alibaba.polardbx.common.constants.IsolationLevel;
import com.alibaba.polardbx.common.constants.TransactionAttribute;
import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.model.lifecycle.Lifecycle;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.properties.MppConfig;
import com.alibaba.polardbx.common.utils.AddressUtils;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.config.ConfigException;
import com.alibaba.polardbx.config.SystemConfig;
import com.alibaba.polardbx.executor.balancer.Balancer;
import com.alibaba.polardbx.executor.utils.SchemaMetaUtil;
import com.alibaba.polardbx.gms.config.impl.ConnPoolConfigManager;
import com.alibaba.polardbx.gms.config.impl.MetaDbInstConfigManager;
import com.alibaba.polardbx.gms.ha.impl.StorageHaManager;
import com.alibaba.polardbx.gms.listener.impl.MetaDbConfigManager;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.metadb.schema.SchemaChangeManager;
import com.alibaba.polardbx.gms.node.ServerInfoHelper;
import com.alibaba.polardbx.gms.privilege.PolarPrivManager;
import com.alibaba.polardbx.gms.statistics.FeatureUsageStatistics;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import com.alibaba.polardbx.gms.topology.ReadOnlyInstConfigCleaner;
import com.alibaba.polardbx.gms.topology.ServerInfoRecord;
import com.alibaba.polardbx.gms.topology.ServerInstIdManager;
import com.alibaba.polardbx.gms.util.InstIdUtil;
import com.alibaba.polardbx.optimizer.config.meta.CostModelWeight;
import com.alibaba.polardbx.optimizer.core.profiler.RuntimeStat;
import com.alibaba.polardbx.optimizer.hint.util.HintUtil;
import com.alibaba.polardbx.optimizer.locality.LocalityManager;
import com.alibaba.polardbx.optimizer.memory.MemoryManager;
import com.alibaba.polardbx.rpc.XConfig;
import com.alibaba.polardbx.rpc.packet.XPacket;
import com.alibaba.polardbx.rpc.pool.XConnectionManager;
import com.alibaba.polardbx.server.util.StringUtil;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Properties;

public final class ServerLoader extends AbstractLifecycle implements Lifecycle {

    protected static final Logger logger = LoggerFactory.getLogger(ServerLoader.class);

    private static final String CLASSPATH_URL_PREFIX = "classpath:";
    private static final String SERVER_ARGS = "serverArgs";

    private final SystemConfig system;

    public ServerLoader() {
        this.system = new SystemConfig();
    }

    @Override
    protected void doInit() {
        this.load();
    }

    @Override
    protected void doDestroy() {
        super.doDestroy();
    }

    public void loadConfig() {
        loadServerProperty();
    }

    private Properties loadServerProperty() {
        // 加载 server.properties 的属性
        String conf = System.getProperty("server.conf", "classpath:server.properties");
        Properties serverProps = new Properties();
        InputStream in = null;
        try {
            if (conf.startsWith(CLASSPATH_URL_PREFIX)) {
                this.system.setPropertyFile(conf);
                conf = StringUtils.substringAfter(conf, CLASSPATH_URL_PREFIX);
                in = CobarServer.class.getClassLoader().getResourceAsStream(conf);
            } else {
                in = new FileInputStream(conf);
                this.system.setPropertyFile(conf);
            }

            if (in != null) {
                serverProps.load(in);
            }
            final String key = "etcd.server.ip.port";
            if (serverProps.get(key) != null) {
                System.getProperties().put(key, serverProps.get(key));
            }
            /**
             * 本机ip端口
             */
            InetAddress address = null;
            try {
                address = InetAddress.getLocalHost();
            } catch (UnknownHostException e) {
                /**
                 * 无法获取ip,略过
                 */
            }
            System.setProperty("ipAddress", address == null ? "" : address.getHostAddress());
            System.setProperty("port", serverProps.getProperty("serverPort"));
            int pid = getPid();
            System.setProperty("pid", String.valueOf(pid));
        } catch (IOException e) {
            throw GeneralUtil.nestedException(e);
        } finally {
            IOUtils.closeQuietly(in);
        }

        // 复制所有system env
        serverProps.putAll(System.getProperties());
        serverProps.putAll(System.getenv());

        // 处理自定义扩展参数
        String serverArgs = System.getProperty(SERVER_ARGS);
        if (StringUtils.isNotEmpty(serverArgs)) {
            String[] args = StringUtils.split(serverArgs, ';');
            for (String arg : args) {
                String[] config = StringUtils.split(arg, '=');
                if (config.length != 2) {
                    throw new ConfigException("config is error : " + config);
                }

                serverProps.put(StringUtils.trimToEmpty(config[0]), StringUtils.trimToEmpty(config[1]));
            }
        }
        // 复制配置到system参数中
        System.setProperties(serverProps);

        configSystem(serverProps);

        // Set private protocol port first(or we will fail to connect metaDB with Xproto).
        XConnectionManager.getInstance().setMetaDbPort(this.system.getMetaDbXprotoPort());
        XConnectionManager.getInstance().setStorageDbPort(this.system.getStorageDbXprotoPort());

        return serverProps;
    }

    private void load() {
        Properties serverProps = loadServerProperty();

        ConfigDataMode.setSupportSingleDbMultiTbs(this.system.isSupportSingleDbMultiTbs());
        ConfigDataMode.setSupportDropAutoSeq(this.system.isSupportDropAutoSeq());
        ConfigDataMode.setAllowSimpleSequence(this.system.isAllowSimpleSequence());
        initPolarDbXComponents();

        /**
         * 初始化，并监听 MANAGER_CONFIG_DATAID:com.alibaba.polardbx.monitor
         */

        serverProps.putAll(system.toMapValues());

        // 复制配置到system参数中
        System.setProperties(serverProps);
        logger.info("Server Properties: " + serverProps.toString());

        // 因为System.setProperties有更新，重新加载ConfigDataMode的值
        ConfigDataMode.reload();

        MemoryManager.getInstance().adjustMemoryLimit(system.getGlobalMemoryLimit());
        logger.info("Global memory pool size: " + FileUtils.byteCountToDisplaySize(system.getGlobalMemoryLimit()));
    }

    private void initPolarDbXComponents() {
        // Set private protocol port first(or we will fail to connect metaDB with Xproto).
        XConnectionManager.getInstance().setMetaDbPort(this.system.getMetaDbXprotoPort());
        XConnectionManager.getInstance().setStorageDbPort(this.system.getStorageDbXprotoPort());

        // Init metadb datasource
        MetaDbDataSource
            .initMetaDbDataSource(this.system.getMetaDbAddr(), this.system.getMetaDbName(),
                this.system.getMetaDbProp(),
                this.system.getMetaDbUser(),
                this.system.getMetaDbPasswd());

        // Do schema change prior to other components' initializations.
        SchemaChangeManager.getInstance().handle();

        // Init ServerInstIdManager
        ServerInstIdManager.getInstance();

        // Init MetaDbConfigManager
        MetaDbConfigManager.getInstance();

        // Init system default properties manager
        MetaDbInstConfigManager.getInstance();

        // Init conn pool manager
        ConnPoolConfigManager.getInstance();

        // Init group configs for read-only inst if need
        DbTopologyManager.initGroupConfigsForReadOnlyInstIfNeed();

        LocalityManager lm = LocalityManager.getInstance();

        Balancer balancer = Balancer.getInstance();
        balancer.enableBalancer(this.system.isEnableBalancer());
        balancer.setBalancerRunningWindow(this.system.getBalancerWindow());

        // Init HaManager to auto refresh storage infos
        StorageHaManager shm = StorageHaManager.getInstance();
        shm.setEnablePrimaryZoneMaintain(this.system.getEnablePrimaryZoneMaintain());
        shm.setPrimaryZoneSupplier(lm.getSupplier());

        // Init port info by meta db
        initPortInfoAndInstId();

        // Init DbTopologManager
        DbTopologyManager.initDbTopologyManager(new SchemaMetaUtil.PolarDbXSchemaMetaCleaner());

        // Init system db
        MetaDbDataSource.initSystemDbIfNeed();

        // Init the polarx priv manager from meta db
        PolarPrivManager.getInstance();

        // Init the cleaner of remvved read-only inst 
        ReadOnlyInstConfigCleaner.getInstance();

        // Init the stat of feature usage
        FeatureUsageStatistics.init();
    }

    protected void initPortInfoAndInstId() {
        String instId = InstIdUtil.getInstId();
        List<ServerInfoRecord> serverInfosOfMetaDb =
            ServerInfoHelper.getServerInfoByMetaDb(AddressUtils.getHostIp(), this.system.getServerPort(), instId);
        if (serverInfosOfMetaDb.size() > 0) {
            ServerInfoRecord serverInfo = serverInfosOfMetaDb.get(0);
            this.system.setManagerPort(serverInfo.mgrPort);
            this.system.setRpcPort(serverInfo.mppPort);
            if (instId == null) {
                // If the instanceId does NOT exists in server.properties
                // or the startup args of JVM, then use the instId from meta db
                this.system.setInstanceId(serverInfo.instId);
            } else {
                // If the instanceId exists in server.properties
                // or the startup args of JVM, then use the instId from meta db
            }
        }
        this.system.setMasterInstanceId(ServerInstIdManager.getInstance().getMasterInstId());
    }

    private void configSystem(Properties serverProps) {
        if (serverProps == null) {
            throw new ConfigException("server properties is null!");
        }

        String initializeGms = serverProps.getProperty("initializeGms");
        if (!StringUtil.isEmpty(initializeGms)) {
            this.system.setInitializeGms(BooleanUtils.toBoolean(initializeGms));
        }
        String forceCleanup = serverProps.getProperty("forceCleanup");
        if (!StringUtil.isEmpty(forceCleanup)) {
            this.system.setForceCleanup(BooleanUtils.toBoolean(forceCleanup));
        }

        String dnList = serverProps.getProperty("dnList");
        if (this.system.isInitializeGms() && StringUtil.isEmpty(dnList)) {
            throw new IllegalArgumentException("dataNodeList must be set when initialize gms");
        }
        if (!StringUtil.isEmpty(dnList)) {
            this.system.setDataNodeList(dnList);
        }

        String rootUser = serverProps.getProperty("rootUser");
        if (!StringUtil.isEmpty(rootUser)) {
            this.system.setMetaDbRootUser(rootUser);
        }
        String rootPassword = serverProps.getProperty("rootPasswd");
        if (!StringUtil.isEmpty(rootPassword)) {
            this.system.setMetaDbRootPasswd(rootPassword);
        }
        String polarxRootUser = serverProps.getProperty("polarxRootUser");
        if (!StringUtil.isEmpty(polarxRootUser)) {
            this.system.setPolarxRootUser(polarxRootUser);
        }
        String polarxRootPasswd = serverProps.getProperty("polarxRootPasswd");
        if (!StringUtil.isEmpty(polarxRootPasswd)) {
            this.system.setPolarxRootPasswd(polarxRootPasswd);
        }

        String maxConnection = serverProps.getProperty("maxConnection");
        if (!StringUtil.isEmpty(maxConnection)) {
            this.system.setMaxConnection(Integer.parseInt(maxConnection));
        }

        String serverPort = serverProps.getProperty("serverPort");
        if (!StringUtil.isEmpty(serverPort)) {
            this.system.setServerPort(Integer.parseInt(serverPort));
        }

        String managerPort = serverProps.getProperty("managerPort");
        if (!StringUtil.isEmpty(managerPort)) {
            this.system.setManagerPort(Integer.parseInt(managerPort));
        }

        String rpcPort = serverProps.getProperty("rpcPort");
        if (!StringUtil.isEmpty(rpcPort)) {
            this.system.setRpcPort(Integer.parseInt(rpcPort));
        }

        String galaxyXProtocol = serverProps.getProperty("galaxyXProtocol");
        if (!StringUtil.isEmpty(galaxyXProtocol)) {
            if (Integer.parseInt(galaxyXProtocol) != 0) {
                XConfig.GALAXY_X_PROTOCOL = true;
            }
        }

        String metaDbXprotoPort = serverProps.getProperty("metaDbXprotoPort");
        if (!StringUtil.isEmpty(metaDbXprotoPort)) {
            this.system.setMetaDbXprotoPort(Integer.parseInt(metaDbXprotoPort));
        }

        String storageDbXprotoPort = serverProps.getProperty("storageDbXprotoPort");
        if (!StringUtil.isEmpty(storageDbXprotoPort)) {
            this.system.setStorageDbXprotoPort(Integer.parseInt(storageDbXprotoPort));
        }

        String mppServer = serverProps.getProperty("mppServer");
        if (!StringUtil.isEmpty(mppServer)) {
            this.system.enableMppServer(Boolean.parseBoolean(mppServer));
        } else {
            this.system.enableMppServer(true);
        }

        String mppWorker = serverProps.getProperty("mppWorker");
        if (!StringUtil.isEmpty(mppWorker)) {
            this.system.enableMppWorker(Boolean.parseBoolean(mppWorker));
        } else {
            this.system.enableMppWorker(true);
        }

        String enableMpp = serverProps.getProperty("enableMpp");
        if (!StringUtil.isEmpty(enableMpp)) {
            this.system.setEnableMpp(Boolean.parseBoolean(enableMpp));
        } else {
            this.system.setEnableMpp(true);
        }

        String enableCpuProfile = serverProps.getProperty("enableCpuProfile");
        if (!StringUtil.isEmpty(enableCpuProfile)) {
            RuntimeStat.setEnableCpuProfile(Boolean.parseBoolean(enableCpuProfile));
        }

        //没有开启coordinator功能时，禁用EnableMpp
        if (!this.system.isMppServer()) {
            this.system.setEnableMpp(false);
        }

        String coronaMode = serverProps.getProperty("coronaMode");
        if (!StringUtil.isEmpty(coronaMode)) {
            this.system.setCoronaMode(Integer.parseInt(coronaMode));
        }

        String maxAllowedPacket = serverProps.getProperty("maxAllowedPacket");
        if (!StringUtil.isEmpty(maxAllowedPacket)) {
            this.system.setMaxAllowedPacket(Integer.parseInt(maxAllowedPacket));
        }

        String socketRecvBuffer = serverProps.getProperty("socketRecvBuffer");
        if (!StringUtil.isEmpty(socketRecvBuffer)) {
            this.system.setSocketRecvBuffer(Integer.parseInt(socketRecvBuffer));
        }

        String socketSendBuffer = serverProps.getProperty("socketSendBuffer");
        if (!StringUtil.isEmpty(socketSendBuffer)) {
            this.system.setSocketSendBuffer(Integer.parseInt(socketSendBuffer));
        }

        String allowCrossDbQuery = serverProps.getProperty("allowCrossDbQuery");
        if (!TStringUtil.isEmpty(allowCrossDbQuery)) {
            this.system.setAllowCrossDbQuery(Boolean.parseBoolean(allowCrossDbQuery));
        }

        String retryErrorSqlOnOldServer = serverProps.getProperty("retryErrorSqlOnOldServer");
        if (!TStringUtil.isEmpty(retryErrorSqlOnOldServer)) {
            this.system.setRetryErrorSqlOnOldServer(Boolean.parseBoolean(retryErrorSqlOnOldServer));
        }

        String charset = serverProps.getProperty("charset");
        if (!StringUtil.isEmpty(charset)) {
            this.system.setCharset(charset);
        }

        String processorCount = serverProps.getProperty("processors");
        if (!StringUtil.isEmpty(processorCount)) {
            this.system.setProcessors(Integer.parseInt(processorCount));
        }

        String processorHandlerCount = serverProps.getProperty("processorHandler");
        if (!StringUtil.isEmpty(processorHandlerCount)) {
            this.system.setProcessorHandler(Integer.parseInt(processorHandlerCount));
        }

        String processorKillExecutorCount = serverProps.getProperty("processorKillExecutor");
        if (!StringUtil.isEmpty(processorKillExecutorCount)) {
            this.system.setProcessorKillExecutor(Integer.parseInt(processorKillExecutorCount));
        }

        String timerExecutorCount = serverProps.getProperty("timerExecutor");
        if (!StringUtil.isEmpty(timerExecutorCount)) {
            this.system.setTimerExecutor(Integer.parseInt(timerExecutorCount));
        }

        String managerExecutorCount = serverProps.getProperty("managerExecutor");
        if (!StringUtil.isEmpty(managerExecutorCount)) {
            this.system.setManagerExecutor(Integer.parseInt(managerExecutorCount));
        }

        String serverExecutorCount = serverProps.getProperty("serverExecutor");
        if (!StringUtil.isEmpty(serverExecutorCount)) {
            this.system.setServerExecutor(Integer.parseInt(serverExecutorCount));
        }

        String timerTaskExecutorCount = serverProps.getProperty("timerTaskExecutor");
        if (!StringUtil.isEmpty(timerTaskExecutorCount)) {
            this.system.setTimerTaskExecutor(Integer.parseInt(timerTaskExecutorCount));
        }

        String idleTimeout = serverProps.getProperty("idleTimeout");
        if (!StringUtil.isEmpty(idleTimeout)) {
            this.system.setIdleTimeout(Integer.parseInt(idleTimeout));
        }

        String processorCheckPeriod = serverProps.getProperty("processorCheckPeriod");
        if (!StringUtil.isEmpty(processorCheckPeriod)) {
            this.system.setProcessorCheckPeriod(Integer.parseInt(processorCheckPeriod));
        }

        String txIsolation = serverProps.getProperty("txIsolation");
        if (!StringUtil.isEmpty(txIsolation)) {
            Integer tx = Integer.valueOf(txIsolation);
            if (tx > 0 && IsolationLevel.fromInt(tx) == null) {
                throw new IllegalArgumentException("unknown txIsolation code : " + tx);
            }
            ConfigDataMode.setTxIsolation(tx);
        } else {
            ConfigDataMode.setTxIsolation(TransactionAttribute.DEFAULT_ISOLATION_LEVEL_POLARX.getCode());
        }

        String parserCommentVersion = serverProps.getProperty("parserCommentVersion");
        if (!StringUtil.isEmpty(parserCommentVersion)) {
            this.system.setParserCommentVersion(Integer.valueOf(parserCommentVersion));
        }

        String sqlRecordCount = serverProps.getProperty("sqlRecordCount");
        if (!StringUtil.isEmpty(sqlRecordCount)) {
            this.system.setSqlRecordCount(Integer.valueOf(sqlRecordCount));
        }

        // 集群名称
        String clusterName = serverProps.getProperty("cluster");
        if (!StringUtil.isEmpty(clusterName)) {
            this.system.setClusterName(clusterName);
        }

        String unitName = serverProps.getProperty("unitName");
        if (!StringUtil.isEmpty(unitName)) {
            this.system.setUnitName(unitName);
        }

        String instanceId = serverProps.getProperty("instanceId");
        if (!StringUtil.isEmpty(instanceId)) {
            this.system.setInstanceId(instanceId);
        }

        // 信任host ip列表
        String trustedIps = serverProps.getProperty("trustedips");
        if (!StringUtil.isEmpty(trustedIps)) {
            this.system.setTrustedIps(trustedIps);
        }

        trustedIps = serverProps.getProperty("trustedIps");
        if (!StringUtil.isEmpty(trustedIps)) {
            this.system.setTrustedIps(trustedIps);
        }

        // 黑名单ip列表
        String blackIps = serverProps.getProperty("blackIps");
        if (!StringUtil.isEmpty(blackIps)) {
            this.system.setBlackIps(blackIps);
        }

        String whiteIps = serverProps.getProperty("whiteIps");
        if (!StringUtil.isEmpty(whiteIps)) {
            this.system.setWhiteIps(whiteIps);
        }

        String slowSqlTime = serverProps.getProperty("slowSqlTime");
        if (!StringUtil.isEmpty(slowSqlTime)) {
            this.system.setSlowSqlTime(Long.valueOf(slowSqlTime));
        }

        String deadLockCheckPeriod = serverProps.getProperty("deadLockCheckPeriod");
        if (!StringUtil.isEmpty(deadLockCheckPeriod)) {
            this.system.setDeadLockCheckPeriod(Integer.parseInt(deadLockCheckPeriod));
        }

        String allowManagerLogin = serverProps.getProperty("allowManagerLogin");
        if (!StringUtil.isEmpty(allowManagerLogin)) {
            this.system.setAllowManagerLogin(Integer.valueOf(allowManagerLogin));
        }

        String enableCollectorAllTables = serverProps.getProperty("enableCollectorAllTables");
        if (!StringUtil.isEmpty(enableCollectorAllTables)) {
            this.system.setEnableCollectorAllTables(Boolean.valueOf(enableCollectorAllTables));
        }

        String enableSpill = serverProps.getProperty("enableSpill");
        if (!StringUtil.isEmpty(enableSpill)) {
            this.system.setEnableSpill(Boolean.valueOf(enableSpill));
        }

        String enableKill = serverProps.getProperty("enableKill");
        if (!StringUtil.isEmpty(enableKill)) {
            this.system.setEnableKill(Boolean.valueOf(enableKill));
        }

        String enableApLimitRate = serverProps.getProperty("enableApLimitRate");
        if (!StringUtil.isEmpty(enableApLimitRate)) {
            this.system.setEnableApLimitRate(Boolean.valueOf(enableApLimitRate));
        }

        String sslEnable = serverProps.getProperty("sslEnable");
        if (!TStringUtil.isEmpty(sslEnable)) {
            this.system.setSslEnbale(Boolean.valueOf(sslEnable));
        }

        String vpcId = serverProps.getProperty("vpcId");
        if (!TStringUtil.isEmpty(vpcId)) {
            this.system.setVpcId(vpcId);
        }

        String globalMemoryLimit = serverProps.getProperty("globalMemoryLimit");
        if (!TStringUtil.isEmpty(globalMemoryLimit)) {
            this.system.setGlobalMemoryLimit(Long.valueOf(globalMemoryLimit));
        }

        String memoryWeight = serverProps.getProperty("memoryWeight");
        if (!TStringUtil.isEmpty(memoryWeight)) {
            CostModelWeight.INSTANCE.setMemoryWeight(Double.valueOf(memoryWeight));
        }

        String ioWeight = serverProps.getProperty("ioWeight");
        if (!TStringUtil.isEmpty(ioWeight)) {
            CostModelWeight.INSTANCE.setIoWeight(Double.valueOf(ioWeight));
        }

        String netWeight = serverProps.getProperty("netWeight");
        if (!TStringUtil.isEmpty(netWeight)) {
            CostModelWeight.INSTANCE.setNetWeight(Double.valueOf(netWeight));
        }

        String buildWeight = serverProps.getProperty("buildWeight");
        if (!TStringUtil.isEmpty(buildWeight)) {
            CostModelWeight.INSTANCE.setBuildWeight(Double.valueOf(buildWeight));
        }

        String probeWeight = serverProps.getProperty("probeWeight");
        if (!TStringUtil.isEmpty(probeWeight)) {
            CostModelWeight.INSTANCE.setProbeWeight(Double.valueOf(probeWeight));
        }

        String mergeWeight = serverProps.getProperty("mergeWeight");
        if (!TStringUtil.isEmpty(mergeWeight)) {
            CostModelWeight.INSTANCE.setMergeWeight(Double.valueOf(mergeWeight));
        }

        String hashAggWeight = serverProps.getProperty("hashAggWeight");
        if (!TStringUtil.isEmpty(hashAggWeight)) {
            CostModelWeight.INSTANCE.setHashAggWeight(Double.valueOf(hashAggWeight));
        }

        String sortAggWeight = serverProps.getProperty("sortAggWeight");
        if (!TStringUtil.isEmpty(sortAggWeight)) {
            CostModelWeight.INSTANCE.setSortAggWeight(Double.valueOf(sortAggWeight));
        }

        String sortWeight = serverProps.getProperty("sortWeight");
        if (!TStringUtil.isEmpty(sortWeight)) {
            CostModelWeight.INSTANCE.setSortWeight(Double.valueOf(sortWeight));
        }

        String avgTupleMatch = serverProps.getProperty("avgTupleMatch");
        if (!TStringUtil.isEmpty(avgTupleMatch)) {
            CostModelWeight.INSTANCE.setAvgTupleMatch(Double.valueOf(avgTupleMatch));
        }

        String nlWeight = serverProps.getProperty("nlWeight");
        if (!TStringUtil.isEmpty(nlWeight)) {
            CostModelWeight.INSTANCE.setNlWeight(Double.valueOf(nlWeight));
        }

        String enableBucketExecutor = serverProps.getProperty("enableBucketExecutor");
        if (!TStringUtil.isEmpty(enableBucketExecutor)) {
            this.system.setEnableBucketExecutor(Boolean.valueOf(enableBucketExecutor));
        }

        /* ========mpp system engine config======== */
        for (Field field : ConnectionProperties.class.getDeclaredFields()) {
            try {
                String key = field.get(ConnectionProperties.class).toString();
                if (key != null && key.toUpperCase().startsWith("MPP_")) {
                    MppConfig.getInstance().loadValue(logger, key, serverProps.getProperty(key));
                }
                DynamicConfig.getInstance().loadValue(logger, key, serverProps.getProperty(key));
            } catch (Throwable e) {
                logger.warn("MppConfigLoader error", e);
            }
        }

        String shardWeight = serverProps.getProperty("shardWeight");
        if (!TStringUtil.isEmpty(shardWeight)) {
            CostModelWeight.INSTANCE.setShardWeight(Double.valueOf(shardWeight));
        }

        String metaDbAddr = serverProps.getProperty("metaDbAddr");
        if (!StringUtil.isEmpty(metaDbAddr)) {
            this.system.setMetaDbAddr(metaDbAddr);
        }

        String metaDbProp = serverProps.getProperty("metaDbProp");
        if (!StringUtil.isEmpty(metaDbProp)) {
            this.system.setMetaDbProp(metaDbProp);
        }

        String metaDbName = serverProps.getProperty("metaDbName");
        if (!StringUtil.isEmpty(metaDbName)) {
            this.system.setMetaDbName(metaDbName);
        }

        String metaDbUser = serverProps.getProperty("metaDbUser");
        if (!StringUtil.isEmpty(metaDbUser)) {
            this.system.setMetaDbUser(metaDbUser);
        }

        String metaDbPasswd = serverProps.getProperty("metaDbPasswd");
        if (!StringUtil.isEmpty(metaDbPasswd)) {
            this.system.setMetaDbPasswd(metaDbPasswd);
        }

        String enableScaleout = serverProps.getProperty("enableScaleout");
        if (!StringUtil.isEmpty(enableScaleout)) {
            this.system.setEnableScaleout(Boolean.valueOf(enableScaleout));
        }

        String enablePrimaryZoneMaintain = serverProps.getProperty("enablePrimaryZoneMaintain");
        if (!StringUtil.isEmpty(enablePrimaryZoneMaintain)) {
            this.system.setEnablePrimaryZoneMaintain(Boolean.parseBoolean(enablePrimaryZoneMaintain));
        }

        String enableBalancer = serverProps.getProperty(ConnectionParams.ENABLE_BALANCER.getName());
        if (!StringUtil.isEmpty(enableBalancer)) {
            this.system.setEnableBalancer(BooleanUtils.toBoolean(enableBalancer));
        }

        String balanceWindow = serverProps.getProperty(ConnectionParams.BALANCER_WINDOW.getName());
        this.system.setBalanceWindow(balanceWindow);

        String dropDatabaseAfterSwitchDatasource = serverProps.getProperty("dropDatabaseAfterSwitchDatasource");
        if (!StringUtil.isEmpty(dropDatabaseAfterSwitchDatasource)) {
            this.system.setDropOldDataBaseAfterSwitchDataSource(Boolean.valueOf(dropDatabaseAfterSwitchDatasource));
        }

        String shardDbCountEachStorageInst = serverProps.getProperty("shardDbCountEachStorageInst");
        if (!StringUtil.isEmpty(shardDbCountEachStorageInst)) {
            this.system.setShardDbCountEachStorageInst(Integer.valueOf(shardDbCountEachStorageInst));
            DbTopologyManager.resetShardDbCountEachStorageInst(this.system.getShardDbCountEachStorageInst());
        }

        String defaultPartitionMode = serverProps.getProperty("defaultPartitionMode");
        if (!StringUtil.isEmpty(shardDbCountEachStorageInst)) {
            this.system.setDefaultPartitionMode(defaultPartitionMode);
            DbTopologyManager.setDefaultPartitionMode(defaultPartitionMode);
        }

        String enableForbidPushDmlWithHint = serverProps.getProperty("enableForbidPushDmlWithHint");
        if (!StringUtil.isEmpty(enableForbidPushDmlWithHint)) {
            this.system.setEnableForbidPushDmlWithHint(Boolean.valueOf(enableForbidPushDmlWithHint));
            HintUtil.enableForbidPushDmlWithHint = this.system.isEnableForbidPushDmlWithHint();
        }

        String supportSingleDbMultiTbs = serverProps.getProperty("supportSingleDbMultiTbs");
        if (!StringUtil.isEmpty(supportSingleDbMultiTbs)) {
            this.system.setSupportSingleDbMultiTbs(Boolean.valueOf(supportSingleDbMultiTbs));
        }

        String supportDropAutoSeq = serverProps.getProperty("supportDropAutoSeq");
        if (!StringUtil.isEmpty(supportDropAutoSeq)) {
            this.system.setSupportDropAutoSeq(Boolean.valueOf(supportDropAutoSeq));
        }

        String allowSimpleSequence = serverProps.getProperty("allowSimpleSequence");
        if (!StringUtil.isEmpty(allowSimpleSequence)) {
            this.system.setAllowSimpleSequence(Boolean.valueOf(allowSimpleSequence));
        }

        String workloadType = serverProps.getProperty("workloadType");
        if (!StringUtil.isEmpty(workloadType)) {
            if ("AP".equalsIgnoreCase(workloadType)) {
                this.system.setWorkloadType("AP");
            } else if ("TP".equalsIgnoreCase(workloadType)) {
                this.system.setWorkloadType("TP");
            }
        }

        String enableRemoteRPC = serverProps.getProperty("enableRemoteRPC");
        if (!StringUtil.isEmpty(enableRemoteRPC)) {
            this.system.setEnableRemoteRPC(Boolean.parseBoolean(enableRemoteRPC));
        }

        String enableMasterMpp = serverProps.getProperty("enableMasterMpp");
        if (!StringUtil.isEmpty(enableMasterMpp)) {
            this.system.setEnableMasterMpp(Boolean.parseBoolean(enableMasterMpp));
        }

        String cclRescheduleTimeoutCheckPeriod = serverProps.getProperty("cclRescheduleTimeoutCheckPeriod");
        if (!StringUtil.isEmpty(cclRescheduleTimeoutCheckPeriod)) {
            this.system.setCclRescheduleTimeoutCheckPeriod(Integer.parseInt(cclRescheduleTimeoutCheckPeriod));
        }
        System.setProperty("cclRescheduleTimeoutCheckPeriod",
            String.valueOf(this.system.getCclRescheduleTimeoutCheckPeriod()));

        String processCclTriggerPeriod = serverProps.getProperty("processCclTriggerPeriod");
        if (!StringUtil.isEmpty(processCclTriggerPeriod)) {
            this.system.setProcessCclTriggerPeriod(Integer.parseInt(processCclTriggerPeriod));
        }
        System.setProperty("processCclTriggerPeriod", String.valueOf(this.system.getProcessCclTriggerPeriod()));

        String enableLogicalDbWarmmingUpStr = serverProps.getProperty("enableLogicalDbWarmmingUp");
        if (!StringUtil.isEmpty(enableLogicalDbWarmmingUpStr)) {
            this.system.setEnableLogicalDbWarmmingUp(Boolean.valueOf(enableLogicalDbWarmmingUpStr));
        }

    }

    public SystemConfig getSystem() {
        return system;
    }

    private static int getPid() {
        RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();
        String name = runtime.getName(); // format: "pid@hostname"
        try {
            return Integer.parseInt(name.substring(0, name.indexOf('@')));
        } catch (Exception e) {
            return -1;
        }
    }
}
