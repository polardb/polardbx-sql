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

package com.alibaba.polardbx;

import com.alibaba.polardbx.atom.TAtomDsStandard;
import com.alibaba.polardbx.common.TddlNode;
import com.alibaba.polardbx.common.cdc.CdcManagerHelper;
import com.alibaba.polardbx.common.charset.CharsetFactory;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.model.lifecycle.Lifecycle;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.utils.AddressUtils;
import com.alibaba.polardbx.common.utils.ExecutorMode;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.extension.ExtensionLoader;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.thread.ExecutorUtil;
import com.alibaba.polardbx.common.utils.thread.NamedThreadFactory;
import com.alibaba.polardbx.common.utils.thread.ServerThreadPool;
import com.alibaba.polardbx.common.utils.version.InstanceVersion;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.config.SchemaConfig;
import com.alibaba.polardbx.config.SystemConfig;
import com.alibaba.polardbx.executor.mpp.deploy.LocalServer;
import com.alibaba.polardbx.executor.mpp.deploy.MppServer;
import com.alibaba.polardbx.executor.mpp.deploy.Server;
import com.alibaba.polardbx.executor.mpp.deploy.ServiceProvider;
import com.alibaba.polardbx.executor.mpp.server.DrdsContextHandler;
import com.alibaba.polardbx.executor.mpp.server.TaskResource;
import com.alibaba.polardbx.gms.config.impl.MetaDbInstConfigManager;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.node.LeaderStatusBridge;
import com.alibaba.polardbx.gms.node.NodeStatusManager;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.gms.util.MetaDbLogUtil;
import com.alibaba.polardbx.gms.util.PasswdUtil;
import com.alibaba.polardbx.manager.ManagerConnectionFactory;
import com.alibaba.polardbx.matrix.jdbc.TDataSource;
import com.alibaba.polardbx.net.NIOAcceptor;
import com.alibaba.polardbx.net.NIOProcessor;
import com.alibaba.polardbx.net.util.TimeUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.expression.ExtraFunctionManager;
import com.alibaba.polardbx.optimizer.memory.MemoryManager;
import com.alibaba.polardbx.optimizer.sequence.ISequenceManager;
import com.alibaba.polardbx.rpc.CdcRpcClient;
import com.alibaba.polardbx.rpc.pool.XConnectionManager;
import com.alibaba.polardbx.server.ServerConnectionFactory;
import com.alibaba.polardbx.ssl.SslContextFactory;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;

import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.ResourceBundle;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author agapple 2014年9月25日 下午4:40:25
 * @since 5.1.13
 */
public class CobarServer extends AbstractLifecycle implements Lifecycle {

    public static final String NAME = "TDDL";

    public static final String VERSION = InstanceVersion.getPostfixVersion();

    private static final Logger logger = LoggerFactory.getLogger(CobarServer.class);
    private static final CobarServer INSTANCE = new CobarServer();

    public static final CobarServer getInstance() {
        return INSTANCE;
    }

    private final CobarConfig config;
    private final ScheduledThreadPoolExecutor scheduler;
    private final ServerThreadPool managerExecutor;
    private final ServerThreadPool timerExecutor;
    private final ServerThreadPool serverExecutor;
    private final ServerThreadPool killExecutor;
    // Global scheduled executor service
    private final ScheduledThreadPoolExecutor timerTaskExecutor;
    private final AtomicBoolean isOnline;
    private final AtomicBoolean forceOffline;
    private long startupTime;
    private NIOProcessor[] processors;
    private NIOAcceptor manager;
    private NIOAcceptor server;
    private ScheduledFuture<?> idleCheckTask;
    private long processorCheckPeriod;

    /**
     * 保存Server本机的地址
     */
    protected String serverHost;
    protected String serverHostKey = "tddlServerHost";

    /**
     * 保存Server的服务端口
     */
    protected String serverPort;
    protected String serverPortKey = "tddlServerPort";

    /**
     * 保存server的监控端口
     */
    protected String managerPort;

    /**
     * MPP 内部通讯端口
     */
    protected String rpcPort;

    private CobarServer() {
        this.config = new CobarConfig();
        SystemConfig system = config.getSystem();
        checkSslEnable(system);
        this.scheduler = new ScheduledThreadPoolExecutor(1, new ThreadFactory() {

            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r, NAME + "Timer");
                thread.setDaemon(true);
                return thread;
            }
        });
        this.timerExecutor = ExecutorUtil.create("TimerExecutor", system.getTimerExecutor());
        this.managerExecutor = ExecutorUtil.create("ManagerExecutor", system.getManagerExecutor());
        this.killExecutor = ExecutorUtil.create("KillExecutor", system.getProcessorKillExecutor());
        this.timerTaskExecutor = ExecutorUtil.createScheduler(system.getTimerTaskExecutor(),
            new NamedThreadFactory("GlobalPeriodicTaskExecutor"),
            new ThreadPoolExecutor.AbortPolicy());
        if (system.isEnableBucketExecutor()) {
            this.serverExecutor =
                ExecutorUtil.create("ServerExecutor", system.getServerExecutor(), system.getDeadLockCheckPeriod(),
                    system.getProcessors());
        } else {
            this.serverExecutor =
                ExecutorUtil.create("ServerExecutor", system.getServerExecutor(), system.getDeadLockCheckPeriod());
        }
        this.isOnline = new AtomicBoolean(true);
        this.forceOffline = new AtomicBoolean(false);
    }

    @Override
    protected void doInit() {
        try {
            // server startup
            logger.info("===============================================");
            logger.info(NAME + " is ready to startup ...\n the Server base version is " + InstanceVersion.getVersion());
            SystemConfig system = config.getSystem();
            ConfigDataMode.setCluster(system.getClusterName());
            logger.info("Startup Cluster : " + system.getClusterName());
            if (system.getUnitName() != null) {
                logger.info("Unit Name : " + system.getUnitName());
            }

            // 启动前检查Server端口与Manager端口是否可用
            // checkAvailableForAllPortsUsedByServer(system);
            scheduler.scheduleWithFixedDelay(updateTime(), 0L, 20, TimeUnit.MILLISECONDS);

            // 保存相关的serverHost、serveerPort与managerPort
            this.serverHost = AddressUtils.getHostIp();
            this.serverPort = String.valueOf(system.getServerPort());
            this.managerPort = String.valueOf(system.getManagerPort());
            this.rpcPort = String.valueOf(system.getRpcPort());
            XConnectionManager.getInstance().setMetaDbPort(system.getMetaDbXprotoPort());
            XConnectionManager.getInstance().setStorageDbPort(system.getStorageDbXprotoPort());

            // 将serverHost与serverPort放进System.getProperties()中，放便底层逻辑抛错能拼出更好的错误信息
            System.getProperties().put(this.serverHostKey, this.serverHost);
            System.getProperties().put(this.serverPortKey, this.serverPort);

            // 需要rpcClient先启动
            this.config.init();

            // startup processors
            logger.info("Startup processors ...");
            processors = new NIOProcessor[system.getProcessors()];
            for (int i = 0; i < processors.length; i++) {
                processors[i] = new NIOProcessor(i, "Processor" + i,
                    this.serverExecutor);
                processors[i].startup();
            }

            processorCheckPeriod = system.getProcessorCheckPeriod();
            processorCheck();

            // 执行一下jar包预热
            warmup();

            // startup manager
            ManagerConnectionFactory mf = new ManagerConnectionFactory();
            mf.setCharset(system.getCharset());
            mf.setIdleTimeout(system.getIdleTimeout());
            manager = new NIOAcceptor(NAME + "Manager", system.getManagerPort(), mf, true);
            manager.setProcessors(processors);
            manager.start();
            logger.info(manager.getName() + " is started and listening on " + manager.getPort());

            // startup server
            ServerConnectionFactory sf = new ServerConnectionFactory();
            sf.setCharset(system.getCharset());
            sf.setIdleTimeout(system.getIdleTimeout());
            sf.setMaxPacketSize(system.getMaxAllowedPacket());
            sf.setSocketRecvBuffer(system.getSocketRecvBuffer());
            sf.setSocketSendBuffer(system.getSocketSendBuffer());
            server = new NIOAcceptor(NAME + "Server", system.getServerPort(), sf, isOnline());
            server.setProcessors(processors);
            server.start();

            // server started
            logger.info(server.getName() + " is started and listening on " + server.getPort());

            if ((system.isMppServer() || system.isMppWorker()) && system.getRpcPort() > 0) {
                // startup native mpp service
                startMppServer(system);
            } else {
                // startup local service
                startLocalServer(system);
            }

            ServiceProvider.getInstance().setServerExecutor(serverExecutor);
            ServiceProvider.getInstance().setTimerTaskExecutor(timerTaskExecutor);

            // Register node status manager into LeaderStatusBridge
            // NOTE: why setup at here?
            // NodeStatusManager is initialized at Server::run, which is after ServerLoader.
            // So we could not register NodeStatusManager at ServerLoader.
            NodeStatusManager nodeStatusManager = ServiceProvider.getInstance().getServer().getStatusManager();
            LeaderStatusBridge.getInstance().setUpNodeStatusManager(nodeStatusManager);
            CdcRpcClient.buildCdcRpcClient();
            tryStartCdcManager();

            logger.info("===============================================");
            logServerStartUp();
            this.startupTime = TimeUtil.currentTimeMillis();
        } catch (Throwable e) {
            throw new TddlRuntimeException(ErrorCode.ERR_SERVER, e, "start failed");
        }
    }

    private void tryStartCdcManager() {
        if (ConfigDataMode.isMasterMode()) {
            // startup cdc center client && cdc manager
            String CDC_STARTUP_MODE =
                MetaDbInstConfigManager.getInstance().getInstProperty(ConnectionProperties.CDC_STARTUP_MODE);
            if (Integer.parseInt(CDC_STARTUP_MODE) == 1) {
                logger.info("Start cdc synchronously: initialize cdc");
                CdcManagerHelper.getInstance().initialize();
            } else if (Integer.parseInt(CDC_STARTUP_MODE) == 2) {
                Thread t = new Thread(() -> {
                    logger.info("Start cdc asynchronously: checking cdcDb");
                    SystemDbHelper.checkOrCreateCdcDb(MetaDbDataSource.getInstance());
                    logger.info("Start cdc asynchronously: initialize cdc");
                    CdcManagerHelper.getInstance().initialize();
                });
                t.start();
            } else {
                logger.info("Do not start cdc");
            }
        }
    }

    private void startLocalServer(SystemConfig system) {
        if (TddlNode.getNodeId() == 0 && System.getProperty("nodeId") != null) {
            TddlNode.setNodeId(Integer.parseInt(System.getProperty("nodeId")));
            logger.warn("mpp set nodeId=" + TddlNode.getNodeId());
        }

        Server localServer = new LocalServer(TddlNode.getNodeId(), this.serverHost, system.getRpcPort());
        ServiceProvider.getInstance().setServer(localServer);
        localServer.run();
    }

    private void startMppServer(SystemConfig system) {
        if (TddlNode.getNodeId() == 0 && System.getProperty("nodeId") != null) {
            TddlNode.setNodeId(Integer.parseInt(System.getProperty("nodeId")));
            logger.warn("mpp set nodeId=" + TddlNode.getNodeId());
        }

        TaskResource.setDrdsContextHandler(new DrdsContextHandler() {

            @Override
            public ExecutionContext makeExecutionContext(
                String schemaName, Map<String, Object> hintCmds, int txIsolation) {
                ExecutionContext ec = new ExecutionContext();
                ec.setSchemaName(schemaName);
                SchemaConfig schema = config.getSchemas().get(ec.getSchemaName());
                TDataSource dataSource = schema.getDataSource();
                if (!dataSource.isInited()) {
                    dataSource.init();
                }
                ec.getExtraCmds().putAll(dataSource.getConnectionProperties());
                ec.setStats(dataSource.getStatistics());
                ec.setPhysicalRecorder(dataSource.getPhysicalRecorder());
                ec.setRecorder(dataSource.getRecorder());
                ec.setExecutorService(dataSource.borrowExecutorService());
                ec.setInternalSystemSql(false);
                ec.setExecuteMode(ExecutorMode.MPP);
                ec.setPrivilegeContext(new MppPrivilegeContext());
                ec.setTxIsolation(txIsolation);
                ec.putAllHintCmds(hintCmds);
                return ec;
            }
        });

        Server mppServer = new MppServer(TddlNode.getNodeId(), system.isMppServer(),
            system.isMppWorker(), this.serverHost, system.getRpcPort());
        ServiceProvider.getInstance().setServer(mppServer);
        mppServer.run();
        logger.info("MppServer is started on " + system.getRpcPort());
    }

    @Override
    protected void doDestroy() {
        try {
            logger.info("===============================================");
            logger.info(NAME + " is ready to stop ...");
            this.offline();

            this.waitNoRunning();

            // 等待10s
            Thread.sleep(10000);

            // while (this.getConnectionCount() != 0 && count++ < 10) {
            // Thread.sleep(10000);
            // }

            // 关闭接入
            server.interrupt();
            server.join(1 * 1000);
            manager.interrupt();
            manager.join(1 * 1000);
            // connector.interrupt();
            // connector.join(1 * 1000);
            // 关闭数据源
            this.config.destroy();

            if (ServiceProvider.getInstance().getServer() != null) {
                ServiceProvider.getInstance().getServer().stop();
            }

            logger.info(server.getName() + " is stoped");
            logger.info("===============================================");
        } catch (InterruptedException e) {
        }
    }

    public void checkSsl() {
        checkSslEnable(this.config.getSystem());
    }

    public void reloadSystemConfig() {
        synchronized (lock) {
            SystemConfig systemConfig = this.config.getSystem();

            // 更新默认编码和空闲时间
            manager.getFactory().setCharset(systemConfig.getCharset());
            manager.getFactory().setIdleTimeout(systemConfig.getIdleTimeout());
            server.getFactory().setCharset(systemConfig.getCharset());
            server.getFactory().setIdleTimeout(systemConfig.getIdleTimeout());

            this.killExecutor.setPoolSize(systemConfig.getProcessorKillExecutor());
            this.timerExecutor.setPoolSize(systemConfig.getTimerExecutor());
            this.serverExecutor.setPoolSize(systemConfig.getServerExecutor());
            this.serverExecutor.setDeadLockCheckPeriod(systemConfig.getDeadLockCheckPeriod());
            this.managerExecutor.setPoolSize(systemConfig.getManagerExecutor());
            this.timerTaskExecutor.setCorePoolSize(systemConfig.getTimerTaskExecutor());

            // Set the size of global memory pool
            final long memoryPoolSize = systemConfig.getGlobalMemoryLimit();
            MemoryManager.getInstance().adjustMemoryLimit(memoryPoolSize);
            logger.info("Global memory pool size set to " + FileUtils.byteCountToDisplaySize(memoryPoolSize));

            Server service = ServiceProvider.getInstance().getServer();
            if (service != null) {
                service.reloadConfig();
            }

            // 更新心跳检查时间
            if (systemConfig.getProcessorCheckPeriod() != processorCheckPeriod) {
                processorCheckPeriod = systemConfig.getProcessorCheckPeriod();
                processorCheck();
            }

            this.config.reloadCluster(systemConfig.getClusterName(),
                systemConfig.getUnitName(),
                systemConfig.getInstanceId());
            CdcRpcClient.buildCdcRpcClient();
            // 设置ip白名单
            List<String> trustIpList = new ArrayList<String>();
            String trustIps = systemConfig.getTrustedIps();
            if (StringUtils.isNotEmpty(trustIps)) {
                trustIpList.add(trustIps);
            }

            trustIps = StringUtils.join(trustIpList, ',');
            config.getClusterQuarantine().resetTrustedIps(trustIps);
            config.getClusterQuarantine().resetBlackList(systemConfig.getBlackIps());
            config.getClusterQuarantine().resetWhiteList(systemConfig.getWhiteIps());

            // 更新慢sql监控
            Map<String, SchemaConfig> schemas = this.config.getSchemas();
            if (schemas != null) {
                for (SchemaConfig schema : schemas.values()) {
                    // 逻辑sql统计
                    schema.getDataSource().getRecorder().setCount(systemConfig.getSqlRecordCount());
                    schema.getDataSource().getRecorder().setMaxSizeThresold(systemConfig.getSlowSqlSizeThresold());
                    schema.getDataSource().getRecorder().setSlowSqlTime(systemConfig.getSlowSqlTime());
                    // 物理sql统计
                    schema.getDataSource().getPhysicalRecorder().setCount(systemConfig.getSqlRecordCount());
                    schema.getDataSource()
                        .getPhysicalRecorder()
                        .setMaxSizeThresold(systemConfig.getSlowSqlSizeThresold());
                    schema.getDataSource().getPhysicalRecorder().setSlowSqlTime(systemConfig.getSlowSqlTime());
                }
            }

            // 设置下maxAllowedPacket
            server.getFactory().setMaxPacketSize(systemConfig.getMaxAllowedPacket());
            manager.getFactory().setMaxPacketSize(systemConfig.getMaxAllowedPacket());

            server.getFactory().setSocketRecvBuffer(systemConfig.getSocketRecvBuffer());
            server.getFactory().setSocketSendBuffer(systemConfig.getSocketSendBuffer());

            checkSslEnable(systemConfig);
        }
    }

    /**
     * 触发一下加载java进程的所有lib
     */
    private void warmup() {
        // init all functions
        ExtraFunctionManager.getExtraFunction("warmup", null, null);

        // init all collation configurations.
        CharsetFactory.INSTANCE.createCharsetHandler();

        // 初始化一下密码
        ExtensionLoader.load(ISequenceManager.class);
        ExtensionLoader.load(TAtomDsStandard.class);
        // 预热mysql资源
        try {
            ResourceBundle.getBundle("com.mysql.jdbc.LocalizedErrorMessages");

        } catch (Exception ignore) {
        }
    }

    /**
     * 检查一下sslEnable设置,明确一下是否可以启用SSL
     */
    private void checkSslEnable(SystemConfig systemConfig) {
        if (systemConfig.isSslEnable()) {
            try {
                if (SslContextFactory.startSupportSsl()) {
                    SslContextFactory.reset();
                    SslContextFactory.getServerContext();
                    logger.warn("ssl.server.config.done");
                } else {
                    logger.warn("ssl:there is no key , so ignore");
                    // 关闭ssl处理
                    systemConfig.setSslEnbale(false);
                }

            } catch (Throwable e) {
                logger.warn("start ssl fail , so ignore", e);
                // 关闭ssl处理
                systemConfig.setSslEnbale(false);
            }
        }
    }

    public String getServerAddress() {
        return AddressUtils.getHostIp() + ":" + this.config.getSystem().getServerPort();
    }

    public CobarConfig getConfig() {
        return config;
    }

    public List<SchemaConfig> getAllSchemaConfigs() {
        return new ArrayList<>(config.getSchemas().values());
    }

    public NIOProcessor[] getProcessors() {
        return processors;
    }

    public ServerThreadPool getManagerExecutor() {
        return managerExecutor;
    }

    public ServerThreadPool getServerExecutor() {
        return serverExecutor;
    }

    public ServerThreadPool getTimerExecutor() {
        return timerExecutor;
    }

    public ScheduledExecutorService getTimerTaskExecutor() {
        return timerTaskExecutor;
    }

    public ServerThreadPool getKillExecutor() {
        return killExecutor;
    }

    public long getStartupTime() {
        return startupTime;
    }

    public boolean isOnline() {
        return isOnline.get();
    }

    public void offline() {
        if (server != null) {
            this.getServer().offline();
        }
        isOnline.set(false);
    }

    public void waitNoRunning() throws InterruptedException {
        while (true) {
            int activeCount = CobarServer.getInstance().getServerExecutor().getActiveCount();
            if (activeCount == 0) {
                break;
            }

            logger.info("Total thread running count: " + activeCount);
            System.out.println("Total thread running count: " + activeCount);
            for (SchemaConfig schema : CobarServer.getInstance().getConfig().getSchemas().values()) {
                String schemaName = schema.getDataSource().getSchemaName();
                long appActiveCount =
                    CobarServer.getInstance().getServerExecutor().getTaskCountBySchemaName(schemaName);

                if (appActiveCount > 0) {
                    logger.info("App " + schemaName + " thread running count: " + appActiveCount);
                    System.out.println("App " + schemaName + " thread running count: " + appActiveCount);
                }
            }

            Thread.sleep(1000);
        }
    }

    public void online() {
        if (forceOffline.get()) {
            // 如果手动设置了关闭,不允许自动上线
            return;
        }

        if (server != null) {
            this.getServer().online();
        }

        isOnline.set(true);
    }

    public void forceOnline() {
        forceOffline.set(false);
        online();
    }

    public void forceOffline() {
        offline();
        forceOffline.set(true);
    }

    // 系统时间定时更新任务
    private Runnable updateTime() {
        return new Runnable() {

            @Override
            public void run() {
                TimeUtil.update();
            }
        };
    }

    // 处理器定时检查任务
    private ScheduledFuture<?> processorCheck() {
        if (idleCheckTask != null) {
            idleCheckTask.cancel(false);
        }

        Runnable scheduleTask = new Runnable() {

            @Override
            public void run() {
                timerExecutor.execute(new Runnable() {

                    @Override
                    public void run() {
                        for (NIOProcessor p : processors) {
                            p.check();
                        }
                    }
                });
            }
        };
        idleCheckTask = scheduler.scheduleWithFixedDelay(scheduleTask, 0L, processorCheckPeriod, TimeUnit.MILLISECONDS);
        return idleCheckTask;
    }

    private void logServerStartUp() {

        StringBuilder logInfoSb = new StringBuilder("");
        SystemConfig systemConfig = CobarServer.getInstance().getConfig().getSystem();

        String instanceId = CobarServer.getInstance().getConfig().getInstanceId();
        String serverLocalHost = AddressUtils.getHostIp();
        Integer serverPort = systemConfig.getServerPort();
        Integer serverHtapPort = -1;
        Integer managerPort = systemConfig.getManagerPort();
        Integer mppRpcPort = systemConfig.getRpcPort();
        String metaDbAddr = systemConfig.getMetaDbAddr();
        String metaDbUser = systemConfig.getMetaDbUser();
        String metaDbEncPasswd = systemConfig.getMetaDbPasswd();
        String metaDbName = systemConfig.getMetaDbName();
        String metaDbProp = systemConfig.getMetaDbProp();

        logInfoSb.append("\n=====================").append("\n");
        logInfoSb.append("startupTime=")
            .append(DateFormat.getDateTimeInstance(DateFormat.MEDIUM, DateFormat.MEDIUM).format(new Date()))
            .append("\n");
        logInfoSb.append("instanceId=").append(instanceId).append("\n");
        logInfoSb.append("serverLocalIp=").append(serverLocalHost).append("\n");
        logInfoSb.append("serverPort=").append(serverPort).append("\n");
        logInfoSb.append("serverHtapPort=").append(serverHtapPort).append("\n");
        logInfoSb.append("managerPort=").append(managerPort).append("\n");
        logInfoSb.append("mppRpcPort=").append(mppRpcPort).append("\n");

        logInfoSb.append("metaDbAddr=").append(metaDbAddr).append("\n");
        logInfoSb.append("metaDbUser=").append(metaDbUser).append("\n");
        logInfoSb.append("metaDbPasswd=").append(metaDbEncPasswd).append("\n");
        logInfoSb.append("metaDbName=").append(metaDbName).append("\n");
        logInfoSb.append("metaDbProp=").append(metaDbProp).append("\n");
        if (metaDbAddr != null) {
            String[] addrArr = metaDbAddr.split(",");
            if (addrArr.length > 0) {
                Pair<String, Integer> ipPort = AddressUtils.getIpPortPairByAddrStr(addrArr[0]);
                logInfoSb.append("metaDbMySqlUrl=")
                    .append(String
                        .format("mysql -h%s -P%s -u%s -p'%s' %s -Ac", ipPort.getKey(), ipPort.getValue(),
                            metaDbUser,
                            PasswdUtil.decrypt(metaDbEncPasswd),
                            metaDbName))
                    .append("\n");
            }
        }
        MetaDbLogUtil.START_UP_LOG.info(logInfoSb.toString());
    }

    public int getConnectionCount() {
        int count = 0;
        for (NIOProcessor p : CobarServer.getInstance().getProcessors()) {
            count += p.getFrontends().size();
        }

        return count;
    }

    public NIOAcceptor getManager() {
        return manager;
    }

    public NIOAcceptor getServer() {
        return server;
    }

    public String getServerHost() {
        return serverHost;
    }

    public void setServerHost(String serverHost) {
        this.serverHost = serverHost;
    }

    public String getServerPort() {
        return serverPort;
    }

    public void setServerPort(String serverPort) {
        this.serverPort = serverPort;
    }

    public String getManagerPort() {
        return managerPort;
    }

    public void setManagerPort(String managerPort) {
        this.managerPort = managerPort;
    }
}
