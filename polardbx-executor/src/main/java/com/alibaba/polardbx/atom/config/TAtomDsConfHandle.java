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

package com.alibaba.polardbx.atom.config;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.polardbx.atom.TAtomDataSource;
import com.alibaba.polardbx.atom.TAtomDataSourceFilter;
import com.alibaba.polardbx.atom.TAtomDbStatusEnum;
import com.alibaba.polardbx.atom.TAtomDbTypeEnum;
import com.alibaba.polardbx.atom.common.TAtomConURLTools;
import com.alibaba.polardbx.atom.common.TAtomConstants;
import com.alibaba.polardbx.atom.config.gms.TAtomDsGmsConfigHelper;
import com.alibaba.polardbx.atom.config.listener.AtomAppConfigChangeListener;
import com.alibaba.polardbx.atom.config.listener.AtomDbStatusListener;
import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.logger.LoggerInit;
import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.model.lifecycle.Lifecycle;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.thread.ExtendedScheduledThreadPoolExecutor;
import com.alibaba.polardbx.common.utils.thread.NamedThreadFactory;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.gms.config.ConnPoolConfHandler;
import com.alibaba.polardbx.gms.config.impl.ConnPoolConfig;
import com.alibaba.polardbx.gms.config.impl.ConnPoolConfigManager;
import com.alibaba.polardbx.rpc.compatible.XDataSource;
import com.alibaba.polardbx.rpc.pool.XConnectionManager;
import org.apache.commons.lang.StringUtils;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.sql.DataSource;
import java.lang.management.ManagementFactory;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 数据库动态切换的Handle类，所有数据库的动态切换 都是由这个类完成
 *
 * @author qihao
 */
public class TAtomDsConfHandle extends AbstractLifecycle implements Lifecycle {

    private static final Logger logger = LoggerFactory.getLogger(TAtomDsConfHandle.class);
    private static final ScheduledExecutorService createScheduler = createScheduler("Druid-ConnectionPool-CreateScheduler-",
        30);
    private static final ScheduledExecutorService destroyScheduler = createScheduler("Druid-ConnectionPool-DestroyScheduler-",
        30);
    private static final AtomicInteger version = new AtomicInteger(1);

    static {
        try {
            MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
            ObjectName sqlName = new ObjectName("com.alibaba.alimonitor.jmonitor:type=DruidSql");
            Class<?> clazz = Class.forName("com.alibaba.druid.stat.JdbcStatManager");
            Object druidSqlStat = clazz.getMethod("getInstance").invoke(null);
            if (!mbeanServer.isRegistered(sqlName)) {
                mbeanServer.registerMBean(druidSqlStat, sqlName);
                logger.info("reg druid mbean(com.alibaba.druid.stat.JdbcStatManager) success.");
            }
        } catch (Throwable e) {
            logger.error(e);
        }
    }

    /**
     * 数据源操作锁，当需要对数据源进行重建或者刷新时需要先获得该锁
     */
    private final ReentrantLock lock = new ReentrantLock();
    private String appName;
    private String dbKey;
    private String unitName;
    /**
     * 运行时配置
     */
    private volatile TAtomDsConfDO runTimeConf = new TAtomDsConfDO();

    /**
     * 本地配置，优先于推送的动态配置
     */
    private TAtomDsConfDO localConf = new TAtomDsConfDO();

    /**
     * Xproto数据源
     */
    private volatile XDataSource xDataSource = null;

    /**
     * 数据库状态改变回调
     */
    /**
     * 记录一下共享这个handle的数据源有哪些,最后一个引用关闭时才触发handle的关闭
     */
    private final Map<String, Integer> dataSourceReferences = Collections.synchronizedMap(new HashMap<String, Integer>());
    private final TAtomDataSource atomDataSource;

    /**
     * <pre>
     *  KEY: listenerName *  VAL: listener
     * </pre>
     */
    private final Map<String, AtomAppConfigChangeListener> atomAppConfigChangeListenerMap =
        new ConcurrentHashMap<String, AtomAppConfigChangeListener>();

    public TAtomDsConfHandle(TAtomDataSource atomDataSource) {
        this.atomDataSource = atomDataSource;
    }

    public TAtomDsConfHandle(TAtomDataSource atomDataSource, TAtomDsConfDO atomDsConf) {
        this.atomDataSource = atomDataSource;
        this.runTimeConf = atomDsConf;
    }

    /**
     * 针对tddl server模型特殊设置，设置netTimeoutForStreamingResults=0
     */
    public static void fillConnectionProperties(TAtomDsConfDO tAtomDsConfDO) throws SQLException {
        Map<String, String> connectionProperties = tAtomDsConfDO.getConnectionProperties();
        if (null == connectionProperties || connectionProperties.isEmpty()) {
            connectionProperties = TAtomConstants.DEFAULT_MYSQL_CONNECTION_PROPERTIES;
            tAtomDsConfDO.setConnectionProperties(connectionProperties);
        }

        // 启动为server模式
        String key = "netTimeoutForStreamingResults";
        if (!connectionProperties.containsKey(key)) {
            connectionProperties.put(key, "0");
        }

        // 关闭autoReconnect标记
        connectionProperties.put("autoReconnect", "false");

        // This will cause a 'streaming' ResultSet to be automatically
        // closed, and any outstanding data still streaming from the server
        // to be discarded if another query is executed before all the data
        // has been read from the server.
        connectionProperties.put("clobberStreamingResults", "true");

        String multiKey = "allowMultiQueries";
        if (!connectionProperties.containsKey(multiKey)) {
            connectionProperties.put(multiKey, "true");
        }

        // 精卫临时添加
        String rewriteBatchedStatements = "rewriteBatchedStatements";
        if (!connectionProperties.containsKey(rewriteBatchedStatements)) {
            connectionProperties.put(rewriteBatchedStatements, "true");
        }

        // 关闭每次读取read-only状态,提升batch性能
        String readOnlyPropagatesToServerKey = "readOnlyPropagatesToServer";
        if (!connectionProperties.containsKey(readOnlyPropagatesToServerKey)) {
            connectionProperties.put(readOnlyPropagatesToServerKey, "false");
        }

        if (ConfigDataMode.isZeroDataTimeToString()) {
            // 是否开启0值时间处理
            String zeroDateTimeBehavior = "zeroDateTimeBehavior";
            if (!connectionProperties.containsKey(zeroDateTimeBehavior)) {
                connectionProperties.put(zeroDateTimeBehavior, "convertToNull");// 将0000-00-00的时间类型返回null
            }
            String yearIsDateType = "yearIsDateType";
            if (!connectionProperties.containsKey(yearIsDateType)) {
                connectionProperties.put(yearIsDateType, "false");// 直接返回字符串，不做year转换date处理
            }
            String noDatetimeStringSync = "noDatetimeStringSync";
            if (!connectionProperties.containsKey(noDatetimeStringSync)) {
                connectionProperties.put(noDatetimeStringSync, "true");// 返回时间类型的字符串,不做时区处理
            }
        }
    }

    /**
     * 将TAtomDsConfDO转换成LocalTxDataSourceDO
     */
    @SuppressWarnings("rawtypes")
    public static DruidDataSource convertTAtomDsConf2DruidConf(String dbKey, TAtomDsConfDO tAtomDsConfDO,
                                                               String dbName) {
        try {
            DruidDataSource localDruidDataSource = new DruidDataSource();
            // Acquire sync-lock while connection is closing
            localDruidDataSource.setAsyncCloseConnectionEnable(true);

            localDruidDataSource.setKeepAlive(tAtomDsConfDO.isEnableKeepAlive());

            if (tAtomDsConfDO.isCreateScheduler()) {
                localDruidDataSource.setCreateScheduler(createScheduler);
            }

            if (tAtomDsConfDO.isDestroyScheduler()) {
                localDruidDataSource.setDestroyScheduler(destroyScheduler);
            }
            // 一下三个是druid监控需要的特殊配置
            // 针对名字加个version，避免动态推送时创建mbean失败
            localDruidDataSource.setName(dbKey + "_" + version.getAndIncrement());
            localDruidDataSource.setTestOnBorrow(false);
            localDruidDataSource.setTestWhileIdle(true);
            localDruidDataSource.setLogDifferentThread(false);// 关闭日志警告
            // 如果读库被释放,启用fail-fast
            localDruidDataSource.setFailFast(true);
            if (tAtomDsConfDO.getMaxWaitThreadCount() == 0) {
                localDruidDataSource.setMaxWaitThreadCount(tAtomDsConfDO.getMaxPoolSize());
            } else {
                localDruidDataSource.setMaxWaitThreadCount(tAtomDsConfDO.getMaxWaitThreadCount());
            }
            localDruidDataSource.setNotFullTimeoutRetryCount(2); // 针对超时未满连接池的默认重试3次
            localDruidDataSource.setUsername(tAtomDsConfDO.getUserName());
            localDruidDataSource.setPassword(tAtomDsConfDO.getPasswd());
            localDruidDataSource.setDriverClassName(tAtomDsConfDO.getDriverClass());
            localDruidDataSource.setExceptionSorterClassName(tAtomDsConfDO.getSorterClass());
            if (tAtomDsConfDO.isUseLoadVariablesFilter()) {
                localDruidDataSource.addFilters(TAtomDataSourceFilter.class.getName());
            }

            // 根据数据库类型设置conURL和setConnectionProperties
            if (TAtomDbTypeEnum.MYSQL == tAtomDsConfDO.getDbTypeEnum()) {
                // 特殊设置下streaming
                fillConnectionProperties(tAtomDsConfDO);
                String conUlr = TAtomConURLTools.getMySqlConURL(tAtomDsConfDO.getIp(),
                    tAtomDsConfDO.getPort(),
                    tAtomDsConfDO.getDbName(),
                    tAtomDsConfDO.getConnectionProperties());
                localDruidDataSource.setUrl(conUlr);
                // 如果可以找到mysqlDriver中的Valid就使用，否则不设置valid
                String validConnnectionCheckerClassName =
                    TAtomConstants.DEFAULT_DRUID_MYSQL_VALID_CONNECTION_CHECKERCLASS;
                try {
                    Class.forName(validConnnectionCheckerClassName);
                    localDruidDataSource.setValidConnectionCheckerClassName(validConnnectionCheckerClassName);
                } catch (ClassNotFoundException e) {
                    logger.info("MYSQL Driver is Not Suport " + validConnnectionCheckerClassName);
                } catch (NoClassDefFoundError e) {
                    logger.info("MYSQL Driver is Not Suport " + validConnnectionCheckerClassName);
                }
                String integrationSorterCalssName = TAtomConstants.DRUID_MYSQL_INTEGRATION_SORTER_CLASS;
                String defaultIntegrationSorterCalssName = TAtomConstants.DEFAULT_DRUID_MYSQL_SORTER_CLASS;
                try {
                    Class integrationSorterCalss = Class.forName(integrationSorterCalssName);
                    if (null != integrationSorterCalss) {
                        localDruidDataSource.setExceptionSorterClassName(integrationSorterCalssName);
                    } else {
                        localDruidDataSource.setExceptionSorterClassName(defaultIntegrationSorterCalssName);
                        logger.info("MYSQL Driver is Not Suport " + integrationSorterCalssName + " use default sorter "
                            + defaultIntegrationSorterCalssName);
                    }
                } catch (ClassNotFoundException e) {
                    logger.info("MYSQL Driver is Not Suport " + integrationSorterCalssName + " use default sorter "
                        + defaultIntegrationSorterCalssName);
                    localDruidDataSource.setExceptionSorterClassName(defaultIntegrationSorterCalssName);
                } catch (NoClassDefFoundError e) {
                    logger.info("MYSQL Driver is Not Suport " + integrationSorterCalssName + " use default sorter "
                        + defaultIntegrationSorterCalssName);
                    localDruidDataSource.setExceptionSorterClassName(defaultIntegrationSorterCalssName);
                }
                localDruidDataSource.setValidationQuery(TAtomConstants.DEFAULT_DRUID_MYSQL_VALIDATION_QUERY);
            }

            // 当已经打开了keepAlive功能，就不需要再设置InitialSize了
            if (!tAtomDsConfDO.isEnableKeepAlive()) {
                // lazy init 先设置为0 后续真正执行时才创建连接
                localDruidDataSource.setInitialSize(tAtomDsConfDO.getInitPoolSize());
            }

            localDruidDataSource.setOnFatalErrorMaxActive(tAtomDsConfDO.getOnFatalErrorMaxActive());

            // 连接池的初始化，不再区分是否是Server
            // 这样保证server的连接池的最少连接数与配置保持一致
            if (tAtomDsConfDO.isEnableKeepAlive()) {
                localDruidDataSource.setMinIdle(adjustMinPoolSize(tAtomDsConfDO));
            } else {
                localDruidDataSource.setMinIdle(tAtomDsConfDO.getMinPoolSize());
            }

            String systemMaxActive = System.getProperty("tddlMaxActive");
            if (systemMaxActive != null) {
                Integer systemMaxActiveInt = Integer.valueOf(systemMaxActive);
                if (systemMaxActiveInt < tAtomDsConfDO.getMaxPoolSize()) {
                    systemMaxActiveInt = tAtomDsConfDO.getMaxPoolSize(); // 如果设置的值比max值小,则按照最大值填充
                }

                localDruidDataSource.setMaxActive(systemMaxActiveInt);
            } else {
                localDruidDataSource.setMaxActive(tAtomDsConfDO.getMaxPoolSize());
            }
            if (tAtomDsConfDO.getPreparedStatementCacheSize() > 0
                && TAtomDbTypeEnum.MYSQL != tAtomDsConfDO.getDbTypeEnum()) {
                localDruidDataSource.setPoolPreparedStatements(true);
                localDruidDataSource
                    .setMaxPoolPreparedStatementPerConnectionSize(tAtomDsConfDO.getPreparedStatementCacheSize());
            }

            if (tAtomDsConfDO.getIdleTimeout() > 0) {
                if (tAtomDsConfDO.getEvictionTimeout() <= 0) {
                    long evictionTimeout = tAtomDsConfDO.getIdleTimeout() / 30;
                    if (evictionTimeout <= 0) {// 最小为1
                        evictionTimeout = 1;
                    }
                    tAtomDsConfDO.setEvictionTimeout(evictionTimeout);
                }

                // 配置一个连接在池中最小生存的时间，单位是毫秒
                localDruidDataSource.setMinEvictableIdleTimeMillis(tAtomDsConfDO.getIdleTimeout() * 60 * 1000);
                // drds keepalive default 60 s
                localDruidDataSource.setKeepAliveBetweenTimeMillis(tAtomDsConfDO.getIdleTimeout() * 60 * 1000 / 30);

            }

            if (tAtomDsConfDO.getBlockingTimeout() > 0) {
                localDruidDataSource.setMaxWait(tAtomDsConfDO.getBlockingTimeout());
            }

            if (tAtomDsConfDO.getEvictionTimeout() > 0) {
                // drds default 30 s
                localDruidDataSource.setTimeBetweenEvictionRunsMillis(tAtomDsConfDO.getEvictionTimeout() * 30 * 1000);
            }

            // 如果数据库不可用,失败重试的时间间隔
            localDruidDataSource.setTimeBetweenConnectErrorMillis(3 * 1000);
            List<String> initSqls = new ArrayList<String>();
            if (!StringUtils.isEmpty(tAtomDsConfDO.getConnectionInitSql())) {
                initSqls.add(tAtomDsConfDO.getConnectionInitSql());
            }
            localDruidDataSource.setConnectionInitSqls(initSqls);

            // 使用当前classloader
            localDruidDataSource.setDriverClassLoader(localDruidDataSource.getClass().getClassLoader());
            localDruidDataSource.setUseUnfairLock(true);

            localDruidDataSource.setDefaultTransactionIsolation(ConfigDataMode.getTxIsolation());

            return localDruidDataSource;
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public static boolean checkLocalTxDataSourceDO(DruidDataSource druidDataSource) {
        if (null == druidDataSource) {
            return false;
        }

        if (TStringUtil.isBlank(druidDataSource.getUrl())) {
            throw new TddlRuntimeException(ErrorCode.ERR_MISS_ATOM_APP_CONFIG, "URL");
        }

        if (TStringUtil.isBlank(druidDataSource.getUsername())) {
            throw new TddlRuntimeException(ErrorCode.ERR_MISS_ATOM_APP_CONFIG, "Username");
        }

        if (TStringUtil.isBlank(druidDataSource.getPassword())) {
            throw new TddlRuntimeException(ErrorCode.ERR_MISS_PASSWD, "Password");
        }

        if (TStringUtil.isBlank(druidDataSource.getDriverClassName())) {
            throw new TddlRuntimeException(ErrorCode.ERR_MISS_ATOM_APP_CONFIG, "DriverClassName");
        }

        if (druidDataSource.getMinIdle() < 0) {
            logger.error("[DsConfig Check] MinIdle Error size is:" + druidDataSource.getMinIdle());
            return false;
        }

        if (druidDataSource.getMaxActive() < 1) {
            logger.error("[DsConfig Check] MaxActive Error size is:" + druidDataSource.getMaxActive());
            return false;
        }

        if (druidDataSource.getMinIdle() > druidDataSource.getMaxActive()) {
            logger.error("[DsConfig Check] MinPoolSize Over MaxPoolSize Minsize is:" + druidDataSource.getMinIdle()
                + "MaxSize is :" + druidDataSource.getMaxActive());
            return false;
        }
        return true;
    }

    private static ScheduledExecutorService createScheduler(String name, int poolSize) {
        String systemPoolSize = System.getProperty("tddl.scheduler.poolSize");
        if (StringUtils.isNotEmpty(systemPoolSize)) {
            poolSize = Integer.valueOf(systemPoolSize);
        }

        return new ExtendedScheduledThreadPoolExecutor(poolSize, new NamedThreadFactory(name, true));
    }

    private static void setXDataSource(XDataSource xDataSource, TAtomDsConfDO cnf) {
        final String encoding =
            null == cnf.getCharacterEncoding() ?
                cnf.getConnectionProperties().get("characterEncoding") : cnf.getCharacterEncoding();
        xDataSource.setDefaultEncodingMySQL(encoding);
        xDataSource.setGetConnTimeoutMillis(cnf.getBlockingTimeout());
        xDataSource.setDefaultQueryTimeoutMillis(cnf.getSocketTimeout());
    }

    /**
     * 初始化方法，创建对应的数据源，只能被调用一次
     */
    @Override
    public void doInit() {
        // 1.初始化参数检查
        if (TStringUtil.isBlank(this.dbKey)) {
            throw new TddlRuntimeException(ErrorCode.ERR_MISS_ATOM_OTHER_CONFIG,
                "DBKey",
                this.dbKey,
                this.appName,
                null,
                this.unitName);
        }

        if (TStringUtil.isBlank(this.appName)) {
            throw new TddlRuntimeException(ErrorCode.ERR_MISS_ATOM_OTHER_CONFIG,
                "AppName",
                this.dbKey,
                this.appName,
                null,
                this.unitName);
        }
        // To Be load By MetaDb
        TAtomDsConfDO newRunTimeConf = runTimeConf;

        lock.lock();
        try {
            // 5.解析配置string成TAtomDsConfDO
            runTimeConf = newRunTimeConf;

            //备库模式下，允许覆盖全局SLAVE_SOCKET_TIMEOUT
            if (!atomDataSource.isMasterDB()) {
                String globalSocketTimeout = System.getProperty(ConnectionProperties.SLAVE_SOCKET_TIMEOUT);
                if (globalSocketTimeout != null
                    && runTimeConf.getConnectionProperties().get(TAtomConfParser.APP_SOCKET_TIMEOUT_KEY) != null) {
                    try {
                        if (Integer.parseInt(globalSocketTimeout) > Integer
                            .parseInt(
                                runTimeConf.getConnectionProperties().get(TAtomConfParser.APP_SOCKET_TIMEOUT_KEY))) {
                            runTimeConf.getConnectionProperties()
                                .put(TAtomConfParser.APP_SOCKET_TIMEOUT_KEY, globalSocketTimeout);
                        }
                    } catch (Throwable e) {
                    }
                }
            }

            // 6.处理本地优先配置
            overConfByLocal(localConf, runTimeConf);
            // 7.如果没有设置本地密码，则用订的密码，初始化passwdManager
            if (TStringUtil.isBlank(this.runTimeConf.getPasswd())) {
                // 检查dbKey和对应的userName是否为空
                if (TStringUtil.isBlank(runTimeConf.getUserName())) {
                    throw new TddlRuntimeException(ErrorCode.ERR_MISS_ATOM_OTHER_CONFIG,
                        "userName",
                        this.dbKey,
                        this.appName,
                        null,
                        this.unitName);
                }
            }

            // Xproto or Druid.
            if (this.runTimeConf.getXport() > 0) {
                final XDataSource dataSource = new XDataSource(
                    this.runTimeConf.getIp(), this.runTimeConf.getXport(),
                    this.runTimeConf.getUserName(), this.runTimeConf.getPasswd(),
                    this.runTimeConf.getDbName(), TAtomDsConfHandle.this.dbKey + "_" + version.getAndIncrement());
                setXDataSource(dataSource, this.runTimeConf);
                this.xDataSource = dataSource;
            } else {
                throw new NotSupportException("jdbc");
            }

            clearDataSourceWrapper();
            ConnPoolConfigManager.getInstance().registerConnPoolConfHandler(dbKey, new ConnPoolConfHandler() {
                @Override
                public void onDataReceived(ConnPoolConfig newConnPoolConfig) {
                    String userName = runTimeConf.getUserName();
                    String connProps = newConnPoolConfig.connProps;
                    int minPoolSize = newConnPoolConfig.minPoolSize;
                    int maxPoolSize = newConnPoolConfig.maxPoolSize;
                    int maxWaitThreadCount = newConnPoolConfig.maxWaitThreadCount;
                    int idleTimeout = newConnPoolConfig.idleTimeout;
                    int blockTimeout = newConnPoolConfig.blockTimeout;
                    String connPoolConfigStr =
                        String.format(TAtomDsGmsConfigHelper.ATOM_APP_CONF_TEMPLATE, userName, minPoolSize,
                            maxPoolSize, maxWaitThreadCount, idleTimeout,
                            blockTimeout,
                            connProps);

                    // Check whether to enable xport.
                    // Note this is Xproto for **STORAGE** node. First check global setting then use the metaDB inst_config.
                    int xport = runTimeConf.getOriginXport();
                    if (XConnectionManager.getInstance().getStorageDbPort() != 0) {
                        // Disabled or force set by server.properties.
                        xport = XConnectionManager.getInstance().getStorageDbPort();
                    } else if (newConnPoolConfig.xprotoStorageDbPort != null) {
                        if (newConnPoolConfig.xprotoStorageDbPort != 0) {
                            xport = newConnPoolConfig.xprotoStorageDbPort; // Disabled or force set by inst_config.
                        } // else auto set by HA.
                    } else {
                        // Bad config? Disable it.
                        xport = -1;
                    }
                    applyAppDbConfig(dbKey, connPoolConfigStr, xport);
                }
            });
        } finally {
            lock.unlock();
        }

    }

    private void clearDataSourceWrapper() {
        // Monitor.removeSnapshotValuesCallback(wrapDataSource);
        // wrapDataSource = null;
    }

    public void applyAppDbConfig(String dataId, String data, int newXport) {
        LoggerInit.TDDL_DYNAMIC_CONFIG.info("[Data Recieved] [DRUID AppConf HandleData] dataId : " + dataId
            + " data: " + data);
        if (null == data || TStringUtil.isBlank(data)) {
            return;
        }
        lock.lock();
        try {
            String appConfStr = data;
            TAtomDsConfDO tmpConf = TAtomConfParser.parserTAtomDsConfDO(null, appConfStr);
            TAtomDsConfDO newConf = TAtomDsConfHandle.this.runTimeConf.clone();
            // 有些既有配置不能变更，所以克隆老的配置，然后将新的set进去
            newConf.setUserName(tmpConf.getUserName());
            newConf.setMinPoolSize(tmpConf.getMinPoolSize());
            newConf.setMaxPoolSize(tmpConf.getMaxPoolSize());
            newConf.setInitPoolSize(tmpConf.getInitPoolSize());
            newConf.setMaxWaitThreadCount(tmpConf.getMaxWaitThreadCount());
            newConf.setIdleTimeout(tmpConf.getIdleTimeout());
            newConf.setBlockingTimeout(tmpConf.getBlockingTimeout());
            newConf.setEvictionTimeout(tmpConf.getEvictionTimeout());
            newConf.setPreparedStatementCacheSize(tmpConf.getPreparedStatementCacheSize());
            newConf.setConnectionProperties(tmpConf.getConnectionProperties());
            newConf.setOnFatalErrorMaxActive(tmpConf.getOnFatalErrorMaxActive());

            // 增加3个具体的实现
            newConf.setWriteRestrictTimes(tmpConf.getWriteRestrictTimes());
            newConf.setReadRestrictTimes(tmpConf.getReadRestrictTimes());
            newConf.setThreadCountRestrict(tmpConf.getThreadCountRestrict());
            newConf.setTimeSliceInMillis(tmpConf.getTimeSliceInMillis());
            newConf.setDriverClass(tmpConf.getDriverClass());

            newConf.setConnectionInitSql(tmpConf.getConnectionInitSql());
            newConf.setThreadCountRestrict(tmpConf.getThreadCountRestrict());
            newConf.setReadRestrictTimes(tmpConf.getReadRestrictTimes());
            newConf.setWriteRestrictTimes(tmpConf.getWriteRestrictTimes());
            newConf.setTimeSliceInMillis(tmpConf.getTimeSliceInMillis());
            newConf.setStrictKeepAlive(tmpConf.getStrictKeepAlive());
            newConf.setEnableKeepAlive(tmpConf.isEnableKeepAlive());

            if (StringUtils.isNotEmpty(tmpConf.getCharacterEncoding())) {
                newConf.setCharacterEncoding(tmpConf.getCharacterEncoding());
            }
            newConf.setDsMode(tmpConf.getDsMode());

            // 处理本地优先配置
            overConfByLocal(TAtomDsConfHandle.this.localConf, newConf);

            // Set new xport config.
            newConf.setXport(newXport);

            if (newConf.getXport() > 0) {
                final XDataSource newDataSource = new XDataSource(
                    newConf.getIp(), newConf.getXport(),
                    newConf.getUserName(), newConf.getPasswd(),
                    newConf.getDbName(), TAtomDsConfHandle.this.dbKey + "_" + version.getAndIncrement());
                setXDataSource(newDataSource, newConf);

                final XDataSource oldXDataSource = TAtomDsConfHandle.this.xDataSource;
                TAtomDsConfHandle.this.xDataSource = newDataSource;

                if (oldXDataSource != null) {
                    oldXDataSource.close();
                }

                // Refresh.
                TAtomDsConfHandle.this.runTimeConf = newConf;
                clearDataSourceWrapper();
            } else {
                throw new NotSupportException("jdbc not support");
            }

            if (!atomAppConfigChangeListenerMap.isEmpty()) {
                for (Map.Entry<String, AtomAppConfigChangeListener> listenerItem : atomAppConfigChangeListenerMap
                    .entrySet()) {
                    String listenerName = listenerItem.getKey();
                    AtomAppConfigChangeListener listener = listenerItem.getValue();
                    try {
                        listener.onAtomConfigChange(listenerName, getAtomDataSource());
                        LoggerInit.TDDL_DYNAMIC_CONFIG
                            .info("[OnAtomAppConfChangeListener] on onAtomConfigChange success! listenerName is "
                                + listenerName + ", dataId : " + dataId);
                        logger.info("[OnAtomAppConfChangeListener] on onAtomConfigChange success! listenerName is "
                            + listenerName + ", dataId : " + dataId);
                    } catch (Throwable e) {
                        LoggerInit.TDDL_DYNAMIC_CONFIG
                            .error("[OnAtomAppConfChangeListener] on onAtomConfigChange error! listenerName is "
                                    + listenerName,
                                e);
                        logger.error("[OnAtomAppConfChangeListener] on onAtomConfigChange error! listenerName is "
                                + listenerName,
                            e);
                    }

                }
            }

        } catch (Throwable e1) {
            throw GeneralUtil.nestedException(e1);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 是用本地配置覆盖传入的TAtomDsConfDO的属性
     */
    private void overConfByLocal(TAtomDsConfDO localDsConfDO, TAtomDsConfDO newDsConfDO) {
        if (null == newDsConfDO || null == localDsConfDO) {
            return;
        }
        if (TStringUtil.isNotBlank(localDsConfDO.getSorterClass())) {
            newDsConfDO.setSorterClass(localDsConfDO.getSorterClass());
        }
        if (TStringUtil.isNotBlank(localDsConfDO.getPasswd())) {
            newDsConfDO.setPasswd(localDsConfDO.getPasswd());
        }
        if (null != localDsConfDO.getConnectionProperties() && !localDsConfDO.getConnectionProperties().isEmpty()) {
            newDsConfDO.setConnectionProperties(localDsConfDO.getConnectionProperties());
        }
    }

    public DataSource getDataSource() {
        return this.xDataSource;
    }

    public void flushDataSource() {
        // 暂时不支持flush 抛错
        logger.error("DRUID DATASOURCE DO NOT SUPPORT FLUSH.");
        throw new RuntimeException("DRUID DATASOURCE DO NOT SUPPORT FLUSH.");
    }

    public void addAtomAppConfigChangeListener(String listenerName, AtomAppConfigChangeListener listener) {
        if (listener == null) {
            return;
        }
        atomAppConfigChangeListenerMap.putIfAbsent(listenerName, listener);
    }

    @Override
    protected void doDestroy() {
        if (this.xDataSource != null) {
            logger.info("[XDataSource Stop] Start!");
            LoggerInit.TDDL_DYNAMIC_CONFIG.info("[XDataSource Stop] Start! dbKey is " + this.dbKey);
            this.xDataSource.close();
            this.xDataSource = null;
            logger.info("[XDataSource Stop] End!");
        }
        clearDataSourceWrapper();
        ConnPoolConfigManager.getInstance().unRegisterConnPoolConfHandler(dbKey);
    }

    public void destroyDataSource() {
        destroy();
    }

    public void setSingleInGroup(boolean isSingleInGroup) {
        this.runTimeConf.setSingleInGroup(isSingleInGroup);
    }

    public void setLocalPasswd(String passwd) {
        if (isInited()) {
            throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, "[AlreadyInit] couldn't Reset passwd !");
        }
        this.localConf.setPasswd(passwd);
    }

    public void setLocalConnectionProperties(Map<String, String> map) {
        if (isInited()) {
            throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, "[AlreadyInit] couldn't Reset connectionProperties !");
        }
        this.localConf.setConnectionProperties(map);
        String driverClass = map.get(TAtomConfParser.APP_DRIVER_CLASS_KEY);
        if (!TStringUtil.isBlank(driverClass)) {
            this.localConf.setDriverClass(driverClass);
        }
    }

    public void setLocalDriverClass(String driverClass) {
        if (isInited()) {
            throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, "[AlreadyInit] couldn't Reset driverClass !");
        }
        this.localConf.setDriverClass(driverClass);
    }

    public void setLocalSorterClass(String sorterClass) {
        if (isInited()) {
            throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, "[AlreadyInit] couldn't Reset sorterClass !");
        }
        this.localConf.setSorterClass(sorterClass);
    }

    public String getUnitName() {
        return unitName;
    }

    public void setUnitName(String unitName) {
        this.unitName = unitName;
    }

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        if (isInited()) {
            throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, "[AlreadyInit] couldn't Reset appName !");
        }
        this.appName = appName;
    }

    public String getDbKey() {
        return dbKey;
    }

    public void setDbKey(String dbKey) {
        if (isInited()) {
            throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, "[AlreadyInit] couldn't Reset dbKey !");
        }
        this.dbKey = dbKey;
    }

    public TAtomDbStatusEnum getStatus() {
        return this.runTimeConf.getDbStautsEnum();
    }

    public TAtomDbTypeEnum getDbType() {
        return this.runTimeConf.getDbTypeEnum();
    }

    public void setDbStatusListeners(List<AtomDbStatusListener> dbStatusListeners) {
    }

    /**
     * 增加对dsHandle的引用，这里同步操作
     */
    public synchronized void addDataSourceReferences(String key) {

        Integer dataSourceReferenceCount = dataSourceReferences.get(key);
        if (dataSourceReferenceCount == null) {
            dataSourceReferenceCount = 1;
        } else {
            ++dataSourceReferenceCount;
        }
        dataSourceReferences.put(key, dataSourceReferenceCount);
    }

    /**
     * 减少对dsHandle的引用，这里同步操作
     */
    public synchronized void removeDataSourceReferences(String key) {

        Integer dataSourceReferenceCount = dataSourceReferences.get(key);
        if (dataSourceReferenceCount == null) {
            return;
        } else {
            --dataSourceReferenceCount;

            if (dataSourceReferenceCount < 1) {
                dataSourceReferences.remove(key);
            } else {
                dataSourceReferences.put(key, dataSourceReferenceCount);
            }
        }

    }

    /**
     * 获取对dsHandle的引用数
     */
    public Map<String, Integer> getDataSourceReferences() {
        return dataSourceReferences;
    }

    public TAtomDsConfDO getRunTimeConf() {
        return runTimeConf;
    }

    public TAtomDataSource getAtomDataSource() {
        return atomDataSource;
    }

    /**
     * 根据开关决定是否严格遵循minPoolSize的设置
     */
    private static int adjustMinPoolSize(TAtomDsConfDO dsConfDO) {
        int minPoolSize = dsConfDO.getMinPoolSize();
        if (!dsConfDO.getStrictKeepAlive()) {
            if (minPoolSize > 5) {
                minPoolSize = 5;
            }
        }
        return minPoolSize;
    }

}
