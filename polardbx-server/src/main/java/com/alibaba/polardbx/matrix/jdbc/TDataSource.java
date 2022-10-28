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

package com.alibaba.polardbx.matrix.jdbc;

import com.alibaba.polardbx.atom.TAtomDataSource;
import com.alibaba.polardbx.common.IdGenerator;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.logger.LoggerInit;
import com.alibaba.polardbx.common.logical.ITConnection;
import com.alibaba.polardbx.common.logical.ITDataSource;
import com.alibaba.polardbx.common.logger.LoggerInit;
import com.alibaba.polardbx.common.model.Group;
import com.alibaba.polardbx.common.model.Group.GroupType;
import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.thread.ServerThreadPool;
import com.alibaba.polardbx.common.utils.timezone.InternalTimeZone;
import com.alibaba.polardbx.common.utils.timezone.TimeZoneUtils;
import com.alibaba.polardbx.common.utils.version.Version;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.PlanExecutor;
import com.alibaba.polardbx.executor.common.RecycleBin;
import com.alibaba.polardbx.executor.common.RecycleBinManager;
import com.alibaba.polardbx.gms.config.InstConfigReceiver;
import com.alibaba.polardbx.gms.config.impl.MetaDbInstConfigManager;
import com.alibaba.polardbx.group.jdbc.TGroupDataSource;
import com.alibaba.polardbx.matrix.config.MatrixConfigHolder;
import com.alibaba.polardbx.optimizer.config.server.DefaultServerConfigManager;
import com.alibaba.polardbx.optimizer.config.server.IServerConfigManager;
import com.alibaba.polardbx.optimizer.statis.SQLRecorder;
import com.alibaba.polardbx.optimizer.tablegroup.TableGroupVersionManager;
import com.alibaba.polardbx.optimizer.utils.OptimizerHelper;
import com.alibaba.polardbx.optimizer.utils.SchemaVersionManager;
import com.alibaba.polardbx.repo.mysql.spi.MyRepository;
import com.alibaba.polardbx.rule.TddlRule;
import com.alibaba.polardbx.stats.MatrixStatistics;
import com.alibaba.polardbx.transaction.TransactionManager;
import com.alibaba.polardbx.transaction.utils.ParamValidationUtils;
import org.apache.commons.lang.StringUtils;

import javax.sql.DataSource;
import java.io.ByteArrayInputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * matrix的jdbc datasource实现
 *
 * @author mengshi.sunmengshi 2013-11-22 下午3:26:14
 * @since 5.0.0
 */
public class TDataSource extends AbstractLifecycle implements ITDataSource {

    public final static Logger logger = LoggerFactory.getLogger(TDataSource.class);
    private boolean sharding = true;
    // 是否不做sharding,如果为false跳过rule初始化
    private String appName = null;
    private String schemaName = null;
    private String unitName = null;
    private PlanExecutor executor = null;
    private Map<String, Object> connectionProperties = new HashMap<>(2);
    private MatrixConfigHolder configHolder;
    private TddlRule tddlRule = null;
    /**
     * 用于并行查询的线程池，全局共享
     */
    private ServerThreadPool globalExecutorService = null;
    private boolean shareGlobalExecutor = false;


    // 写入模式，取值: center/unit (如果是center，并且当前是unit环境，则不启动tddl)
    private String writeMode = null;
    private boolean stressTestValid = false;
    private SQLRecorder physicalRecorder = null;
    private SQLRecorder recorder = null;

    private MatrixStatistics statistics = new MatrixStatistics();

    private IServerConfigManager serverConfigManager = null;

    public static String globalConnectionProperties = null;

    private InternalTimeZone logicalDbTimeZone = null;
    /**
     * Id Generator for internal sql execution
     */
    private IdGenerator traceIdGenerator = null;

    private boolean isDefaultDb = false;

    private boolean destroyed = false;

    public TDataSource() {
        this.useTryLock = true;
    }

    @Override
    public boolean isInited() {
        return isInited;
    }

    @Override
    public void doInit() {
        if (destroyed) {
            logger.warn("TDataSource is destroyed, here forbid init the " + appName + "!");
            throw new RuntimeException("TDataSource is destroyed!");
        }
        LoggerInit.TDDL_DYNAMIC_CONFIG.info("--------------");
        LoggerInit.TDDL_DYNAMIC_CONFIG.info("TDataSource start init");
        LoggerInit.TDDL_DYNAMIC_CONFIG.info("appName is: " + appName);
        LoggerInit.TDDL_DYNAMIC_CONFIG.info("unitName is: " + unitName);
        LoggerInit.TDDL_DYNAMIC_CONFIG.info("writeMode is: " + this.writeMode);

        this.traceIdGenerator = IdGenerator.getIdGenerator();

        loadConnectionProperties();
        if (!GeneralUtil
            .getPropertyBoolean(this.connectionProperties, ConnectionProperties.ENABLE_VERSION_CHECK, true)) {
            System.setProperty("tddl.version.check", "false");
        }

        TableGroupVersionManager.segmentLockSize = GeneralUtil.getPropertyInt(
            this.connectionProperties,
            ConnectionProperties.TG_MDL_SEGMENT_SIZE,
            TableGroupVersionManager.segmentLockSize
        );

        SchemaVersionManager.segmentLockSize = GeneralUtil.getPropertyInt(
            this.connectionProperties,
            ConnectionProperties.DB_MDL_SEGMENT_SIZE,
            SchemaVersionManager.segmentLockSize
        );

        Version.checkVersion();
        this.executor = new PlanExecutor();
        executor.init();

        if (serverConfigManager == null) {
            serverConfigManager = new DefaultServerConfigManager(this);
        }
        OptimizerHelper.init(this.serverConfigManager);

        MatrixConfigHolder configHolder = new MatrixConfigHolder();
        this.configHolder = configHolder;

        // if time zone config of shard router from config center is valid, set it as local time
        // zone, Otherwise set system time zone as local time.
        String shardRouterTimeZoneConfig = GeneralUtil.getPropertyString(this.connectionProperties,
            ConnectionProperties.SHARD_ROUTER_TIME_ZONE,
            "null").replace("\"", "");
        if (!this.configHolder.setShardRouterTimeZoneSuccess(shardRouterTimeZoneConfig)) {
            this.configHolder.setShardRouterDefaultTimeZone(InternalTimeZone.defaultTimeZone);
        }

        configHolder.setDataSource(this);
        configHolder.setAppName(appName);
        configHolder.setUnitName(unitName);
        configHolder.setSchemaName(schemaName);
        configHolder.setSharding(this.sharding);
        configHolder.setTddlRule(this.tddlRule);
        configHolder.setStatistics(this.statistics);
        configHolder.setServerConfigManager(serverConfigManager);
        configHolder.init();
        this.configHolder = configHolder;
        afterInitConfigHolder();

        if (physicalRecorder == null) {
            physicalRecorder = new SQLRecorder(100);
        }

        if (recorder == null) {
            recorder = new SQLRecorder(100);
        }

        try {
            RecycleBinManager.instance.initApp(appName, schemaName, this, this.getConnectionProperties());
        } catch (Throwable e) {
            logger.error("init recyclebin error," + appName + ":" + appName);
        }

        MatrixStatistics.setApp(schemaName, appName);
        if (!ConfigDataMode.isFastMock()) {
            for (TGroupDataSource gds : this.getGroupDataSources()) {
                for (TAtomDataSource ads : gds.getAtomDataSources()) {
                    MatrixStatistics.setAtom(appName,
                        gds.getDbGroupKey(),
                        ads.getDbKey(),
                        ads.getHost() + ":" + ads.getPort());
                }
            }
        }
    }

    private void afterInitConfigHolder() {
        putBloomFilterProperties();

    }

    /**
     * Set by storage info whether supports bloom filter
     */
    private void putBloomFilterProperties() {
        boolean storageSupportsBloomFilter = configHolder.getStorageInfoManager().supportsBloomFilter();
        connectionProperties.put(ConnectionProperties.STORAGE_SUPPORTS_BLOOM_FILTER, storageSupportsBloomFilter);

        if (storageSupportsBloomFilter) {
            boolean storageSupportsXxHash = configHolder.getStorageInfoManager().supportsXxHash();
            connectionProperties.put(ConnectionProperties.ENABLE_RUNTIME_FILTER_XXHASH, storageSupportsXxHash);
        }
    }

    private void loadConnectionProperties() {
        String data;
        if (ConfigDataMode.isFastMock()) {
            Properties prop = new Properties();
            if (this.connectionProperties != null && !this.connectionProperties.isEmpty()) {
                prop.putAll(this.connectionProperties);
            }
            data = prop.toString();
            parseConnectionProperties(data, "");
        } else {
            // To be load by MetaDB
            Properties prop = new Properties();
            if (this.connectionProperties != null && !this.connectionProperties.isEmpty()) {
                prop.putAll(this.connectionProperties);
            }

            MetaDbInstConfigManager.getInstance().registerDbReceiver(this.schemaName, new InstConfigReceiver() {
                @Override
                public void apply(Properties props) {
                    applyConnectionProperties(props);
                }
            });
        }

    }

    public synchronized void parseConnectionProperties(String data, String globalData) {

        LoggerInit.TDDL_DYNAMIC_CONFIG.info("Connection Properties init");
        LoggerInit.TDDL_DYNAMIC_CONFIG.info("appName is: " + appName);
        LoggerInit.TDDL_DYNAMIC_CONFIG.info("unitName is: " + unitName);
        LoggerInit.TDDL_DYNAMIC_CONFIG.info("data: " + data + ", global data:" + globalData);

        /**
         * 默认打开开关
         */
        System.setProperty(ConnectionProperties.HINT_PARSER_FLAG, "true");

        if (TStringUtil.isEmpty(data)) {
            logger.info("connection properties data is null for the app" + appName);
            return;
        }

        /**
         * 全局配置做底，APPNAME级别配置覆盖上去
         */
        Properties globalP = new Properties();
        try {
            globalP.load(new ByteArrayInputStream(globalData.getBytes("utf8")));
        } catch (Throwable e) {
            logger.error("error when parse global connection properties", e);
            throw GeneralUtil.nestedException(e);
        }

        Properties p = new Properties();
        try {
            p.load(new ByteArrayInputStream(data.getBytes("utf8")));
        } catch (Throwable e) {
            logger.error("error when parse connection properties", e);
            throw GeneralUtil.nestedException(e);
        }

        if (p.isEmpty()) {
            return;
        }

        globalP.putAll(p);// APPNAME配置为准，覆盖掉全局配置
        applyConnectionProperties(globalP);
    }

    public synchronized void applyConnectionProperties(Properties dbProperties) {

        Properties globalP = dbProperties;

        if (this.connectionProperties == null) {
            this.connectionProperties = new HashMap<>();
        }

        // 还是允许覆盖好了
        for (Object key : globalP.keySet()) {
            if (key == null) {
                continue;
            }
            this.putConnectionProperties(key.toString(), globalP.getProperty(key.toString()));
        }

        if (configHolder != null && configHolder.isInited()) {
            // Reload connection properties only after Sequence Manager
            // has been initialized along with MatrixConfigHolder.
            configHolder.getExecutorContext().getSequenceManager().reloadConnProps(schemaName, connectionProperties);
        }

        /**
         * 是否开启HINT PARSER 模式识别HINT
         */
        if (GeneralUtil.getPropertyBoolean(this.connectionProperties, ConnectionProperties.HINT_PARSER_FLAG, true)) {
            System.setProperty(ConnectionProperties.HINT_PARSER_FLAG, "true");
        } else {
            System.setProperty(ConnectionProperties.HINT_PARSER_FLAG, "false");
        }

        // Dynamic set the logical db default timezone
        // if time zone config from config center is valid, set it as local time
        // zone and put it into server variables.
        // Otherwise set system time zone as local time.
        String logicalDbTimeZoneConfig = GeneralUtil.getPropertyString(this.connectionProperties,
            ConnectionProperties.LOGICAL_DB_TIME_ZONE,
            "null").replace("\"", "");
        if (TStringUtil.isEmpty(logicalDbTimeZoneConfig) || "null".equals(logicalDbTimeZoneConfig)) {
            logicalDbTimeZone = InternalTimeZone.defaultTimeZone;
        } else {
            InternalTimeZone tz = TimeZoneUtils.convertFromMySqlTZ(logicalDbTimeZoneConfig);
            if (tz != null) {
                logicalDbTimeZone = tz;
            } else {
                logicalDbTimeZone = InternalTimeZone.defaultTimeZone;
            }
        }

        // pass to recyclebin
        RecycleBin recycleBin = RecycleBinManager.instance.getByAppName(appName);
        if (recycleBin != null) {
            recycleBin.setCmds(this.getConnectionProperties());
        }

        // Reset all timer tasks.
        if (ParamValidationUtils.isAnyTimerTaskParam(globalP)) {
            TransactionManager tm = TransactionManager.getInstance(schemaName);
            if (null != tm && tm.isInited()) {
                tm.resetAllTimerTasks();
            }
        }

        logger.info("load connection properties ok");
        logger.info(String.valueOf(this.connectionProperties));
        LoggerInit.TDDL_DYNAMIC_CONFIG.info("load connection properties ok");
        LoggerInit.TDDL_DYNAMIC_CONFIG.info(String.valueOf(this.connectionProperties));
    }

    @Override
    public ITConnection getConnection() throws SQLException {
        try {
            if (!isInited()) {
                init();
            }

            return new TConnection(this);
        } catch (Exception e) {
            throw new SQLException(e);

        }
    }

    public ServerThreadPool borrowExecutorService() {
        return globalExecutorService;
    }

    public void releaseExecutorService(ServerThreadPool executor) {
        // ignore
    }

    @Override
    public void doDestroy() {
        if (destroyed) {
            logger.warn(String.format("TDataSource:%s is destroyed, so ignore now!!!", appName));
            return;
        }
        LoggerInit.TDDL_DYNAMIC_CONFIG.info("--------------");
        LoggerInit.TDDL_DYNAMIC_CONFIG.info("TDataSource stop");
        LoggerInit.TDDL_DYNAMIC_CONFIG.info("appName is: " + appName);
        LoggerInit.TDDL_DYNAMIC_CONFIG.info("unitName is: " + unitName);

        if (!shareGlobalExecutor && globalExecutorService != null) {
            globalExecutorService.shutdownNow();
        }

        if (configHolder != null) {
            configHolder.destroy();
        }

        if (executor != null && executor.isInited()) {
            executor.destroy();
        }

        MetaDbInstConfigManager.getInstance().unRegisterDbReceiver(this.schemaName);

        if (statistics != null) {
            MatrixStatistics.removeSchema(statistics, appName);
        }

        if (traceIdGenerator != null) {
            IdGenerator.remove(traceIdGenerator);
        }

        RecycleBinManager.instance.destroyApp(appName);
        TransactionManager.removeSchema(schemaName);
    }

    public List<TGroupDataSource> getGroupDataSources() {
        MyRepository repo = (MyRepository) this.getConfigHolder()
            .getExecutorContext()
            .getRepositoryHolder()
            .get(GroupType.MYSQL_JDBC.toString());
        List<TGroupDataSource> groupDataSources = new ArrayList();

        for (Group group : this.getConfigHolder().getMatrix().getGroups()) {
            if (GroupType.MYSQL_JDBC.equals(group.getType())) {
                TGroupDataSource ds = repo.getDataSource(group.getName());
                if (ds != null) {
                    groupDataSources.add(ds);
                }
            }
        }

        return groupDataSources;
    }

    public PlanExecutor getExecutor() {
        return this.executor;
    }

    public Map<String, Object> getConnectionProperties() {
        return this.connectionProperties;
    }

    public void setConnectionProperties(Map<String, Object> cp) {
        this.connectionProperties = cp;
    }

    public void putConnectionProperties(String name, Object value) {
        if (this.connectionProperties == null) {
            this.connectionProperties = new HashMap<String, Object>();
        }

        this.connectionProperties.put(name, value);
    }

    public void setAppName(String appName) {
        if (appName != null) {
            appName = appName.trim();
        }
        this.appName = appName;
    }

    public void setSchemaName(String schemaName) {
        if (schemaName != null) {
            schemaName = schemaName.trim();
        }
        this.schemaName = schemaName;
    }

    public String getSchemaName() {
        if (StringUtils.isEmpty(schemaName)) {
            this.schemaName = appName;
        }

        return schemaName;
    }

    public MatrixConfigHolder getConfigHolder() {
        return this.configHolder;
    }

    public String getUnitName() {
        return unitName;
    }

    public void setUnitName(String unitName) {
        this.unitName = unitName;
    }

    public boolean isSharding() {
        return sharding;
    }

    public void setSharding(boolean sharding) {
        this.sharding = sharding;
    }

    public void setWriteMode(String writeMode) {
        this.writeMode = writeMode;
    }

    public String getWriteMode() {
        return writeMode;
    }

    public String getAppName() {
        return appName;
    }

    public void setGlobalExecutorService(ServerThreadPool globalExecutorService) {
        this.globalExecutorService = globalExecutorService;
        this.shareGlobalExecutor = true;
    }

    public SQLRecorder getPhysicalRecorder() {
        return this.physicalRecorder;

    }

    public void setPhysicalRecorder(SQLRecorder recorder) {
        this.physicalRecorder = recorder;
    }

    public boolean isStressTestValid() {
        return stressTestValid;
    }

    public MatrixStatistics getStatistics() {
        return statistics;
    }

    public void setStatistics(MatrixStatistics statistics) {
        this.statistics = statistics;
    }

    @Override
    protected String getNotAvailableErrorMsg() {
        return "DataSource" + " is not available, please check and try again later, AppName is:" + this.appName;
    }

    public SQLRecorder getRecorder() {
        return recorder;
    }

    public void setRecorder(SQLRecorder recorder) {
        this.recorder = recorder;
    }

    public IServerConfigManager getServerConfigManager() {
        return serverConfigManager;
    }

    public void setServerConfigManager(IServerConfigManager serverConfigManager) {
        this.serverConfigManager = serverConfigManager;
    }

    public InternalTimeZone getLogicalDbTimeZone() {
        return logicalDbTimeZone;
    }

    public void setTraceIdGenerator(IdGenerator traceIdGenerator) {
        IdGenerator oldIdGenerator = this.traceIdGenerator;
        this.traceIdGenerator = traceIdGenerator;
        if (oldIdGenerator != null && !oldIdGenerator.equals(traceIdGenerator)) {
            IdGenerator.remove(oldIdGenerator);
        }
    }

    public boolean isDefaultDb() {
        return isDefaultDb;
    }

    public void setDefaultDb(boolean defaultDb) {
        isDefaultDb = defaultDb;
    }

    @Override
    public void destroy() {
        if (!destroyed) {
            super.destroy();
            destroyed = true;
        }
    }
}
