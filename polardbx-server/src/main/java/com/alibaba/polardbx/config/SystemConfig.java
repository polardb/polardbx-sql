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

package com.alibaba.polardbx.config;

import com.alibaba.polardbx.CobarServer;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.privilege.PasswdRuleConfig;
import com.alibaba.polardbx.common.properties.MppConfig;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.convertor.Convertor;
import com.alibaba.polardbx.common.utils.convertor.ConvertorHelper;
import com.alibaba.polardbx.common.utils.thread.ThreadCpuStatUtil;
import com.alibaba.polardbx.common.utils.version.InstanceVersion;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import com.alibaba.polardbx.instance.InstanceType;
import com.alibaba.polardbx.optimizer.memory.MemorySetting;
import com.taobao.tddl.common.utils.TddlToStringStyle;
import org.apache.commons.lang.ObjectUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.ToStringBuilder;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

/**
 * 系统基础配置项
 *
 * @author xianmao.hexm 2011-1-11 下午02:14:04
 */
public final class SystemConfig {

    private static final int DEFAULT_PORT = 8507;
    private static final int DEFAULT_PROCESSOR = ThreadCpuStatUtil.NUM_CORES;

    private String propertyFile = "server.properties";

    @Immutable
    private int serverPort = DEFAULT_PORT;
    @Immutable
    private int managerPort = DEFAULT_PORT + 100;
    @Immutable
    private int rpcPort = 9090;
    @Immutable
    private int metaDbXprotoPort = 0; // Default enable.
    @Immutable
    private int storageDbXprotoPort = 0; // Default enable.
    @Immutable
    private volatile int processors = DEFAULT_PROCESSOR;
    @Immutable
    private volatile int parserCommentVersion = 50148;

    private volatile long idleTimeout = 8 * 3600 * 1000L;
    private volatile String charset = "UTF-8";
    @Deprecated
    private volatile int processorHandler = DEFAULT_PROCESSOR;
    private volatile int processorKillExecutor = DEFAULT_PROCESSOR;
    private volatile int timerExecutor = DEFAULT_PROCESSOR;
    private volatile int serverExecutor = DEFAULT_PROCESSOR * processors;
    private volatile int managerExecutor = DEFAULT_PROCESSOR;

    private volatile int timerTaskExecutor = DEFAULT_PROCESSOR;
    private volatile int metaDbExecutor = DEFAULT_PROCESSOR;
    private volatile long processorCheckPeriod = 15 * 1000L;

    private volatile int sqlRecordCount = 100;

    // corona集群名称
    private volatile String clusterName = null;
    private volatile String unitName = null;
    private volatile String instanceId = null;
    private volatile String instanceType = null;
    private volatile String masterInstanceId = null;
    // 信任的ip子网列表
    private volatile String trustedIps = null;
    // 全局黑名单,不允许访问任何DRDS DB,防攻击
    private volatile String blackIps = null;
    private volatile String whiteIps = null;
    private volatile long slowSqlTime = 1000;
    // slow log记录sql的最大长度
    private volatile int slowSqlSizeThresold = 16384;
    private volatile int deadLockCheckPeriod = 5 * 100;
    // manager统计采集周期
    private volatile int maxConnection = 20000;
    // 是否开启manager端口的用户登录,临时兼容下老的系统监控,以后会关闭用户登录manager端口
    private volatile int allowManagerLogin = 1;

    // 是不是内部corona的模式，提供端口号和ip，向manager获取clustername
    private volatile int coronaMode = 0;

    // 最大处理的package大小
    private int maxAllowedPacket = 16 * 1024 * 1024;

    private int socketRecvBuffer = 32 * 1024;
    private int socketSendBuffer = 64 * 1024;

    /**
     * 是否需要将在Calcite上执行异常的SQL在老Server的逻辑上进行重试，默认是打开，在随机SQL测试时要关闭
     */
    protected boolean retryErrorSqlOnOldServer = true;

    private volatile boolean sslEnable;

    private String versionPrefix = InstanceVersion.getVersion();

    /**
     * in vpc ,provide vpc Id
     */
    @Immutable
    private String vpcId = "";

    /**
     * 是否允许进行跨单元查询
     */
    private boolean allowCrossDbQuery = false;

    private volatile int sqlSimpleMaxLen = 16000;

    private volatile long globalMemoryLimit = getDefaultMemoryLimit();

    /**
     * 是否打开MPP Server，默认关闭
     */
    private boolean enableMppServer = false;

    /**
     * 是否打开MPP Worker功能，默认关闭
     */
    private boolean enableMppWorker = false;

    /**
     * 针对SQL使用走MPP路由，默认不走，通过hint方式开启
     */
    private boolean enableMpp = false;

    /**
     * 默认使用保留一个备库的备库路由策略， RESERVE_ONE_SLAVE
     */
    private String apHaMode = "RESERVE_ONE_SLAVE";

    /**
     * 默认情况下只是采集配置了路由规则的表统计信息
     */
    private boolean enableCollectorAllTables = false;

    /**
     * 开启密码规则验证，实例级别可配置
     */
    private volatile PasswdRuleConfig passwordRuleConfig;

    /**
     * 是否开启根据内存自动kill功能，实例级别配置项
     */
    private boolean enableKill = false;

    private boolean enableApLimitRate = false;

    /**
     * 是否针对executor启用分桶模式
     */
    @Immutable
    private boolean enableBucketExecutor = true;

    /**
     * The addr info of metaDB
     * <p>
     * <pre>
     * metaDbAddr=ip1:port1,ip2:port2,ip3:port3
     *
     * If metaDbAddr has only one addr, then it will be treated as leader;
     *
     * If metaDbAddr has 3 addr, then it will check and fetch leader info automatically
     *
     * </pre>
     */
    private String metaDbAddr = "";

    /**
     * The dbName of metaDB
     */
    private String metaDbName = MetaDbDataSource.DEFAULT_META_DB_NAME;

    /**
     * The connection properties of metaDB
     */
    private String metaDbProp = MetaDbDataSource.DEFAULT_META_DB_PROPS;

    /**
     * The User of MetaDB
     */
    private String metaDbUser = "";

    /**
     * The encode passed of MetaDB
     */
    private String metaDbPasswd = "";

    /**
     * enable scaleout feature or not
     */
    private boolean enableScaleout = false;

    /**
     * Enable maintain primary-zone: automatic switch leader to primary-zone
     */
    private boolean enablePrimaryZoneMaintain = false;

    /**
     * Enable background balancer
     */
    private boolean enableBalancer = false;

    /**
     * Running time window of balancer
     */
    private String balanceWindow;

    private boolean dropOldDataBaseAfterSwitchDataSource = true;

    /**
     * specify the default phy db count each storage inst
     */
    private int shardDbCountEachStorageInst = DbTopologyManager.DEFAULT_SHARD_DB_COUNT_EACH_STORAGE_INST;

    /**
     * the default partition mode of create database
     */
    private String defaultPartitionMode = "drds";

    /**
     * enable to forbid push dml with hint
     */
    private boolean enableForbidPushDmlWithHint = true;

    /**
     * Some special settings that are forbidden externally while are allowed internally for test.
     */
    private boolean supportSingleDbMultiTbs = false;
    private boolean supportDropAutoSeq = false;
    private boolean allowSimpleSequence = false;

    /**
     * for test
     */
    private String workloadType;
    private boolean enableRemoteRPC;
    private boolean enableMasterMpp;

    /**
     * ccl reschedule timeout period
     */
    private int cclRescheduleTimeoutCheckPeriod = 5;
    private int processCclTriggerPeriod = 2;

    /**
     * Only used when initializing cluster
     */
    private boolean initializeGms;
    private boolean forceCleanup = false;
    private String dataNodeList;
    private String metaDbRootUser = "root";
    private String metaDbRootPasswd = "";
    private String polarxRootUser = "polardbx_root";
    private String polarxRootPasswd = "";

    /**
     * logical-db-warmming-up-executor pool size
     */
    private int logicalDbWarmmingUpExecutorPoolSize = 4;

    /**
     * enable auto warm up for logical db
     */
    private boolean enableLogicalDbWarmmingUp;

    public static long getDefaultMemoryLimit() {
        // By default reserve 3GB space for general use, unless the remaining
        // space is less than 1GB
        return Math.max(Runtime.getRuntime().maxMemory() - (3L << 30), 1L << 30);
    }

    public static long getMaxMemoryLimit() {
        long maxMem = Runtime.getRuntime().maxMemory();
        return Math.max(maxMem - (2L << 30), Math.round(maxMem * MemorySetting.DEFAULT_GLOBAL_POOL_PROPORTION));
    }

    public void setConfig(String key, String value) {
        try {
            Field field = this.getClass().getDeclaredField(key);
            if (!field.isAccessible()) {
                field.setAccessible(true);
            }

            Immutable o = field.getAnnotation(Immutable.class);
            if (o != null) {
                throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, "immutable variable : " + key);
            }

            Class clazz = field.getType();
            Convertor convertor = ConvertorHelper.getInstance().getConvertor(String.class, clazz);
            if (clazz == String.class) {
                field.set(this, value);
            } else {
                field.set(this, convertor.convert(value, clazz));
            }
            CobarServer.getInstance().reloadSystemConfig();
            // 复制配置到system参数中
            System.setProperty(key, value);
        } catch (NoSuchFieldException e) {
            if (key.equals("session.sql_mode")) {
                return;
            }
            throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, "unknow variable : " + key);
        } catch (IllegalArgumentException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, "illegal argument : " + value);
        } catch (IllegalAccessException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, "unknow variable : " + key);
        }
    }

    public void setInstanceProp(Properties p) {
        try {
            Field[] fields = this.getClass().getDeclaredFields();
            for (Field field : fields) {
                if (Modifier.isStatic(field.getModifiers())) {
                    continue;
                }
                if (!field.isAccessible()) {
                    field.setAccessible(true);
                }
                Immutable o = field.getAnnotation(Immutable.class);
                if (o != null) {
                    continue;
                }

                String name = field.getName();
                if (p.containsKey(name)) {
                    String value = p.getProperty(name);
                    Class clazz = field.getType();
                    Convertor convertor = ConvertorHelper.getInstance().getConvertor(String.class, clazz);
                    if (clazz == String.class) {
                        field.set(this, value);
                    } else {
                        field.set(this, convertor.convert(value, clazz));
                    }

                    // 复制配置到system参数中
                    System.setProperty(name, value);
                }
            }
        } catch (Throwable e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public Map<String, String> toMapValues() {
        Map<String, String> values = new TreeMap<String, String>();
        try {
            Field[] fields = this.getClass().getDeclaredFields();
            for (Field field : fields) {
                if (Modifier.isStatic(field.getModifiers())) {
                    continue;
                }

                if (!field.isAccessible()) {
                    field.setAccessible(true);
                }

                String name = field.getName();
                Object value = field.get(this);
                Class clazz = field.getType();
                if (clazz == String.class) {
                    values.put(name, ObjectUtils.toString(value, "NULL"));
                } else {
                    Convertor convertor = ConvertorHelper.getInstance().getConvertor(clazz, String.class);
                    Object strvalue = convertor.convert(value, String.class);
                    values.put(name, ObjectUtils.toString(strvalue, "NULL"));
                }
            }

            if (enableMppServer || enableMppWorker) {
                Field[] mppFields = MppConfig.getInstance().getClass().getDeclaredFields();
                for (Field field : mppFields) {
                    if (Modifier.isStatic(field.getModifiers())) {
                        continue;
                    }

                    if (!field.isAccessible()) {
                        field.setAccessible(true);
                    }

                    String name = "mpp_" + field.getName();
                    Object value = field.get(MppConfig.getInstance());
                    Class clazz = field.getType();
                    if (clazz == String.class) {
                        values.put(name, ObjectUtils.toString(value, "NULL"));
                    } else {
                        Convertor convertor = ConvertorHelper.getInstance().getConvertor(clazz, String.class);
                        Object strvalue = convertor.convert(value, String.class);
                        values.put(name, ObjectUtils.toString(strvalue, "NULL"));
                    }
                }
            }

        } catch (Throwable e) {
            throw GeneralUtil.nestedException(e);
        }

        return values;
    }

    // ================= setter / getter =====================

    public String getCharset() {
        return charset;
    }

    public void setCharset(String charset) {
        this.charset = charset;
    }

    public int getServerPort() {
        return serverPort;
    }

    public void setServerPort(int serverPort) {
        this.serverPort = serverPort;
    }

    public int getManagerPort() {
        return managerPort;
    }

    public void setManagerPort(int managerPort) {
        this.managerPort = managerPort;
    }

    public int getRpcPort() {
        return rpcPort;
    }

    public void setRpcPort(int rpcPort) {
        this.rpcPort = rpcPort;
    }

    public int getMetaDbXprotoPort() {
        return metaDbXprotoPort;
    }

    public void setMetaDbXprotoPort(int metaDbXprotoPort) {
        this.metaDbXprotoPort = metaDbXprotoPort;
    }

    public int getStorageDbXprotoPort() {
        return storageDbXprotoPort;
    }

    public void setStorageDbXprotoPort(int storageDbXprotoPort) {
        this.storageDbXprotoPort = storageDbXprotoPort;
    }

    public int getProcessors() {
        return processors;
    }

    public void setProcessors(int processors) {
        this.processors = processors;
    }

    public int getProcessorHandler() {
        return processorHandler;
    }

    public void setProcessorHandler(int processorExecutor) {
        this.processorHandler = processorExecutor;
    }

    public int getServerExecutor() {
        return serverExecutor;
    }

    public void setServerExecutor(int serverExecutor) {
        this.serverExecutor = serverExecutor;
    }

    public int getProcessorKillExecutor() {
        return processorKillExecutor;
    }

    public void setProcessorKillExecutor(int processorKillExecutor) {
        this.processorKillExecutor = processorKillExecutor;
    }

    public int getManagerExecutor() {
        return managerExecutor;
    }

    public void setManagerExecutor(int managerExecutor) {
        this.managerExecutor = managerExecutor;
    }

    public int getTimerExecutor() {
        return timerExecutor;
    }

    public void setTimerExecutor(int timerExecutor) {
        this.timerExecutor = timerExecutor;
    }

    public long getIdleTimeout() {
        return idleTimeout;
    }

    public void setIdleTimeout(long idleTimeout) {
        this.idleTimeout = idleTimeout;
    }

    public long getProcessorCheckPeriod() {
        return processorCheckPeriod;
    }

    public void setProcessorCheckPeriod(long processorCheckPeriod) {
        this.processorCheckPeriod = processorCheckPeriod;
    }

    public int getParserCommentVersion() {
        return parserCommentVersion;
    }

    public void setParserCommentVersion(int parserCommentVersion) {
        this.parserCommentVersion = parserCommentVersion;
    }

    public int getSqlRecordCount() {
        return sqlRecordCount;
    }

    public void setSqlRecordCount(int sqlRecordCount) {
        this.sqlRecordCount = sqlRecordCount;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public String getTrustedIps() {
        return trustedIps;
    }

    public void setTrustedIps(String trustedIps) {
        this.trustedIps = trustedIps;
    }

    public String getBlackIps() {
        return blackIps;
    }

    public void setBlackIps(String blackIps) {
        this.blackIps = blackIps;
    }

    public String getWhiteIps() {
        return whiteIps;
    }

    public void setWhiteIps(String whiteIps) {
        this.whiteIps = whiteIps;
    }

    public long getSlowSqlTime() {
        return slowSqlTime;
    }

    public void setSlowSqlTime(long slowSqlTime) {
        this.slowSqlTime = slowSqlTime;
    }

    public int getDeadLockCheckPeriod() {
        return deadLockCheckPeriod;
    }

    public void setDeadLockCheckPeriod(int deadLockCheckPeriod) {
        this.deadLockCheckPeriod = deadLockCheckPeriod;
    }

    public String getUnitName() {
        return unitName;
    }

    public void setUnitName(String unitName) {
        this.unitName = unitName;
    }

    public int getSlowSqlSizeThresold() {
        return slowSqlSizeThresold;
    }

    public void setSlowSqlSizeThresold(int slowSqlSizeThresold) {
        this.slowSqlSizeThresold = slowSqlSizeThresold;
    }

    public int getMaxConnection() {
        return maxConnection;
    }

    public void setMaxConnection(int maxConnection) {
        this.maxConnection = maxConnection;
    }

    public int getAllowManagerLogin() {
        return allowManagerLogin;
    }

    public void setAllowManagerLogin(int allowManagerLogin) {
        this.allowManagerLogin = allowManagerLogin;
    }

    public String getInstanceId() {
        return instanceId;
    }

    public void setInstanceId(String instanceId) {
        this.instanceId = instanceId;
        this.clusterName = null;
        this.unitName = null;

        // Pass the instanceId to GroupDataSource By System.properties
        // In PolarDb-X mode tddl-gms will getInstId from System.properties
        System.setProperty("instanceId", instanceId);
    }

    public boolean isPublicInstance() {
        return InstanceType.parse(instanceType) == InstanceType.PUBLIC;
    }

    public String getInstanceType() {
        return instanceType;
    }

    public void setInstanceType(String instanceType) {
        this.instanceType = instanceType;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, TddlToStringStyle.DEFAULT_STYLE);
    }

    public int getCoronaMode() {
        return coronaMode;
    }

    public void setCoronaMode(int coronaMode) {
        this.coronaMode = coronaMode;
    }

    public int getMaxAllowedPacket() {
        return maxAllowedPacket;
    }

    public void setMaxAllowedPacket(int maxAllowedPacket) {
        this.maxAllowedPacket = maxAllowedPacket;
    }

    public int getSocketRecvBuffer() {
        return socketRecvBuffer;
    }

    public void setSocketRecvBuffer(int socketRecvBuffer) {
        this.socketRecvBuffer = socketRecvBuffer;
    }

    public int getSocketSendBuffer() {
        return socketSendBuffer;
    }

    public void setSocketSendBuffer(int socketSendBuffer) {
        this.socketSendBuffer = socketSendBuffer;
    }

    public String getVersionPrefix() {
        return versionPrefix;
    }

    public void setVersionPrefix(String versionPrefix) {
        this.versionPrefix = versionPrefix;
    }

    public boolean isRetryErrorSqlOnOldServer() {
        return retryErrorSqlOnOldServer;
    }

    public void setRetryErrorSqlOnOldServer(boolean retryErrorSqlOnOldServer) {
        this.retryErrorSqlOnOldServer = retryErrorSqlOnOldServer;
    }

    public boolean isSslEnable() {
        return sslEnable;
    }

    public void setSslEnbale(boolean sslEnable) {
        this.sslEnable = sslEnable;
    }

    public String getVpcId() {
        return vpcId;
    }

    public void setVpcId(String vpcId) {
        this.vpcId = vpcId;
    }

    public boolean isAllowCrossDbQuery() {
        return allowCrossDbQuery;
    }

    public void setAllowCrossDbQuery(boolean allowCrossDbQuery) {
        this.allowCrossDbQuery = allowCrossDbQuery;
    }

    public int getSqlSimpleMaxLen() {
        return sqlSimpleMaxLen;
    }

    public void setSqlSimpleMaxLen(int sqlSimpleMaxLen) {
        this.sqlSimpleMaxLen = sqlSimpleMaxLen;
    }

    public long getGlobalMemoryLimit() {
        return globalMemoryLimit;
    }

    public void setGlobalMemoryLimit(long globalMemoryLimit) {
        this.globalMemoryLimit = globalMemoryLimit;
    }

    public boolean isMppServer() {
        return enableMppServer;
    }

    public void enableMppServer(boolean enableMppServer) {
        this.enableMppServer = enableMppServer;
    }

    public boolean isMppWorker() {
        return enableMppWorker;
    }

    public void enableMppWorker(boolean enableMppWorker) {
        this.enableMppWorker = enableMppWorker;
    }

    public boolean isEnableMpp() {
        return enableMpp;
    }

    public void setEnableMpp(boolean enableMpp) {
        this.enableMpp = enableMpp;
    }

    public boolean isEnableBucketExecutor() {
        return enableBucketExecutor;
    }

    public void setEnableBucketExecutor(boolean enableBucketExecutor) {
        this.enableBucketExecutor = enableBucketExecutor;
    }

    public boolean isEnableCollectorAllTables() {
        return enableCollectorAllTables;
    }

    public void setEnableCollectorAllTables(boolean enableCollectorAllTables) {
        this.enableCollectorAllTables = enableCollectorAllTables;
    }

    public boolean isEnableSpill() {
        return MemorySetting.ENABLE_SPILL;
    }

    public void setEnableSpill(boolean enableSpill) {
        MemorySetting.ENABLE_SPILL = enableSpill;
    }

    public boolean isEnableKill() {
        return enableKill;
    }

    public void setEnableKill(boolean enableKill) {
        this.enableKill = enableKill;
        MemorySetting.ENABLE_KILL = enableKill;
    }

    public boolean isEnableApLimitRate() {
        return enableApLimitRate;
    }

    public void setEnableApLimitRate(boolean enableApLimitRate) {
        this.enableApLimitRate = enableApLimitRate;
        MemorySetting.ENABLE_LIMIT_APRATE = enableApLimitRate;
    }

    public String getMetaDbUser() {
        return metaDbUser;
    }

    public void setMetaDbUser(String metaDbUser) {
        this.metaDbUser = metaDbUser;
    }

    public String getMetaDbPasswd() {
        return metaDbPasswd;
    }

    public void setMetaDbPasswd(String metaDbPasswd) {
        this.metaDbPasswd = metaDbPasswd;
    }

    public int getMetaDbExecutor() {
        return metaDbExecutor;
    }

    public void setMetaDbExecutor(int metaDbExecutor) {
        this.metaDbExecutor = metaDbExecutor;
    }

    public String getMetaDbAddr() {
        return metaDbAddr;
    }

    public String getMetaDbFirstAddr() {
        String[] result = StringUtils.split(metaDbAddr, ",");
        if (result == null || result.length == 0) {
            return null;
        }
        return result[0];
    }

    public void setMetaDbAddr(String metaDbAddr) {
        this.metaDbAddr = metaDbAddr;
    }

    public String getMetaDbName() {
        return metaDbName;
    }

    public void setMetaDbName(String metaDbName) {
        this.metaDbName = metaDbName;
    }

    public String getMetaDbProp() {
        return metaDbProp;
    }

    public void setMetaDbProp(String metaDbProp) {
        this.metaDbProp = metaDbProp;
    }

    public boolean getEnableScaleout() {
        return enableScaleout;
    }

    public void setEnableScaleout(boolean enableScaleout) {
        this.enableScaleout = enableScaleout;
    }

    public boolean getEnablePrimaryZoneMaintain() {
        return this.enablePrimaryZoneMaintain;
    }

    public void setEnablePrimaryZoneMaintain(boolean enable) {
        this.enablePrimaryZoneMaintain = enable;
    }

    public boolean isEnableBalancer() {
        return this.enableBalancer;
    }

    public void setEnableBalancer(boolean enable) {
        this.enableBalancer = enable;
    }

    public void setBalanceWindow(String window) {
        this.balanceWindow = window;
    }

    public String getBalancerWindow() {
        return this.balanceWindow;
    }

    public boolean getDropOldDataBaseAfterSwitchDataSource() {
        return dropOldDataBaseAfterSwitchDataSource;
    }

    public void setDropOldDataBaseAfterSwitchDataSource(boolean dropOldDataBaseAfterSwitchDataSource) {
        this.dropOldDataBaseAfterSwitchDataSource = dropOldDataBaseAfterSwitchDataSource;
    }

    public PasswdRuleConfig getPasswordRuleConfig() {
        return passwordRuleConfig;
    }

    public void setPasswordRuleConfig(PasswdRuleConfig passwdRuleConfig) {
        this.passwordRuleConfig = passwdRuleConfig;
    }

    public int getShardDbCountEachStorageInst() {
        return shardDbCountEachStorageInst;
    }

    public void setShardDbCountEachStorageInst(int shardDbCountEachStorageInst) {
        this.shardDbCountEachStorageInst = shardDbCountEachStorageInst;
    }

    public boolean isEnableForbidPushDmlWithHint() {
        return enableForbidPushDmlWithHint;
    }

    public void setEnableForbidPushDmlWithHint(boolean enableForbidPushDmlWithHint) {
        this.enableForbidPushDmlWithHint = enableForbidPushDmlWithHint;
    }

    public boolean isSupportSingleDbMultiTbs() {
        return supportSingleDbMultiTbs;
    }

    public void setSupportSingleDbMultiTbs(boolean supportSingleDbMultiTbs) {
        this.supportSingleDbMultiTbs = supportSingleDbMultiTbs;
    }

    public boolean isSupportDropAutoSeq() {
        return supportDropAutoSeq;
    }

    public void setSupportDropAutoSeq(boolean supportDropAutoSeq) {
        this.supportDropAutoSeq = supportDropAutoSeq;
    }

    public boolean isAllowSimpleSequence() {
        return allowSimpleSequence;
    }

    public void setAllowSimpleSequence(boolean allowSimpleSequence) {
        this.allowSimpleSequence = allowSimpleSequence;
    }

    public String getMasterInstanceId() {
        return masterInstanceId;
    }

    public void setMasterInstanceId(String masterInstanceId) {
        this.masterInstanceId = masterInstanceId;
    }

    public String getWorkloadType() {
        return workloadType;
    }

    public void setWorkloadType(String workloadType) {
        this.workloadType = workloadType;
    }

    public boolean isEnableRemoteRPC() {
        return enableRemoteRPC;
    }

    public void setEnableRemoteRPC(boolean enableRemoteRPC) {
        this.enableRemoteRPC = enableRemoteRPC;
    }

    public boolean isEnableMasterMpp() {
        return enableMasterMpp;
    }

    public void setEnableMasterMpp(boolean enableMasterMpp) {
        this.enableMasterMpp = enableMasterMpp;
    }

    public int getTimerTaskExecutor() {
        return timerTaskExecutor;
    }

    public void setTimerTaskExecutor(int timerTaskExecutor) {
        this.timerTaskExecutor = timerTaskExecutor;
    }

    public int getCclRescheduleTimeoutCheckPeriod() {
        return cclRescheduleTimeoutCheckPeriod;
    }

    public void setCclRescheduleTimeoutCheckPeriod(int cclRescheduleTimeoutCheckPeriod) {
        this.cclRescheduleTimeoutCheckPeriod = cclRescheduleTimeoutCheckPeriod;
    }

    public int getProcessCclTriggerPeriod() {
        return processCclTriggerPeriod;
    }

    public void setProcessCclTriggerPeriod(int processCclTriggerPeriod) {
        this.processCclTriggerPeriod = processCclTriggerPeriod;
    }

    public boolean getEnableLogicalDbWarmmingUp() {
        return enableLogicalDbWarmmingUp;
    }

    public void setEnableLogicalDbWarmmingUp(boolean enableLogicalDbWarmmingUp) {
        this.enableLogicalDbWarmmingUp = enableLogicalDbWarmmingUp;
    }

    public int getLogicalDbWarmmingUpExecutorPoolSize() {
        return logicalDbWarmmingUpExecutorPoolSize;
    }

    public void setLogicalDbWarmmingUpExecutorPoolSize(int logicalDbWarmmingUpExecutorPoolSize) {
        this.logicalDbWarmmingUpExecutorPoolSize = logicalDbWarmmingUpExecutorPoolSize;
    }

    public boolean isInitializeGms() {
        return initializeGms;
    }

    public void setInitializeGms(boolean value) {
        this.initializeGms = value;
    }

    public String getDataNodeList() {
        return dataNodeList;
    }

    public void setDataNodeList(String dataNodeList) {
        this.dataNodeList = dataNodeList;
    }

    public String getMetaDbRootUser() {
        return metaDbRootUser;
    }

    public void setMetaDbRootUser(String metaDbRootUser) {
        this.metaDbRootUser = metaDbRootUser;
    }

    public String getMetaDbRootPasswd() {
        return metaDbRootPasswd;
    }

    public void setMetaDbRootPasswd(String metaDbRootPasswd) {
        this.metaDbRootPasswd = metaDbRootPasswd;
    }

    public String getPolarxRootUser() {
        return polarxRootUser;
    }

    public void setPolarxRootUser(String polarxRootUser) {
        this.polarxRootUser = polarxRootUser;
    }

    public String getPolarxRootPasswd() {
        return polarxRootPasswd;
    }

    public void setPolarxRootPasswd(String polarxRootPasswd) {
        this.polarxRootPasswd = polarxRootPasswd;
    }

    public String getPropertyFile() {
        return propertyFile;
    }

    public void setPropertyFile(String propertyFile) {
        this.propertyFile = propertyFile;
    }

    public boolean isForceCleanup() {
        return forceCleanup;
    }

    public void setForceCleanup(boolean forceCleanup) {
        this.forceCleanup = forceCleanup;
    }

    public String getDefaultPartitionMode() {
        return defaultPartitionMode;
    }

    public void setDefaultPartitionMode(String defaultPartitionMode) {
        this.defaultPartitionMode = defaultPartitionMode;
    }
}
