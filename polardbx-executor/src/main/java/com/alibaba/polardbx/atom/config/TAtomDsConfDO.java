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

import com.alibaba.polardbx.atom.TAtomDbStatusEnum;
import com.alibaba.polardbx.atom.TAtomDbTypeEnum;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.taobao.tddl.common.utils.TddlToStringStyle;
import org.apache.commons.lang.builder.ToStringBuilder;

import java.util.HashMap;
import java.util.Map;

/**
 * TAtom数据源全局和应用配置的DO
 *
 * @author qihao
 * @author shenxun
 */
public class TAtomDsConfDO implements Cloneable {

    /**
     * 默认初始化的线程池连接数量
     */
    public static final int defaultInitPoolSize = 0;

    /**
     * 默认初始化的defaultMaxWait druid专用，目前是和jboss的blockingTimeout是同一个配置。运维人员请注意。
     */
    public static final int defaultMaxWait = 5000;

    private String ip;

    private String port;

    private int originXport = -1;

    private int xport = -1;

    private String dbName;

    private String userName;

    private String passwd;

    private String driverClass;

    private String sorterClass;

    private int preparedStatementCacheSize;

    private int initPoolSize = defaultInitPoolSize;

    private int minPoolSize;

    private int maxPoolSize;

    private int blockingTimeout = defaultMaxWait;

    private long idleTimeout;

    private long evictionTimeout;                                                // 逐出线程运行时间,单位分钟

    private final TAtomDbTypeEnum dbTypeEnum = TAtomDbTypeEnum.MYSQL;

    private TAtomDbStatusEnum dbStautsEnum;

    private String dbStatus;

    private Map<String, String> connectionProperties = new HashMap<String, String>();

    /**
     * 写 次数限制
     */
    private int writeRestrictTimes;

    /**
     * 读 次数限制
     */
    private int readRestrictTimes;

    /**
     * 统计时间片
     */
    private int timeSliceInMillis;

    /**
     * 线程技术count限制
     */
    private int threadCountRestrict;

    /**
     * 允许并发读的最大个数，0为不限制
     */
    private int maxConcurrentReadRestrict;

    /**
     * 允许并发写的最大个数，0为不限制
     */
    private int maxConcurrentWriteRestrict;

    /**
     * 物理连接属性里的charset
     */
    private String characterEncoding;

    private volatile boolean isSingleInGroup;

    private String connectionInitSql;

    private int maxWaitThreadCount = 0;

    private boolean useLoadVariablesFilter = true;

    /**
     * 所有连接池共用一个线程池用来创建连接
     */
    private boolean createScheduler = true;

    /**
     * 所有连接池共用一个线程池用来销毁连接
     */
    private boolean destroyScheduler = true;

    private int onFatalErrorMaxActive = 8;

    /**
     * 数据源的模式，主要用于区分ds是AP专用还是TP专用
     */
    private String dsMode = null;

    /**
     * 是否打开Druid的KeepAlive功能，默认打开
     */
    private boolean enableKeepAlive = true;

    /**
     * 是否强制执行严格的keepAlive功能, 如果为true, minIdle为客户实际的minIdle; 如果为false,
     * minIdle会被强制设为5
     */
    private boolean strictKeepAlive = false;

    private int defaultTransactionIsolation;

    public int getOnFatalErrorMaxActive() {
        return onFatalErrorMaxActive;
    }

    public void setOnFatalErrorMaxActive(int onFatalErrorMaxActive) {
        this.onFatalErrorMaxActive = onFatalErrorMaxActive;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public int getMaxWaitThreadCount() {
        return this.maxWaitThreadCount;
    }

    public void setMaxWaitThreadCount(int maxWaitThreadCount) {
        this.maxWaitThreadCount = maxWaitThreadCount;
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }

    public int getOriginXport() {
        return originXport;
    }

    public void setOriginXport(int originXport) {
        this.originXport = originXport;
    }

    public int getXport() {
        return xport;
    }

    public void setXport(int xport) {
        this.xport = xport;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getPasswd() {
        return passwd;
    }

    public void setPasswd(String passwd) {
        this.passwd = passwd;
    }

    public String getDriverClass() {
        if (TStringUtil.isBlank(driverClass) && null != this.dbTypeEnum) {
            return this.dbTypeEnum.getDriverClass();
        }
        return driverClass;
    }

    public void setDriverClass(String driverClass) {
        this.driverClass = driverClass;
    }

    public String getSorterClass() {
        if (TStringUtil.isBlank(sorterClass) && null != this.dbTypeEnum) {
            return this.dbTypeEnum.getSorterClass();
        }
        return sorterClass;
    }

    public void setSorterClass(String sorterClass) {
        this.sorterClass = sorterClass;
    }

    public int getPreparedStatementCacheSize() {
        return preparedStatementCacheSize;
    }

    public void setPreparedStatementCacheSize(int preparedStatementCacheSize) {
        this.preparedStatementCacheSize = preparedStatementCacheSize;
    }

    public int getInitPoolSize() {
        return initPoolSize;
    }

    public void setInitPoolSize(int initPoolSize) {
        this.initPoolSize = initPoolSize;
    }

    public int getMinPoolSize() {
        return minPoolSize;
    }

    public void setMinPoolSize(int minPoolSize) {
        this.minPoolSize = minPoolSize;
    }

    public int getMaxPoolSize() {
        return maxPoolSize;
    }

    public void setMaxPoolSize(int maxPoolSize) {
        this.maxPoolSize = maxPoolSize;
    }

    public int getBlockingTimeout() {
        return blockingTimeout;
    }

    public void setBlockingTimeout(int blockingTimeout) {
        this.blockingTimeout = blockingTimeout;
    }

    public long getIdleTimeout() {
        return idleTimeout;
    }

    public void setIdleTimeout(long idleTimeout) {
        this.idleTimeout = idleTimeout;
    }

    public Map<String, String> getConnectionProperties() {
        return connectionProperties;
    }

    public long getSocketTimeout() {
        long socketTimeout = -1;
        if (connectionProperties != null) {
            String socketTimeoutStr = connectionProperties.get(TAtomConfParser.APP_SOCKET_TIMEOUT_KEY);
            if (socketTimeoutStr != null) {
                socketTimeout = Long.valueOf(socketTimeoutStr);
                return socketTimeout;
            }
        }
        return socketTimeout;
    }

    public String getDbType() {
        return dbTypeEnum.name().toLowerCase();
    }

    public String getDbStatus() {
        return dbStatus;
    }

    public void setDbStatus(String dbStatus) {
        this.dbStatus = dbStatus;
        if (TStringUtil.isNotBlank(dbStatus)) {
            this.dbStautsEnum = TAtomDbStatusEnum.getAtomDbStatusEnumByType(dbStatus);
        }
    }

    public TAtomDbStatusEnum getDbStautsEnum() {
        return dbStautsEnum;
    }

    public TAtomDbTypeEnum getDbTypeEnum() {
        return dbTypeEnum;
    }

    public void setConnectionProperties(Map<String, String> connectionProperties) {
        this.connectionProperties = connectionProperties;
    }

    public int getWriteRestrictTimes() {
        return writeRestrictTimes;
    }

    public void setWriteRestrictTimes(int writeRestrictTimes) {
        this.writeRestrictTimes = writeRestrictTimes;
    }

    public int getReadRestrictTimes() {
        return readRestrictTimes;
    }

    public void setReadRestrictTimes(int readRestrictTimes) {
        this.readRestrictTimes = readRestrictTimes;
    }

    public int getThreadCountRestrict() {
        return threadCountRestrict;
    }

    public void setThreadCountRestrict(int threadCountRestrict) {
        this.threadCountRestrict = threadCountRestrict;
    }

    public int getTimeSliceInMillis() {
        return timeSliceInMillis;
    }

    public void setTimeSliceInMillis(int timeSliceInMillis) {
        this.timeSliceInMillis = timeSliceInMillis;
    }

    public int getMaxConcurrentReadRestrict() {
        return maxConcurrentReadRestrict;
    }

    public void setMaxConcurrentReadRestrict(int maxConcurrentReadRestrict) {
        this.maxConcurrentReadRestrict = maxConcurrentReadRestrict;
    }

    public int getMaxConcurrentWriteRestrict() {
        return maxConcurrentWriteRestrict;
    }

    public void setMaxConcurrentWriteRestrict(int maxConcurrentWriteRestrict) {
        this.maxConcurrentWriteRestrict = maxConcurrentWriteRestrict;
    }

    public boolean isSingleInGroup() {
        return isSingleInGroup;
    }

    public void setSingleInGroup(boolean isSingleInGroup) {
        this.isSingleInGroup = isSingleInGroup;
    }

    public String getConnectionInitSql() {
        return connectionInitSql;
    }

    public void setConnectionInitSql(String connectionInitSql) {
        this.connectionInitSql = connectionInitSql;
    }

    public long getEvictionTimeout() {
        return evictionTimeout;
    }

    public void setEvictionTimeout(long evictionTimeout) {
        this.evictionTimeout = evictionTimeout;
    }

    @Override
    public TAtomDsConfDO clone() {
        TAtomDsConfDO tAtomDsConfDO = null;
        try {
            tAtomDsConfDO = (TAtomDsConfDO) super.clone();
        } catch (CloneNotSupportedException e) {
        }
        return tAtomDsConfDO;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, TddlToStringStyle.DEFAULT_STYLE);
    }

    public void setUseLoadVariablesFilter(boolean b) {
        this.useLoadVariablesFilter = b;
    }

    public boolean isUseLoadVariablesFilter() {
        return useLoadVariablesFilter;
    }

    public boolean isCreateScheduler() {
        return createScheduler;
    }

    public void setCreateScheduler(boolean createScheduler) {
        this.createScheduler = createScheduler;
    }

    public boolean isDestroyScheduler() {
        return destroyScheduler;
    }

    public void setDestroyScheduler(boolean destroyScheduler) {
        this.destroyScheduler = destroyScheduler;
    }

    public String getDsMode() {
        return dsMode;
    }

    public void setDsMode(String dsMode) {
        this.dsMode = dsMode;
    }

    public void setStrictKeepAlive(boolean isStrictKeepAlive) {
        this.strictKeepAlive = isStrictKeepAlive;
    }

    public String getCharacterEncoding() {
        return characterEncoding;
    }

    public void setCharacterEncoding(String characterEncoding) {
        this.characterEncoding = characterEncoding;
    }

    // To prevent physical db connection resources from running out due to
    // unexpected physical db connection pool
    // configuration after upgrading to latest from a version below 5.3.9.
    public boolean getStrictKeepAlive() {
        return strictKeepAlive;
    }

    public boolean isEnableKeepAlive() {
        return enableKeepAlive;
    }

    public void setEnableKeepAlive(boolean enableKeepAlive) {
        this.enableKeepAlive = enableKeepAlive;
    }

    public int getDefaultTransactionIsolation() {
        return defaultTransactionIsolation;
    }

    public void setDefaultTransactionIsolation(int defaultTransactionIsolation) {
        this.defaultTransactionIsolation = defaultTransactionIsolation;
    }
}
