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

import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.config.ConnPoolConfHandler;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.rpc.pool.XConnectionManager;

import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author chenghui.lch
 */
public class ConnPoolConfigManager extends AbstractLifecycle {
    private static final Logger logger = LoggerFactory.getLogger(ConnPoolConfigManager.class);

    private volatile ConnPoolConfig globalConnPoolConfig;

    /**
     * <pre>
     *     key: dbname (lowercase)
     *     val: the db config receiver of one db
     * </pre>
     */
    protected Map<String, ConnPoolConfHandler> atomConnPoolConfigHandlerMap = new ConcurrentHashMap<>();

    protected static ConnPoolConfigManager instance = new ConnPoolConfigManager();

    public static ConnPoolConfigManager getInstance() {
        if (!instance.isInited()) {
            synchronized (instance) {
                if (!instance.isInited()) {
                    instance.init();
                }
            }
        }
        return instance;
    }

    protected ConnPoolConfigManager() {
    }

    @Override
    protected void doInit() {
        initDefaultConnPoolConfig();
    }

    public void registerConnPoolConfHandler(String dbKey, ConnPoolConfHandler handler) {
        synchronized (this) {
            atomConnPoolConfigHandlerMap.putIfAbsent(dbKey.toLowerCase(), handler);
        }
    }

    public void unRegisterConnPoolConfHandler(String dbKey) {
        synchronized (this) {
            atomConnPoolConfigHandlerMap.remove(dbKey.toLowerCase());
        }
    }

    public final ConnPoolConfig getConnPoolConfig() {
        return globalConnPoolConfig;
    }

    public void refreshConnPoolConfig(Properties properties) {

        // Extract conn pool configs from props
        ConnPoolConfig connPoolConfig = extractConnPoolConfigFromProperteis(properties);

        // check if conn pool change
        ConnPoolConfig newConnConf = checkIfConnPoolConfigChanged(connPoolConfig);
        if (newConnConf == null) {
            return;
        }

        // Change global setting of XConnectionManager.
        if (newConnConf.xprotoMaxClientPerInstance != null) {
            XConnectionManager.getInstance()
                .setMaxClientPerInstance(newConnConf.xprotoMaxClientPerInstance);
        }
        if (newConnConf.xprotoMaxSessionPerClient != null) {
            XConnectionManager.getInstance()
                .setMaxSessionPerClient(newConnConf.xprotoMaxSessionPerClient);
        }
        if (newConnConf.xprotoMaxPooledSessionPerInstance != null) {
            XConnectionManager.getInstance()
                .setMaxPooledSessionPerInstance(newConnConf.xprotoMaxPooledSessionPerInstance);
        }
        if (newConnConf.xprotoMinPooledSessionPerInstance != null) {
            XConnectionManager.getInstance()
                .setMinPooledSessionPerInstance(newConnConf.xprotoMinPooledSessionPerInstance);
        }
        if (newConnConf.xprotoSessionAgingTime != null) {
            XConnectionManager.getInstance()
                .setSessionAgingTimeMillis(newConnConf.xprotoSessionAgingTime);
        }
        if (newConnConf.xprotoSlowThreshold != null) {
            XConnectionManager.getInstance()
                .setSlowThresholdMillis(newConnConf.xprotoSlowThreshold);
        }
        if (newConnConf.xprotoAuth != null) {
            XConnectionManager.getInstance()
                .setEnableAuth(newConnConf.xprotoAuth);
        }
        if (newConnConf.xprotoAutoCommitOptimize != null) {
            XConnectionManager.getInstance()
                .setEnableAutoCommitOptimize(newConnConf.xprotoAutoCommitOptimize);
        }
        if (newConnConf.xprotoXplan != null) {
            XConnectionManager.getInstance()
                .setEnableXplan(newConnConf.xprotoXplan);
        }
        if (newConnConf.xprotoXplanExpendStar != null) {
            XConnectionManager.getInstance()
                .setEnableXplanExpendStar(newConnConf.xprotoXplanExpendStar);
        }
        if (newConnConf.xprotoXplanTableScan != null) {
            XConnectionManager.getInstance()
                .setEnableXplanTableScan(newConnConf.xprotoXplanTableScan);
        }
        if (newConnConf.xprotoTrxLeakCheck != null) {
            XConnectionManager.getInstance()
                .setEnableTrxLeakCheck(newConnConf.xprotoTrxLeakCheck);
        }
        if (newConnConf.xprotoMessageTimestamp != null) {
            XConnectionManager.getInstance()
                .setEnableMessageTimestamp(newConnConf.xprotoMessageTimestamp);
        }
        if (newConnConf.xprotoPlanCache != null) {
            XConnectionManager.getInstance()
                .setEnablePlanCache(newConnConf.xprotoPlanCache);
        }
        if (newConnConf.xprotoChunkResult != null) {
            XConnectionManager.getInstance()
                .setEnableChunkResult(newConnConf.xprotoChunkResult);
        }
        if (newConnConf.xprotoPureAsyncMpp != null) {
            XConnectionManager.getInstance()
                .setEnablePureAsyncMpp(newConnConf.xprotoPureAsyncMpp);
        }
        if (newConnConf.xprotoDirectWrite != null) {
            XConnectionManager.getInstance()
                .setEnableDirectWrite(newConnConf.xprotoDirectWrite);
        }
        if (newConnConf.xprotoFeedback != null) {
            XConnectionManager.getInstance()
                .setEnableFeedback(newConnConf.xprotoFeedback);
        }
        if (newConnConf.xprotoChecker != null) {
            XConnectionManager.getInstance()
                .setEnableChecker(newConnConf.xprotoChecker);
        }
        if (newConnConf.xprotoMaxPacketSize != null) {
            XConnectionManager.getInstance()
                .setMaxPacketSize(newConnConf.xprotoMaxPacketSize);
        }

        // apply new config for conn pools
        for (String dbKeyStr : atomConnPoolConfigHandlerMap.keySet()) {
            ConnPoolConfHandler handler = atomConnPoolConfigHandlerMap.get(dbKeyStr);
            if (handler != null) {
                handler.onDataReceived(newConnConf);
            }
        }
        this.globalConnPoolConfig = newConnConf;
    }

    private ConnPoolConfig extractConnPoolConfigFromProperteis(Properties properties) {
        String connProps = null;
        Integer minPoolSize = null;
        Integer maxPoolSize = null;
        Integer maxWaitThreadCount = null;
        Integer idleTimeout = null;
        Integer blockTimeout = null;
        String xprotoConfig = null;
        Long xprotoFlag = null;
        Integer xprotoStorageDbPort = null;
        Integer xprotoMaxClientPerInstance = null;
        Integer xprotoMaxSessionPerClient = null;
        Integer xprotoMaxPooledSessionPerInstance = null;
        Integer xprotoMinPooledSessionPerInstance = null;
        Long xprotoSessionAgingTime = null;
        Long xprotoSlowThreshold = null;
        Boolean xprotoAuth = null;
        Boolean xprotoAutoCommitOptimize = null;
        Boolean xprotoXplan = null;
        Boolean xprotoXplanExpendStar = null;
        Boolean xprotoXplanTableScan = null;
        Boolean xprotoTrxLeakCheck = null;
        Boolean xprotoMessageTimestamp = null;
        Boolean xprotoPlanCache = null;
        Boolean xprotoChunkResult = null;
        Boolean xprotoPureAsyncMpp = null;
        Boolean xprotoDirectWrite = null;
        Boolean xprotoFeedback = null;
        Boolean xprotoChecker = null;
        Long xprotoMaxPacketSize = null;

        String propVal = null;
        propVal = properties.getProperty(ConnectionProperties.CONN_POOL_PROPERTIES);
        if (propVal != null) {
            connProps = String.valueOf(propVal);
        }

        propVal = properties.getProperty(ConnectionProperties.CONN_POOL_MIN_POOL_SIZE);
        if (propVal != null) {
            minPoolSize = Integer.valueOf(propVal);
        }

        propVal = properties.getProperty(ConnectionProperties.CONN_POOL_MAX_POOL_SIZE);
        if (propVal != null) {
            maxPoolSize = Integer.valueOf(propVal);
        }

        propVal = properties.getProperty(ConnectionProperties.CONN_POOL_MAX_WAIT_THREAD_COUNT);
        if (propVal != null) {
            maxWaitThreadCount = Integer.valueOf(propVal);
        }

        propVal = properties.getProperty(ConnectionProperties.CONN_POOL_IDLE_TIMEOUT);
        if (propVal != null) {
            idleTimeout = Integer.valueOf(propVal);
        }

        propVal = properties.getProperty(ConnectionProperties.CONN_POOL_BLOCK_TIMEOUT);
        if (propVal != null) {
            blockTimeout = Integer.valueOf(propVal);
        }

        propVal = properties.getProperty(ConnectionProperties.CONN_POOL_XPROTO_CONFIG);
        if (propVal != null) {
            xprotoConfig = propVal;
        }

        propVal = properties.getProperty(ConnectionProperties.CONN_POOL_XPROTO_FLAG);
        if (propVal != null) {
            xprotoFlag = Long.valueOf(propVal);
        }

        propVal = properties.getProperty(ConnectionProperties.CONN_POOL_XPROTO_STORAGE_DB_PORT);
        if (propVal != null) {
            xprotoStorageDbPort = Integer.valueOf(propVal);
        }

        propVal = properties.getProperty(ConnectionProperties.CONN_POOL_XPROTO_MAX_CLIENT_PER_INST);
        if (propVal != null) {
            xprotoMaxClientPerInstance = Integer.valueOf(propVal);
        }

        propVal = properties.getProperty(ConnectionProperties.CONN_POOL_XPROTO_MAX_SESSION_PER_CLIENT);
        if (propVal != null) {
            xprotoMaxSessionPerClient = Integer.valueOf(propVal);
        }

        propVal = properties.getProperty(ConnectionProperties.CONN_POOL_XPROTO_MAX_POOLED_SESSION_PER_INST);
        if (propVal != null) {
            xprotoMaxPooledSessionPerInstance = Integer.valueOf(propVal);
        }

        propVal = properties.getProperty(ConnectionProperties.CONN_POOL_XPROTO_MIN_POOLED_SESSION_PER_INST);
        if (propVal != null) {
            xprotoMinPooledSessionPerInstance = Integer.valueOf(propVal);
        }

        propVal = properties.getProperty(ConnectionProperties.CONN_POOL_XPROTO_SESSION_AGING_TIME);
        if (propVal != null) {
            xprotoSessionAgingTime = Long.valueOf(propVal);
        }

        propVal = properties.getProperty(ConnectionProperties.CONN_POOL_XPROTO_SLOW_THRESH);
        if (propVal != null) {
            xprotoSlowThreshold = Long.valueOf(propVal);
        }

        propVal = properties.getProperty(ConnectionProperties.CONN_POOL_XPROTO_AUTH);
        if (propVal != null) {
            xprotoAuth = Boolean.valueOf(propVal);
        }

        propVal = properties.getProperty(ConnectionProperties.CONN_POOL_XPROTO_AUTO_COMMIT_OPTIMIZE);
        if (propVal != null) {
            xprotoAutoCommitOptimize = Boolean.valueOf(propVal);
        }

        propVal = properties.getProperty(ConnectionProperties.CONN_POOL_XPROTO_XPLAN);
        if (propVal != null) {
            xprotoXplan = Boolean.valueOf(propVal);
        }

        propVal = properties.getProperty(ConnectionProperties.CONN_POOL_XPROTO_XPLAN_EXPEND_STAR);
        if (propVal != null) {
            xprotoXplanExpendStar = Boolean.valueOf(propVal);
        }

        propVal = properties.getProperty(ConnectionProperties.CONN_POOL_XPROTO_XPLAN_TABLE_SCAN);
        if (propVal != null) {
            xprotoXplanTableScan = Boolean.valueOf(propVal);
        }

        propVal = properties.getProperty(ConnectionProperties.CONN_POOL_XPROTO_TRX_LEAK_CHECK);
        if (propVal != null) {
            xprotoTrxLeakCheck = Boolean.valueOf(propVal);
        }

        propVal = properties.getProperty(ConnectionProperties.CONN_POOL_XPROTO_MESSAGE_TIMESTAMP);
        if (propVal != null) {
            xprotoMessageTimestamp = Boolean.valueOf(propVal);
        }

        propVal = properties.getProperty(ConnectionProperties.CONN_POOL_XPROTO_PLAN_CACHE);
        if (propVal != null) {
            xprotoPlanCache = Boolean.valueOf(propVal);
        }

        propVal = properties.getProperty(ConnectionProperties.CONN_POOL_XPROTO_CHUNK_RESULT);
        if (propVal != null) {
            xprotoChunkResult = Boolean.valueOf(propVal);
        }

        propVal = properties.getProperty(ConnectionProperties.CONN_POOL_XPROTO_PURE_ASYNC_MPP);
        if (propVal != null) {
            xprotoPureAsyncMpp = Boolean.valueOf(propVal);
        }

        propVal = properties.getProperty(ConnectionProperties.CONN_POOL_XPROTO_DIRECT_WRITE);
        if (propVal != null) {
            xprotoDirectWrite = Boolean.valueOf(propVal);
        }

        propVal = properties.getProperty(ConnectionProperties.CONN_POOL_XPROTO_FEEDBACK);
        if (propVal != null) {
            xprotoFeedback = Boolean.valueOf(propVal);
        }

        propVal = properties.getProperty(ConnectionProperties.CONN_POOL_XPROTO_CHECKER);
        if (propVal != null) {
            xprotoChecker = Boolean.valueOf(propVal);
        }

        propVal = properties.getProperty(ConnectionProperties.CONN_POOL_XPROTO_MAX_PACKET_SIZE);
        if (propVal != null) {
            xprotoMaxPacketSize = Long.valueOf(propVal);
        }

        ConnPoolConfig connPoolConfig = new ConnPoolConfig();
        connPoolConfig.connProps = connProps;
        connPoolConfig.minPoolSize = minPoolSize;
        connPoolConfig.maxPoolSize = maxPoolSize;
        connPoolConfig.maxWaitThreadCount = maxWaitThreadCount;
        connPoolConfig.idleTimeout = idleTimeout;
        connPoolConfig.blockTimeout = blockTimeout;
        connPoolConfig.xprotoConfig = xprotoConfig;
        connPoolConfig.xprotoFlag = xprotoFlag;
        connPoolConfig.xprotoStorageDbPort = xprotoStorageDbPort;
        connPoolConfig.xprotoMaxClientPerInstance = xprotoMaxClientPerInstance;
        connPoolConfig.xprotoMaxSessionPerClient = xprotoMaxSessionPerClient;
        connPoolConfig.xprotoMaxPooledSessionPerInstance = xprotoMaxPooledSessionPerInstance;
        connPoolConfig.xprotoMinPooledSessionPerInstance = xprotoMinPooledSessionPerInstance;
        connPoolConfig.xprotoSessionAgingTime = xprotoSessionAgingTime;
        connPoolConfig.xprotoSlowThreshold = xprotoSlowThreshold;
        connPoolConfig.xprotoAuth = xprotoAuth;
        connPoolConfig.xprotoAutoCommitOptimize = xprotoAutoCommitOptimize;
        connPoolConfig.xprotoXplan = xprotoXplan;
        connPoolConfig.xprotoXplanExpendStar = xprotoXplanExpendStar;
        connPoolConfig.xprotoXplanTableScan = xprotoXplanTableScan;
        connPoolConfig.xprotoTrxLeakCheck = xprotoTrxLeakCheck;
        connPoolConfig.xprotoMessageTimestamp = xprotoMessageTimestamp;
        connPoolConfig.xprotoPlanCache = xprotoPlanCache;
        connPoolConfig.xprotoChunkResult = xprotoChunkResult;
        connPoolConfig.xprotoPureAsyncMpp = xprotoPureAsyncMpp;
        connPoolConfig.xprotoDirectWrite = xprotoDirectWrite;
        connPoolConfig.xprotoFeedback = xprotoFeedback;
        connPoolConfig.xprotoChecker = xprotoChecker;
        connPoolConfig.xprotoMaxPacketSize = xprotoMaxPacketSize;
        connPoolConfig.defaultTransactionIsolation = DynamicConfig.getInstance().getTxIsolation();

        return connPoolConfig;
    }

    protected ConnPoolConfig checkIfConnPoolConfigChanged(ConnPoolConfig connPoolConfig) {
        ConnPoolConfig tmpConnPoolConfig = new ConnPoolConfig();
        tmpConnPoolConfig.connProps = globalConnPoolConfig.connProps;
        tmpConnPoolConfig.minPoolSize = globalConnPoolConfig.minPoolSize;
        tmpConnPoolConfig.maxPoolSize = globalConnPoolConfig.maxPoolSize;
        tmpConnPoolConfig.maxWaitThreadCount = globalConnPoolConfig.maxWaitThreadCount;
        tmpConnPoolConfig.idleTimeout = globalConnPoolConfig.idleTimeout;
        tmpConnPoolConfig.blockTimeout = globalConnPoolConfig.blockTimeout;
        tmpConnPoolConfig.xprotoConfig = globalConnPoolConfig.xprotoConfig;
        tmpConnPoolConfig.xprotoFlag = globalConnPoolConfig.xprotoFlag;
        tmpConnPoolConfig.xprotoStorageDbPort = globalConnPoolConfig.xprotoStorageDbPort;
        tmpConnPoolConfig.xprotoMaxClientPerInstance = globalConnPoolConfig.xprotoMaxClientPerInstance;
        tmpConnPoolConfig.xprotoMaxSessionPerClient = globalConnPoolConfig.xprotoMaxSessionPerClient;
        tmpConnPoolConfig.xprotoMaxPooledSessionPerInstance = globalConnPoolConfig.xprotoMaxPooledSessionPerInstance;
        tmpConnPoolConfig.xprotoMinPooledSessionPerInstance = globalConnPoolConfig.xprotoMinPooledSessionPerInstance;
        tmpConnPoolConfig.xprotoSessionAgingTime = globalConnPoolConfig.xprotoSessionAgingTime;
        tmpConnPoolConfig.xprotoSlowThreshold = globalConnPoolConfig.xprotoSlowThreshold;
        tmpConnPoolConfig.xprotoAuth = globalConnPoolConfig.xprotoAuth;
        tmpConnPoolConfig.xprotoAutoCommitOptimize = globalConnPoolConfig.xprotoAutoCommitOptimize;
        tmpConnPoolConfig.xprotoXplan = globalConnPoolConfig.xprotoXplan;
        tmpConnPoolConfig.xprotoXplanExpendStar = globalConnPoolConfig.xprotoXplanExpendStar;
        tmpConnPoolConfig.xprotoXplanTableScan = globalConnPoolConfig.xprotoXplanTableScan;
        tmpConnPoolConfig.xprotoTrxLeakCheck = globalConnPoolConfig.xprotoTrxLeakCheck;
        tmpConnPoolConfig.xprotoMessageTimestamp = globalConnPoolConfig.xprotoMessageTimestamp;
        tmpConnPoolConfig.xprotoPlanCache = globalConnPoolConfig.xprotoPlanCache;
        tmpConnPoolConfig.xprotoChunkResult = globalConnPoolConfig.xprotoChunkResult;
        tmpConnPoolConfig.xprotoPureAsyncMpp = globalConnPoolConfig.xprotoPureAsyncMpp;
        tmpConnPoolConfig.xprotoDirectWrite = globalConnPoolConfig.xprotoDirectWrite;
        tmpConnPoolConfig.xprotoFeedback = globalConnPoolConfig.xprotoFeedback;
        tmpConnPoolConfig.xprotoChecker = globalConnPoolConfig.xprotoChecker;
        tmpConnPoolConfig.xprotoMaxPacketSize = globalConnPoolConfig.xprotoMaxPacketSize;
        tmpConnPoolConfig.defaultTransactionIsolation = globalConnPoolConfig.defaultTransactionIsolation;

        boolean isChanged = false;
        if (connPoolConfig.connProps != null && !tmpConnPoolConfig.connProps
            .equalsIgnoreCase(connPoolConfig.connProps)) {
            isChanged = true;
            tmpConnPoolConfig.connProps = connPoolConfig.connProps;
        }
        if (connPoolConfig.minPoolSize != null && !tmpConnPoolConfig.minPoolSize.equals(connPoolConfig.minPoolSize)) {
            isChanged = true;
            tmpConnPoolConfig.minPoolSize = connPoolConfig.minPoolSize;
        }

        if (connPoolConfig.maxPoolSize != null && !tmpConnPoolConfig.maxPoolSize.equals(connPoolConfig.maxPoolSize)) {
            isChanged = true;
            tmpConnPoolConfig.maxPoolSize = connPoolConfig.maxPoolSize;
        }

        if (connPoolConfig.maxWaitThreadCount != null && !tmpConnPoolConfig.maxWaitThreadCount
            .equals(connPoolConfig.maxWaitThreadCount)) {
            isChanged = true;
            tmpConnPoolConfig.maxWaitThreadCount = connPoolConfig.maxWaitThreadCount;
        }

        if (connPoolConfig.idleTimeout != null && !tmpConnPoolConfig.idleTimeout.equals(connPoolConfig.idleTimeout)) {
            isChanged = true;
            tmpConnPoolConfig.idleTimeout = connPoolConfig.idleTimeout;
        }

        if (connPoolConfig.blockTimeout != null && !tmpConnPoolConfig.blockTimeout
            .equals(connPoolConfig.blockTimeout)) {
            isChanged = true;
            tmpConnPoolConfig.blockTimeout = connPoolConfig.blockTimeout;
        }

        if (connPoolConfig.xprotoConfig != null && !tmpConnPoolConfig.xprotoConfig
            .equals(connPoolConfig.xprotoConfig)) {
            isChanged = true;
            tmpConnPoolConfig.xprotoConfig = connPoolConfig.xprotoConfig;
        }

        if (connPoolConfig.xprotoFlag != null && !tmpConnPoolConfig.xprotoFlag.equals(connPoolConfig.xprotoFlag)) {
            isChanged = true;
            tmpConnPoolConfig.xprotoFlag = connPoolConfig.xprotoFlag;
        }

        if (connPoolConfig.xprotoStorageDbPort != null &&
            !tmpConnPoolConfig.xprotoStorageDbPort.equals(connPoolConfig.xprotoStorageDbPort)) {
            isChanged = true;
            tmpConnPoolConfig.xprotoStorageDbPort = connPoolConfig.xprotoStorageDbPort;
        }

        if (connPoolConfig.xprotoMaxClientPerInstance != null &&
            !tmpConnPoolConfig.xprotoMaxClientPerInstance.equals(connPoolConfig.xprotoMaxClientPerInstance)) {
            isChanged = true;
            tmpConnPoolConfig.xprotoMaxClientPerInstance = connPoolConfig.xprotoMaxClientPerInstance;
        }

        if (connPoolConfig.xprotoMaxSessionPerClient != null &&
            !tmpConnPoolConfig.xprotoMaxSessionPerClient.equals(connPoolConfig.xprotoMaxSessionPerClient)) {
            isChanged = true;
            tmpConnPoolConfig.xprotoMaxSessionPerClient = connPoolConfig.xprotoMaxSessionPerClient;
        }

        if (connPoolConfig.xprotoMaxPooledSessionPerInstance != null &&
            !tmpConnPoolConfig.xprotoMaxPooledSessionPerInstance
                .equals(connPoolConfig.xprotoMaxPooledSessionPerInstance)) {
            isChanged = true;
            tmpConnPoolConfig.xprotoMaxPooledSessionPerInstance = connPoolConfig.xprotoMaxPooledSessionPerInstance;
        }

        if (connPoolConfig.xprotoMinPooledSessionPerInstance != null &&
            !tmpConnPoolConfig.xprotoMinPooledSessionPerInstance
                .equals(connPoolConfig.xprotoMinPooledSessionPerInstance)) {
            isChanged = true;
            tmpConnPoolConfig.xprotoMinPooledSessionPerInstance = connPoolConfig.xprotoMinPooledSessionPerInstance;
        }

        if (connPoolConfig.xprotoSessionAgingTime != null &&
            !tmpConnPoolConfig.xprotoSessionAgingTime.equals(connPoolConfig.xprotoSessionAgingTime)) {
            isChanged = true;
            tmpConnPoolConfig.xprotoSessionAgingTime = connPoolConfig.xprotoSessionAgingTime;
        }

        if (connPoolConfig.xprotoSlowThreshold != null &&
            !tmpConnPoolConfig.xprotoSlowThreshold.equals(connPoolConfig.xprotoSlowThreshold)) {
            isChanged = true;
            tmpConnPoolConfig.xprotoSlowThreshold = connPoolConfig.xprotoSlowThreshold;
        }

        if (connPoolConfig.xprotoAuth != null &&
            !tmpConnPoolConfig.xprotoAuth.equals(connPoolConfig.xprotoAuth)) {
            isChanged = true;
            tmpConnPoolConfig.xprotoAuth = connPoolConfig.xprotoAuth;
        }

        if (connPoolConfig.xprotoAutoCommitOptimize != null &&
            !tmpConnPoolConfig.xprotoAutoCommitOptimize.equals(connPoolConfig.xprotoAutoCommitOptimize)) {
            isChanged = true;
            tmpConnPoolConfig.xprotoAutoCommitOptimize = connPoolConfig.xprotoAutoCommitOptimize;
        }

        if (connPoolConfig.xprotoXplan != null &&
            !tmpConnPoolConfig.xprotoXplan.equals(connPoolConfig.xprotoXplan)) {
            isChanged = true;
            tmpConnPoolConfig.xprotoXplan = connPoolConfig.xprotoXplan;
        }

        if (connPoolConfig.xprotoXplanExpendStar != null &&
            !tmpConnPoolConfig.xprotoXplanExpendStar.equals(connPoolConfig.xprotoXplanExpendStar)) {
            isChanged = true;
            tmpConnPoolConfig.xprotoXplanExpendStar = connPoolConfig.xprotoXplanExpendStar;
        }

        if (connPoolConfig.xprotoXplanTableScan != null &&
            !tmpConnPoolConfig.xprotoXplanTableScan.equals(connPoolConfig.xprotoXplanTableScan)) {
            isChanged = true;
            tmpConnPoolConfig.xprotoXplanTableScan = connPoolConfig.xprotoXplanTableScan;
        }

        if (connPoolConfig.xprotoTrxLeakCheck != null &&
            !tmpConnPoolConfig.xprotoTrxLeakCheck.equals(connPoolConfig.xprotoTrxLeakCheck)) {
            isChanged = true;
            tmpConnPoolConfig.xprotoTrxLeakCheck = connPoolConfig.xprotoTrxLeakCheck;
        }

        if (connPoolConfig.xprotoMessageTimestamp != null &&
            !tmpConnPoolConfig.xprotoMessageTimestamp.equals(connPoolConfig.xprotoMessageTimestamp)) {
            isChanged = true;
            tmpConnPoolConfig.xprotoMessageTimestamp = connPoolConfig.xprotoMessageTimestamp;
        }

        if (connPoolConfig.xprotoPlanCache != null &&
            !tmpConnPoolConfig.xprotoPlanCache.equals(connPoolConfig.xprotoPlanCache)) {
            isChanged = true;
            tmpConnPoolConfig.xprotoPlanCache = connPoolConfig.xprotoPlanCache;
        }

        if (connPoolConfig.xprotoChunkResult != null &&
            !tmpConnPoolConfig.xprotoChunkResult.equals(connPoolConfig.xprotoChunkResult)) {
            isChanged = true;
            tmpConnPoolConfig.xprotoChunkResult = connPoolConfig.xprotoChunkResult;
        }

        if (connPoolConfig.xprotoPureAsyncMpp != null &&
            !tmpConnPoolConfig.xprotoPureAsyncMpp.equals(connPoolConfig.xprotoPureAsyncMpp)) {
            isChanged = true;
            tmpConnPoolConfig.xprotoPureAsyncMpp = connPoolConfig.xprotoPureAsyncMpp;
        }

        if (connPoolConfig.xprotoDirectWrite != null &&
            !tmpConnPoolConfig.xprotoDirectWrite.equals(connPoolConfig.xprotoDirectWrite)) {
            isChanged = true;
            tmpConnPoolConfig.xprotoDirectWrite = connPoolConfig.xprotoDirectWrite;
        }

        if (connPoolConfig.xprotoFeedback != null &&
            !tmpConnPoolConfig.xprotoFeedback.equals(connPoolConfig.xprotoFeedback)) {
            isChanged = true;
            tmpConnPoolConfig.xprotoFeedback = connPoolConfig.xprotoFeedback;
        }

        if (connPoolConfig.xprotoChecker != null &&
            !tmpConnPoolConfig.xprotoChecker.equals(connPoolConfig.xprotoChecker)) {
            isChanged = true;
            tmpConnPoolConfig.xprotoChecker = connPoolConfig.xprotoChecker;
        }

        if (connPoolConfig.xprotoMaxPacketSize != null &&
            !tmpConnPoolConfig.xprotoMaxPacketSize.equals(connPoolConfig.xprotoMaxPacketSize)) {
            isChanged = true;
            tmpConnPoolConfig.xprotoMaxPacketSize = connPoolConfig.xprotoMaxPacketSize;
        }

        if (connPoolConfig.defaultTransactionIsolation != null &&
            !connPoolConfig.defaultTransactionIsolation.equals(tmpConnPoolConfig.defaultTransactionIsolation)) {
            isChanged = true;
            tmpConnPoolConfig.defaultTransactionIsolation = connPoolConfig.defaultTransactionIsolation;
        }

        if (!isChanged) {
            return null;
        }

        return tmpConnPoolConfig;

    }

    public static ConnPoolConfig buildDefaultConnPoolConfig() {
        String connProps;
        Integer minPoolSize;
        Integer maxPoolSize;
        Integer maxWaitThreadCount;
        Integer idleTimeout;
        Integer blockTimeout;
        String xprotoConfig;
        Long xprotoFlag;
        Integer xprotoStorageDbPort;
        Integer xprotoMaxClientPerInstance;
        Integer xprotoMaxSessionPerClient;
        Integer xprotoMaxPooledSessionPerInstance;
        Integer xprotoMinPooledSessionPerInstance;
        Long xprotoSessionAgingTime;
        Long xprotoSlowThreshold;
        Boolean xprotoAuth;
        Boolean xprotoAutoCommitOptimize;
        Boolean xprotoXplan;
        Boolean xprotoXplanExpendStar;
        Boolean xprotoXplanTableScan;
        Boolean xprotoTrxLeakCheck;
        Boolean xprotoMessageTimestamp;
        Boolean xprotoPlanCache;
        Boolean xprotoChunkResult;
        Boolean xprotoPureAsyncMpp;
        Boolean xprotoDirectWrite;
        Boolean xprotoFeedback;
        Boolean xprotoChecker;
        Long xprotoMaxPacketSize;

        Map<String, String> emptyExtraCmds = new HashMap<>();
        ParamManager paramManager = new ParamManager(emptyExtraCmds);
        connProps = paramManager.getString(ConnectionParams.CONN_POOL_PROPERTIES);
        minPoolSize = paramManager.getInt(ConnectionParams.CONN_POOL_MIN_POOL_SIZE);
        maxPoolSize = paramManager.getInt(ConnectionParams.CONN_POOL_MAX_POOL_SIZE);
        maxWaitThreadCount = paramManager.getInt(ConnectionParams.CONN_POOL_MAX_WAIT_THREAD_COUNT);
        idleTimeout = paramManager.getInt(ConnectionParams.CONN_POOL_IDLE_TIMEOUT);
        blockTimeout = paramManager.getInt(ConnectionParams.CONN_POOL_BLOCK_TIMEOUT);
        xprotoConfig = paramManager.getString(ConnectionParams.CONN_POOL_XPROTO_CONFIG);
        xprotoFlag = paramManager.getLong(ConnectionParams.CONN_POOL_XPROTO_FLAG);
        xprotoStorageDbPort = paramManager.getInt(ConnectionParams.CONN_POOL_XPROTO_STORAGE_DB_PORT);
        xprotoMaxClientPerInstance = paramManager.getInt(ConnectionParams.CONN_POOL_XPROTO_MAX_CLIENT_PER_INST);
        xprotoMaxSessionPerClient = paramManager.getInt(ConnectionParams.CONN_POOL_XPROTO_MAX_SESSION_PER_CLIENT);
        xprotoMaxPooledSessionPerInstance =
            paramManager.getInt(ConnectionParams.CONN_POOL_XPROTO_MAX_POOLED_SESSION_PER_INST);
        xprotoMinPooledSessionPerInstance =
            paramManager.getInt(ConnectionParams.CONN_POOL_XPROTO_MIN_POOLED_SESSION_PER_INST);
        xprotoSessionAgingTime = paramManager.getLong(ConnectionParams.CONN_POOL_XPROTO_SESSION_AGING_TIME);
        xprotoSlowThreshold = paramManager.getLong(ConnectionParams.CONN_POOL_XPROTO_SLOW_THRESH);
        xprotoAuth = paramManager.getBoolean(ConnectionParams.CONN_POOL_XPROTO_AUTH);
        xprotoAutoCommitOptimize = paramManager.getBoolean(ConnectionParams.CONN_POOL_XPROTO_AUTO_COMMIT_OPTIMIZE);
        xprotoXplan = paramManager.getBoolean(ConnectionParams.CONN_POOL_XPROTO_XPLAN);
        xprotoXplanExpendStar = paramManager.getBoolean(ConnectionParams.CONN_POOL_XPROTO_XPLAN_EXPEND_STAR);
        xprotoXplanTableScan = paramManager.getBoolean(ConnectionParams.CONN_POOL_XPROTO_XPLAN_TABLE_SCAN);
        xprotoTrxLeakCheck = paramManager.getBoolean(ConnectionParams.CONN_POOL_XPROTO_TRX_LEAK_CHECK);
        xprotoMessageTimestamp = paramManager.getBoolean(ConnectionParams.CONN_POOL_XPROTO_MESSAGE_TIMESTAMP);
        xprotoPlanCache = paramManager.getBoolean(ConnectionParams.CONN_POOL_XPROTO_PLAN_CACHE);
        xprotoChunkResult = paramManager.getBoolean(ConnectionParams.CONN_POOL_XPROTO_CHUNK_RESULT);
        xprotoPureAsyncMpp = paramManager.getBoolean(ConnectionParams.CONN_POOL_XPROTO_PURE_ASYNC_MPP);
        xprotoDirectWrite = paramManager.getBoolean(ConnectionParams.CONN_POOL_XPROTO_DIRECT_WRITE);
        xprotoFeedback = paramManager.getBoolean(ConnectionParams.CONN_POOL_XPROTO_FEEDBACK);
        xprotoChecker = paramManager.getBoolean(ConnectionParams.CONN_POOL_XPROTO_CHECKER);
        xprotoMaxPacketSize = paramManager.getLong(ConnectionParams.CONN_POOL_XPROTO_MAX_PACKET_SIZE);

        ConnPoolConfig connPoolConfig = new ConnPoolConfig();
        connPoolConfig.connProps = connProps;
        connPoolConfig.minPoolSize = minPoolSize;
        connPoolConfig.maxPoolSize = maxPoolSize;
        connPoolConfig.maxWaitThreadCount = maxWaitThreadCount;
        connPoolConfig.idleTimeout = idleTimeout;
        connPoolConfig.blockTimeout = blockTimeout;
        connPoolConfig.xprotoConfig = xprotoConfig;
        connPoolConfig.xprotoFlag = xprotoFlag;
        connPoolConfig.xprotoStorageDbPort = xprotoStorageDbPort;
        connPoolConfig.xprotoMaxClientPerInstance = xprotoMaxClientPerInstance;
        connPoolConfig.xprotoMaxSessionPerClient = xprotoMaxSessionPerClient;
        connPoolConfig.xprotoMaxPooledSessionPerInstance = xprotoMaxPooledSessionPerInstance;
        connPoolConfig.xprotoMinPooledSessionPerInstance = xprotoMinPooledSessionPerInstance;
        connPoolConfig.xprotoSessionAgingTime = xprotoSessionAgingTime;
        connPoolConfig.xprotoSlowThreshold = xprotoSlowThreshold;
        connPoolConfig.xprotoAuth = xprotoAuth;
        connPoolConfig.xprotoAutoCommitOptimize = xprotoAutoCommitOptimize;
        connPoolConfig.xprotoXplan = xprotoXplan;
        connPoolConfig.xprotoXplanExpendStar = xprotoXplanExpendStar;
        connPoolConfig.xprotoXplanTableScan = xprotoXplanTableScan;
        connPoolConfig.xprotoTrxLeakCheck = xprotoTrxLeakCheck;
        connPoolConfig.xprotoMessageTimestamp = xprotoMessageTimestamp;
        connPoolConfig.xprotoPlanCache = xprotoPlanCache;
        connPoolConfig.xprotoChunkResult = xprotoChunkResult;
        connPoolConfig.xprotoPureAsyncMpp = xprotoPureAsyncMpp;
        connPoolConfig.xprotoDirectWrite = xprotoDirectWrite;
        connPoolConfig.xprotoFeedback = xprotoFeedback;
        connPoolConfig.xprotoChecker = xprotoChecker;
        connPoolConfig.xprotoMaxPacketSize = xprotoMaxPacketSize;
        return connPoolConfig;
    }

    protected void initDefaultConnPoolConfig() {

        // Fetch the values of props from meta db
        ConnPoolConfig connPoolConfig = getConnPoolConfigFromMetaDbConn();
        if (connPoolConfig == null) {
            // Inst_config table is empty
            connPoolConfig = buildDefaultConnPoolConfig();
        }
        this.globalConnPoolConfig = connPoolConfig;

        // Init XConnectionManager first time.
        if (connPoolConfig.xprotoMaxClientPerInstance != null) {
            XConnectionManager.getInstance()
                .setMaxClientPerInstance(connPoolConfig.xprotoMaxClientPerInstance);
        }
        if (connPoolConfig.xprotoMaxSessionPerClient != null) {
            XConnectionManager.getInstance()
                .setMaxSessionPerClient(connPoolConfig.xprotoMaxSessionPerClient);
        }
        if (connPoolConfig.xprotoMaxPooledSessionPerInstance != null) {
            XConnectionManager.getInstance()
                .setMaxPooledSessionPerInstance(connPoolConfig.xprotoMaxPooledSessionPerInstance);
        }
        if (connPoolConfig.xprotoMinPooledSessionPerInstance != null) {
            XConnectionManager.getInstance()
                .setMinPooledSessionPerInstance(connPoolConfig.xprotoMinPooledSessionPerInstance);
        }
        if (connPoolConfig.xprotoSessionAgingTime != null) {
            XConnectionManager.getInstance()
                .setSessionAgingTimeMillis(connPoolConfig.xprotoSessionAgingTime);
        }
        if (connPoolConfig.xprotoSlowThreshold != null) {
            XConnectionManager.getInstance()
                .setSlowThresholdMillis(connPoolConfig.xprotoSlowThreshold);
        }
        if (connPoolConfig.xprotoAuth != null) {
            XConnectionManager.getInstance()
                .setEnableAuth(connPoolConfig.xprotoAuth);
        }
        if (connPoolConfig.xprotoAutoCommitOptimize != null) {
            XConnectionManager.getInstance()
                .setEnableAutoCommitOptimize(connPoolConfig.xprotoAutoCommitOptimize);
        }
        if (connPoolConfig.xprotoXplan != null) {
            XConnectionManager.getInstance()
                .setEnableXplan(connPoolConfig.xprotoXplan);
        }
        if (connPoolConfig.xprotoXplanExpendStar != null) {
            XConnectionManager.getInstance()
                .setEnableXplanExpendStar(connPoolConfig.xprotoXplanExpendStar);
        }
        if (connPoolConfig.xprotoXplanTableScan != null) {
            XConnectionManager.getInstance()
                .setEnableXplanTableScan(connPoolConfig.xprotoXplanTableScan);
        }
        if (connPoolConfig.xprotoTrxLeakCheck != null) {
            XConnectionManager.getInstance()
                .setEnableTrxLeakCheck(connPoolConfig.xprotoTrxLeakCheck);
        }
        if (connPoolConfig.xprotoMessageTimestamp != null) {
            XConnectionManager.getInstance()
                .setEnableMessageTimestamp(connPoolConfig.xprotoMessageTimestamp);
        }
        if (connPoolConfig.xprotoPlanCache != null) {
            XConnectionManager.getInstance()
                .setEnablePlanCache(connPoolConfig.xprotoPlanCache);
        }
        if (connPoolConfig.xprotoChunkResult != null) {
            XConnectionManager.getInstance()
                .setEnableChunkResult(connPoolConfig.xprotoChunkResult);
        }
        if (connPoolConfig.xprotoPureAsyncMpp != null) {
            XConnectionManager.getInstance()
                .setEnablePureAsyncMpp(connPoolConfig.xprotoPureAsyncMpp);
        }
        if (connPoolConfig.xprotoDirectWrite != null) {
            XConnectionManager.getInstance()
                .setEnableDirectWrite(connPoolConfig.xprotoDirectWrite);
        }
        if (connPoolConfig.xprotoFeedback != null) {
            XConnectionManager.getInstance()
                .setEnableFeedback(connPoolConfig.xprotoFeedback);
        }
        if (connPoolConfig.xprotoChecker != null) {
            XConnectionManager.getInstance()
                .setEnableChecker(connPoolConfig.xprotoChecker);
        }
        if (connPoolConfig.xprotoMaxPacketSize != null) {
            XConnectionManager.getInstance()
                .setMaxPacketSize(connPoolConfig.xprotoMaxPacketSize);
        }
    }

    protected ConnPoolConfig getConnPoolConfigFromMetaDbConn() {
        Properties props = null;
        try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {
            props = MetaDbInstConfigManager.loadPropertiesFromMetaDbConn(metaDbConn);
        } catch (Throwable ex) {
            throw GeneralUtil.nestedException(ex);
        }
        ConnPoolConfig newConnPoolConfig = extractConnPoolConfigFromProperteis(props);
        return newConnPoolConfig;
    }

    @Override
    public void destroy() {
        super.destroy();
    }

}
