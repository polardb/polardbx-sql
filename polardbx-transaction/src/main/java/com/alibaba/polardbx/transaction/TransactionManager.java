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

package com.alibaba.polardbx.transaction;

import com.alibaba.polardbx.common.TrxIdGenerator;
import com.alibaba.polardbx.common.constants.TransactionAttribute;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ITransactionPolicy;
import com.alibaba.polardbx.common.jdbc.ITransactionPolicy.TransactionClass;
import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.utils.extension.Activate;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.common.StorageInfoManager;
import com.alibaba.polardbx.executor.spi.ITransactionManager;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.utils.ITimestampOracle;
import com.alibaba.polardbx.optimizer.utils.ITransaction;
import com.alibaba.polardbx.optimizer.utils.ITransactionManagerUtil;
import com.alibaba.polardbx.transaction.async.AsyncTaskQueue;
import com.alibaba.polardbx.transaction.async.DeadlockDetectionTaskWrapper;
import com.alibaba.polardbx.transaction.async.RotateGlobalTxLogTaskWrapper;
import com.alibaba.polardbx.transaction.log.GlobalTxLogManager;
import com.alibaba.polardbx.transaction.tso.ClusterTimestampOracle;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.alibaba.polardbx.common.constants.ServerVariables.MODIFIABLE_DEADLOCK_DETECTION_PARAM;
import static com.alibaba.polardbx.common.constants.ServerVariables.MODIFIABLE_PURGE_TRANS_PARAM;
import static com.alibaba.polardbx.gms.topology.SystemDbHelper.DEFAULT_DB_NAME;

/**
 * Transaction Manager
 *
 * @since 5.1.28
 */
@Activate(order = 1)
public class TransactionManager extends AbstractLifecycle implements ITransactionManager, ITransactionManagerUtil {

    private static final ConcurrentMap<String, TransactionManager> schemaMap = new ConcurrentHashMap<>();

    protected Map<String, Object> properties;
    protected String schemaName;
    protected StorageInfoManager storageManager;

    /**
     * cache of on-going transactions
     */
    private final ConcurrentMap<Long, ITransaction> trans = new ConcurrentHashMap<>();

    private final GlobalTxLogManager globalTxLogManager = new GlobalTxLogManager();

    private TransactionExecutor executor;

    private TimerTask xaRecoverTask;
    private TimerTask bestEffortRecoverTask;

    private RotateGlobalTxLogTaskWrapper cleanTask;

    private static DeadlockDetectionTaskWrapper deadlockDetectionTask;

    private TimerTask mdlDeadlockDetectionTask;
    private TimerTask killTimeoutTransactionTask;

    // Since heartbeat task must run on PolarDB-X instances, only one task is need
    private static TimerTask tsoHeartbeatTask;

    // Since purge tso task must run on PolarDB-X instances, only one task is need
    private static TimerTask tsoPurgeTask;

    private static final ITimestampOracle timestampOracle;

    // fair=true is necessary to prevent starvation
    private final ReadWriteLock lock = new ReentrantReadWriteLock(true);

    // Whether it is the first time a XA recover task runs.
    private volatile boolean firstRecover = true;

    public Boolean isFirstRecover() {
        return firstRecover;
    }

    public void setFirstRecover(boolean f) {
        firstRecover = f;
    }

    static {
        timestampOracle = new ClusterTimestampOracle();
        timestampOracle.init();
    }

    @Override
    protected void doInit() {

        // 事务执行器
        executor = (TransactionExecutor) ExecutorContext.getContext(schemaName).getTopologyExecutor();
        // 全局事务日志
        globalTxLogManager.setTransactionExecutor(executor);
        globalTxLogManager.init();

        schemaMap.put(schemaName.toLowerCase(), this);

        enableMdlDeadlockDetection();

        // Enable TSO heartbeat for PolarDB-X master instances
        if (ConfigDataMode.isMasterMode() && storageManager.supportTsoHeartbeat()) {
            enableTsoHeartbeat();
        }
        if (ConfigDataMode.isMasterMode() && storageManager.supportPurgeTso()) {
            enableTsoPurge();
        }

        // Schedule timer tasks.
        scheduleDeadlockDetectionTask();
    }

    public static TransactionManager getInstance(String schema) {
        return schemaMap.get(schema.toLowerCase());
    }

    public static void removeSchema(String schema) {
        schemaMap.remove(schema.toLowerCase());
    }

    @Override
    public void register(ITransaction transaction) {
        trans.put(transaction.getId(), transaction);
    }

    @Override
    public void unregister(long txid) {
        trans.remove(txid);
    }

    @Override
    public void prepare(String schemaName, Map<String, Object> properties, StorageInfoManager storageManager) {
        this.schemaName = schemaName;
        this.properties = properties;
        this.storageManager = storageManager;
    }

    @Override
    protected void doDestroy() {
        globalTxLogManager.destroy();
        if (cleanTask != null) {
            cleanTask.cancel();
            cleanTask = null;
        }
        if (xaRecoverTask != null) {
            xaRecoverTask.cancel();
            xaRecoverTask = null;
        }
        if (bestEffortRecoverTask != null) {
            bestEffortRecoverTask.cancel();
            bestEffortRecoverTask = null;
        }
        if (mdlDeadlockDetectionTask != null) {
            mdlDeadlockDetectionTask.cancel();
            mdlDeadlockDetectionTask = null;
        }
        super.doDestroy();
    }

    /**
     * 返回全局逻辑时钟, 可能为 null.
     */
    @Override
    public ITimestampOracle getTimestampOracle() {
        return timestampOracle;
    }

    @Override
    public ITransaction createTransaction(TransactionClass trxConfig, ExecutionContext executionContext) {
        String expectedTrxConfig = executionContext.getParamManager().getString(ConnectionParams.TRX_CLASS_REQUIRED);
        if (expectedTrxConfig != null && !expectedTrxConfig.equalsIgnoreCase(trxConfig.toString())) {
            throw new TddlRuntimeException(ErrorCode.ERR_TRANS,
                String.format("Incorrect transaction class, expected: %s, actual: %s", expectedTrxConfig, trxConfig));
        }

        ITransaction trx;

        switch (trxConfig) {
        case XA:
            // 如果启用了 DRDS XA 事务，定期检查 XA RECOVER
            enableXaRecoverScan();
            trx = new XATransaction(executionContext, this);
            break;
        case TSO:
            enableXaRecoverScan();
            trx = new TsoTransaction(executionContext, this);
            break;
        case AUTO_COMMIT_SINGLE_SHARD:
            boolean enableTrxSingleShardOptimization =
                executionContext.getParamManager().getBoolean(ConnectionParams.ENABLE_TRX_SINGLE_SHARD_OPTIMIZATION);
            if (enableTrxSingleShardOptimization) {
                final boolean lizard1PC = storageManager.supportLizard1PCTransaction();
                final boolean omitTso = storageManager.supportCtsTransaction() || lizard1PC;
                trx = new AutoCommitSingleShardTsoTransaction(executionContext, this, omitTso, lizard1PC);
                break;
            }
        case TSO_READONLY:
            trx = new ReadOnlyTsoTransaction(executionContext, this);
            break;
        case AUTO_COMMIT:
            trx = new AutoCommitTransaction(executionContext, this);
            break;
        case ALLOW_READ_CROSS_DB:
            trx = new AllowReadTransaction(executionContext, this);
            break;
        case MPP_READ_ONLY_TRANSACTION:
            trx = new MppReadOnlyTransaction(executionContext, this);
            break;
        default:
            throw new AssertionError("TransactionType: " + trxConfig.name() + " not supported");
        }
        enableKillTimeoutTransaction();
        // Schedule log cleaning task.
        enableLogCleanTask();

        // 配置会在执行器里设置
        return trx;
    }

    @Override
    public ConcurrentMap<Long, ITransaction> getTransactions() {
        return trans;
    }

    public TransactionExecutor getTransactionExecutor() {
        return executor;
    }

    public void setExecutor(TransactionExecutor executor) {
        this.executor = executor;
    }

    public GlobalTxLogManager getGlobalTxLogManager() {
        return globalTxLogManager;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void enableLogCleanTask() {
        if (ConfigDataMode.isFastMock() || SystemDbHelper.isDBBuildInExceptCdc(schemaName)) {
            return;
        }
        if (null == cleanTask) {
            synchronized (this) {
                if (null == cleanTask) {
                    try {
                        cleanTask = new RotateGlobalTxLogTaskWrapper(properties, schemaName, executor.getAsyncQueue(),
                            executor);
                    } catch (Throwable t) {
                        cleanTask = null;
                        TransactionLogger.warn("Failed to init log clean task.");
                    }
                }
            }
        }
    }

    private void scheduleDeadlockDetectionTask() {
        // Schedule deadlock detection task if it is null.
        if (null == deadlockDetectionTask) {
            synchronized (TransactionManager.class) {
                if (null == deadlockDetectionTask) {
                    try {
                        deadlockDetectionTask =
                            new DeadlockDetectionTaskWrapper(properties, schemaMap.keySet(), executor.getAsyncQueue());
                    } catch (Throwable t) {
                        deadlockDetectionTask = null;
                        TransactionLogger.warn("Failed to init deadlock detection task.");
                    }
                }
            }
        }
    }

    public void enableXaRecoverScan() {
        // stop xa schedule work
        if (ConfigDataMode.isFastMock() || SystemDbHelper.isDBBuildInExceptCdc(schemaName)) {
            return;
        }
        if (!executor.isXaAvailable()) {
            return;
        }
        if (xaRecoverTask == null) {
            synchronized (this) {
                if (xaRecoverTask == null) {
                    AsyncTaskQueue asyncQueue = executor.getAsyncQueue();

                    ParamManager paramManager = new ParamManager(properties);
                    int xaRecoverInterval = paramManager.getInt(ConnectionParams.XA_RECOVER_INTERVAL);

                    xaRecoverTask = asyncQueue.scheduleXARecoverTask(executor, xaRecoverInterval);
                }
            }
        }
    }

    void enableMdlDeadlockDetection() {
        if (ConfigDataMode.isFastMock() || SystemDbHelper.isDBBuildInExceptCdc(schemaName)) {
            return;
        }
        ParamManager paramManager = new ParamManager(properties);
        if (!storageManager.supportMdlDeadlockDetection()) {
            return;
        }
        if (mdlDeadlockDetectionTask == null) {
            synchronized (this) {
                if (mdlDeadlockDetectionTask == null) {
                    AsyncTaskQueue asyncQueue = executor.getAsyncQueue();

                    int interval =
                        5 * paramManager.getInt(ConnectionParams.DEADLOCK_DETECTION_INTERVAL); // reuse this variable
                    int mdlWaitTimeoutInSec =
                        paramManager.getInt(ConnectionParams.PHYSICAL_DDL_MDL_WAITING_TIMEOUT); // reuse this variable
                    mdlDeadlockDetectionTask =
                        asyncQueue.scheduleMdlDeadlockDetectionTask(executor, interval, mdlWaitTimeoutInSec);
                }
            }
        }
    }

    public void enableKillTimeoutTransaction() {
        if (ConfigDataMode.isFastMock() || SystemDbHelper.isDBBuildInExceptCdc(schemaName)) {
            return;
        }
        ParamManager paramManager = new ParamManager(properties);
        if (paramManager.getLong(ConnectionParams.MAX_TRX_DURATION) <= 0) {
            // Ignore timeout time less than 1s.
            return;
        }
        if (killTimeoutTransactionTask == null) {
            synchronized (this) {
                if (killTimeoutTransactionTask == null) {
                    AsyncTaskQueue asyncQueue = executor.getAsyncQueue();
                    int interval =
                        paramManager.getInt(ConnectionParams.DEADLOCK_DETECTION_INTERVAL); // reuse this variable
                    killTimeoutTransactionTask =
                        asyncQueue.scheduleKillTimeoutTransactionTask(executor, interval);
                }
            }
        }
    }

    void enableTsoHeartbeat() {
        if (ConfigDataMode.isFastMock() || SystemDbHelper.isDBBuildInExceptCdc(schemaName)) {
            return;
        }
        if (tsoHeartbeatTask == null) {
            synchronized (this) {
                if (tsoHeartbeatTask == null) {
                    AsyncTaskQueue asyncQueue = executor.getAsyncQueue();

                    ParamManager paramManager = new ParamManager(properties);
                    int heartbeatInterval = paramManager.getInt(ConnectionParams.TSO_HEARTBEAT_INTERVAL);

                    tsoHeartbeatTask =
                        asyncQueue.scheduleTsoHeartbeatTask(getTimestampOracle(), heartbeatInterval);
                }
            }
        }
    }

    void enableTsoPurge() {
        if (ConfigDataMode.isFastMock() || SystemDbHelper.isDBBuildInExceptCdc(schemaName)) {
            return;
        }
        if (tsoPurgeTask == null) {
            synchronized (this) {
                if (tsoPurgeTask == null) {
                    AsyncTaskQueue asyncQueue = executor.getAsyncQueue();

                    ParamManager paramManager = new ParamManager(properties);
                    int heartbeatInterval = paramManager.getInt(ConnectionParams.TSO_HEARTBEAT_INTERVAL);

                    tsoPurgeTask =
                        asyncQueue.scheduleTsoPurgeTask(heartbeatInterval);
                }
            }
        }
    }

    /**
     * Generate transaction id. Reuse the txid (traceId in int64) in execution context unless it's not set
     */
    @Override
    public long generateTxid(ExecutionContext executionContext) {
        if (executionContext.getTxId() != 0) {
            return executionContext.getTxId();
        } else {
            return TrxIdGenerator.getInstance().nextId();
        }
    }

    @Override
    public ITransactionPolicy getDefaultDistributedTrxPolicy(ExecutionContext context) {
        if (storageManager.supportTso()
            && context.getTxIsolation() > Connection.TRANSACTION_READ_COMMITTED) {
            return TransactionAttribute.DEFAULT_TRANSACTION_POLICY_POLARX;
        } else if (storageManager.supportXA()) {
            return TransactionAttribute.DEFAULT_TRANSACTION_POLICY;
        }
        return null;
    }

    /**
     * Cross-partition transaction must acquire shared lock
     */
    public Lock sharedLock() {
        return lock.readLock();
    }

    /**
     * Use to disable all cross-partition transactions
     */
    public Lock exclusiveLock() {
        return lock.writeLock();
    }

    public boolean isMdlDeadlockDetectionEnable() {
        return mdlDeadlockDetectionTask != null;
    }

    public void cancelMdlDeadlockDetection() {
        if (this.mdlDeadlockDetectionTask != null) {
            this.mdlDeadlockDetectionTask.cancel();
            this.mdlDeadlockDetectionTask = null;
        }
    }

    /**
     * get the min tso timestamp.
     */
    public long getMinSnapshotSeq() {

        long minSnapshotTime =
            getTimestampOracle().nextTimestamp() - (DynamicConfig.getInstance().getPurgeHistoryMs() << 22);
        for (Iterator<Map.Entry<Long, ITransaction>> iterator = trans.entrySet().iterator(); iterator.hasNext(); ) {
            ITransaction transaction = iterator.next().getValue();
            if (transaction instanceof ITsoTransaction) {
                if (!((ITsoTransaction) transaction).snapshotSeqIsEmpty()) {
                    minSnapshotTime =
                        Math.min(minSnapshotTime,
                            ((ITsoTransaction) transaction).getSnapshotSeq() - (1
                                << ClusterTimestampOracle.BitReserved));
                }
            }
        }
        return minSnapshotTime;
    }

    /**
     * @return All parameters of currently running timer tasks
     */
    public Map<String, String> getCurrentTimerTaskParams() {
        final Map<String, String> allCurrentParams = new HashMap<>(2);

        if (null != cleanTask) {
            final Map<String, String> actualParam = cleanTask.getCurrentParam();
            if (null != actualParam) {
                for (String paramName : MODIFIABLE_PURGE_TRANS_PARAM) {
                    allCurrentParams.put(paramName, actualParam.get(paramName));
                }
            }
        }

        if (null != deadlockDetectionTask) {
            final Map<String, String> actualParam = deadlockDetectionTask.getCurrentParam();
            if (null != actualParam) {
                for (String paramName : MODIFIABLE_DEADLOCK_DETECTION_PARAM) {
                    allCurrentParams.put(paramName, actualParam.get(paramName));
                }
            }
        }

        return allCurrentParams;
    }

    public void resetAllTimerTasks() {
        if (null == cleanTask) {
            cleanTask = new RotateGlobalTxLogTaskWrapper(properties, schemaName, executor.getAsyncQueue(), executor);
        } else {
            cleanTask.resetTask();
        }
        // Try to reset deadlock task in polardbx schema.
        if (DEFAULT_DB_NAME.equalsIgnoreCase(schemaName) || !schemaMap.containsKey(DEFAULT_DB_NAME)) {
            if (null == deadlockDetectionTask) {
                synchronized (TransactionManager.class) {
                    if (null == deadlockDetectionTask) {
                        try {
                            deadlockDetectionTask =
                                new DeadlockDetectionTaskWrapper(properties, schemaMap.keySet(),
                                    executor.getAsyncQueue());
                        } catch (Throwable t) {
                            deadlockDetectionTask = null;
                            TransactionLogger.warn("Failed to init deadlock detection task.");
                        }
                    }
                }
            } else {
                deadlockDetectionTask.resetTask();
            }
        }
    }

    /**
     * @return All active transactions in this schema, or in all schemas if schema is null.
     */
    public static Collection<ITransaction> getTransactions(String schema) {
        if (null != schema) {
            TransactionManager tm = TransactionManager.getInstance(schema);
            return tm == null ? new ArrayList<>() : tm.getTransactions().values();
        }

        // If schema is null, return all transactions in all schemas.
        final List<ITransaction> allTransactions = new ArrayList<>();
        for (TransactionManager tm : schemaMap.values()) {
            allTransactions.addAll(tm.getTransactions().values());
        }
        return allTransactions;
    }
}
