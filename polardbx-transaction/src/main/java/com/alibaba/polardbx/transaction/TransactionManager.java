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
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.utils.extension.Activate;
import com.alibaba.polardbx.common.utils.version.InstanceVersion;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.common.StorageInfoManager;
import com.alibaba.polardbx.executor.spi.ITransactionManager;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.utils.ITimestampOracle;
import com.alibaba.polardbx.optimizer.utils.ITransaction;
import com.alibaba.polardbx.optimizer.utils.ITransactionManagerUtil;
import com.alibaba.polardbx.transaction.async.AsyncTaskQueue;
import com.alibaba.polardbx.transaction.async.DeadlockDetectionTaskWrapper;
import com.alibaba.polardbx.transaction.async.RotateGlobalTxLogTaskWrapper;
import com.alibaba.polardbx.transaction.async.SyncPointTaskWrapper;
import com.alibaba.polardbx.transaction.async.TransactionIdleTimeoutTaskWrapper;
import com.alibaba.polardbx.transaction.async.TransactionStatisticsTaskWrapper;
import com.alibaba.polardbx.transaction.log.GlobalTxLogManager;
import com.alibaba.polardbx.transaction.trx.AllowReadTransaction;
import com.alibaba.polardbx.transaction.trx.ArchiveTransaction;
import com.alibaba.polardbx.transaction.trx.AsyncCommitTransaction;
import com.alibaba.polardbx.transaction.trx.AutoCommitSingleShardTsoTransaction;
import com.alibaba.polardbx.transaction.trx.AutoCommitTransaction;
import com.alibaba.polardbx.transaction.trx.AutoCommitTsoTransaction;
import com.alibaba.polardbx.transaction.trx.BestEffortTransaction;
import com.alibaba.polardbx.transaction.trx.CobarStyleTransaction;
import com.alibaba.polardbx.transaction.trx.ITsoTransaction;
import com.alibaba.polardbx.transaction.trx.IgnoreBinlogTransaction;
import com.alibaba.polardbx.transaction.trx.MppReadOnlyTransaction;
import com.alibaba.polardbx.transaction.trx.ReadOnlyTsoTransaction;
import com.alibaba.polardbx.transaction.trx.SyncPointTransaction;
import com.alibaba.polardbx.transaction.trx.TsoTransaction;
import com.alibaba.polardbx.transaction.trx.XATransaction;
import com.alibaba.polardbx.transaction.trx.XATsoTransaction;
import com.alibaba.polardbx.transaction.tso.ClusterTimestampOracle;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.StampedLock;

import static com.alibaba.polardbx.common.constants.ServerVariables.MODIFIABLE_DEADLOCK_DETECTION_PARAM;
import static com.alibaba.polardbx.common.constants.ServerVariables.MODIFIABLE_PURGE_TRANS_PARAM;
import static com.alibaba.polardbx.common.constants.ServerVariables.MODIFIABLE_TRANSACTION_STATISTICS_PARAM;
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

    private static TransactionStatisticsTaskWrapper transactionStatisticsTask;

    private static TransactionIdleTimeoutTaskWrapper transactionIdleTimeoutTask;

    private static SyncPointTaskWrapper syncPointTask;

    private static TimerTask mdlDeadlockDetectionTask;
    private TimerTask killTimeoutTransactionTask;

    // Since heartbeat task must run on PolarDB-X instances, only one task is need
    private static TimerTask tsoHeartbeatTask;

    // Since purge tso task must run on PolarDB-X instances, only one task is need
    private static volatile TimerTask tsoPurgeTask;

    private static final ITimestampOracle timestampOracle;

    private final StampedLock lock = new StampedLock();

    // Whether it is the first time a XA recover task runs.
    private volatile boolean firstRecover = true;

    public Boolean isFirstRecover() {
        return firstRecover;
    }

    public void setFirstRecover(boolean f) {
        firstRecover = f;
    }

    private static final AtomicInteger asyncCommitTasks = new AtomicInteger(0);

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
        if (ConfigDataMode.needInitMasterModeResource()
            && storageManager.supportTsoHeartbeat()) {
            enableTsoHeartbeat();
        }
        if (ConfigDataMode.needInitMasterModeResource()
            && storageManager.supportPurgeTso()) {
            enableTsoPurge();
        }

        // Schedule timer tasks.
        scheduleTimerTask();
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
        if (killTimeoutTransactionTask != null) {
            killTimeoutTransactionTask.cancel();
            killTimeoutTransactionTask = null;
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
            if ((supportXaTso() || InstanceVersion.isMYSQL80())
                && executionContext.isEnableXaTso()) {
                trx = new XATsoTransaction(executionContext, this);
            } else {
                trx = new XATransaction(executionContext, this);
            }
            break;
        case TSO:
            if (storageManager.isSupportSyncPoint() && executionContext.isMarkSyncPoint()) {
                trx = new SyncPointTransaction(executionContext, this);
            } else if (executionContext.enableAsyncCommit() && supportAsyncCommit()) {
                trx = new AsyncCommitTransaction(executionContext, this);
            } else {
                trx = new TsoTransaction(executionContext, this);
            }
            break;
        case AUTO_COMMIT_SINGLE_SHARD:
            boolean enableTrxSingleShardOptimization =
                executionContext.getParamManager().getBoolean(ConnectionParams.ENABLE_TRX_SINGLE_SHARD_OPTIMIZATION);
            if (enableTrxSingleShardOptimization) {
                final boolean lizard1PC = storageManager.supportLizard1PCTransaction();
                final boolean omitTso = storageManager.supportCtsTransaction() || lizard1PC;
                trx = new AutoCommitSingleShardTsoTransaction(executionContext, this, omitTso, lizard1PC);
            } else {
                trx = new TsoTransaction(executionContext, this);
            }
            break;
        case TSO_READONLY:
            trx = new ReadOnlyTsoTransaction(executionContext, this);
            break;
        case AUTO_COMMIT:
            if (storageManager.isSupportMarkDistributed() && executionContext.isEnableAutoCommitTso()) {
                trx = new AutoCommitTsoTransaction(executionContext, this);
            } else {
                trx = new AutoCommitTransaction(executionContext, this);
            }
            break;
        case ALLOW_READ_CROSS_DB:
            trx = new AllowReadTransaction(executionContext, this);
            break;
        case MPP_READ_ONLY_TRANSACTION:
            trx = new MppReadOnlyTransaction(executionContext, this);
            break;
        case COLUMNAR_READ_ONLY_TRANSACTION:
            trx = new ColumnarTransaction(executionContext, this);
            break;
        case ARCHIVE:
            trx = new ArchiveTransaction(executionContext, this);
            break;
        case IGNORE_BINLOG_TRANSACTION:
            trx = new IgnoreBinlogTransaction(executionContext, this);
            break;
        default:
            throw new AssertionError("TransactionType: " + trxConfig.name() + " not supported");
        }

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
        if (!ConfigDataMode.needDNResource() || SystemDbHelper.isDBBuildInExceptCdc(schemaName)) {
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

    private void scheduleTransactionIdleTimeoutTask() {
        // Schedule deadlock detection task if it is null.
        if (null == transactionIdleTimeoutTask) {
            synchronized (TransactionManager.class) {
                if (null == transactionIdleTimeoutTask) {
                    try {
                        transactionIdleTimeoutTask =
                            new TransactionIdleTimeoutTaskWrapper(properties, executor.getAsyncQueue());
                    } catch (Throwable t) {
                        transactionIdleTimeoutTask = null;
                        TransactionLogger.warn("Failed to init transaction idle timeout task.");
                    }
                }
            }
        }
    }

    public void scheduleTransactionStatisticsTask() {
        if (null == transactionStatisticsTask) {
            synchronized (TransactionManager.class) {
                if (null == transactionStatisticsTask) {
                    try {
                        transactionStatisticsTask =
                            new TransactionStatisticsTaskWrapper(properties, executor.getAsyncQueue());
                    } catch (Throwable t) {
                        transactionStatisticsTask = null;
                        TransactionLogger.warn("Failed to init deadlock detection task.");
                    }
                }
            }
        }
    }

    public void scheduleSyncPointTask() {
        if (null == syncPointTask) {
            synchronized (TransactionManager.class) {
                if (null == syncPointTask) {
                    try {
                        syncPointTask = new SyncPointTaskWrapper(properties, executor.getAsyncQueue());
                    } catch (Throwable t) {
                        syncPointTask = null;
                        TransactionLogger.warn("Failed to init sync point task.");
                    }
                }
            }
        }
    }

    public void enableXaRecoverScan() {
        // stop xa schedule work
        if (!ConfigDataMode.needDNResource() || SystemDbHelper.isDBBuildInExceptCdc(schemaName)) {
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

                    xaRecoverTask = asyncQueue.scheduleXARecoverTask(executor, xaRecoverInterval, supportAsyncCommit());
                }
            }
        }
    }

    public void enableBestEffortRecoverScan() {
        // stop xa schedule work
        if (!ConfigDataMode.needDNResource() || SystemDbHelper.isDBBuildInExceptCdc(schemaName)) {
            return;
        }
        if (bestEffortRecoverTask == null) {
            synchronized (this) {
                if (bestEffortRecoverTask == null) {
                    AsyncTaskQueue asyncQueue = executor.getAsyncQueue();

                    ParamManager paramManager = new ParamManager(properties);
                    int interval = paramManager.getInt(ConnectionParams.XA_RECOVER_INTERVAL); // reuse this variable

                    bestEffortRecoverTask =
                        asyncQueue.scheduleBestEffortRecoverTask(executor, globalTxLogManager, interval);
                }
            }
        }
    }

    void enableMdlDeadlockDetection() {
        if (!ConfigDataMode.needDNResource() || SystemDbHelper.isDBBuildInExceptCdc(schemaName)) {
            return;
        }
        ParamManager paramManager = new ParamManager(properties);
        if (!storageManager.supportMdlDeadlockDetection()) {
            return;
        }
        if (mdlDeadlockDetectionTask == null) {
            synchronized (TransactionManager.class) {
                if (mdlDeadlockDetectionTask == null) {
                    AsyncTaskQueue asyncQueue = executor.getAsyncQueue();

                    int interval =
                        5 * paramManager.getInt(ConnectionParams.DEADLOCK_DETECTION_INTERVAL); // reuse this variable
                    int mdlWaitTimeoutInSec =
                        paramManager.getInt(ConnectionParams.PHYSICAL_DDL_MDL_WAITING_TIMEOUT); // reuse this variable
                    // keySet is a shadow copy of schemaMap's key, which can refer to comment keySet
                    // So if any schema removed or added, they will be mapped into mdlDeadlockDetectionTask
                    mdlDeadlockDetectionTask =
                        asyncQueue.scheduleMdlDeadlockDetectionTask(executor, interval, schemaMap.keySet(),
                            mdlWaitTimeoutInSec);
                }
            }
        }
    }

    public void enableKillTimeoutTransaction() {
        if (!ConfigDataMode.needDNResource() || SystemDbHelper.isDBBuildInExceptCdc(schemaName)) {
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
        if (!ConfigDataMode.needDNResource() || SystemDbHelper.isDBBuildInExceptCdc(schemaName)) {
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
        if (!ConfigDataMode.needDNResource() || SystemDbHelper.isDBBuildInExceptCdc(schemaName)) {
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

    public StampedLock getLock() {
        return lock;
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
    @Override
    public long getMinSnapshotSeq() {

        long minSnapshotTime =
            getTimestampOracle().nextTimestamp() - (DynamicConfig.getInstance().getPurgeHistoryMs() << 22);
        for (Iterator<Map.Entry<Long, ITransaction>> iterator = trans.entrySet().iterator(); iterator.hasNext(); ) {
            ITransaction transaction = iterator.next().getValue();
            if (transaction instanceof ITsoTransaction) {
                if (transaction instanceof ColumnarTransaction) {
                    // skip columnar transaction
                    continue;
                }
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

    @Override
    public long getColumnarMinSnapshotSeq() {
        long minSnapshotSeq = Long.MAX_VALUE;
        for (ITransaction transaction : trans.values()) {
            if (transaction instanceof ColumnarTransaction) {
                if (!((ITsoTransaction) transaction).snapshotSeqIsEmpty()) {
                    minSnapshotSeq = Math.min(minSnapshotSeq, ((ITsoTransaction) transaction).getSnapshotSeq());
                }
            }
        }
        return minSnapshotSeq;
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

        if (null != transactionStatisticsTask) {
            final Map<String, String> actualParam = transactionStatisticsTask.getCurrentParam();
            if (null != actualParam) {
                for (String paramName : MODIFIABLE_TRANSACTION_STATISTICS_PARAM) {
                    allCurrentParams.put(paramName, actualParam.get(paramName));
                }
            }
        }

        return allCurrentParams;
    }

    public void resetAllTimerTasks() {
        if (null == cleanTask) {
            enableLogCleanTask();
        } else {
            cleanTask.resetTask();
        }

        // Try to reset deadlock task in polardbx schema.
        if (DEFAULT_DB_NAME.equalsIgnoreCase(schemaName) || !schemaMap.containsKey(DEFAULT_DB_NAME)) {
            if (null == deadlockDetectionTask) {
                scheduleDeadlockDetectionTask();
            } else {
                deadlockDetectionTask.resetTask();
            }

            if (null == transactionStatisticsTask) {
                scheduleTransactionStatisticsTask();
            } else {
                transactionStatisticsTask.resetTask();
            }

            if (null == transactionIdleTimeoutTask) {
                scheduleTransactionIdleTimeoutTask();
            } else {
                transactionIdleTimeoutTask.resetTask();
            }

            if (null == syncPointTask) {
                scheduleSyncPointTask();
            } else {
                syncPointTask.resetTask();
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

    @Override
    public boolean supportAsyncCommit() {
        return storageManager.supportAsyncCommit();
    }


    public static boolean isExceedAsyncCommitTaskLimit() {
        return asyncCommitTasks.get() >= InstConfUtil.getInt(ConnectionParams.ASYNC_COMMIT_TASK_LIMIT);
    }

    public static void addAsyncCommitTask() {
        asyncCommitTasks.incrementAndGet();
    }

    public static void finishAsyncCommitTask() {
        asyncCommitTasks.decrementAndGet();
    }

    public static boolean shouldWriteEventLog(long lastLogTime) {
        // Write event log every 7 * 24 hours.
        return lastLogTime == 0 || ((System.nanoTime() - lastLogTime) / 1000000000) > 604800;
    }

    public static void turnOffAutoSavepointOpt() throws SQLException {
        // Set global.
        Properties properties = new Properties();
        properties.setProperty(ConnectionProperties.ENABLE_X_PROTO_OPT_FOR_AUTO_SP, "false");
        MetaDbUtil.setGlobal(properties);
    }

    @Override
    public void scheduleTimerTask() {
        enableXaRecoverScan();
        enableKillTimeoutTransaction();
        enableLogCleanTask();
        scheduleDeadlockDetectionTask();
        scheduleTransactionStatisticsTask();
        scheduleTransactionIdleTimeoutTask();
        scheduleSyncPointTask();
    }

    @Override
    public boolean supportXaTso() {
        return storageManager.isSupportMarkDistributed();
    }
}
