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
import com.alibaba.polardbx.transaction.log.GlobalTxLogManager;
import com.alibaba.polardbx.transaction.tso.ClusterTimestampOracle;

import java.sql.Connection;
import java.util.Iterator;
import java.util.Map;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Transaction Manager
 *
 * @author <a href="mailto:changyuan.lh@taobao.com">Changyuan.lh</a>
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
    private TimerTask cleanTask;
    private TimerTask deadlockDetectionTask;
    private TimerTask mdlDeadlockDetectionTask;
    private TimerTask killTimeoutTransactionTask;

    // Since heartbeat task must run on PolarDB-X instances, only one task is need
    private static TimerTask tsoHeartbeatTask;

    // Since purge tso task must run on PolarDB-X instances, only one task is need
    private static TimerTask tsoPurgeTask;

    private static final ITimestampOracle timestampOracle;

    // fair=true is necessary to prevent starvation
    private final ReadWriteLock lock = new ReentrantReadWriteLock(true);

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

        enableDeadlockDetection();
        enableMdlDeadlockDetection();

        // Enable TSO heartbeat for PolarDB-X master instances
        if (ConfigDataMode.isMasterMode() && storageManager.supportTsoHeartbeat()) {
            enableTsoHeartbeat();
        }
        if (ConfigDataMode.isMasterMode() && storageManager.supportPurgeTso()) {
            enableTsoPurge();
        }
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
        if (deadlockDetectionTask != null) {
            deadlockDetectionTask.cancel();
            deadlockDetectionTask = null;
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
                boolean omitTso = storageManager.supportCtsTransaction();
                trx = new AutoCommitSingleShardTsoTransaction(executionContext, this, omitTso);
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
            throw new AssertionError("TransactionType: " + trxConfig.name() + " not supproted");
        }

        // Schedule tasks to clean transaction logs
        enableAutoCleanTrans();
        enableKillTimeoutTransaction();

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

    private void enableAutoCleanTrans() {
        // stop xa schedule work
        if (ConfigDataMode.isFastMock() || SystemDbHelper.isDBBuildInExceptCdc(schemaName)) {
            return;
        }
        if (cleanTask == null) {
            synchronized (this) {
                if (cleanTask == null) {
                    AsyncTaskQueue asyncQueue = executor.getAsyncQueue();

                    ParamManager paramManager = new ParamManager(properties);
                    int purgeInterval = paramManager.getInt(ConnectionParams.PURGE_TRANS_INTERVAL);
                    int purgeBefore = paramManager.getInt(ConnectionParams.PURGE_TRANS_BEFORE);

                    cleanTask = asyncQueue.scheduleAutoCleanTask(executor, purgeInterval, purgeBefore);
                }
            }
        }
    }

    void enableXaRecoverScan() {
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

    void enableDeadlockDetection() {
        if (ConfigDataMode.isFastMock() || SystemDbHelper.isDBBuildInExceptCdc(schemaName)) {
            return;
        }
        ParamManager paramManager = new ParamManager(properties);
        if (!paramManager.getBoolean(ConnectionParams.ENABLE_DEADLOCK_DETECTION)) {
            return;
        }
        if (!storageManager.supportDeadlockDetection()) {
            return;
        }
        if (deadlockDetectionTask == null) {
            synchronized (this) {
                if (deadlockDetectionTask == null) {
                    AsyncTaskQueue asyncQueue = executor.getAsyncQueue();

                    int interval =
                        paramManager.getInt(ConnectionParams.DEADLOCK_DETECTION_INTERVAL); // reuse this variable

                    deadlockDetectionTask =
                        asyncQueue.scheduleDeadlockDetectionTask(executor, interval);
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

    void enableKillTimeoutTransaction() {
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
}
