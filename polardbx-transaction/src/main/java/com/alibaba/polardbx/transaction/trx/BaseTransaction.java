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

package com.alibaba.polardbx.transaction.trx;

import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.IConnection;
import com.alibaba.polardbx.common.jdbc.IDataSource;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.type.TransactionType;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.druid.sql.ast.SqlType;
import com.alibaba.polardbx.executor.spi.ITransactionManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.utils.ITransaction;
import com.alibaba.polardbx.optimizer.utils.ITransactionManagerUtil;
import com.alibaba.polardbx.optimizer.utils.InventoryMode;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import com.alibaba.polardbx.stats.CurrentTransactionStatistics;
import com.alibaba.polardbx.stats.TransactionStatistics;
import com.alibaba.polardbx.transaction.TransactionLogger;
import com.alibaba.polardbx.transaction.TransactionManager;

import java.sql.SQLException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import static com.alibaba.polardbx.common.jdbc.ITransactionPolicy.TransactionClass.SUPPORT_INVENTORY_TRANSACTION;

/**
 * Base class of non-distributed transaction
 */
public abstract class BaseTransaction implements ITransaction {

    private final static Logger logger = LoggerFactory.getLogger(BaseTransaction.class);

    protected final ReentrantLock lock = new ReentrantLock();
    protected long id;
    protected ExecutionContext executionContext;
    protected final int isolationLevel;
    protected final boolean autoCommit;
    /**
     * Whether this transaction is an async commit transaction.
     */
    protected boolean asyncCommit;
    /**
     * Whether this transaction is under committing.
     */
    protected boolean underCommitting;
    private volatile boolean closed;
    private ErrorCode errorCode;

    private String originalError;

    protected final ITransactionManager manager;

    // Transaction-level statistics.
    protected String statisticSchema = null;
    protected TransactionStatistics stat = new TransactionStatistics();

    protected boolean begun = false;

    protected InventoryMode inventoryMode;

    private final long idleTimeout;
    private final long idleROTimeout;
    private final long idleRWTimeout;

    protected boolean useTrxLogV2 = false;

    public BaseTransaction(ExecutionContext executionContext, ITransactionManager manager) {
        super();
        this.id = manager.generateTxid(executionContext);
        this.executionContext = executionContext;
        this.isolationLevel = executionContext.getTxIsolation();
        this.autoCommit = executionContext.isAutoCommit();
        this.manager = manager;
        this.underCommitting = false;
        this.asyncCommit = false;
        this.idleTimeout = executionContext.getIdleTrxTimeout();
        this.idleROTimeout = executionContext.getIdleROTrxTimeout();
        this.idleRWTimeout = executionContext.getIdleRWTrxTimeout();
        // Register to manager so that we can see it in SHOW TRANS
        manager.register(this);
    }

    @Override
    public IConnection getConnection(String schemaName, String group, IDataSource ds, RW rw) throws SQLException {
        return getConnection(schemaName, group, ds, rw, this.executionContext);
    }

    @Override
    public IConnection getConnection(String schemaName, String group, Long grpConnId, IDataSource ds, RW rw,
                                     ExecutionContext ec)
        throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void setExecutionContext(ExecutionContext executionContext) {
        this.executionContext = executionContext;
    }

    @Override
    public long getId() {
        return id; // 返回事务 ID
    }

    @Override
    public void close() {
        manager.unregister(id);
        this.closed = true;

        try {
            updateStatisticsWhenClosed();
        } catch (Throwable t) {
            logger.error("Collect trx statistics failed.", t);
        }
    }

    @Override
    public boolean isClosed() {
        return this.closed;
    }

    @Override
    public void kill() throws SQLException {
        lock.lock();
        try {
            stat.setIfUnknown(TransactionStatistics.Status.KILL);
            this.getConnectionHolder().kill();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void clearTrxContext() {
    }

    /**
     * If crucial error is set, current transaction can't continue or be
     * committed.
     */
    @Override
    public void setCrucialError(ErrorCode errorCode, String cause) {
        this.errorCode = errorCode;
        this.originalError = cause;
    }

    @Override
    public ErrorCode getCrucialError() {
        return this.errorCode;
    }

    /**
     * Check whether the transaction is destined to abort before commission.
     */
    @Override
    public void checkCanContinue() {
        // For now, only ERR_GLOBAL_SECONDARY_INDEX_CONTINUE_AFTER_WRITE_FAIL
        // could happen here.
        if (errorCode != null) {
            throw new TddlRuntimeException(errorCode, originalError);
        }
    }

    @Override
    public ExecutionContext getExecutionContext() {
        return executionContext;
    }

    @Override
    public State getState() {
        // For 1PC transactions there is only one state
        return State.RUNNING;
    }

    @Override
    public boolean isDistributed() {
        return false;
    }

    @Override
    public boolean isStrongConsistent() {
        return false;
    }

    public TransactionManager getManager() {
        return (TransactionManager) manager;
    }

    @Override
    public void setMdlWaitTime(long waitTime) {
        stat.mdlWaitTime += waitTime;
    }

    @Override
    public long getStartTimeInMs() {
        return stat.startTimeInMs;
    }

    @Override
    public void setStartTimeInMs(long startTime) {
        stat.startTimeInMs = startTime;
    }

    @Override
    public void setStartTime(long t) {
        stat.startTime = t;
    }

    @Override
    public void setSqlStartTime(long t) {
        stat.sqlStartTime = t;
    }

    @Override
    public void setSqlFinishTime(long t) {
        stat.sqlFinishTime = t;
    }

    @Override
    public boolean isBegun() {
        return begun;
    }

    @Override
    public InventoryMode getInventoryMode() {
        return inventoryMode;
    }

    @Override
    public void setInventoryMode(InventoryMode inventoryMode) {
        if (inventoryMode != null && !getTransactionClass().isA(SUPPORT_INVENTORY_TRANSACTION)) {
            throw new UnsupportedOperationException(
                "Don't support the Inventory Hint on current Transaction Policy, current transaction type is "
                    + getTransactionClass());
        } else {
            this.inventoryMode = inventoryMode;
        }
    }

    @Override
    public ITransactionManagerUtil getTransactionManagerUtil() {
        return this.manager;
    }

    @Override
    public boolean handleStatementError(Throwable t) {
        return false;
    }

    @Override
    public void releaseAutoSavepoint() {
        // do nothing
    }

    @Override
    public void releaseDirtyReadConnections() {
        // do nothing
    }

    @Override
    public boolean isUnderCommitting() {
        return underCommitting;
    }

    @Override
    public boolean isAsyncCommit() {
        return asyncCommit;
    }

    protected void recordTransaction() {
        if (!DynamicConfig.getInstance().isEnableTransactionStatistics()) {
            return;
        }
        Optional.ofNullable(OptimizerContext.getTransStat(statisticSchema)).ifPresent(stats -> {
            switch (getType()) {
            case XA:
                stats.countXA.incrementAndGet();
                break;
            case TSO:
            case TSO_MPP:
            case TSO_SSR:
            case TSO_RO:
                stats.countTSO.incrementAndGet();
                break;
            default:
                break;
            }
        });

        TransactionLogger.debug(id, getType() + " transaction begins");
    }

    @Override
    public TransactionStatistics getStat() {
        return stat;
    }

    private void updateStatisticsWhenClosed() {
        if (TransactionType.AUTO_COMMIT == getType()) {
            return;
        }

        if (stat.startTime <= 0) {
            return;
        }

        stat.durationTime = System.nanoTime() - stat.startTime;
        final long durationTimeInMs = stat.durationTime / 1000000;

        if (DynamicConfig.getInstance().isEnableTransactionStatistics()
            && DynamicConfig.getInstance().getSlowTransThreshold() < durationTimeInMs
            && executionContext.isUserSql()) {
            stat.finishTimeInMs = stat.startTimeInMs + durationTimeInMs;
            stat.readOnly = !isRwTransaction();
            stat.transactionType = getType();
            stat.sqlCount = null == executionContext.getSqlId() ? 0 : executionContext.getSqlId();
            stat.activeTime = stat.readTime + stat.writeTime;
            stat.idleTime = stat.durationTime - stat.activeTime - stat.commitTime - stat.rollbackTime;

            // Record slow trans in log, time unit is microsecond.
            SQLRecorderLogger.slowTransLogger.info(SQLRecorderLogger.slowTransFormat.format(new Object[] {
                "V1", executionContext.getTraceId(), stat.transactionType, String.valueOf(stat.startTimeInMs),
                String.valueOf(stat.finishTimeInMs), String.valueOf(stat.durationTime / 1000), stat.status,
                String.valueOf(stat.activeTime / 1000), String.valueOf(stat.idleTime / 1000),
                String.valueOf(stat.writeTime / 1000), String.valueOf(stat.readTime / 1000),
                String.valueOf(stat.writeAffectRows), String.valueOf(stat.readReturnRows),
                String.valueOf(stat.mdlWaitTime / 1000), String.valueOf(stat.getTsoTime / 1000),
                String.valueOf(stat.prepareTime / 1000), String.valueOf(stat.trxLogTime / 1000),
                String.valueOf(stat.commitTime / 1000), String.valueOf(stat.rollbackTime / 1000),
                String.valueOf(stat.sqlCount), String.valueOf(stat.rwSqlCount),
                Long.toHexString(stat.trxTemplate.hash()), String.valueOf(useTrxLogV2 ? 1 : 0)
            }));

            if (null != statisticSchema) {
                Optional.ofNullable(OptimizerContext.getTransStat(statisticSchema))
                    .ifPresent(s -> s.addSlowTrans(stat));
            }
        }
    }

    @Override
    public void updateCurrentStatistics(CurrentTransactionStatistics currentStat,
                                        long durationTimeMs) {
        // MUST NOT access not-thread-safe variables.
        if (isClosed()) {
            return;
        }
        switch (getType()) {
        case TSO_MPP:
        case TSO_RO:
        case TSO_SSR:
            currentStat.countSlow++;
            currentStat.countSlowRO++;
            currentStat.durationTimeRO.add(durationTimeMs);
            currentStat.durationTime.add(durationTimeMs);
            break;
        }
    }

    @Override
    public void updateStatisticsWhenStatementFinished(AtomicLong rowCount) {
        long currentTime = System.nanoTime();
        long logicalSqlDurationTime =
            currentTime - executionContext.getLogicalSqlStartTime();

        if (SqlType.isDML(executionContext.getSqlType())) {
            stat.writeTime += logicalSqlDurationTime;
            stat.writeAffectRows += rowCount.get();
        } else {
            stat.readTime += logicalSqlDurationTime;
            stat.readReturnRows += rowCount.get();
        }
        stat.rwSqlCount++;
        if (null != executionContext && null != executionContext.getSqlTemplateId()) {
            stat.trxTemplate.update(executionContext.getSqlTemplateId().getBytes());
        }
        stat.sqlFinishTime = currentTime;
    }

    @Override
    public void setLastActiveTime() {
        this.stat.lastActiveTime = System.nanoTime();
    }

    @Override
    public long getLastActiveTime() {
        return this.stat.lastActiveTime;
    }

    @Override
    public void resetLastActiveTime() {
        this.stat.lastActiveTime = 0;
    }

    @Override
    public long getIdleTimeout() {
        return idleTimeout;
    }

    @Override
    public long getIdleROTimeout() {
        return idleROTimeout;
    }

    @Override
    public long getIdleRWTimeout() {
        return idleRWTimeout;
    }
}
