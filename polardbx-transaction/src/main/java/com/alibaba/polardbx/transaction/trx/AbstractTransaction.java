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

import com.alibaba.polardbx.common.constants.TransactionAttribute;
import com.alibaba.polardbx.common.eventlogger.EventLogger;
import com.alibaba.polardbx.common.eventlogger.EventType;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.IConnection;
import com.alibaba.polardbx.common.jdbc.IDataSource;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.thread.LockUtils;
import com.alibaba.polardbx.druid.sql.ast.SqlType;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.server.IServerConfigManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.utils.FailureInjectionFlag;
import com.alibaba.polardbx.optimizer.utils.GroupConnId;
import com.alibaba.polardbx.optimizer.utils.IDistributedTransaction;
import com.alibaba.polardbx.stats.CurrentTransactionStatistics;
import com.alibaba.polardbx.stats.TransactionStatistics;
import com.alibaba.polardbx.transaction.TransactionLogger;
import com.alibaba.polardbx.transaction.TransactionManager;
import com.alibaba.polardbx.transaction.async.AsyncTaskQueue;
import com.alibaba.polardbx.transaction.connection.TransactionConnectionHolder;
import com.alibaba.polardbx.transaction.jdbc.DeferredConnection;
import com.alibaba.polardbx.transaction.jdbc.SavePoint;
import com.alibaba.polardbx.transaction.log.ConnectionContext;
import com.alibaba.polardbx.transaction.log.GlobalTxLogManager;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.StampedLock;
import java.util.regex.Pattern;

import static com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_CONTINUE_AFTER_WRITE_FAIL;
import static com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_TRANS_CONTINUE_AFTER_WRITE_FAIL;
import static com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_TRANS_ROLLBACK_STATEMENT_FAIL;
import static com.alibaba.polardbx.transaction.jdbc.DeferredConnection.INVALID_AUTO_SAVEPOINT;

/**
 * Base class of Distributed Transaction
 *
 * @author eric.fy
 * @since 5.1.28
 */
public abstract class AbstractTransaction extends BaseTransaction implements IDistributedTransaction {

    private final static Logger logger = LoggerFactory.getLogger(AbstractTransaction.class);

    protected final TransactionConnectionHolder connectionHolder;
    protected final Map<String, IDataSource> dataSourceCache = new HashMap<>();
    protected final List<String> savepoints = new ArrayList<>();

    /**
     * 事务是否跨库
     */
    protected boolean isCrossGroup = false;

    /**
     * 事务的主库
     */
    protected String primaryGroup = null;

    protected long primaryGroupUid;
    protected IConnection primaryConnection = null;
    protected GroupConnId primaryGrpConnId = null;
    protected ConnectionContext connectionContext = null;

    protected String primarySchema = null;
    protected Set<String> otherSchemas = new HashSet<>();

    protected final GlobalTxLogManager globalTxLogManager;

    protected boolean disabled = false;

    protected FailureInjectionFlag failureFlag = FailureInjectionFlag.EMPTY;

    protected volatile boolean prepared = false;

    protected volatile State state = State.RUNNING;

    // Max trx duration, in seconds.
    private final long maxTime;

    // Only rollback statement when the following errors occur.
    private static final List<Pattern> canRollbackStatementErrors = ImmutableList.of(
        Pattern.compile(".*Lock wait timeout exceeded; try restarting transaction.*"),
        Pattern.compile(".*Duplicate entry.*"),
        Pattern.compile(".*Data too long for column.*"),
        Pattern.compile(".*Out of range value for column.*"),
        Pattern.compile(".*\\[TDDL-4602]\\[ERR_CONVERTOR].*"),
        Pattern.compile(".*Incorrect datetime value.*"),
        Pattern.compile(".*Incorrect time value.*"),
        Pattern.compile(".*Data truncated for column.*"),
        Pattern.compile(".*doesn't have a default value.*"),
        Pattern.compile(".*Cannot delete or update a parent row: a foreign key constraint fails.*"),
        Pattern.compile(".*Option SET_DEFAULT.*"),
        Pattern.compile(".*Column .* cannot be null.*")
    );

    public AbstractTransaction(ExecutionContext executionContext, TransactionManager manager) {
        super(executionContext, manager);
        this.connectionHolder =
            new TransactionConnectionHolder(this, lock, executionContext);
        this.globalTxLogManager = manager.getGlobalTxLogManager();

        // Limits.
        maxTime = GeneralUtil.getPropertyLong(executionContext.getServerVariables(),
            ConnectionProperties.MAX_TRX_DURATION.toLowerCase(),
            executionContext.getParamManager().getLong(ConnectionParams.MAX_TRX_DURATION));
    }

    @Override
    public boolean isCrossGroup() {
        return isCrossGroup;
    }

    @Override
    public String getPrimaryGroup() {
        return primaryGroup;
    }

    public String getTraceId() {
        return Long.toHexString(id);
    }

    @Override
    public void setFailureFlag(FailureInjectionFlag failureFlag) {
        this.failureFlag = failureFlag;
    }

    @Override
    public TransactionConnectionHolder getConnectionHolder() {
        return connectionHolder;
    }

    public long getMaxTime() {
        return maxTime;
    }

    public long getPrimaryGroupUid() {
        return primaryGroupUid;
    }

    @Override
    public IConnection getConnection(String schema, String group, IDataSource ds, RW rw, ExecutionContext ec)
        throws SQLException {
        return getConnection(schema, group, 0L, ds, rw, ec);
    }

    @Override
    public boolean isRwTransaction() {
        return null != primarySchema;
    }

    @Override
    public IConnection getConnection(String schema, String group, Long grpConnId, IDataSource ds, RW rw,
                                     ExecutionContext ec)
        throws SQLException {
        if (group == null) {
            throw new IllegalArgumentException("group name is null");
        }

        if (isClosed()) {
            throw new TddlRuntimeException(ErrorCode.ERR_QUERY_CANCLED, group);
        }

        Lock lock = this.lock;
        lock.lock();

        try {
            checkTerminated();

            if (!begun) {
                statisticSchema = schema;
                recordTransaction();
                begun = true;
            }

            boolean shouldHoldConnection = connectionHolder.shouldHoldConnection(rw);
            boolean shouldParticipateTransaction = connectionHolder.shouldParticipateTransaction(rw, group, grpConnId);
            boolean requirePrimaryGroup =
                shouldHoldConnection && shouldParticipateTransaction;

            if (requirePrimaryGroup && !isCrossGroup && primaryGroup == null) {
                primaryGroup = group;
                primarySchema = schema;
                primaryGroupUid = IServerConfigManager.getGroupUniqueId(schema, group);
                recordRwTransaction();
            }

            IConnection conn = connectionHolder.getConnection(schema, group, grpConnId, ds, rw);
            GroupConnId newGrpConnId = new GroupConnId(group, grpConnId);
            dataSourceCache.put(group, ds);

            if (requirePrimaryGroup) {
                if (!isCrossGroup) {
                    if (primaryConnection == null) {
                        primaryConnection = conn;
                        primaryGrpConnId = newGrpConnId;
                    } else if (!checkIfCrossGroup(newGrpConnId)) {
                        if (inventoryMode != null && inventoryMode.isInventoryHint()) {
                            inventoryMode.resetInventoryMode();
                            throw new TddlRuntimeException(ErrorCode.ERR_IVENTORY_HINT_NOT_SUPPORT_CROSS_SHARD,
                                "Inventory hint is not allowed when the transaction involves more than one group!");
                        }
                        beginCrossGroup();
                    }
                } else {
                    if (inventoryMode != null && inventoryMode.isInventoryHint()) {
                        inventoryMode.resetInventoryMode();
                        throw new TddlRuntimeException(ErrorCode.ERR_IVENTORY_HINT_NOT_SUPPORT_CROSS_SHARD,
                            "Inventory hint is not allowed when the transaction involves more than one group!");
                    }
                    if (!primarySchema.equals(schema)) {
                        otherSchemas.add(schema);
                    }
                }
            }

            return conn;
        } finally {
            lock.unlock();
        }
    }

    private boolean checkIfCrossGroup(GroupConnId grpConnId) {
        if (primaryGrpConnId == null) {
            return false;
        }
        return primaryGrpConnId.equals(grpConnId);
    }

    /**
     * 事务开始写入
     */
    private void recordRwTransaction() {
        if (!DynamicConfig.getInstance().isEnableTransactionStatistics()) {
            return;
        }
        Optional.ofNullable(OptimizerContext.getTransStat(primarySchema)).ifPresent(s -> {
            switch (getType()) {
            case XA:
                s.countXARW.incrementAndGet();
                break;
            case TSO:
                s.countTSORW.incrementAndGet();
                break;
            default:
                break;
            }
        });

        TransactionLogger.debug(id, getType() + " rw transaction begins");
    }

    @Override
    public void updateCurrentStatistics(CurrentTransactionStatistics currentStat, long durationTimeMs) {
        // MUST NOT access not-thread-safe variables.
        if (isClosed()) {
            return;
        }
        currentStat.durationTime.add(durationTimeMs);
        currentStat.countSlow++;
        if (isRwTransaction()) {
            currentStat.countSlowRW++;
            currentStat.durationTimeRW.add(durationTimeMs);
        } else {
            currentStat.countSlowRO++;
            currentStat.durationTimeRO.add(durationTimeMs);
        }
    }

    public void beginNonParticipant(String group, IConnection conn) throws SQLException {
        // Compatible with MaxScale. MaxScale will send START TRANSACTION READ ONLY to RO,
        // then send ROLLBACK;INSERT... to RW because they cannot handle multi statement correctly.
        conn.executeLater("BEGIN");
        // conn.executeLater("START TRANSACTION READ ONLY");
    }

    public void rollbackNonParticipant(String group, IConnection conn) {
        try {
            conn.executeLater("ROLLBACK");
        } catch (Throwable e) {
            logger.error("Rollback non-participant group failed on " + group, e);
            conn.discard(e);
        }
    }

    protected void rollbackNonParticipantSync(String group, IConnection conn) {
        try {
            conn.forceRollback();
        } catch (Throwable e) {
            logger.error("Rollback non-participant group failed on " + group, e);
        }
    }

    /**
     * 开启跨库事务
     */
    private void beginCrossGroup() {
        Lock lock = this.lock;
        lock.lock();
        try {
            if (isCrossGroup) {
                return;
            }

            isCrossGroup = true;

            if (this instanceof BestEffortTransaction) {
                // 传递连接的上下文信息
                ConnectionContext context = new ConnectionContext();
                Map<String, Object> variables = executionContext.getServerVariables();
                if (variables != null) {
                    // shadow copy
                    variables = new HashMap<>(executionContext.getServerVariables());
                } else {
                    variables = new HashMap<>();
                }
                context.setVariables(variables);
                this.connectionContext = context;
            }

        } finally {
            lock.unlock();
        }

        TransactionLogger.debug(id, "Cross-group enabled. Primary group: {0}", primaryGroup);
    }

    /**
     * Do cleanup on all held connections. Read connection don't need cleanup.
     */
    public final void cleanupAllConnections() {
        forEachHeldConnection((heldConn) -> {

            if (!isCrossGroup && (inventoryMode != null && inventoryMode.isRollbackOnFail())) {
                return;
            }
            innerCleanupAllConnections(heldConn.getGroup(), heldConn.getRawConnection(), heldConn.getParticipated());
        });
    }

    protected abstract void innerCleanupAllConnections(String group, IConnection conn,
                                                       TransactionConnectionHolder.ParticipatedState participatedState);

    protected void cleanupParticipateConn(String group, IConnection conn) {
        try {
            cleanup(group, conn);
        } catch (Throwable e) {
            conn.discard(e);
            logger.warn("Cleanup trans failed on group " + group, e);
        }
    }

    @Override
    public void tryClose(IConnection conn, String group) throws SQLException {
        if (group == null) {
            throw new IllegalArgumentException("group name is null");
        }
        connectionHolder.tryClose(conn, group);
    }

    /**
     * Stop all opened statements and mark this transaction as 'killed' (so cause exception later)
     */
    @Override
    public void kill() {
        lock.lock();
        try {
            if (isUnderCommitting()) {
                TransactionLogger.info(id, "[TSO][Async Commit] Kill trx failed since it is committing");
                return;
            }

            if (!isClosed()) {
                stat.setIfUnknown(TransactionStatistics.Status.KILL);

                disabled = true;

                connectionHolder.kill();

                TransactionLogger.warn(id, "Killed");
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void tryClose() throws SQLException {
        if (isClosed()) {
            return;
        }

        // TConnection 每次执行完语句会调用 tryClose
        if (autoCommit) {
            rollback();
        }
    }

    @Override
    public void close() {
        lock.lock();
        try {
            if (isUnderCommitting()) {
                TransactionLogger.debug(id, "[TSO][Async Commit] Close trx failed since it is in async commit");
                return;
            }

            try {
                super.close();
            } catch (Throwable t) {
                logger.error("Call BaseTransaction.close() failed.", t);
            }

            try {
                // Do cleanup on all held connections.
                cleanupAllConnections();
            } catch (Throwable t) {
                logger.error("Clean up connections failed.", t);
            }

            try {
                // Close connections here in case that transaction was not
                // committed, or interrupted during committing
                connectionHolder.closeAllConnections();
            } catch (Throwable t) {
                logger.error("Close connections failed.", t);
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void savepoint(final String savepoint) {
        Lock lock = this.lock;
        lock.lock();

        try {
            forEachHeldConnection(new TransactionConnectionHolder.Action() {
                @Override
                public boolean condition(TransactionConnectionHolder.HeldConnection heldConn) {
                    return heldConn.isParticipated();
                }

                @Override
                public void execute(TransactionConnectionHolder.HeldConnection heldConn) {
                    try {
                        SavePoint.set(heldConn.getRawConnection(), savepoint);
                    } catch (Throwable e) {
                        logger.error("Set savepoint failed, connection is " + heldConn.getRawConnection(), e);
                        throw GeneralUtil.nestedException(e);
                    }
                }
            });
            savepoints.add(savepoint);
        } catch (RuntimeException e) {
            kill(); // 直接 KILL (fast-fail), 防止用户再执行语句
            throw e;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void rollbackTo(final String savepoint) {
        Lock lock = this.lock;
        lock.lock();

        try {
            forEachHeldConnection(new TransactionConnectionHolder.Action() {
                @Override
                public boolean condition(TransactionConnectionHolder.HeldConnection heldConn) {
                    return heldConn.isParticipated();
                }

                @Override
                public void execute(TransactionConnectionHolder.HeldConnection heldConn) {
                    try {
                        SavePoint.rollback(heldConn.getRawConnection(), savepoint);
                    } catch (Throwable e) {
                        logger.error(
                            "Transaction rollback to savepoint failed, connection is " + heldConn.getRawConnection(),
                            e);
                        throw GeneralUtil.nestedException(e);
                    }
                }
            });
        } catch (RuntimeException e) {
            kill(); // 直接 KILL (fast-fail), 防止用户再执行语句
            throw e;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void release(final String savepoint) {
        Lock lock = this.lock;
        lock.lock();

        try {
            forEachHeldConnection(new TransactionConnectionHolder.Action() {
                @Override
                public boolean condition(TransactionConnectionHolder.HeldConnection heldConn) {
                    return heldConn.isParticipated();
                }

                @Override
                public void execute(TransactionConnectionHolder.HeldConnection heldConn) {
                    try {
                        SavePoint.release(heldConn.getRawConnection(), savepoint);
                    } catch (Throwable e) {
                        logger.error("Transaction rollback failed, connection is " + heldConn.getRawConnection(), e);
                        throw GeneralUtil.nestedException(e);
                    }
                }
            });
            savepoints.remove(savepoint);
        } finally {
            lock.unlock();
        }
    }

    public abstract void begin(String schema, String group, IConnection conn) throws SQLException;

    protected abstract void cleanup(String group, IConnection conn) throws SQLException;

    public void reinitializeConnection(String schema, String group, IConnection conn) throws SQLException {
        // do nothing
    }

    @Override
    public abstract void commit();

    @Override
    public abstract void rollback();

    /**
     * Set transaction context value during commit
     */
    protected final void updateExtraAppNames() {
        connectionContext.setOtherSchemas(new ArrayList<>(otherSchemas));
    }

    protected final void checkTerminated() {
        if (disabled) {
            throw new TddlRuntimeException(ErrorCode.ERR_TRANS_TERMINATED);
        }
    }

    protected final void forEachHeldConnection(TransactionConnectionHolder.Action action) {
        final AsyncTaskQueue asyncQueue = getManager().getTransactionExecutor().getAsyncQueue();
        connectionHolder.forEachConnection(asyncQueue, action);
    }

    protected enum TransactionCommitState {
        SUCCESS, FAILURE, UNKNOWN
    }

    protected final void beforePrimaryCommit() throws SQLException {
        if (failureFlag.failBeforePrimaryCommit) {
            throw new SQLException("forced by FAIL_BEFORE_PRIMARY_COMMIT");
        }
    }

    protected final void duringPrimaryCommit() throws SQLException {
        if (failureFlag.failDuringPrimaryCommit) {
            throw new SQLException("forced by FAIL_DURING_PRIMARY_COMMIT");
        }
    }

    protected final void afterPrimaryCommit() throws SQLException {
        if (failureFlag.failAfterPrimaryCommit) {
            throw new SQLException("forced by FAIL_AFTER_PRIMARY_COMMIT");
        }
    }

    protected final void beforeWriteCommitLog() {
        if (failureFlag.delayBeforeWriteCommitLog) {
            try {
                TimeUnit.SECONDS.sleep(10);
            } catch (InterruptedException ignored) {
            }
        }
    }

    protected final Collection<Pair<StampedLock, Long>> acquireSharedLock() {
        // Acquire lock of primary schema.
        List<Pair<StampedLock, Long>> locks = new ArrayList<>(otherSchemas.size() + 1);
        if (null == primarySchema) {
            // This transaction has no write connection, so no need to acquire distributed lock.
            return locks;
        }
        acquireSchemaLock(locks, primarySchema);
        // Acquire locks of other schemas.
        for (String schema : otherSchemas) {
            acquireSchemaLock(locks, schema);
        }
        return locks;
    }

    /**
     * Acquire lock of {schema} and add it into {locks}.
     */
    private void acquireSchemaLock(List<Pair<StampedLock, Long>> locks, String schema) {
        try {
            StampedLock lock =
                ((TransactionManager) ExecutorContext.getContext(schema).getTransactionManager()).getLock();
            long stamp = lock.tryReadLock(TransactionAttribute.TRANS_COMMIT_LOCK_TIMEOUT, TimeUnit.MILLISECONDS);
            if (0 == stamp) {
                throw new TddlRuntimeException(ErrorCode.ERR_TRANS, "Acquiring distributed transaction lock timeout");
            }
            locks.add(Pair.of(lock, stamp));
        } catch (InterruptedException | TddlRuntimeException ex) {
            // Unlock locked locks before throw exception
            LockUtils.releaseReadStampLocks(locks);
            throw GeneralUtil.nestedException(ex);
        }
    }

    @Override
    public State getState() {
        return this.state;
    }

    @Override
    public boolean isDistributed() {
        return true;
    }

    @Override
    public void clearFlashbackArea() {
        try {
            AtomicReference<Throwable> error = new AtomicReference<>(null);
            forEachHeldConnection(new TransactionConnectionHolder.Action() {
                @Override
                public boolean condition(TransactionConnectionHolder.HeldConnection heldConn) {
                    // For all connections.
                    return true;
                }

                @Override
                public void execute(TransactionConnectionHolder.HeldConnection heldConn) {
                    try {
                        heldConn.getRawConnection().disableFlashbackArea();
                    } catch (Throwable t) {
                        error.set(t);
                    }
                }
            });
            if (null != error.get()) {
                throw error.get();
            }
        } catch (Throwable t) {
            // For safety, prevent trx from continuing.
            setCrucialError(ErrorCode.ERR_FLASHBACK_AREA, t.getMessage());
        }
    }

    @Override
    public boolean handleStatementError(Throwable t) {
        lock.lock();
        try {
            if (this.isClosed()) {
                return false;
            }

            if (null != getInventoryMode() && getInventoryMode().isInventoryHint()) {
                // Skip handling errors for inventory mode.
                return false;
            }

            if (this.getCrucialError() == ERR_TRANS_ROLLBACK_STATEMENT_FAIL) {
                // This transaction should only be rolled back.
                return false;
            }

            if (!this.connectionHolder.isSupportAutoSavepoint()) {
                return false;
            }

            final ExecutionContext ec = this.getExecutionContext();
            // The trace id of the current failed statement.
            final String traceId = ec.getTraceId();

            final boolean rollbackStatement = shouldRollbackStatement(t);
            final ConcurrentLinkedDeque<String> errorList = new ConcurrentLinkedDeque<>();

            final AtomicBoolean invalid = new AtomicBoolean(false);
            final AtomicBoolean setSavepoint = new AtomicBoolean(false);

            // Rollback this statement in each write connection.
            forEachHeldConnection(new TransactionConnectionHolder.Action() {
                @Override
                public boolean condition(TransactionConnectionHolder.HeldConnection heldConn) {
                    // Only rollback trx in write connection.
                    return heldConn.isParticipated();
                }

                @Override
                public void execute(TransactionConnectionHolder.HeldConnection heldConn) {
                    final IConnection conn = heldConn.getRawConnection();
                    if (!(conn instanceof DeferredConnection)) {
                        if (rollbackStatement) {
                            errorList.add("Not support rolling back a statement in non-deferred connection");
                        }
                        return;
                    }

                    final DeferredConnection deferredConn = (DeferredConnection) conn;
                    final String traceIdInConn = deferredConn.getAutoSavepointMark();

                    if (null == traceIdInConn) {
                        // This connection is not involved in this failed statement.
                        return;
                    }

                    if (StringUtils.equalsIgnoreCase(INVALID_AUTO_SAVEPOINT, traceIdInConn)) {
                        invalid.set(true);
                        deferredConn.setAutoSavepointMark(null);
                        return;
                    }

                    if (!StringUtils.equalsIgnoreCase(traceId, traceIdInConn)) {
                        errorList.add("Found unexpected unreleased savepoint.");
                        return;
                    }

                    // This connection has set an auto savepoint.
                    setSavepoint.set(true);

                    /* We will do two things here:
                     * 1. If the current statement should be rolled back, send a rollback to savepoint statement.
                     * 2. Release the savepoint set before.
                     */
                    try {
                        if (rollbackStatement) {
                            deferredConn.rollbackAutoSavepoint(traceId, ec.getSchemaName());
                            if (AbstractTransaction.this instanceof TsoTransaction &&
                                isolationLevel != Connection.TRANSACTION_READ_COMMITTED) {
                                // For tso transaction with snapshot isolation,
                                // send snapshot_seq again since 'rollback to savepoint'
                                // may clear the snapshot_seq.
                                ((TsoTransaction) AbstractTransaction.this).sendSnapshotSeq(deferredConn);
                            }
                            if (AbstractTransaction.this instanceof XATsoTransaction) {
                                ((XATsoTransaction) AbstractTransaction.this).setInnodbMarkDistributed(deferredConn);
                            }
                        }
                        deferredConn.releaseAutoSavepoint(traceId, ec.getSchemaName(), false);
                    } catch (Throwable e) {
                        errorList.add(e.getMessage());
                    } finally {
                        deferredConn.setAutoSavepointMark(null);
                    }
                }
            });

            if (setSavepoint.get() && invalid.get()) {
                logger.warn("Handling errors for statement " + traceId
                    + " fails. Some connections have set auto-savepoint, but others do not. "
                    + "Original error is: " + t.getMessage());
                // For safety, prevent this trx from committing.
                setCrucialError(ERR_TRANS_ROLLBACK_STATEMENT_FAIL,
                    "Some connections have set auto-savepoint, but others do not.");
                EventLogger.log(EventType.AUTO_SP_ERR,
                    primarySchema + ": Some connections have set auto-savepoint, but others do not. "
                        + "Original error is: " + t.getMessage());
            } else if (!errorList.isEmpty()) {
                // Print each error message to log.
                logger.warn("Handling errors for statement " + traceId + " fails."
                    + "Original error is: " + t.getMessage());
                errorList.forEach(logger::warn);
                // For safety, prevent this trx from committing.
                setCrucialError(ERR_TRANS_ROLLBACK_STATEMENT_FAIL, errorList.getFirst());
                EventLogger.log(EventType.AUTO_SP_ERR, primarySchema + ": Rollback auto-savepoint failed. "
                    + "Original error is: " + t.getMessage());
            } else if (rollbackStatement) {
                // Clear the crucial error after rolling back the statement.
                setCrucialError(null, null);
                logger.warn("Statement " + traceId + " rolled back, caused by " + t.getMessage());
                return true;
            }
            return false;
        } finally {
            lock.unlock();
        }
    }

    protected boolean shouldRollbackStatement(Throwable t) {
        /* Rollback statement when:
         * 1. the current statement is a DML;
         * 2. and it fails when it is executing in multi-shards.
         * */
        if (SqlType.isDML(this.getExecutionContext().getSqlType())
            && (this.getCrucialError() == ERR_TRANS_CONTINUE_AFTER_WRITE_FAIL
            || this.getCrucialError() == ERR_GLOBAL_SECONDARY_INDEX_CONTINUE_AFTER_WRITE_FAIL)) {
            final String errorMessage = t.getMessage();
            for (Pattern errorPattern : canRollbackStatementErrors) {
                if (errorPattern.matcher(errorMessage).matches()) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public void releaseAutoSavepoint() {
        lock.lock();
        try {
            if (this.isClosed() || !this.connectionHolder.isSupportAutoSavepoint()) {
                return;
            }

            if (this.getCrucialError() == ERR_TRANS_ROLLBACK_STATEMENT_FAIL) {
                // This transaction should only be rolled back.
                return;
            }

            final List<String> errorList = new ArrayList<>();

            final ExecutionContext ec = this.getExecutionContext();
            // The trace id of the current statement.
            final String traceId = ec.getTraceId();

            boolean invalid = false;
            boolean setSavepoint = false;

            List<TransactionConnectionHolder.HeldConnection> connections = connectionHolder.getAllWriteConn();
            for (TransactionConnectionHolder.HeldConnection heldConn : connections) {
                final IConnection conn = heldConn.getRawConnection();
                if (!(conn instanceof DeferredConnection)) {
                    continue;
                }

                final DeferredConnection deferredConn = (DeferredConnection) conn;
                final String traceIdInConn = deferredConn.getAutoSavepointMark();

                if (traceIdInConn == null) {
                    // This connection does not involve in this logical sql.
                    continue;
                }

                if (StringUtils.equalsIgnoreCase(INVALID_AUTO_SAVEPOINT, traceIdInConn)) {
                    invalid = true;
                    deferredConn.setAutoSavepointMark(null);
                    continue;
                }

                if (!StringUtils.equalsIgnoreCase(traceId, traceIdInConn)) {
                    // The previous savepoint has not been released, which should not happen.
                    errorList.add("Found unexpected unreleased savepoint.");
                    continue;
                }

                setSavepoint = true;

                // Release savepoint.
                try {
                    deferredConn.releaseAutoSavepoint(traceId, ec.getSchemaName(), true);
                } catch (Throwable e) {
                    errorList.add(e.getMessage());
                } finally {
                    deferredConn.setAutoSavepointMark(null);
                }
            }

            if (setSavepoint && invalid) {
                logger.warn("Release auto-savepoints for for statement " + traceId
                    + " fails. Some connections have set auto-savepoint, but others do not.");
                // For safety, prevent this trx from committing.
                setCrucialError(ERR_TRANS_ROLLBACK_STATEMENT_FAIL,
                    "Some connections have set auto-savepoint, but others do not.");
                EventLogger.log(EventType.AUTO_SP_ERR,
                    primarySchema + ": Some connections have set auto-savepoint, but others do not.");
            } else if (!errorList.isEmpty()) {
                // Print each error message to log.
                logger.warn("Release auto-savepoints for " + traceId + " fails.");
                errorList.forEach(logger::warn);
                // For safety, prevent this trx from committing.
                setCrucialError(ERR_TRANS_ROLLBACK_STATEMENT_FAIL, errorList.get(0));
                EventLogger.log(EventType.AUTO_SP_ERR, primarySchema + ": Release auto-savepoint failed. "
                    + "Caused by " + errorList.get(0));
            }
        } finally {
            lock.unlock();
        }
    }
}
