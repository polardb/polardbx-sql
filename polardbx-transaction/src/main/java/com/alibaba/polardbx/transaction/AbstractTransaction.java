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

import com.alibaba.polardbx.common.constants.TransactionAttribute;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.IConnection;
import com.alibaba.polardbx.common.jdbc.IDataSource;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.druid.sql.ast.SqlType;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.server.IServerConfigManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.utils.FailureInjectionFlag;
import com.alibaba.polardbx.optimizer.utils.GroupConnId;
import com.alibaba.polardbx.optimizer.utils.IDistributedTransaction;
import com.alibaba.polardbx.stats.TransactionStatistics;
import com.alibaba.polardbx.transaction.async.AsyncTaskQueue;
import com.alibaba.polardbx.transaction.jdbc.DeferredConnection;
import com.alibaba.polardbx.transaction.jdbc.SavePoint;
import com.alibaba.polardbx.transaction.log.ConnectionContext;
import com.alibaba.polardbx.transaction.log.GlobalTxLogManager;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.StringUtils;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import static com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_CONTINUE_AFTER_WRITE_FAIL;
import static com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_TRANS_CONTINUE_AFTER_WRITE_FAIL;
import static com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_TRANS_ROLLBACK_STATEMENT_FAIL;
import static com.alibaba.polardbx.transaction.TransactionConnectionHolder.BEFORE_SET_SAVEPOINT;

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

    // Max trx duration, in seconds.
    private final long maxTime;

    // Only rollback statement when the following errors occur.
    final List<String> enableRollbackStatementErrors = ImmutableList.of(
        "Lock wait timeout exceeded; try restarting transaction",
        "Duplicate entry",
        "Data too long for column",
        "Out of range value for column",
        "[TDDL-4602][ERR_CONVERTOR]",
        "Incorrect datetime value",
        "Incorrect time value",
        "Data truncated for column"
    );

    public AbstractTransaction(ExecutionContext executionContext, TransactionManager manager) {
        super(executionContext, manager);
        this.connectionHolder =
            new TransactionConnectionHolder(this, lock, executionContext, manager.getTransactionExecutor());
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

    @Override
    public IConnection getConnection(String schema, String group, IDataSource ds, RW rw, ExecutionContext ec)
        throws SQLException {
        return getConnection(schema, group, 0L, ds, rw, ec);
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
                beginTransaction();
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
                beginRwTransaction(primarySchema);
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
    private void beginRwTransaction(String primarySchema) {
        TransactionStatistics stats = OptimizerContext.getContext(primarySchema).getStatistics().getTransactionStats();
        switch (getType()) {
        case XA:
            stats.countXA.incrementAndGet();
            break;
        case TSO:
            stats.countTSO.incrementAndGet();
            break;
        default:
            break;
        }

        TransactionLogger.debug(id, getType() + " transaction begins");
    }

    protected void beginNonParticipant(String group, IConnection conn) throws SQLException {
        // Compatible with MaxScale. MaxScale will send START TRANSACTION READ ONLY to RO,
        // then send ROLLBACK;INSERT... to RW because they cannot handle multi statement correctly.
        conn.executeLater("BEGIN");
        // conn.executeLater("START TRANSACTION READ ONLY");
    }

    protected void rollbackNonParticipant(String group, IConnection conn) {
        try {
            conn.executeLater("ROLLBACK");
        } catch (Throwable e) {
            logger.error("Rollback non-participant group failed on " + group, e);
            discard(group, conn, e);
        }
    }

    protected void rollbackNonParticipantSync(String group, IConnection conn) {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("ROLLBACK");
        } catch (Throwable e) {
            logger.error("Rollback non-participant group failed on " + group, e);
            discard(group, conn, e);
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

        } finally {
            lock.unlock();
        }

        TransactionLogger.debug(id, "Cross-group enabled. Primary group: {0}", primaryGroup);
    }

    /**
     * Do cleanup on all held connections. Read connection don't need cleanup.
     */
    protected final void cleanupAllConnections() {
        forEachHeldConnection((group, conn, participatedState) -> {

            if (!isCrossGroup && (inventoryMode != null && inventoryMode.isRollbackOnFail())) {
                return;
            }
            innerCleanupAllConnections(group, conn, participatedState);
        });
    }

    protected abstract void innerCleanupAllConnections(String group, IConnection conn,
                                                       TransactionConnectionHolder.ParticipatedState participatedState);

    protected void cleanupParticipateConn(String group, IConnection conn) {
        try {
            cleanup(group, conn);
        } catch (Throwable e) {
            discard(group, conn, e);
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
        Lock lock = this.lock;
        lock.lock();

        try {
            if (!isClosed()) {
                disabled = true;

                connectionHolder.kill();

                // Note: 无需清理回滚所有连接，否则有并发问题
                // cleanupAllConnections();
                // connectionHolder.closeAllConnections();

                TransactionLogger.info(id, "Killed");
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
        Lock lock = this.lock;
        lock.lock();

        try {
            super.close();

            // Do cleanup on all held connections.
            cleanupAllConnections();

            // Close connections here in case that transaction was not
            // committed, or interrupted during committing
            connectionHolder.closeAllConnections();
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
                public boolean condition(String group, IConnection conn,
                                         TransactionConnectionHolder.ParticipatedState participated) {
                    return participated.participatedTrx();
                }

                @Override
                public void execute(String group, IConnection conn,
                                    TransactionConnectionHolder.ParticipatedState participated) {
                    try {
                        SavePoint.set(conn, savepoint);
                    } catch (Throwable e) {
                        logger.error("Set savepoint failed, connection is " + conn, e);
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
                public boolean condition(String group, IConnection conn,
                                         TransactionConnectionHolder.ParticipatedState participated) {
                    return participated.participatedTrx();
                }

                @Override
                public void execute(String group, IConnection conn,
                                    TransactionConnectionHolder.ParticipatedState participated) {
                    try {
                        SavePoint.rollback(conn, savepoint);
                    } catch (Throwable e) {
                        logger.error("Transaction rollback failed, connection is " + conn, e);
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
                public boolean condition(String group, IConnection conn,
                                         TransactionConnectionHolder.ParticipatedState participated) {
                    return participated.participatedTrx();
                }

                @Override
                public void execute(String group, IConnection conn,
                                    TransactionConnectionHolder.ParticipatedState participated) {
                    try {
                        SavePoint.release(conn, savepoint);
                    } catch (Throwable e) {
                        logger.error("Transaction rollback failed, connection is " + conn, e);
                        throw GeneralUtil.nestedException(e);
                    }
                }
            });
            savepoints.remove(savepoint);
        } finally {
            lock.unlock();
        }
    }

    public abstract TransactionType getType();

    protected abstract void begin(String schema, String group, IConnection conn) throws SQLException;

    protected abstract void cleanup(String group, IConnection conn) throws SQLException;

    protected void reinitializeConnection(String schema, String group, IConnection conn) throws SQLException {
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

    enum TransactionCommitState {
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

    protected final Collection<Lock> acquireSharedLock() {
        List<Lock> locks = new ArrayList<>(otherSchemas.size() + 1);
        locks.add(((TransactionManager) ExecutorContext.getContext(primarySchema).getTransactionManager())
            .sharedLock());
        for (String schema : otherSchemas) {
            locks.add(((TransactionManager) ExecutorContext.getContext(schema).getTransactionManager())
                .sharedLock());
        }
        for (int i = 0; i < locks.size(); i++) {
            try {
                boolean succeed =
                    locks.get(i).tryLock(TransactionAttribute.TRANS_COMMIT_LOCK_TIMEOUT, TimeUnit.MILLISECONDS);
                if (!succeed) {
                    throw new TddlRuntimeException(ErrorCode.ERR_TRANS,
                        "Acquiring distributed transaction lock timeout");
                }
            } catch (InterruptedException | TddlRuntimeException ex) {
                // unlock locked locks before throw exception
                for (int j = 0; j < i; j++) {
                    locks.get(j).unlock();
                }
                throw GeneralUtil.nestedException(ex);
            }
        }
        return locks;
    }

    protected final void releaseSharedLock(Collection<Lock> sharedLocks) {
        for (Lock sharedLock : sharedLocks) {
            sharedLock.unlock();
        }
    }

    @Override
    public State getState() {
        return prepared ? State.PREPARED : State.RUNNING;
    }

    @Override
    public boolean isDistributed() {
        return true;
    }

    @Override
    public boolean handleStatementError(Throwable t) {
        if (null != getInventoryMode() && getInventoryMode().isInventoryHint()) {
            // Skip handling errors for inventory mode.
            return false;
        }

        if (this.getCrucialError() == ERR_TRANS_ROLLBACK_STATEMENT_FAIL) {
            // This transaction should only be rolled back.
            return false;
        }

        final ExecutionContext ec = this.getExecutionContext();
        // The trace id of the current failed statement.
        final String traceId = ec.getTraceId();
        if (!this.connectionHolder.isSupportAutoSavepoint()) {
            return false;
        }

        final boolean rollbackStatement = shouldRollbackStatement(t);
        final ConcurrentLinkedDeque<String> errorList = new ConcurrentLinkedDeque<>();

        // Rollback this statement in each write connection.
        forEachHeldConnection(new TransactionConnectionHolder.Action() {
            @Override
            public boolean condition(String group, IConnection conn,
                                     TransactionConnectionHolder.ParticipatedState participated) {
                // Only rollback trx in write connection.
                return participated.participatedTrx();
            }

            @Override
            public void execute(String group, IConnection conn,
                                TransactionConnectionHolder.ParticipatedState participated) {
                if (!(conn instanceof DeferredConnection)) {
                    if (rollbackStatement) {
                        errorList.add("Not support to rollback a statement in non-deferred connection");
                    }
                    return;
                }

                final DeferredConnection deferredConn = (DeferredConnection) conn;
                final String traceIdInConn = deferredConn.getTraceId();

                if (StringUtils.equalsIgnoreCase(BEFORE_SET_SAVEPOINT, traceIdInConn)) {
                    // Some errors occur when setting auto savepoint,
                    // and hence this trx only can be rolled back.
                    errorList.add("Errors occur when setting auto savepoint");
                    return;
                }

                if (!StringUtils.equalsIgnoreCase(traceId, traceIdInConn)) {
                    // This connection is not involved in this failed statement.
                    return;
                }

                /* We will do two things here:
                 * 1. If the current statement should be rolled back, send a rollback to savepoint statement.
                 * 2. Since this statement fails to execute, the auto savepoint may not be set successfully.
                 *    And "RELEASE SAVEPOINT" may fails to execute and make the next user-statement fails.
                 *    So we release the savepoint here and set the trace id to null.
                 */
                try (final Statement stmt = deferredConn.createStatement()) {
                    final StringBuilder executeSql = new StringBuilder();
                    if (rollbackStatement) {
                        executeSql.append("ROLLBACK TO SAVEPOINT ")
                            .append(TStringUtil.backQuote(traceId))
                            .append(";");
                    }
                    executeSql.append("RELEASE SAVEPOINT ")
                        .append(TStringUtil.backQuote(traceId));
                    stmt.execute(executeSql.toString());
                } catch (Throwable e) {
                    errorList.add(e.getMessage());
                } finally {
                    deferredConn.setTraceId(null);
                }
            }
        });

        if (!errorList.isEmpty()) {
            // Print each error message to log.
            logger.warn("Handling errors for statement " + traceId + " fails.");
            errorList.forEach(logger::warn);
            // For safety, prevent this trx from committing.
            setCrucialError(ERR_TRANS_ROLLBACK_STATEMENT_FAIL);
        } else if (rollbackStatement) {
            // Clear the crucial error after rolling back the statement.
            setCrucialError(null);
            logger.warn("Statement " + traceId + " rolled back.");
            return true;
        }
        return false;
    }

    private boolean shouldRollbackStatement(Throwable t) {
        /* Rollback statement when:
        * 1. the current statement is a DML;
        * 2. and it fails when it is executing in multi-shards.
        * */
        if (SqlType.isDML(this.getExecutionContext().getSqlType())
            && (this.getCrucialError() == ERR_TRANS_CONTINUE_AFTER_WRITE_FAIL
            || this.getCrucialError() == ERR_GLOBAL_SECONDARY_INDEX_CONTINUE_AFTER_WRITE_FAIL)) {
            final String errorMessage = t.getMessage();
            for (String error : enableRollbackStatementErrors) {
                if (StringUtils.containsIgnoreCase(errorMessage, error)) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public void releaseAutoSavepoint() {
        if (!this.connectionHolder.isSupportAutoSavepoint()) {
            return;
        }

        if (this.getCrucialError() == ERR_TRANS_ROLLBACK_STATEMENT_FAIL) {
            // This transaction should only be rolled back.
            return;
        }

        final ConcurrentLinkedDeque<String> errorList = new ConcurrentLinkedDeque<>();

        final ExecutionContext ec = this.getExecutionContext();
        // The trace id of the current statement.
        final String traceId = ec.getTraceId();

        forEachHeldConnection(new TransactionConnectionHolder.Action() {
            @Override
            public boolean condition(String group, IConnection conn,
                                     TransactionConnectionHolder.ParticipatedState participated) {
                // Release savepoint only in write connection.
                return participated.participatedTrx();
            }

            @Override
            public void execute(String group, IConnection conn,
                                TransactionConnectionHolder.ParticipatedState participated) {
                if (!(conn instanceof DeferredConnection)) {
                    return;
                }

                final DeferredConnection deferredConn = (DeferredConnection) conn;
                final String traceIdInConn = deferredConn.getTraceId();

                if (traceIdInConn == null) {
                    // This connection does not involve in this logical sql.
                    return;
                }

                if (StringUtils.equalsIgnoreCase(BEFORE_SET_SAVEPOINT, traceIdInConn)) {
                    // Some errors occur when setting auto savepoint,
                    // and hence this trx only can be rolled back.
                    errorList.add("Errors occur when setting auto savepoint");
                    return;
                }

                if (!StringUtils.equalsIgnoreCase(traceId, traceIdInConn)) {
                    // The previous savepoint has not been released, which should not happen.
                    errorList.add("Previous savepoint has not been released yet!");
                    return;
                }

                // Release savepoint.
                try {
                    deferredConn.executeLater("RELEASE SAVEPOINT " + TStringUtil.backQuote(traceId));
                } catch (Throwable e) {
                    errorList.add(e.getMessage());
                } finally {
                    deferredConn.setTraceId(null);
                }
            }
        });

        if (!errorList.isEmpty()) {
            // Print each error message to log.
            logger.warn("Release auto-savepoints for " + traceId + " fails.");
            errorList.forEach(logger::warn);
            // For safety, prevent this trx from committing.
            setCrucialError(ERR_TRANS_ROLLBACK_STATEMENT_FAIL);
        }
    }
}
