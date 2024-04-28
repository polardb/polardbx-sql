package com.alibaba.polardbx.transaction.trx;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.IConnection;
import com.alibaba.polardbx.common.jdbc.IDataSource;
import com.alibaba.polardbx.common.jdbc.ITransactionPolicy;
import com.alibaba.polardbx.common.type.TransactionType;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.thread.LockUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.transaction.connection.TransactionConnectionHolder;
import com.alibaba.polardbx.transaction.TransactionLogger;
import com.alibaba.polardbx.transaction.TransactionManager;
import com.alibaba.polardbx.transaction.TransactionState;
import com.alibaba.polardbx.transaction.jdbc.SavePoint;
import com.alibaba.polardbx.transaction.log.GlobalTxLogManager;
import com.alibaba.polardbx.transaction.log.RedoLogManager;

import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Statement;
import java.text.MessageFormat;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.StampedLock;

/**
 * DRDS Best Effort Transaction
 *
 * @author <a href="mailto:eric.fy@alibaba-inc.com">Eric Fu</a>
 * @since 5.3.2
 */
public class BestEffortTransaction extends AbstractTransaction {

    private final static Logger logger = LoggerFactory.getLogger(BestEffortTransaction.class);

    private final Map<String, RedoLogManager> redoLogMangers = new HashMap<>();

    public BestEffortTransaction(ExecutionContext executionContext, TransactionManager manager) {
        super(executionContext, manager);
    }

    @Override
    public TransactionType getType() {
        return TransactionType.BED;
    }

    /**
     * 初始化一个事务连接
     */
    @Override
    public void begin(String schema, String group, IConnection conn) throws SQLException {
        try {
            conn.executeLater("begin");

            for (String savepoint : savepoints) {
                SavePoint.setLater(conn, savepoint);
            }
        } catch (SQLException e) {
            logger.error("Best-effort transaction init failed on " + group + ":" + e.getMessage());
            throw e;
        }

        // Enable best-effort recovery scanning in case that user only use cross-schema transaction in that schema
        TransactionManager.getInstance(schema).enableBestEffortRecoverScan();
    }

    @Override
    protected void cleanup(String group, IConnection conn) throws SQLException {
        if (conn.isClosed()) {
            return;
        }

        try {
            conn.forceRollback();
        } catch (Throwable e) {
            throw GeneralUtil.nestedException("Rollback failed", e);
        }
    }

    @Override
    public void commit() {
        Lock lock = this.lock;
        lock.lock();

        try {
            checkTerminated();
            checkCanContinue();

            if (!isCrossGroup) {
                if (primaryConnection != null) {
                    if (inventoryMode != null && inventoryMode.isCommitOnSuccess()) {
                        connectionHolder.closeAllConnections();
                        return;
                    }
                    try (Statement stmt = primaryConnection.createStatement()) {
                        stmt.execute("commit");
                    } catch (Throwable e) {
                        logger.error("Single-db transaction commit failed on " + primaryGroup, e);
                        throw GeneralUtil.nestedException(e);
                    }

                    connectionHolder.closeAllConnections();
                }
                return;
            }

            Collection<Pair<StampedLock, Long>> txSharedLocks = acquireSharedLock();
            try {
                commitCrossGroupTrans();
            } finally {
                LockUtils.releaseReadStampLocks(txSharedLocks);
            }
        } finally {
            lock.unlock();
        }
    }

    private void commitCrossGroupTrans() {
        if (!otherSchemas.isEmpty()) {
            updateExtraAppNames();
        }

        /*
         * 写入 global_tx_log 状态为 SUCCEED （还未提交）
         */
        try {
            GlobalTxLogManager.append(id, getType(), TransactionState.SUCCEED, connectionContext, primaryConnection);
        } catch (SQLIntegrityConstraintViolationException ex) {
            // 被抢占 Rollback 了，停止提交
            throw new TddlRuntimeException(ErrorCode.ERR_TRANS, "Transaction ID exists. Commit interrupted");
        } catch (SQLException ex) {
            throw new TddlRuntimeException(ErrorCode.ERR_TRANS_LOG, ex,
                "Failed to update transaction state on group: " + primaryGroup);
        }

        // Whether the local transaction on primary group succeeded or not, or may be unknown
        TransactionCommitState commitState = TransactionCommitState.FAILURE;

        RuntimeException exception = null;
        try {
            /*
             * Step 1. PREPARE non-primary groups，若抛出异常则中止
             */
            forEachHeldConnection(new TransactionConnectionHolder.Action() {
                @Override
                public boolean condition(TransactionConnectionHolder.HeldConnection heldConn) {
                    return heldConn.getRawConnection() != primaryConnection;
                }

                @Override
                public void execute(TransactionConnectionHolder.HeldConnection heldConn) {
                    final String group = heldConn.getGroup();
                    final IConnection conn = heldConn.getRawConnection();
                    RedoLogManager redoLogManager = redoLogMangers.get(group);
                    if (redoLogManager != null) {
                        long start = System.nanoTime();

                        // Open a separate connection to write redo-logs
                        redoLogManager.writeBatch(dataSourceCache.get(group));

                        // Then delete them in current transaction immediately
                        RedoLogManager.clean(id, conn);

                        // Ensure that the redo-log must be locked as soon as possible
                        if (System.nanoTime() - start > 5 * 1_000_000_000L) {
                            throw new TddlRuntimeException(ErrorCode.ERR_TRANS_COMMIT, "Write redo-log timeout");
                        }
                    }

                }
            });

            this.prepared = true;
            this.state = State.PREPARED;

            /*
             * Step 2. 提交 primary group 来更新事务状态（标志着事务的成功）
             */
            try (Statement stmt = primaryConnection.createStatement()) {
                beforePrimaryCommit();
                commitState = TransactionCommitState.UNKNOWN;
                duringPrimaryCommit();
                stmt.execute("commit");
                afterPrimaryCommit();
                commitState = TransactionCommitState.SUCCESS;
                primaryConnection.close();
            } catch (Throwable ex) {
                String message = MessageFormat
                    .format("Failed to commit primary group {0}: {1}, TRANS_ID = {2}", primaryGroup, ex.getMessage(),
                        id);
                throw new TddlRuntimeException(ErrorCode.ERR_TRANS_COMMIT, ex, message);
            }

        } catch (RuntimeException ex) {
            exception = ex;
        }

        if (commitState == TransactionCommitState.SUCCESS) {
            /*
             * Commit non-primary groups if succeed
             */
            forEachHeldConnection((heldConn) -> {
                final IConnection conn = heldConn.getRawConnection();
                if (conn != primaryConnection) {
                    try (Statement stmt = conn.createStatement()) {
                        stmt.execute("commit");
                        conn.close();
                    } catch (SQLException ex) {
                        // Don't worry. Will be recovered later.
                        logger.warn("Commit non-primary group " + heldConn.getGroup() + " failed");
                    }
                }
            });

            TransactionLogger.info(id, "Committed (2PC)");

        } else if (commitState == TransactionCommitState.UNKNOWN) {
            /*
             * Transaction state is unknown so we cannot do anything unless we know the actual transaction state.
             * This case does not happen frequently. Just leave it to the recovering thread.
             */
            TransactionLogger.error(id, "Aborted with unknown commit state");
        }

        // 清理回滚所有连接
        cleanupAllConnections();

        // 在更新 global_tx_log 的事务状态前，必须先释放连接，从而释放之前加上的锁
        connectionHolder.closeAllConnections();

        if (commitState == TransactionCommitState.FAILURE) {
            /*
             * Delete the redo-logs if primary connection commit failed
             */
            forEachHeldConnection((heldConn) -> {
                if (heldConn.getRawConnection() != primaryConnection) {
                    RedoLogManager.clean(id, dataSourceCache.get(heldConn.getGroup()));
                }
            });

            TransactionLogger.error(id, "Aborted by committing failed");
        }

        if (exception != null) {
            throw exception;
        }
    }

    @Override
    public void rollback() {
        Lock lock = this.lock;
        lock.lock();

        try {
            // 清理回滚所有连接
            cleanupAllConnections();

            connectionHolder.closeAllConnections();

            TransactionLogger.warn(id, "Aborted");

        } finally {
            lock.unlock();
        }
    }

    public RedoLogManager getRedoLogManager(String schema, String group, IDataSource ds) {
        if (group == null) {
            throw new IllegalArgumentException("group name is null");
        }

        Lock lock = this.lock;
        lock.lock();

        try {
            RedoLogManager redoLogManager;
            if ((redoLogManager = redoLogMangers.get(group)) != null) {
                return redoLogManager;
            }

            IConnection connection = null;
            try {
                connection = getConnection(schema, group, ds, RW.WRITE);
                redoLogManager = new RedoLogManager(group, id);
                redoLogMangers.put(group, redoLogManager);

                return redoLogManager;
            } catch (SQLException ex) {
                String message = "Failed to get connection for group " + group + ": " + ex.getMessage();
                throw new TddlRuntimeException(ErrorCode.ERR_TRANS, ex, message);
            } finally {
                if (connection != null) {
                    try {
                        tryClose(connection, group);
                    } catch (SQLException ex) {
                        // impossible
                    }
                }
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    protected void innerCleanupAllConnections(String group, IConnection conn,
                                              TransactionConnectionHolder.ParticipatedState participatedState) {
        switch (participatedState) {
        case NONE:
            rollbackNonParticipantSync(group, conn);
            return;
        case SHARE_READVIEW_READ:       // equivalent to participated read
        case WRITTEN:
            cleanupParticipateConn(group, conn);
        }
    }

    @Override
    public ITransactionPolicy.TransactionClass getTransactionClass() {
        return ITransactionPolicy.TransactionClass.BEST_EFFORT;
    }
}
