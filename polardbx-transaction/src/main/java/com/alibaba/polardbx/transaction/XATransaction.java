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

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.IConnection;
import com.alibaba.polardbx.common.jdbc.ITransactionPolicy;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.transaction.async.AsyncTaskQueue;
import com.alibaba.polardbx.transaction.jdbc.SavePoint;
import com.alibaba.polardbx.transaction.utils.XAUtils;

import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Statement;
import java.text.MessageFormat;

/**
 * DRDS XA 事务
 *
 * @author <a href="mailto:eric.fy@taobao.com">Eric Fu</a>
 * @since 5.1.28
 */
public class XATransaction extends ShareReadViewTransaction {

    private final static Logger logger = LoggerFactory.getLogger(XATransaction.class);

    private final static String TRX_LOG_PREFIX = "[" + ITransactionPolicy.TransactionClass.XA + "]";

    public XATransaction(ExecutionContext executionContext, TransactionManager manager) {
        super(executionContext, manager);
    }

    @Override
    public TransactionType getType() {
        return TransactionType.XA;
    }

    /**
     * 初始化一个事务连接
     */
    @Override
    protected void begin(String schema, String group, IConnection conn) throws SQLException {
        this.shareReadView = executionContext.isShareReadView();
        try {
            if (shareReadView) {
                beginWithShareReadView(group, conn);
            } else {
                beginWithoutShareReadView(group, conn);
            }
            for (String savepoint : savepoints) {
                SavePoint.setLater(conn, savepoint);
            }
        } catch (SQLException e) {
            logger.error("XA Transaction init failed on " + group + ":" + e.getMessage());
            throw e;
        }

        // Enable xa recovery scanning in case that user only use cross-schema transaction in that schema
        TransactionManager.getInstance(schema).enableXaRecoverScan();
        TransactionManager.getInstance(schema).enableKillTimeoutTransaction();
    }

    private void beginWithShareReadView(String group, IConnection conn) throws SQLException {
        if (inventoryMode != null) {
            // 共享readview不支持inventory hint
            throw new UnsupportedOperationException("Don't support the Inventory Hint on XA with readview! "
                + "Try with setting share_read_view=off.");
        } else {
            conn.executeLater(TURN_ON_TXN_GROUP_SQL);
            conn.executeLater("XA START " + getXid(group, conn, true));
        }
    }

    private void beginWithoutShareReadView(String group, IConnection conn) throws SQLException {
        if (primaryConnection != null) {
            conn.executeLater("XA START " + getXid(group, conn, false));
        } else {
            conn.executeLater("begin");
        }
    }

    @Override
    protected String getTrxLoggerPrefix() {
        return TRX_LOG_PREFIX;
    }

    @Override
    protected void writeCommitLog(IConnection logConn) {
        // Write global_tx_log on Primary Group (not committed yet)
        try {
            beforeWriteCommitLog();
            globalTxLogManager
                .append(id, getType(), TransactionState.SUCCEED, connectionContext, logConn);
        } catch (SQLIntegrityConstraintViolationException ex) {
            // 被抢占 Rollback 了，停止提交
            throw new TddlRuntimeException(ErrorCode.ERR_TRANS, "Transaction ID exists. Commit failed");
        } catch (SQLException ex) {
            throw new TddlRuntimeException(ErrorCode.ERR_TRANS_LOG, ex,
                "Failed to update transaction state on group: " + primaryGroup);
        }
    }

    /**
     * Commit transaction without participants or with only one participant.\]
     * <p>
     * Use XA COMMIT ONE PHASE to commit transaction with only one shard.
     */
    @Override
    protected void commitOneShardTrx() {
        if (inventoryMode != null && inventoryMode.isCommitOnSuccess()) {
            connectionHolder.closeAllConnections();
            return;
        }
        super.commitOneShardTrx();
    }

    @Override
    protected void innerCommitOneShardTrx(String group, IConnection conn) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            if (shareReadView) {
                String xid = getXid(group, conn, true);
                stmt.execute(getXACommitOnePhaseSqls(xid));
            } else {
                stmt.execute("COMMIT");
            }
        }
    }

    @Override
    protected void innerRollback(String group, IConnection conn) {
        if (!shareReadView && conn == primaryConnection) {
            rollbackPrimary(group, primaryConnection);
            return;
        }
        super.innerRollback(group, conn);
    }

    /**
     * Cleanup primary connection with normal ROLLBACK.
     */
    private void rollbackPrimary(String group, IConnection conn) {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("rollback");
        } catch (Throwable e) {
            // discard primary connection if cleanup failed.
            discard(group, conn, e);

            logger.warn("Rollback primary failed on group " + group, e);
        }
    }

    /**
     * Commit transaction with multiple participants.
     */
    @Override
    protected void commitMultiShardTrx() {
        if (!otherSchemas.isEmpty()) {
            updateExtraAppNames();
        }

        // Whether the local transaction on primary group succeeded or not, or may be unknown
        TransactionCommitState commitState = TransactionCommitState.FAILURE;

        RuntimeException exception = null;
        try {
            /*
             * Step 1. XA PREPARE non-primary groups，若抛出异常则中止
             */
            prepareConnections();
            this.prepared = true;

            /*
             * Step 2. XA COMMIT ONE PHASE 提交 primary group 来更新事务状态（标志着事务的成功）
             */
            try (Statement stmt = primaryConnection.createStatement()) {
                beforePrimaryCommit();
                commitState = TransactionCommitState.UNKNOWN;
                duringPrimaryCommit();
                commitPrimary(stmt);
                afterPrimaryCommit();
                commitState = TransactionCommitState.SUCCESS;
            } catch (Throwable ex) {
                String message = MessageFormat
                    .format("Failed to commit primary group {0}: {1}, TRANS_ID = {2}",
                        primaryGroup, ex.getMessage(), getTraceId());
                throw new TddlRuntimeException(ErrorCode.ERR_TRANS_COMMIT, ex, message);
            }

        } catch (RuntimeException ex) {
            exception = ex;
        }

        if (commitState == TransactionCommitState.FAILURE) {
            /*
             * XA 失败回滚：XA ROLLBACK 提交刚才 PREPARE 的连接
             */
            rollbackConnections();

            TransactionLogger.error(id, "Aborted by committing failed");

        } else if (commitState == TransactionCommitState.SUCCESS) {
            /*
             * XA 提交成功：XA COMMIT 提交刚才 PREPARE 的其他连接
             */
            commitConnections();

            TransactionLogger.info(id, "Committed (XA)");
        } else {
            /*
             * Transaction state is unknown so we cannot do anything unless we know the actual transaction state.
             * This case does not happen frequently. Just leave it to the recovering thread.
             */
            discardConnections();

            TransactionLogger.error(id, "Aborted with unknown commit state");
        }

        // 在更新 global_tx_log 的事务状态前，必须先释放连接，从而释放之前加上的锁
        connectionHolder.closeAllConnections();

        if (exception != null) {
            throw exception;
        }
    }

    /**
     * Commit the primary connection
     */
    private void commitPrimary(Statement stmt) throws SQLException {
        if (shareReadView) {
            String xid = getXid(primaryGroup, primaryConnection, true);
            stmt.execute(getXACommitOnePhaseSqls(xid));
        } else {
            stmt.execute("COMMIT");
        }
    }

    /**
     * Prepare on all XA connections and write global_tx_log on primary group
     */
    @Override
    protected void prepareConnections() {
        forEachHeldConnection(new TransactionConnectionHolder.Action() {

            @Override
            public void execute(String group, IConnection conn, boolean participated) {
                if (!participated) {
                    commitNonParticipantSync(group, conn);
                } else if (conn == primaryConnection) {
                    writeCommitLog(conn);
                } else {
                    prepare(group, conn);
                }
            }

            private void prepare(String group, IConnection conn) {
                // XA transaction must be 'ACTIVE' state here.
                String xid = getXid(group, conn, shareReadView);
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute("XA END " + xid + "; XA PREPARE " + xid);
                } catch (SQLException e) {
                    throw new TddlRuntimeException(ErrorCode.ERR_TRANS_COMMIT, e, "XA PREPARE failed: " + xid);
                }
            }
        });
    }

    @Override
    protected void commitConnections() {
        forEachHeldConnection(new TransactionConnectionHolder.Action() {
            @Override
            public boolean condition(String group, IConnection conn, boolean participated) {
                return conn != primaryConnection && participated;
            }

            @Override
            public void execute(String group, IConnection conn, boolean participated) {
                // XA transaction must be 'PREPARED' state here.
                String xid = getXid(group, conn, shareReadView);
                try (Statement stmt = conn.createStatement()) {
                    try {
                        stmt.execute("XA COMMIT " + xid);
                    } catch (SQLException ex) {
                        if (ex.getErrorCode() == com.alibaba.polardbx.ErrorCode.ER_XAER_NOTA) {
                            logger.warn("XA COMMIT got ER_XAER_NOTA: " + xid, ex);
                        } else {
                            throw GeneralUtil.nestedException(ex);
                        }
                    }
                } catch (Throwable e) {
                    // discard connection if something failed.
                    discard(group, conn, e);

                    logger.warn("XA COMMIT failed: " + xid, e);

                    // Retry XA COMMIT in asynchronous task.
                    AsyncTaskQueue asyncQueue = getManager().getTransactionExecutor().getAsyncQueue();
                    asyncQueue.submit(
                        () -> XAUtils.commitUntilSucceed(id, primaryGroupUid, group, dataSourceCache.get(group)));
                }
            }
        });
    }

    @Override
    protected void cleanup(String group, IConnection conn) throws SQLException {
        if (conn.isClosed()) {
            return;
        }
        if (!shareReadView && conn == primaryConnection) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("rollback");
            }
        } else {
            String xid = getXid(group, conn, shareReadView);
            try (Statement stmt = conn.createStatement()) {
                stmt.execute(getXARollbackSqls(xid));
            } catch (SQLException e) {
                // discard connection if cleanup failed.
                throw GeneralUtil.nestedException("XA END and ROLLBACK failed: " + xid, e);
            }
        }
    }

    @Override
    public ITransactionPolicy.TransactionClass getTransactionClass() {
        return ITransactionPolicy.TransactionClass.XA;
    }
}
