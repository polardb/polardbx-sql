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
import com.alibaba.polardbx.common.jdbc.MasterSlave;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.type.TransactionType;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.thread.LockUtils;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.rpc.pool.XConnection;
import com.alibaba.polardbx.transaction.async.AsyncTaskQueue;
import com.alibaba.polardbx.transaction.jdbc.SavePoint;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Statement;
import java.util.Enumeration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_TRANS_COMMIT;

/**
 * TSO Transaction, with global MVCC support
 */
public class TsoTransaction extends ShareReadViewTransaction implements ITsoTransaction {

    private final static Logger logger = LoggerFactory.getLogger(TsoTransaction.class);

    private final static String TRX_LOG_PREFIX = "[" + ITransactionPolicy.TransactionClass.TSO + "]";

    public final static String SET_REMOVE_DISTRIBUTED_TRX = "SET polarx_remove_d_trx = true";
    public final static String SET_DISTRIBUTED_TRX_ID = "SET polarx_distributed_trx_id = %s";
    public final static String SET_ASYNC_COMMIT_PREPARE_INFO =
        "SET innodb_prepare_seq = %s"
            + ", polarx_distributed_trx_id = %s"
            + ", polarx_n_trx_branches = %s"
            + ", polarx_n_participants = %s";

    private long snapshotTimestamp = -1L;
    protected long commitTimestamp = -1L;
    /**
     * 0 means no prepare sequence.
     */
    private long prepareTimestamp = 0L;
    private AtomicLong minCommitTimestamp;
    private AtomicInteger nPreparedDn;

    public TsoTransaction(ExecutionContext executionContext,
                          TransactionManager manager) {
        super(executionContext, manager);
        // Set get TSO timeout.
        manager.getTimestampOracle()
            .setTimeout(executionContext.getParamManager().getLong(ConnectionParams.GET_TSO_TIMEOUT));
    }

    @Override
    protected String getTrxLoggerPrefix() {
        return TRX_LOG_PREFIX;
    }

    @Override
    public TransactionType getType() {
        return TransactionType.TSO;
    }

    @Override
    public long getSnapshotSeq() {
        return snapshotTimestamp;
    }

    @Override
    public boolean snapshotSeqIsEmpty() {
        return snapshotTimestamp <= 0;
    }

    @Override
    protected void beginNonParticipant(String group, IConnection conn) throws SQLException {
        if (snapshotTimestamp < 0) {
            snapshotTimestamp = nextTimestamp(t -> stat.getTsoTime += t);
        }

        super.beginNonParticipant(group, conn);
        sendSnapshotSeq(conn);
    }

    @Override
    protected void begin(String schema, String group, IConnection conn) throws SQLException {
        if (snapshotTimestamp < 0) {
            snapshotTimestamp = nextTimestamp(t -> stat.getTsoTime += t);
        }
        String xid = getXid(group, conn);
        try {
            final XConnection xConnection;
            if (conn.isWrapperFor(XConnection.class) &&
                (xConnection = conn.unwrap(XConnection.class)).supportMessageTimestamp()) {
                conn.flushUnsent();
                if (shareReadView) {
                    xConnection.execUpdate(TURN_ON_TXN_GROUP_SQL, null, true);
                }
                xConnection.execUpdate("XA START " + xid, null, true);
            } else {
                if (shareReadView) {
                    conn.executeLater(TURN_ON_TXN_GROUP_SQL);
                }
                conn.executeLater("XA START " + xid);
            }
            sendSnapshotSeq(conn);

            for (String savepoint : savepoints) {
                SavePoint.setLater(conn, savepoint);
            }
        } catch (SQLException e) {
            logger.error("TSO Transaction init failed on " + group + ":" + e.getMessage());
            throw e;
        }

        // Enable xa recovery scanning in case that user only use cross-schema transaction in that schema
        TransactionManager.getInstance(schema).enableXaRecoverScan();
        TransactionManager.getInstance(schema).enableKillTimeoutTransaction();
    }

    @Override
    public void reinitializeConnection(String schema, String group, IConnection conn) throws SQLException {
        if (isolationLevel != Connection.TRANSACTION_READ_COMMITTED) {
            return;
        }

        if (snapshotTimestamp < 0) {
            snapshotTimestamp = nextTimestamp(t -> stat.getTsoTime += t);
        }

        try {
            sendSnapshotSeq(conn);
        } catch (SQLException e) {
            logger.error(
                "Reinitialize physical connection for TSO Transaction failed on " + group + ":" + e.getMessage());
            throw e;
        }
    }

    @Override
    public void updateSnapshotTimestamp() {
        if (isolationLevel != Connection.TRANSACTION_READ_COMMITTED) {
            return;
        }

        snapshotTimestamp = nextTimestamp(t -> stat.getTsoTime += t);
    }

    @Override
    protected void afterPrepare() {
        // Get commit timestamp
        this.commitTimestamp = nextTimestamp(t -> stat.getTsoTime += t);
    }

    @Override
    protected void writeCommitLog(IConnection logConn) throws SQLException {
        globalTxLogManager.append(id, getType(), TransactionState.SUCCEED, connectionContext,
            commitTimestamp, logConn);
    }

    @Override
    protected void prepareConnections(boolean asyncCommit) {
        forEachHeldConnection((heldConn) -> {
            switch (heldConn.getParticipated()) {
            case NONE:
                rollbackNonParticipantSync(heldConn.getGroup(), heldConn.getRawConnection());
                break;
            case SHARE_READVIEW_READ:
                rollbackNonParticipantShareReadViewSync(heldConn.getGroup(), heldConn.getRawConnection());
                break;
            case WRITTEN:
                prepareParticipatedConn(heldConn, asyncCommit);
                break;
            }
        });
    }

    private void prepareParticipatedConn(TransactionConnectionHolder.HeldConnection heldConn, boolean asyncCommit) {
        final IConnection conn = heldConn.getRawConnection();
        final String group = heldConn.getGroup();
        // XA transaction must be 'ACTIVE' state here.
        try {
            if (asyncCommit) {
                execAsyncCommitPrepareSql(heldConn);
            } else {
                execTsoPrepareSql(heldConn);
            }
        } catch (Throwable e) {
            throw new TddlRuntimeException(ERR_TRANS_COMMIT, e,
                "XA PREPARE failed: " + getXid(group, conn));
        }

    }

    private void execTsoPrepareSql(TransactionConnectionHolder.HeldConnection heldConn) throws SQLException {
        final IConnection conn = heldConn.getRawConnection();
        final String group = heldConn.getGroup();
        String xid = getXid(group, conn);
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("XA END " + xid + ';' + " XA PREPARE " + xid);
        }
    }

    private void execAsyncCommitPrepareSql(TransactionConnectionHolder.HeldConnection heldConn) throws SQLException {
        final IConnection conn = heldConn.getRawConnection();
        final String group = heldConn.getGroup();
        String xid = getXid(group, conn);

        final String innodbAsyncCommitInfo = String.format(
            SET_ASYNC_COMMIT_PREPARE_INFO,
            prepareTimestamp,
            id,
            connectionHolder.getDnBranchMap().get(heldConn.getDnInstId()),
            connectionHolder.getDnBranchMap().size());

        conn.executeLater(innodbAsyncCommitInfo);
        conn.executeLater("XA END " + xid);
        if (conn.isWrapperFor(XConnection.class)) {
            conn.unwrap(XConnection.class).getSession().setChunkResult(false);
        }
        try (final Statement stmt = conn.createStatement();
            final ResultSet rs = stmt.executeQuery("XA PREPARE " + xid)) {
            if (rs.next()) {
                // Get min commit timestamp.
                final long localMinCommitTimestamp = rs.getLong(1);
                if (0 == localMinCommitTimestamp) {
                    // Not the last prepared branch.
                    return;
                }
                long globalCommitTimestamp = minCommitTimestamp.get();
                while (globalCommitTimestamp < localMinCommitTimestamp && !minCommitTimestamp
                    .compareAndSet(globalCommitTimestamp, localMinCommitTimestamp)) {
                    globalCommitTimestamp = minCommitTimestamp.get();
                }
                nPreparedDn.incrementAndGet();
            }
        } catch (Throwable e) {
            throw new TddlRuntimeException(ErrorCode.ERR_TRANS_COMMIT, e, "XA PREPARE failed: " + xid);
        }
    }

    @Override
    protected void innerCommitOneShardTrx(String group, IConnection conn) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            String xid = getXid(group, conn);
            stmt.execute(getXACommitOnePhaseSqls(xid));
        }
    }

    @Override
    protected void commitMultiShardTrx() {
        if (executionContext.enableAsyncCommit() && manager.supportAsyncCommit()) {
            asyncCommitMultiShardTrx();
        } else {
            syncCommitMultiShardTrx();
        }
    }

    /**
     * Normal 2PC.
     */
    private void syncCommitMultiShardTrx() {
        long prepareStartTime = System.nanoTime();
        // Whether succeed to write commit log, or may be unknown
        TransactionCommitState commitState = TransactionCommitState.FAILURE;

        RuntimeException exception = null;
        try {
            // XA PREPARE on all groups
            prepareConnections(false);
            stat.prepareTime = System.nanoTime() - prepareStartTime;
            TransactionLogger.info(id, "[TSO] Prepared");

            this.prepared = true;
            this.state = State.PREPARED;

            // Get commit timestamp and Write commit log via an external connection
            commitTimestamp = nextTimestamp(t -> stat.getTsoTime += t);

            if (isCrossGroup && !executionContext.getParamManager()
                .getBoolean(ConnectionParams.TSO_OMIT_GLOBAL_TX_LOG)) {
                long logStartTime = System.nanoTime();
                try (IConnection logConn = dataSourceCache.get(primaryGroup).getConnection(MasterSlave.MASTER_ONLY)) {
                    beforePrimaryCommit();
                    commitState = TransactionCommitState.UNKNOWN;

                    duringPrimaryCommit();
                    writeCommitLog(logConn);

                    afterPrimaryCommit();
                } catch (SQLIntegrityConstraintViolationException ex) {
                    // Conflict global_tx_log is found, interrupt.
                    throw new TddlRuntimeException(ErrorCode.ERR_TRANS, ex,
                        "Transaction ID exists. Commit interrupted");
                } catch (SQLException ex) {
                    throw new TddlRuntimeException(ErrorCode.ERR_TRANS_LOG, ex,
                        "Failed to write commit state on group: " + primaryGroup);
                }
                stat.trxLogTime = System.nanoTime() - logStartTime;
            }

            commitState = TransactionCommitState.SUCCESS;
        } catch (RuntimeException ex) {
            exception = ex;
        }

        if (commitState == TransactionCommitState.FAILURE) {
            /*
             * XA 失败回滚：XA ROLLBACK 提交刚才 PREPARE 的连接
             */
            rollbackConnections();

            TransactionLogger.error(id, "[TSO] Aborted by committing failed");

        } else if (commitState == TransactionCommitState.SUCCESS) {
            /*
             * XA 提交成功：XA COMMIT 提交刚才 PREPARE 的其他连接
             */
            TransactionLogger.info(id, "[TSO] Commit Point");

            commitConnections();

            TransactionLogger.info(id, "[TSO] Committed");
        } else {
            /*
             * Transaction state is unknown so we cannot do anything unless we
             * know the actual transaction state. This case does not happen
             * frequently. Just leave it to the recovering thread.
             */
            discardConnections();

            TransactionLogger.error(id, "[TSO] Aborted with unknown commit state");
        }

        connectionHolder.closeAllConnections();
        if (exception != null) {
            throw exception;
        }
    }

    /**
     * Async commit.
     */
    private void asyncCommitMultiShardTrx() {
        long prepareStartTime = System.nanoTime();
        boolean canAsyncCommit = true;

        // Whether succeed to write commit log, or may be unknown
        TransactionCommitState commitState = TransactionCommitState.UNKNOWN;

        RuntimeException exception = null;
        try {
            // Get prepare timestamp.
            prepareTimestamp = executionContext.omitPrepareTs() ? 0 : nextTimestamp(t -> stat.getTsoTime += t);

            minCommitTimestamp = new AtomicLong(0L);
            // Number of actually prepared DNs.
            nPreparedDn = new AtomicInteger(0);

            try {
                beforePrimaryCommit();
                duringPrimaryCommit();
                // XA PREPARE on all groups
                prepareConnections(true);
                afterPrimaryCommit();
                TransactionLogger.info(id, "[TSO][Async Commit] Prepared");
            } catch (SQLException e) {
                throw new TddlRuntimeException(ERR_TRANS_COMMIT, "Error when async commit.", e);
            }

            // Expect all involved DNs are successfully prepared.
            if (nPreparedDn.get() == connectionHolder.getDnBranchMap().size()) {
                prepared = true;
                state = State.PREPARED;

                // Let max(min-commit-timestamp) be the final commit timestamp.
                commitTimestamp = convertFromMinCommitSeq(minCommitTimestamp.get());

                if (TransactionManager.isExceedAsyncCommitTaskLimit()) {
                    canAsyncCommit = false;
                } else {
                    TransactionManager.addAsyncCommitTask();
                }

                if (InstConfUtil.getBool(ConnectionParams.ENABLE_TRX_SINGLE_SHARD_OPTIMIZATION) && canAsyncCommit) {
                    // If we run commit phase in async-mode and single shard optimization is on,
                    // use commit timestamp to push the max sequence on each involved DN before responding to client.
                    // This ensure the "read-your-own-writes" consistency.
                    pushMaxSeq();
                }

                commitState = TransactionCommitState.SUCCESS;
            } else {
                StringBuilder errorMsg =
                    new StringBuilder("Async Commit prepare failed, number of prepared DNs does not match, expected "
                        + connectionHolder.getDnBranchMap().size() + ", actual " + nPreparedDn.get()
                        + ", all DN: ");
                final Enumeration<String> iter = connectionHolder.getDnBranchMap().keys();
                while (iter.hasMoreElements()) {
                    errorMsg.append(iter.nextElement());
                }
                exception = new TddlRuntimeException(ERR_TRANS_COMMIT, errorMsg.toString());
            }
        } catch (RuntimeException ex) {
            exception = ex;
        }

        stat.prepareTime = System.nanoTime() - prepareStartTime;

        boolean closeConnection = true;

        if (commitState == TransactionCommitState.SUCCESS) {
            if (canAsyncCommit) {
                underCommitting = true;
                asyncCommit = true;
                // Avoid closing connections, and they will be closed after async commit.
                closeConnection = false;

                commitConnectionsAsync();

                // Detach this trx from connection.
                executionContext = null;
                TransactionLogger.info(id, "[TSO][Async Commit] Async Committed.");
            } else {
                commitConnections();
            }
        } else {
            /*
             * Transaction state is unknown so we cannot do anything unless we
             * know the actual transaction state. This case does not happen
             * frequently. Just leave it to the recovering thread.
             */
            discardConnections();

            TransactionLogger.error(id, "[TSO][Async Commit] Aborted with unknown commit state");
        }

        if (closeConnection) {
            connectionHolder.closeAllConnections();
        }

        if (exception != null) {
            throw exception;
        }
    }

    /**
     * Commit all connections including primary group
     */
    @Override
    protected void commitConnections() {
        forEachHeldConnection(new TransactionConnectionHolder.Action() {
            @Override
            public boolean condition(TransactionConnectionHolder.HeldConnection heldConn) {
                // Ignore non-participant connections. They were committed during prepare phase.
                return heldConn.isParticipated();
            }

            @Override
            public void execute(TransactionConnectionHolder.HeldConnection heldConn) {
                commitOneBranch(heldConn);
            }
        });
    }

    /**
     * Commit the leader branch for each DN to push the max sequence, used by Async Commit.
     */
    private void pushMaxSeq() {
        forEachHeldConnection(new TransactionConnectionHolder.Action() {
            @Override
            public boolean condition(TransactionConnectionHolder.HeldConnection heldConn) {
                // Ignore non-participant connections. They were committed during prepare phase.
                return heldConn.isDnLeader();
            }

            @Override
            public void execute(TransactionConnectionHolder.HeldConnection heldConn) {
                if (InstConfUtil.getBool(ConnectionParams.ASYNC_COMMIT_PUSH_MAX_SEQ_ONLY_LEADER)) {
                    pushMaxSeqOnlyLeader(heldConn);
                } else {
                    commitOneBranch(heldConn);
                }
            }
        });
    }

    protected void commitOneBranch(TransactionConnectionHolder.HeldConnection heldConn) {
        IConnection conn = heldConn.getRawConnection();
        if (heldConn.isDnLeader() && manager.supportAsyncCommit()) {
            try {
                conn.executeLater(SET_REMOVE_DISTRIBUTED_TRX);
            } catch (SQLException e) {
                // discard connection if something failed.
                conn.discard(e);
                throw new TddlRuntimeException(ERR_TRANS_COMMIT, e);
            }
        }

        // XA transaction must be 'PREPARED' state here.
        String xid = getXid(heldConn.getGroup(), conn);
        try (Statement stmt = conn.createStatement()) {
            try {
                final XConnection xConnection;
                if (conn.isWrapperFor(XConnection.class) &&
                    (xConnection = conn.unwrap(XConnection.class)).supportMessageTimestamp()) {
                    conn.flushUnsent();
                    xaCommitXConn(xConnection, xid);
                } else {
                    stmt.execute(getXACommitWithTsoSql(xid));
                }
                heldConn.setCommitted(true);
            } catch (SQLException ex) {
                if (ex.getErrorCode() == ErrorCode.ER_XAER_NOTA.getCode()) {
                    logger.warn("XA COMMIT got ER_XAER_NOTA: " + xid, ex);
                } else {
                    throw GeneralUtil.nestedException(ex);
                }
            }
        } catch (Throwable e) {
            // discard connection if something failed.
            conn.discard(e);
        }
    }

    /**
     * Push max sequence in DN leader.
     */
    protected void pushMaxSeqOnlyLeader(TransactionConnectionHolder.HeldConnection heldConn) {
        IConnection conn = heldConn.getRawConnection();

        // XA transaction must be 'PREPARED' state here.
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("SET GLOBAL innodb_push_seq = " + commitTimestamp);
        } catch (Throwable e) {
            // discard connection if something failed.
            conn.discard(e);
        }
    }

    /**
     * Commit all connections including primary group asynchronously.
     */
    public void commitConnectionsAsync() {
        final AsyncTaskQueue asyncQueue = getManager().getTransactionExecutor().getAsyncQueue();
        asyncQueue.submit(() -> {
            lock.lock();
            try {
                commitConnectionsAsyncInner();
            } catch (Throwable t) {
                logger.error("Async Commit: Commit connections failed.", t);
            } finally {
                TransactionManager.finishAsyncCommitTask();
                lock.unlock();
            }
        });
    }

    private void commitConnectionsAsyncInner() {
        try {
            TransactionLogger.debug(id, "[TSO][Async Commit] Start async commit");
            forEachHeldConnection(new TransactionConnectionHolder.Action() {
                @Override
                public boolean condition(TransactionConnectionHolder.HeldConnection heldConn) {
                    // Ignore non-participant connections. They were committed during prepare phase.
                    return heldConn.isParticipated() && !heldConn.isCommitted();
                }

                @Override
                public void execute(TransactionConnectionHolder.HeldConnection heldConn) {
                    commitOneBranch(heldConn);
                }
            });
        } finally {
            // Async commit finished.
            TransactionLogger.debug(id, "[TSO][Async Commit] Async commit finished");
            this.underCommitting = false;

            try {
                LockUtils.releaseReadStampLocks(txSharedLocks);
            } catch (Throwable t) {
                logger.error("Release shared lock after async commit failed.", t);
            }

            // Close all connections.
            connectionHolder.closeAllConnections();

            // Close this transaction.
            this.close();
        }
    }

    protected void xaCommitXConn(XConnection xConnection, String xid) throws SQLException {
        xConnection.setLazyCommitSeq(commitTimestamp);
        xConnection.execUpdate("XA COMMIT " + xid);
        if (shareReadView) {
            xConnection.execUpdate(TURN_OFF_TXN_GROUP_SQL, null, true);
        }
    }

    /**
     * SET innodb_commit_seq; XA COMMIT; Turn off share read view;
     */
    protected String getXACommitWithTsoSql(String xid) {
        final StringBuilder sb = new StringBuilder();
        // Set commit timestamp and XA commit.
        sb.append("SET innodb_commit_seq = ").append(commitTimestamp).append("; XA COMMIT ").append(xid).append("; ");

        // Reset share review flag.
        if (shareReadView) {
            sb.append(TURN_OFF_TXN_GROUP_SQL);
        }

        return sb.toString();
    }

    @Override
    public ITransactionPolicy.TransactionClass getTransactionClass() {
        return ITransactionPolicy.TransactionClass.TSO;
    }

    /**
     * @return a valid commit sequence.
     */
    public static long convertFromMinCommitSeq(long minCommitSeq) {
        if (isMinCommitSeq(minCommitSeq)) {
            return (minCommitSeq & (~1));
        }
        return minCommitSeq;
    }

    /**
     * @return true if the given sequence is a min commit sequence.
     */
    public static boolean isMinCommitSeq(long seq) {
        return (1 == (seq & 1));
    }

}
