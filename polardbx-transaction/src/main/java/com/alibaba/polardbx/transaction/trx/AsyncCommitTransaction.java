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

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.IConnection;
import com.alibaba.polardbx.common.jdbc.ITransactionPolicy;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.thread.LockUtils;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.utils.ITransaction;
import com.alibaba.polardbx.rpc.pool.XConnection;
import com.alibaba.polardbx.transaction.connection.TransactionConnectionHolder;
import com.alibaba.polardbx.transaction.TransactionLogger;
import com.alibaba.polardbx.transaction.TransactionManager;
import com.alibaba.polardbx.transaction.async.AsyncTaskQueue;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Enumeration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_TRANS_COMMIT;

/**
 * This transaction use async commit for 2PC.
 *
 * @author yaozhili
 */
public class AsyncCommitTransaction extends TsoTransaction {
    private final static Logger logger = LoggerFactory.getLogger(AsyncCommitTransaction.class);
    /**
     * 0 means no prepare sequence.
     */
    private long prepareTimestamp = 0L;
    private AtomicLong minCommitTimestamp;
    private AtomicInteger nPreparedDn;

    public final static String SET_REMOVE_DISTRIBUTED_TRX = "SET polarx_remove_d_trx = true";
    public final static String SET_ASYNC_COMMIT_PREPARE_INFO =
        "SET innodb_prepare_seq = %s"
            + ", polarx_distributed_trx_id = %s"
            + ", polarx_n_trx_branches = %s"
            + ", polarx_n_participants = %s";

    private final static String TRX_LOG_PREFIX = "[" + ITransactionPolicy.TransactionClass.TSO_ASYNC_COMMIT + "]";

    public AsyncCommitTransaction(ExecutionContext executionContext,
                                  TransactionManager manager) {
        super(executionContext, manager);
    }

    @Override
    protected String getTrxLoggerPrefix() {
        return TRX_LOG_PREFIX;
    }

    @Override
    protected void commitMultiShardTrx() {
        long prepareStartTime = System.nanoTime();
        boolean canAsyncCommit = true;

        // Whether succeed to write commit log, or may be unknown
        AbstractTransaction.TransactionCommitState commitState = AbstractTransaction.TransactionCommitState.UNKNOWN;

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
                prepareConnections();
                afterPrimaryCommit();
                TransactionLogger.info(id, "[TSO][Async Commit] Prepared");
            } catch (SQLException e) {
                throw new TddlRuntimeException(ERR_TRANS_COMMIT, "Error when async commit.", e);
            }

            // Expect all involved DNs are successfully prepared.
            if (nPreparedDn.get() == connectionHolder.getDnBranchMap().size()) {
                prepared = true;
                state = ITransaction.State.PREPARED;

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

                commitState = AbstractTransaction.TransactionCommitState.SUCCESS;
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

        if (commitState == AbstractTransaction.TransactionCommitState.SUCCESS) {
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

    @Override
    protected void prepareParticipatedConn(TransactionConnectionHolder.HeldConnection heldConn) {
        // XA transaction must be 'ACTIVE' state here.
        try {
            execAsyncCommitPrepareSql(heldConn);
        } catch (Throwable e) {
            final IConnection conn = heldConn.getRawConnection();
            final String group = heldConn.getGroup();
            throw new TddlRuntimeException(ERR_TRANS_COMMIT, e, "XA PREPARE failed: " + getXid(group, conn));
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
                // It is the last prepared branch on this DN, make it the DN leader.
                heldConn.setDnLeader(true);
                nPreparedDn.incrementAndGet();
            }
        } catch (Throwable e) {
            throw new TddlRuntimeException(ErrorCode.ERR_TRANS_COMMIT, e, "XA PREPARE failed: " + xid);
        }
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

    @Override
    protected void beforeCommitOneBranchHook(TransactionConnectionHolder.HeldConnection heldConn) {
        IConnection conn = heldConn.getRawConnection();
        if (heldConn.isDnLeader()) {
            try {
                conn.executeLater(SET_REMOVE_DISTRIBUTED_TRX);
            } catch (SQLException e) {
                // discard connection if something failed.
                conn.discard(e);
                throw new TddlRuntimeException(ERR_TRANS_COMMIT, e);
            }
        }
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

    /**
     * Push max sequence in DN leader.
     */
    private void pushMaxSeqOnlyLeader(TransactionConnectionHolder.HeldConnection heldConn) {
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
