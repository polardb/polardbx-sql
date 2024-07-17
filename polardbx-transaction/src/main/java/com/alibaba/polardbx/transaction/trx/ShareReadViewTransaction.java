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
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.thread.LockUtils;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.transaction.connection.TransactionConnectionHolder;
import com.alibaba.polardbx.transaction.TransactionLogger;
import com.alibaba.polardbx.transaction.TransactionManager;
import com.alibaba.polardbx.transaction.async.AsyncTaskQueue;
import com.alibaba.polardbx.stats.TransactionStatistics;
import com.alibaba.polardbx.transaction.utils.XAUtils;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.StampedLock;

/**
 * Support sharing a read view based on XA stmts
 * in multiple connections inside the transaction
 */
public abstract class ShareReadViewTransaction extends AbstractTransaction {

    private final static Logger logger = LoggerFactory.getLogger(ShareReadViewTransaction.class);
    protected boolean shareReadView;

    public static final String TURN_OFF_TXN_GROUP_SQL = "SET innodb_transaction_group = OFF";
    public static final String TURN_ON_TXN_GROUP_SQL = "SET innodb_transaction_group = ON";
    /**
     * do not modify MAX_READ_VIEW_COUNT,
     * since read view sequence only supports 4-digit number
     */
    private static final int MAX_READ_VIEW_COUNT = 10000;

    private final AtomicInteger readViewConnCounter = new AtomicInteger(0);

    protected Collection<Pair<StampedLock, Long>> txSharedLocks = null;

    public ShareReadViewTransaction(ExecutionContext executionContext,
                                    TransactionManager manager) {
        super(executionContext, manager);
        this.shareReadView = executionContext.isShareReadView();
    }

    protected String getXid(String group, IConnection conn) {
        if (conn.getTrxXid() != null) {
            return conn.getTrxXid();
        }
        conn.setInShareReadView(shareReadView);
        String xid;
        if (shareReadView) {
            xid = XAUtils.toXidString(id, group, primaryGroupUid, getReadViewSeq(group));
        } else {
            xid = XAUtils.toXidString(id, group, primaryGroupUid);
        }
        conn.setTrxXid(xid);
        return xid;
    }

    /**
     * 获取 ReadView 序列号
     * 用于区分同一个 ReadView 下不同的连接
     */
    protected int getReadViewSeq(String group) {
        int readViewCount = readViewConnCounter.getAndIncrement();
        if (readViewCount >= MAX_READ_VIEW_COUNT) {
            throw new TddlRuntimeException(ErrorCode.ERR_CONCURRENT_TRANSACTION, group,
                "share read view connections exceeds limit " + MAX_READ_VIEW_COUNT);
        }
        return readViewCount % MAX_READ_VIEW_COUNT;
    }

    protected String getXAIdleRollbackSqls(String xid) {
        if (shareReadView) {
            return String.format("XA ROLLBACK %s ;" + TURN_OFF_TXN_GROUP_SQL, xid);
        } else {
            return String.format("XA ROLLBACK %s ;", xid);
        }
    }

    protected String getXARollbackSqls(String xid) {
        if (shareReadView) {
            return String.format("XA END %s ; XA ROLLBACK %s ;" + TURN_OFF_TXN_GROUP_SQL,
                xid, xid);
        } else {
            return String.format("XA END %s ; XA ROLLBACK %s ;",
                xid, xid);
        }
    }

    protected String getXACommitOnePhaseSqls(String xid) {
        if (shareReadView) {
            return String.format("XA END %s ; XA COMMIT %s ONE PHASE ;" + TURN_OFF_TXN_GROUP_SQL,
                xid, xid);
        } else {
            return String.format("XA END %s ; XA COMMIT %s ONE PHASE ;",
                xid, xid);
        }
    }

    protected void rollbackNonParticipantShareReadViewSync(String group, IConnection conn) {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(getXARollbackSqls(getXid(group, conn)));
        } catch (Throwable e) {
            logger.error("Rollback non-participant share readview group failed on " + group, e);
            conn.discard(e);
        }
    }

    /**
     * Commit transaction without participants or with only one participant.
     * <p>
     * Use XA COMMIT ONE PHASE to commit transaction with only one shard.
     */
    protected void commitOneShardTrx() {
        forEachHeldConnection((heldConn) -> {
            switch (heldConn.getParticipated()) {
            case NONE:
                rollbackNonParticipantSync(heldConn.getGroup(), heldConn.getRawConnection());
                break;
            case SHARE_READVIEW_READ:
                rollbackNonParticipantShareReadViewSync(heldConn.getGroup(), heldConn.getRawConnection());
                break;
            case WRITTEN:
                if (heldConn.getRawConnection() != primaryConnection) {
                    throw new AssertionError("commitOneShardTrx with non-primary participant");
                }

                try {
                    innerCommitOneShardTrx(heldConn.getGroup(), heldConn.getRawConnection());
                } catch (Throwable e) {
                    logger.error("XA COMMIT ONE PHASE failed on " + primaryGroup, e);
                    throw GeneralUtil.nestedException(e);
                }
                break;
            }
        });

        connectionHolder.closeAllConnections();
    }

    protected abstract void innerCommitOneShardTrx(String group, IConnection conn) throws SQLException;

    /**
     * Cleanup a transaction connection (write-connection)
     */
    @Override
    protected void cleanup(String group, IConnection conn) throws SQLException {
        if (conn.isClosed()) {
            return;
        }

        // XA transaction must be 'ACTIVE' state on cleanup.
        String xid = getXid(group, conn);
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(getXARollbackSqls(xid));
        } catch (SQLException e) {
            // discard connection if cleanup failed.
            throw GeneralUtil.nestedException("XA END and ROLLBACK failed: " + xid, e);
        }
    }

    /**
     * Rollback all XA connections, including primary connection.
     */
    protected void rollbackConnections() {
        forEachHeldConnection(new TransactionConnectionHolder.Action() {
            @Override
            public boolean condition(TransactionConnectionHolder.HeldConnection heldConn) {
                // Ignore non-participant connections. They were committed during prepare phase.
                return heldConn.isParticipated();
            }

            @Override
            public void execute(TransactionConnectionHolder.HeldConnection heldConn) {
                innerRollback(heldConn.getGroup(), heldConn.getRawConnection());
            }
        });
    }

    protected void innerRollback(String group, IConnection conn) {
        // XA transaction must in 'ACTIVE', 'IDLE' or 'PREPARED' state, so ROLLBACK first.
        String xid = getXid(group, conn);
        try (Statement stmt = conn.createStatement()) {
            try {
                stmt.execute(getXAIdleRollbackSqls(xid));
            } catch (SQLException ex) {
                if (ex.getErrorCode() == ErrorCode.ER_XAER_RMFAIL.getCode()) {
                    // XA ROLLBACK got ER_XAER_RMFAIL, XA transaction must in 'ACTIVE' state, so END and ROLLBACK.
                    stmt.execute(getXARollbackSqls(xid));
                } else if (ex.getErrorCode() == ErrorCode.ER_XAER_NOTA.getCode()) {
                    logger.warn("XA ROLLBACK got ER_XAER_NOTA: " + xid, ex);
                } else {
                    throw GeneralUtil.nestedException(ex);
                }
            }
        } catch (Throwable e) {
            // discard connection if something failed.
            conn.discard(e);

            logger.warn("XA ROLLBACK failed: " + xid, e);

            // Retry XA ROLLBACK in asynchronous task.
            AsyncTaskQueue asyncQueue = getManager().getTransactionExecutor().getAsyncQueue();
            asyncQueue.submit(
                () -> XAUtils.rollbackUntilSucceed(id, xid, dataSourceCache.get(group)));
        }
    }

    @Override
    public void commit() {
        long commitStartTime = System.nanoTime();
        lock.lock();
        try {
            checkTerminated();
            checkCanContinue();

            if (!isCrossGroup && executionContext.isEnable1PCOpt()) {
                commitOneShardTrx();
                return;
            }

            Optional.ofNullable(OptimizerContext.getTransStat(primarySchema))
                .ifPresent(s -> s.countCrossGroup.incrementAndGet());

            this.txSharedLocks = acquireSharedLock();
            try {
                commitMultiShardTrx();
            } finally {
                if (!isAsyncCommit()) {
                    LockUtils.releaseReadStampLocks(txSharedLocks);
                }
            }
        } catch (Throwable t) {
            Optional.ofNullable(OptimizerContext.getTransStat(statisticSchema))
                .ifPresent(s -> s.countCommitError.incrementAndGet());
            stat.setIfUnknown(TransactionStatistics.Status.COMMIT_FAIL);
            throw t;
        } finally {
            stat.setIfUnknown(TransactionStatistics.Status.COMMIT);
            stat.commitTime = System.nanoTime() - commitStartTime;
            lock.unlock();
        }
    }

    /**
     * Commit transaction with multiple participants.
     */
    protected abstract void commitMultiShardTrx();

    protected abstract String getTrxLoggerPrefix();

    protected abstract void writeCommitLog(IConnection logConn) throws SQLException;

    /**
     * Prepare on all XA connections
     */
    protected abstract void prepareConnections();

    /**
     * Commit all connections including primary group
     */
    protected abstract void commitConnections();

    @Override
    public void rollback() {
        long rollbackStartTime = System.nanoTime();
        lock.lock();
        try {
            cleanupAllConnections();
            connectionHolder.closeAllConnections();

            TransactionLogger.warn(id, getTrxLoggerPrefix() + " Aborted");

            Optional.ofNullable(OptimizerContext.getTransStat(statisticSchema))
                .ifPresent(s -> s.countRollback.incrementAndGet());
            stat.setIfUnknown(TransactionStatistics.Status.ROLLBACK);
        } catch (Throwable t) {
            Optional.ofNullable(OptimizerContext.getTransStat(statisticSchema))
                .ifPresent(s -> s.countRollbackError.incrementAndGet());
            stat.setIfUnknown(TransactionStatistics.Status.ROLLBACK_FAIL);
            throw t;
        } finally {
            stat.rollbackTime = System.nanoTime() - rollbackStartTime;
            lock.unlock();
        }
    }

    @Override
    public void innerCleanupAllConnections(String group, IConnection conn,
                                           TransactionConnectionHolder.ParticipatedState participatedState) {
        switch (participatedState) {
        case NONE:
            rollbackNonParticipantSync(group, conn);
            return;
        case SHARE_READVIEW_READ:
            rollbackNonParticipantShareReadViewSync(group, conn);
            return;
        case WRITTEN:
            cleanupParticipateConn(group, conn);
            return;
        }
    }

    protected void discardConnections() {
        forEachHeldConnection((heldConn) -> {
            // XA transaction must be 'PREPARED' state here, The
            // primary commit state is unknown, so we don't know how to
            // ROLLBACK or COMMIT.
            heldConn.getRawConnection().discard(null);
        });
    }

    @Override
    public boolean isStrongConsistent() {
        return true;
    }

    @Override
    public boolean allowMultipleReadConns() {
        return executionContext.isShareReadView();
    }

    @Override
    public boolean allowMultipleWriteConns() {
        return executionContext.isAllowGroupMultiWriteConns() && executionContext.isShareReadView();
    }
}
