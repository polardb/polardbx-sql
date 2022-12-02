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
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.transaction.async.AsyncTaskQueue;
import com.alibaba.polardbx.transaction.utils.XAUtils;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;

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
            xid = XAUtils.XATransInfo.toXidString(id, group, primaryGroupUid, getReadViewSeq(group));
        } else {
            xid = XAUtils.XATransInfo.toXidString(id, group, primaryGroupUid);
        }
        conn.setTrxXid(xid);
        return xid;
    }

    /**
     * 获取 ReadView 序列号
     * 用于区分同一个 ReadView 下不同的连接
     */
    private int getReadViewSeq(String group) {
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
            discard(group, conn, e);
        }
    }

    /**
     * Commit transaction without participants or with only one participant.
     * <p>
     * Use XA COMMIT ONE PHASE to commit transaction with only one shard.
     */
    protected void commitOneShardTrx() {
        forEachHeldConnection((group, conn, participated) -> {
            switch (participated) {
            case NONE:
                rollbackNonParticipantSync(group, conn);
                break;
            case SHARE_READVIEW_READ:
                rollbackNonParticipantShareReadViewSync(group, conn);
                break;
            case WRITTEN:
                if (conn != primaryConnection) {
                    throw new AssertionError("commitOneShardTrx with non-primary participant");
                }

                try {
                    innerCommitOneShardTrx(group, conn);
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
            public boolean condition(String group, IConnection conn,
                                     TransactionConnectionHolder.ParticipatedState participated) {
                // Ignore non-participant connections. They were committed during prepare phase.
                return participated.participatedTrx();
            }

            @Override
            public void execute(String group, IConnection conn,
                                TransactionConnectionHolder.ParticipatedState participated) {
                innerRollback(group, conn);
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
                if (ex.getErrorCode() == com.alibaba.polardbx.ErrorCode.ER_XAER_RMFAIL) {
                    // XA ROLLBACK got ER_XAER_RMFAIL, XA transaction must in 'ACTIVE' state, so END and ROLLBACK.
                    stmt.execute(getXARollbackSqls(xid));
                } else if (ex.getErrorCode() == com.alibaba.polardbx.ErrorCode.ER_XAER_NOTA) {
                    logger.warn("XA ROLLBACK got ER_XAER_NOTA: " + xid, ex);
                } else {
                    throw GeneralUtil.nestedException(ex);
                }
            }
        } catch (Throwable e) {
            // discard connection if something failed.
            discard(group, conn, e);

            logger.warn("XA ROLLBACK failed: " + xid, e);

            // Retry XA ROLLBACK in asynchronous task.
            AsyncTaskQueue asyncQueue = getManager().getTransactionExecutor().getAsyncQueue();
            asyncQueue.submit(
                () -> XAUtils.rollbackUntilSucceed(id, primaryGroupUid, group, dataSourceCache.get(group)));
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
                commitOneShardTrx();
                return;
            }

            Collection<Lock> txSharedLocks = acquireSharedLock();
            try {
                commitMultiShardTrx();
            } finally {
                releaseSharedLock(txSharedLocks);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Commit transaction with multiple participants.
     */
    protected abstract void commitMultiShardTrx();

    protected abstract String getTrxLoggerPrefix();

    protected void afterPrepare() {

    }

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
        Lock lock = this.lock;
        lock.lock();

        try {
            cleanupAllConnections();
            connectionHolder.closeAllConnections();

            TransactionLogger.info(id, getTrxLoggerPrefix() + " Aborted");
        } finally {
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
        forEachHeldConnection((group, conn, participated) -> {
            // XA transaction must be 'PREPARED' state here, The
            // primary commit state is unknown, so we don't know how to
            // ROLLBACK or COMMIT.
            discard(group, conn, null);
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
