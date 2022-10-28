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
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.rpc.pool.XConnection;
import com.alibaba.polardbx.transaction.jdbc.SavePoint;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Statement;

/**
 * TSO Transaction, with global MVCC support
 */
public class TsoTransaction extends ShareReadViewTransaction implements ITsoTransaction {

    private final static Logger logger = LoggerFactory.getLogger(TsoTransaction.class);

    private final static String TRX_LOG_PREFIX = "[" + ITransactionPolicy.TransactionClass.TSO + "]";

    private long snapshotTimestamp = -1L;
    private long commitTimestamp = -1L;

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
            snapshotTimestamp = nextTimestamp();
        }

        super.beginNonParticipant(group, conn);
        sendSnapshotSeq(conn);
    }

    @Override
    protected void begin(String schema, String group, IConnection conn) throws SQLException {
        if (snapshotTimestamp < 0) {
            snapshotTimestamp = nextTimestamp();
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
            snapshotTimestamp = nextTimestamp();
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

        snapshotTimestamp = nextTimestamp();
    }

    @Override
    protected void afterPrepare() {
        // Get commit timestamp
        this.commitTimestamp = nextTimestamp();
    }

    @Override
    protected void writeCommitLog(IConnection logConn) throws SQLException {
        globalTxLogManager.append(id, getType(), TransactionState.SUCCEED, connectionContext,
            commitTimestamp, logConn);
    }

    @Override
    protected void prepareConnections() {
        forEachHeldConnection((group, conn, participated) -> {
            switch (participated) {
            case NONE:
                rollbackNonParticipantSync(group, conn);
                break;
            case SHARE_READVIEW_READ:
                rollbackNonParticipantShareReadViewSync(group, conn);
                break;
            case WRITTEN:
                prepareParticipatedConn(group, conn);
                break;
            }
        });
    }

    private void prepareParticipatedConn(String group, IConnection conn) {
        String xid = getXid(group, conn);
        // XA transaction must be 'ACTIVE' state here.
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("XA END " + xid + ';' + " XA PREPARE " + xid);
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
        if (!otherSchemas.isEmpty()) {
            updateExtraAppNames();
        }

        // Whether succeed to write commit log, or may be unknown
        TransactionCommitState commitState = TransactionCommitState.FAILURE;

        RuntimeException exception = null;
        try {
            // XA PREPARE on all groups
            prepareConnections();
            TransactionLogger.info(id, "[TSO] Prepared");

            // Get commit timestamp and Write commit log via an external connection
            commitTimestamp = nextTimestamp();

            if (!executionContext.getParamManager().getBoolean(ConnectionParams.TSO_OMIT_GLOBAL_TX_LOG)) {
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
     * Commit all connections including primary group
     */
    @Override
    protected void commitConnections() {
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
                // XA transaction must be 'PREPARED' state here.
                String xid = getXid(group, conn);
                try (Statement stmt = conn.createStatement()) {
                    try {
                        final XConnection xConnection;
                        if (conn.isWrapperFor(XConnection.class) &&
                            (xConnection = conn.unwrap(XConnection.class)).supportMessageTimestamp()) {
                            conn.flushUnsent();
                            xConnection.setLazyCommitSeq(commitTimestamp);
                            xConnection.execUpdate("XA COMMIT " + xid);
                        } else {
                            stmt.execute(getXACommitWithTsoSql(xid));
                        }
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
                }
            }
        });
    }

    private String getXACommitWithTsoSql(String xid) {
        if (shareReadView) {
            return String
                .format("SET innodb_commit_seq = %d; XA COMMIT %s;" + TURN_OFF_TXN_GROUP_SQL, commitTimestamp, xid);
        } else {
            return String.format("SET innodb_commit_seq = %d; XA COMMIT %s;", commitTimestamp, xid);
        }
    }

    @Override
    public ITransactionPolicy.TransactionClass getTransactionClass() {
        return ITransactionPolicy.TransactionClass.TSO;
    }
}
