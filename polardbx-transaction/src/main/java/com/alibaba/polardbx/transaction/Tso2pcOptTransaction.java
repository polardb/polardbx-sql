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
import com.alibaba.polardbx.common.type.TransactionType;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.rpc.pool.XConnection;

import java.sql.SQLException;
import java.sql.Statement;

import static com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_TRANS_COMMIT;

/**
 * @author yaozhili
 */
public class Tso2pcOptTransaction extends TsoTransaction {

    private final static Logger logger = LoggerFactory.getLogger(TsoTransaction.class);

    private final static String TRX_LOG_PREFIX = "[" + ITransactionPolicy.TransactionClass.TSO_2PC_OPT + "]";

    public Tso2pcOptTransaction(ExecutionContext executionContext,
                                TransactionManager manager) {
        super(executionContext, manager);
    }

    @Override
    protected String getTrxLoggerPrefix() {
        return TRX_LOG_PREFIX;
    }

    @Override
    public TransactionType getType() {
        return TransactionType.TSO_2PC_OPT;
    }

    @Override
    public ITransactionPolicy.TransactionClass getTransactionClass() {
        return ITransactionPolicy.TransactionClass.TSO_2PC_OPT;
    }

    @Override
    protected void commitMultiShardTrx() {
        long prepareStartTime = System.nanoTime();
        boolean canAsyncCommit = true;

        // Whether succeed to write commit log, or may be unknown
        TransactionCommitState commitState = TransactionCommitState.UNKNOWN;

        RuntimeException exception = null;
        try {
            prepareConnections(false);
            commitTimestamp = nextTimestamp(t -> stat.getTsoTime += t);

            if (TransactionManager.isExceedAsyncCommitTaskLimit()) {
                canAsyncCommit = false;
            }

            // Commit primary.
            try {
                duringPrimaryCommit();
                if (canAsyncCommit) {
                    commitPrimaryAndPushMaxSeq();
                } else {
                    commitOneBranch(primaryHeldConn);
                }
                afterPrimaryCommit();
            } catch (SQLException ex) {
                throw new TddlRuntimeException(ErrorCode.ERR_TRANS_LOG, ex,
                    "Failed to write commit state on group: " + primaryGroup);
            }

            commitState = TransactionCommitState.SUCCESS;
        } catch (RuntimeException ex) {
            exception = ex;
        }

        stat.prepareTime = System.nanoTime() - prepareStartTime;

        boolean closeConnection = true;

        if (commitState == TransactionCommitState.SUCCESS) {
            if (canAsyncCommit) {
                TransactionManager.addAsyncCommitTask();
                underCommitting = true;
                asyncCommit = true;
                // Avoid closing connections, and they will be closed after async commit.
                closeConnection = false;

                commitConnectionsAsync();

                // Detach this trx from connection.
                executionContext = null;
                TransactionLogger.info(id, "[TSO][RDS 2PC] Async Committed.");
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

            TransactionLogger.error(id, "[TSO][RDS 2PC] Aborted with unknown commit state");
        }

        if (closeConnection) {
            connectionHolder.closeAllConnections();
        }

        if (exception != null) {
            throw exception;
        }
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
                final IConnection conn = heldConn.getRawConnection();
                final String group = heldConn.getGroup();
                try {
                    String xid = getXid(group, conn);
                    String prepareSql;
                    if (primaryConnection == conn) {
                        prepareSql = String.format("call dbms_xa.prepare_with_trx_slot('%s', '%s', 1)", getGtrid(),
                            getBqual(group, conn));
                    } else {
                        prepareSql = " XA PREPARE " + xid;
                    }
                    try (Statement stmt = conn.createStatement()) {
                        stmt.execute("XA END " + xid + ';' + prepareSql);
                    }
                } catch (Throwable e) {
                    throw new TddlRuntimeException(ERR_TRANS_COMMIT, e,
                        "XA PREPARE failed: " + getXid(group, conn));
                }
                break;
            }
        });
    }

    private void commitPrimaryAndPushMaxSeq() {
        forEachHeldConnection(new TransactionConnectionHolder.Action() {
            @Override
            public boolean condition(TransactionConnectionHolder.HeldConnection heldConn) {
                // Ignore non-participant connections. They were committed during prepare phase.
                return heldConn.isParticipated()
                    && (heldConn.getRawConnection() == primaryConnection || heldConn.isDnLeader());
            }

            @Override
            public void execute(TransactionConnectionHolder.HeldConnection heldConn) {
                if (heldConn.getRawConnection() == primaryConnection) {
                    // Commit primary.
                    commitOneBranch(heldConn);
                } else {
                    // Push max seq.
                    pushMaxSeqOnlyLeader(heldConn);
                }
            }
        });
    }

    @Override
    protected void commitOneBranch(TransactionConnectionHolder.HeldConnection heldConn) {
        IConnection conn = heldConn.getRawConnection();
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

}
