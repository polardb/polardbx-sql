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
import com.alibaba.polardbx.common.jdbc.IConnection;
import com.alibaba.polardbx.common.jdbc.ITransactionPolicy;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.version.InstanceVersion;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.rpc.pool.XConnection;
import com.alibaba.polardbx.transaction.TransactionManager;
import com.alibaba.polardbx.transaction.jdbc.SavePoint;

import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This transaction is the same as XA transaction, but will commit like a TSO transaction.
 *
 * @author yaozhili
 */
public class XATsoTransaction extends TsoTransaction {

    private final static Logger logger = LoggerFactory.getLogger(TsoTransaction.class);

    private final static String TRX_LOG_PREFIX = "[" + ITransactionPolicy.TransactionClass.XA_TSO + "]";

    private final static String SET_INNODB_MARK_DISTRIBUTED = "set innodb_mark_distributed = true";

    private static final AtomicLong xaTsoFailedLastTime = new AtomicLong(0);
    private static final AtomicLong xaTsoFailedCnt = new AtomicLong(0);

    public XATsoTransaction(ExecutionContext executionContext,
                            TransactionManager manager) {
        super(executionContext, manager);
        long lastLogTime = TransactionAttribute.LAST_LOG_XA_TSO.get();
        if (TransactionManager.shouldWriteEventLog(lastLogTime)
            && TransactionAttribute.LAST_LOG_XA_TSO.compareAndSet(lastLogTime, System.nanoTime())) {
            EventLogger.log(EventType.TRX_INFO, "Found use of XA_CTS.");
        }
    }

    @Override
    protected String getTrxLoggerPrefix() {
        return TRX_LOG_PREFIX;
    }

    @Override
    public void begin(String schema, String group, IConnection conn) throws SQLException {
        try {
            if (shareReadView && inventoryMode != null) {
                // 共享readview不支持inventory hint
                throw new UnsupportedOperationException("Don't support the Inventory Hint on XA with readview! "
                    + "Try with setting share_read_view=off.");
            } else {
                // Mark this trx as a distributed cts trx.
                if (shareReadView) {
                    conn.executeLater(ShareReadViewTransaction.TURN_ON_TXN_GROUP_SQL);
                }
                conn.executeLater("XA START " + getXid(group, conn));

                // Send mark to DN.
                setInnodbMarkDistributed(conn);
            }
            for (String savepoint : savepoints) {
                SavePoint.setLater(conn, savepoint);
            }
        } catch (SQLException e) {
            logger.error("XA Transaction init failed on " + group + ":" + e.getMessage());
            throw e;
        }
    }

    @Override
    public void beginNonParticipant(String group, IConnection conn) throws SQLException {
        conn.executeLater("BEGIN");
    }

    @Override
    public void reinitializeConnection(String schema, String group, IConnection conn) throws SQLException {
        // Do nothing.
    }

    @Override
    public void updateSnapshotTimestamp() {
        // Do nothing.
    }

    @Override
    public long getSnapshotSeq() {
        // Do nothing.
        return -1;
    }

    @Override
    public boolean snapshotSeqIsEmpty() {
        return true;
    }

    @Override
    public void sendSnapshotSeq(IConnection conn) {
        // Do nothing.
    }

    public void setInnodbMarkDistributed(IConnection conn) throws SQLException {
        if (InstanceVersion.isMYSQL80()) {
            // For 8.0, no need to send innodb_mark_distributed,
            // because TSO read transactions will always wait for prepared XA transactions.
            return;
        }
        XConnection xConnection;
        if (conn.isWrapperFor(XConnection.class) &&
            (xConnection = conn.unwrap(XConnection.class)).supportMarkDistributed()) {
            conn.flushUnsent();
            xConnection.setLazyMarkDistributed();
        } else {
            conn.executeLater(SET_INNODB_MARK_DISTRIBUTED);
        }
    }

    @Override
    public void commit() {
        try {
            super.commit();
        } catch (Throwable t) {
            logger.error("XA_TSO Error.", t);
            EventLogger.log(EventType.TRX_ERR, "XA_TSO Error: " + t.getMessage());
            throw t;
        }
    }

    public static void disable() {
        Properties properties = new Properties();
        properties.setProperty(ConnectionProperties.ENABLE_XA_TSO, "false");
        properties.setProperty(ConnectionProperties.ENABLE_AUTO_COMMIT_TSO, "false");
        try {
            MetaDbUtil.setGlobal(properties);
        } catch (Throwable t0) {
            logger.error("Turn off cts option failed.", t0);
        }
    }

    @Override
    public ITransactionPolicy.TransactionClass getTransactionClass() {
        return ITransactionPolicy.TransactionClass.XA_TSO;
    }

}
