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

import com.alibaba.polardbx.common.jdbc.IConnection;
import com.alibaba.polardbx.common.jdbc.IDataSource;
import com.alibaba.polardbx.common.jdbc.ITransactionPolicy;
import com.alibaba.polardbx.common.jdbc.MasterSlave;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.type.TransactionType;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.utils.ITimestampOracle;
import com.alibaba.polardbx.optimizer.utils.ITransaction;
import com.alibaba.polardbx.rpc.pool.XConnection;
import com.alibaba.polardbx.transaction.async.AsyncTaskQueue;
import com.alibaba.polardbx.transaction.jdbc.DeferredConnection;

import java.sql.SQLException;
import java.sql.Statement;


public class AutoCommitSingleShardTsoTransaction extends AutoCommitTransaction implements ITsoTransaction {

    final private boolean omitTso;
    final private boolean lizard1PC;
    final private ITimestampOracle tso;
    private long snapshotSeq = -1;

    public AutoCommitSingleShardTsoTransaction(ExecutionContext ec, TransactionManager tm, boolean omitTso,
                                               boolean lizard1PC) {
        super(ec, tm);
        this.omitTso = omitTso;
        this.lizard1PC = lizard1PC;
        this.tso = tm.getTimestampOracle();
    }

    @Override
    public long getSnapshotSeq() {
        if (snapshotSeq == -1) {
            snapshotSeq = tso.nextTimestamp();
        }
        return snapshotSeq;
    }

    @Override
    public boolean snapshotSeqIsEmpty() {
        return snapshotSeq <= 0;
    }

    @Override
    public IConnection getConnection(String schemaName, String group, IDataSource ds, RW rw, ExecutionContext ec)
        throws SQLException {
        if (!begun) {
            statisticSchema = schemaName;
            recordTransaction();
            begun = true;
        }

        MasterSlave masterSlave = ExecUtils.getMasterSlave(
            false, rw.equals(ITransaction.RW.WRITE), executionContext);

        IConnection conn = super.getRealConnection(schemaName, group, ds, masterSlave);

        conn = new DeferredConnection(conn, ec.getParamManager().getBoolean(
            ConnectionParams.USING_RDS_RESULT_SKIP));

        conn = sendLsn(conn, schemaName, group, masterSlave, this::getSnapshotSeq);

        // For replica read, get snapshot_seq before getting LSN, and send it to replica to ensure consistency.
        if (omitTso && snapshotSeqIsEmpty()) {
            useCtsTransaction(conn, lizard1PC);
        } else {
            sendSnapshotSeq(conn);
        }

        return conn;
    }

    @Override
    public void tryClose(IConnection conn, String groupName) throws SQLException {
        if (conn.isWrapperFor(XConnection.class)) {
            super.tryClose(conn, groupName);
            return;
        }

        lock.lock();
        try {
            if (!DynamicConfig.getInstance().enableExtremePerformance()) {
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute("ROLLBACK");
                } catch (Throwable e) {
                    logger.error("Cleanup readonly transaction branch failed on " + groupName, e);
                    discard(groupName, conn, e);
                    return;
                }
            }

            this.getConnectionHolder().tryClose(conn, groupName);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() {
        if (DynamicConfig.getInstance().enableExtremePerformance()) {
            return;
        }
        AsyncTaskQueue asyncQueue = getManager().getTransactionExecutor().getAsyncQueue();
        ch.forEachConnection(asyncQueue, (conn) -> {

            try {
                if (conn.isClosed() || conn.isWrapperFor(XConnection.class)) {
                    return;
                }
            } catch (SQLException ignored) {
                // Is jdbc connection
            }

            try (Statement stmt = conn.createStatement()) {
                stmt.execute("ROLLBACK");
            } catch (Throwable e) {
                logger.error("Cleanup single shard readonly transaction failed!", e);
                discard("", conn, e);
            }
        });

        super.close();
    }

    @Override
    public ITransactionPolicy.TransactionClass getTransactionClass() {
        return ITransactionPolicy.TransactionClass.AUTO_COMMIT_SINGLE_SHARD;
    }

    @Override
    public TransactionType getType() {
        return TransactionType.TSO_SSR;
    }

//    @Override
//    protected void updateSlowTransaction() {
//        updateSlowTSOTransaction(statisticSchema);
//    }
}
