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
import com.alibaba.polardbx.common.jdbc.MasterSlave;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.common.TopologyHandler;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.utils.ITimestampOracle;
import com.alibaba.polardbx.optimizer.utils.ITransaction;
import com.alibaba.polardbx.rpc.pool.XConnection;
import com.alibaba.polardbx.transaction.async.AsyncTaskQueue;
import com.alibaba.polardbx.transaction.jdbc.DeferredConnection;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import static com.alibaba.polardbx.transaction.TransactionConnectionHolder.needReadLsn;

public class AutoCommitSingleShardTsoTransaction extends AutoCommitTransaction implements ITsoTransaction {

    final private boolean omitTso;
    final private ITimestampOracle tso;
    private long snapshotSeq = -1;
    final private boolean consistentReplicaRead;

    public AutoCommitSingleShardTsoTransaction(ExecutionContext ec, TransactionManager tm, boolean omitTso) {
        super(ec, tm);
        this.omitTso = omitTso;
        this.tso = tm.getTimestampOracle();
        this.consistentReplicaRead = executionContext.getParamManager().getBoolean(
            ConnectionParams.ENABLE_CONSISTENT_REPLICA_READ);
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
        MasterSlave masterSlave = ExecUtils.getMasterSlave(
            false, rw.equals(ITransaction.RW.WRITE), executionContext);

        boolean needReadLsn = needReadLsn(this, schemaName, masterSlave, consistentReplicaRead);

        IConnection conn = super.getSelfConnection(schemaName, group, ds, masterSlave);

        conn = new DeferredConnection(conn, ec.getParamManager().getBoolean(
            ConnectionParams.USING_RDS_RESULT_SKIP));

        if (needReadLsn) {
            TopologyHandler topology;
            if (schemaName != null) {
                topology = ExecutorContext.getContext(schemaName).getTopologyExecutor().getTopology();
            } else {
                topology = ((com.alibaba.polardbx.transaction.TransactionManager) manager).getTransactionExecutor()
                    .getTopology();
            }
            final Map<String, Long> lsnMap = new HashMap<>(1);
            ExecUtils.getLsn(topology, group, lsnMap);
            Long masterLsn = lsnMap.get(group);
            conn.executeLater("SET read_lsn = " + masterLsn.toString());
        }

        if (omitTso) {
            useCtsTransaction(conn);
        } else if (tso != null) {
            sendSnapshotSeq(conn);
        }
        return conn;
    }

    @Override
    public void tryClose(IConnection conn, String groupName) throws SQLException {
        try {
            if (conn.isWrapperFor(XConnection.class)) {
                super.tryClose(conn, groupName);
                return;
            }
        } catch (SQLException ignored) {
            // Is jdbc connection
        }

        lock.lock();
        try {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("ROLLBACK");
            } catch (Throwable e) {
                logger.error("Cleanup readonly transaction branch failed on " + groupName, e);
                discard(groupName, conn, e);
                return;
            }

            this.getConnectionHolder().tryClose(conn, groupName);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() {
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
}
