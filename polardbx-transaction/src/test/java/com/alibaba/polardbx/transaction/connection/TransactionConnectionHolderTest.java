/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.transaction.connection;

import com.alibaba.polardbx.common.jdbc.IConnection;
import com.alibaba.polardbx.common.jdbc.ITransactionPolicy;
import com.alibaba.polardbx.common.type.TransactionType;
import com.alibaba.polardbx.common.utils.thread.ExecutorUtil;
import com.alibaba.polardbx.group.jdbc.TGroupDirectConnection;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.rpc.client.XClient;
import com.alibaba.polardbx.rpc.client.XSession;
import com.alibaba.polardbx.rpc.pool.XClientPool;
import com.alibaba.polardbx.rpc.pool.XConnection;
import com.alibaba.polardbx.transaction.TransactionManager;
import com.alibaba.polardbx.transaction.async.AsyncTaskQueue;
import com.alibaba.polardbx.transaction.log.GlobalTxLogManager;
import com.alibaba.polardbx.transaction.trx.AbstractTransaction;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransactionConnectionHolderTest {
    private static class MockTransaction extends AbstractTransaction {
        public MockTransaction(ExecutionContext executionContext, TransactionManager manager) {
            super(executionContext, manager);
        }

        @Override
        public void begin(String schema, String group, IConnection conn) throws SQLException {
        }

        @Override
        protected void cleanup(String group, IConnection conn) throws SQLException {

        }

        @Override
        protected void innerCleanupAllConnections(String group, IConnection conn,
                                                  TransactionConnectionHolder.ParticipatedState participatedState) {

        }

        @Override
        public void commit() {
        }

        @Override
        public void rollback() {
        }

        @Override
        public ITransactionPolicy.TransactionClass getTransactionClass() {
            return null;
        }

        @Override
        public TransactionType getType() {
            return null;
        }
    }

    @Test
    public void testScanReads() throws SQLException {
        TransactionManager transactionManager = mock(TransactionManager.class);
        GlobalTxLogManager txLogManager = mock(GlobalTxLogManager.class);
        when(transactionManager.getGlobalTxLogManager()).thenReturn(txLogManager);
        when(transactionManager.generateTxid(any())).thenReturn(100L);
        doNothing().when(transactionManager).register(any());

        ExecutionContext ec = new ExecutionContext();
        MockTransaction tx = new MockTransaction(ec, transactionManager);

        final TransactionConnectionHolder holder = tx.getConnectionHolder();
        final TransactionConnectionHolder.HeldConnection heldConn =
            Mockito.mock(TransactionConnectionHolder.HeldConnection.class);
        holder.getGroupHeldReadConns().put("group", new ArrayList<>());
        holder.getGroupHeldReadConns().get("group").add(heldConn);

        when(heldConn.isIdle()).thenReturn(true);
        final TGroupDirectConnection conn = Mockito.mock(TGroupDirectConnection.class);
        when(heldConn.getRawConnection()).thenReturn(conn);
        when(conn.isWrapperFor(XConnection.class)).thenReturn(true);
        final XConnection xconn = Mockito.mock(XConnection.class);
        when(conn.unwrap(XConnection.class)).thenReturn(xconn);
        final XSession session = Mockito.mock(XSession.class);
        when(xconn.getSession()).thenReturn(session);
        final XClient client = Mockito.mock(XClient.class);
        when(session.getClient()).thenReturn(client);
        final XClientPool pool = Mockito.mock(XClientPool.class);
        when(client.getPool()).thenReturn(pool);
        when(pool.isChangingLeader()).thenReturn(true);

        final List<TransactionConnectionHolder.HeldConnection> got = new ArrayList<>();
        final AsyncTaskQueue asyncQueue = new AsyncTaskQueue("schema", ExecutorUtil.create("ServerExecutor", 1));
        holder.releaseDirtyReadConnectionIfNoWriteConnection(asyncQueue, got::add);

        Assert.assertEquals(1, got.size());
        Assert.assertEquals(heldConn, got.get(0));
    }
}
