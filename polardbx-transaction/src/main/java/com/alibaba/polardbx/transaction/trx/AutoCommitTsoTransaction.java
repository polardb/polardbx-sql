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
import com.alibaba.polardbx.common.jdbc.IDataSource;
import com.alibaba.polardbx.common.jdbc.ITransactionPolicy;
import com.alibaba.polardbx.executor.spi.ITransactionManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.rpc.pool.XConnection;
import com.alibaba.polardbx.transaction.TransactionManager;

import java.sql.SQLException;

/**
 * This transaction is auto-committed after statement is executed, and uses max sequence of DN as commit sequence.
 *
 * @author yaozhili
 */
public class AutoCommitTsoTransaction extends AutoCommitTransaction {

    private final static String SET_INNODB_MARK_DISTRIBUTED = "set innodb_mark_distributed = true";

    public AutoCommitTsoTransaction(ExecutionContext ec, ITransactionManager manager) {
        super(ec, manager);
        long lastLogTime = TransactionAttribute.LAST_LOG_AUTO_COMMIT_TSO.get();
        if (TransactionManager.shouldWriteEventLog(lastLogTime)
            && TransactionAttribute.LAST_LOG_AUTO_COMMIT_TSO.compareAndSet(lastLogTime, System.nanoTime())) {
            EventLogger.log(EventType.TRX_INFO, "Found use of AUTO_COMMIT_CTS.");
        }
    }

    @Override
    public IConnection getConnection(String schemaName, String groupName, IDataSource ds, RW rw, ExecutionContext ec)
        throws SQLException {
        IConnection conn = super.getConnection(schemaName, groupName, ds, rw, ec);
        XConnection xConnection;
        if (conn.isWrapperFor(XConnection.class) &&
            (xConnection = conn.unwrap(XConnection.class)).supportMarkDistributed()) {
            conn.flushUnsent();
            xConnection.setLazyMarkDistributed();
        } else {
            conn.executeLater(SET_INNODB_MARK_DISTRIBUTED);
        }
        return conn;
    }

    @Override
    public ITransactionPolicy.TransactionClass getTransactionClass() {
        return ITransactionPolicy.TransactionClass.AUTO_COMMIT_TSO;
    }
}
