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

import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.common.jdbc.IConnection;
import com.alibaba.polardbx.common.jdbc.IDataSource;
import com.alibaba.polardbx.common.jdbc.ITransactionPolicy;
import com.alibaba.polardbx.executor.spi.ITransactionManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.utils.IMppReadOnlyTransaction;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

public class MppReadOnlyTransaction extends AutoCommitTransaction implements IMppReadOnlyTransaction, ITsoTransaction {

    private long tsoTimestamp = -1;
    private Map<String, Long> lsnMap;
    private boolean omitTso;
    private boolean lizard1PC;

    public MppReadOnlyTransaction(ExecutionContext ec, ITransactionManager manager) {
        super(ec, manager);
    }

    @Override
    public long getSnapshotSeq() {
        return tsoTimestamp;
    }

    @Override
    public boolean snapshotSeqIsEmpty() {
        return tsoTimestamp <= 0;
    }

    @Override
    public void setTsoTimestamp(long tsoTimestamp) {
        this.tsoTimestamp = tsoTimestamp;
    }

    @Override
    public void setLsnMap(Map<String, Long> lsnMap) {
        this.lsnMap = lsnMap;
    }

    @Override
    public void enableOmitTso(boolean omitTso, boolean lizard1PC) {
        this.omitTso = omitTso;
        this.lizard1PC = lizard1PC;
    }

    @Override
    public IConnection getConnection(String schemaName, String group, Long grpConnId, IDataSource ds, RW rw, ExecutionContext ec)
        throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public IConnection getConnection(String schemaName, String groupName, IDataSource ds, RW rw, ExecutionContext ec)
        throws SQLException {
        IConnection connection = super.getConnection(schemaName, groupName, ds, rw, ec);
        if (lsnMap.get(groupName) != null) {
            //为了支持主实例也可以运行MPP的情况，目前主实例只能去和主库连接，所以不需要使用LSN
            try (Statement stmt = connection.createStatement()) {
                stmt.execute(String.format("SET read_lsn = %d", lsnMap.get(groupName)));
            }
        }
        if (omitTso) {
            useCtsTransaction(connection, lizard1PC);
        } else {
            sendSnapshotSeq(connection);
        }
        return connection;
    }

    @Override
    public ITransactionPolicy.TransactionClass getTransactionClass() {
        return ITransactionPolicy.TransactionClass.MPP_READ_ONLY_TRANSACTION;
    }
}
