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

import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.common.jdbc.IConnection;
import com.alibaba.polardbx.common.jdbc.IDataSource;
import com.alibaba.polardbx.common.jdbc.ITransactionPolicy;
import com.alibaba.polardbx.common.jdbc.MasterSlave;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.type.TransactionType;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.spi.ITransactionManager;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.utils.IMppReadOnlyTransaction;
import com.alibaba.polardbx.transaction.jdbc.DeferredConnection;

import java.sql.SQLException;
import java.util.Map;

public class MppReadOnlyTransaction extends AutoCommitTransaction implements IMppReadOnlyTransaction, ITsoTransaction {

    private long snapshotTimestamp = -1;
    private Map<String, Long> dnLsnMap;
    private boolean omitTso;
    private boolean lizard1PC;

    public MppReadOnlyTransaction(ExecutionContext ec, ITransactionManager manager) {
        super(ec, manager);
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
    public void setSnapshotTimestamp(long snapshotTimestamp) {
        this.snapshotTimestamp = snapshotTimestamp;
    }

    @Override
    public void setDnLsnMap(Map<String, Long> dnLsnMap) {
        this.dnLsnMap = dnLsnMap;
    }

    @Override
    public void enableOmitTso(boolean omitTso, boolean lizard1PC) {
        this.omitTso = omitTso;
        this.lizard1PC = lizard1PC;
    }

    @Override
    public IConnection getConnection(String schemaName, String group, Long grpConnId, IDataSource ds, RW rw,
                                     ExecutionContext ec)
        throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public TransactionType getType() {
        return TransactionType.TSO_MPP;
    }

    @Override
    public IConnection getConnection(String schemaName, String groupName, IDataSource ds, RW rw, ExecutionContext ec)
        throws SQLException {
        if (!begun) {
            statisticSchema = schemaName;
            recordTransaction();
            begun = true;
        }
        MasterSlave masterSlave = ExecUtils.getMasterSlave(false, rw.equals(RW.WRITE), ec);
        IConnection connection = getRealConnection(schemaName, groupName, ds, masterSlave);
        String masterId = ds.getMasterDNId();
        connection = new DeferredConnection(connection, ec.getParamManager().getBoolean(
            ConnectionParams.USING_RDS_RESULT_SKIP));
        if (dnLsnMap.get(masterId) != null && (ConfigDataMode.isRowSlaveMode()
            || DynamicConfig.getInstance().enableFollowReadForPolarDBX())) {
            //为了支持主实例也可以运行MPP的情况，目前主实例只能去和主库连接，所以不需要使用LSN
            connection.executeLater(String.format("SET read_lsn = %d", dnLsnMap.get(masterId)));
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
