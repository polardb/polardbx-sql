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

package com.alibaba.polardbx.transaction.sync;

import com.alibaba.polardbx.common.IdGenerator;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.sync.ISyncAction;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.utils.ITransaction;
import com.alibaba.polardbx.transaction.TransactionManager;
import com.alibaba.polardbx.transaction.TransactionConnectionHolder;

import java.util.Collection;

/**
 * @author zhuangtianyi
 */
public class FetchTransForDeadlockDetectionSyncAction implements ISyncAction {

    private String schema;

    public FetchTransForDeadlockDetectionSyncAction() {
    }

    public FetchTransForDeadlockDetectionSyncAction(String schema) {
        this.schema = schema;
    }

    @Override
    public ResultCursor sync() {
        TransactionManager tm = TransactionManager.getInstance(schema);
        Collection<ITransaction> transactions = tm.getTransactions().values();
        ArrayResultCursor result = new ArrayResultCursor("TRANSACTIONS");
        result.addColumn("TRANS_ID", DataTypes.LongType);
        result.addColumn("GROUP", DataTypes.StringType);
        result.addColumn("CONN_ID", DataTypes.LongType);
        result.addColumn("FRONTEND_CONN_ID", DataTypes.LongType);
        result.addColumn("START_TIME", DataTypes.LongType);
        result.addColumn("SQL", DataTypes.StringType);

        final long beforeTimeMillis = System.currentTimeMillis() - 1000L;
        final long beforeTxid = IdGenerator.assembleId(beforeTimeMillis, 0, 0);

        for (ITransaction tran : transactions) {
            if (!tran.isDistributed()) {
                continue;
            }
            // Do deadlock detection only for transactions that take longer than 1s.
            if (tran.getId() >= beforeTxid) {
                continue;
            }
            long frontendConnId = tran.getExecutionContext().getConnId();
            final String sql = tran.getExecutionContext().getOriginSql();
            final String truncatedSql = (sql == null) ? "" : sql.substring(0, Math.min(sql.length(), 4096));

            ((TransactionConnectionHolder) tran.getConnectionHolder()).handleConnIds((group, connId) -> {
                result.addRow(new Object[] {
                    tran.getId(),
                    group,
                    connId,
                    frontendConnId,
                    tran.getStartTime(),
                    truncatedSql});
            });
        }

        return result;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    @Override
    public String toString() {
        return "FetchTransForDeadlockDetectionSyncAction(schema = " + schema + ")";
    }
}
