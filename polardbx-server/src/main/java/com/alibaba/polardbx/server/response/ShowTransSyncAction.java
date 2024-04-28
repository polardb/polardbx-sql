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

package com.alibaba.polardbx.server.response;

import com.alibaba.polardbx.CobarServer;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.sync.ISyncAction;
import com.alibaba.polardbx.matrix.jdbc.TDataSource;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.utils.ITransaction;
import com.alibaba.polardbx.transaction.ColumnarTransaction;
import com.alibaba.polardbx.transaction.TransactionManager;
import com.alibaba.polardbx.transaction.trx.BaseTransaction;

import java.util.Collection;

public class ShowTransSyncAction implements ISyncAction {
    private final static Logger logger = LoggerFactory.getLogger(ShowTransSyncAction.class);

    private String db;
    private boolean columnar;

    public ShowTransSyncAction() {
    }

    public ShowTransSyncAction(boolean columnar) {
        this.columnar = columnar;
    }

    public ShowTransSyncAction(String db, boolean columnar) {
        this.db = db;
        this.columnar = columnar;
    }

    public String getDb() {
        return db;
    }

    public void setDb(String db) {
        this.db = db;
    }

    public boolean isColumnar() {
        return columnar;
    }

    public void setColumnar(boolean columnar) {
        this.columnar = columnar;
    }

    @Override
    public ResultCursor sync() {
        TDataSource ds = CobarServer.getInstance().getConfig().getSchemas().get(db).getDataSource();
        TransactionManager tm =
            (TransactionManager) ds.getConfigHolder().getExecutorContext().getTransactionManager();
        Collection<ITransaction> transactions = tm.getTransactions().values();

        ArrayResultCursor result = new ArrayResultCursor("TRANSACTIONS");
        result.addColumn("TRANS_ID", DataTypes.StringType);
        result.addColumn("TYPE", DataTypes.StringType);
        result.addColumn("DURATION_MS", DataTypes.LongType);
        result.addColumn("STATE", DataTypes.StringType);
        result.addColumn("PROCESS_ID", DataTypes.LongType);
        if (isColumnar()) {
            result.addColumn("TSO", DataTypes.LongType);
        }

        long currentTimeMs = System.currentTimeMillis();
        for (ITransaction transaction : transactions) {
            if (isColumnar() && !(transaction instanceof ColumnarTransaction)) {
                continue;
            }
            if (transaction.isBegun() && !transaction.isClosed()) {
                final BaseTransaction tx = (BaseTransaction) transaction;
                final String transId = Long.toHexString(tx.getId());
                final String type = tx.getTransactionClass().toString();
                final long duration = currentTimeMs - tx.getStartTimeInMs();
                final String state = transaction.getState().toString();
                final long processId;
                if (null == transaction.getExecutionContext()
                    || transaction.getExecutionContext().getConnection() == null) {
                    processId = -1;
                } else {
                    processId = transaction.getExecutionContext().getConnection().getId();
                }

                if (isColumnar()) {
                    final long tso = ((ColumnarTransaction) tx).getSnapshotSeq();
                    result.addRow(new Object[] {transId, type, duration, state, processId, tso});
                } else {
                    result.addRow(new Object[] {transId, type, duration, state, processId});
                }
            }
        }

        return result;
    }
}
