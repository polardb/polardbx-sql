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
import com.alibaba.polardbx.transaction.BaseTransaction;
import com.alibaba.polardbx.transaction.TransactionManager;

import java.util.Collection;

public class ShowTransSyncAction implements ISyncAction {
    private final static Logger logger = LoggerFactory.getLogger(ShowTransSyncAction.class);

    private String db;

    public ShowTransSyncAction() {
    }

    public ShowTransSyncAction(String db) {
        this.db = db;
    }

    public String getDb() {
        return db;
    }

    public void setDb(String db) {
        this.db = db;
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

        long currentTimeMs = System.currentTimeMillis();
        for (ITransaction transaction : transactions) {
            if (transaction.isBegun() && !transaction.isClosed()) {
                final BaseTransaction tx = (BaseTransaction) transaction;
                final String transId = Long.toHexString(tx.getId());
                final String type = tx.getTransactionClass().toString();
                final long duration = currentTimeMs - tx.getStartTime();
                final String state = transaction.getState().toString();
                final long processId;
                if (transaction.getExecutionContext().getConnection() == null) {
                    processId = -1;
                } else {
                    processId = transaction.getExecutionContext().getConnection().getId();
                }

                result.addRow(new Object[] {transId, type, duration, state, processId});
            }
        }

        return result;
    }
}
