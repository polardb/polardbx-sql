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

import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.sync.ISyncAction;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.utils.ITransaction;
import com.alibaba.polardbx.transaction.TransactionManager;

import java.util.Collection;

/**
 * @author dylan
 */
public class FetchAllTransSyncAction implements ISyncAction {

    private String schema;

    private boolean fetchSql;

    public FetchAllTransSyncAction() {
    }

    public FetchAllTransSyncAction(String schema) {
        this.schema = schema;
        this.fetchSql = false;
    }

    public FetchAllTransSyncAction(String schema, boolean fetchSql) {
        this.schema = schema;
        this.fetchSql = fetchSql;
    }

    @Override
    public ResultCursor sync() {
        ArrayResultCursor result = new ArrayResultCursor("TRANSACTIONS");
        result.addColumn("TRANS_ID", DataTypes.LongType);
        result.addColumn("GROUP", DataTypes.StringType);
        result.addColumn("CONN_ID", DataTypes.LongType);
        result.addColumn("FRONTEND_CONN_ID", DataTypes.LongType);
        result.addColumn("START_TIME", DataTypes.LongType);
        if (fetchSql) {
            result.addColumn("SQL", DataTypes.StringType);
        }
        result.addColumn("DDL", DataTypes.BooleanType);

        Collection<ITransaction> transactions = TransactionManager.getTransactions(schema);
        for (ITransaction tran : transactions) {
            long frontendConnId = tran.getExecutionContext().getConnId();

            String sql = tran.getExecutionContext().getOriginSql();
            final String sqlSubString;
            if (fetchSql) {
                sqlSubString = sql != null ? sql.substring(0, Math.min(sql.length(), 4096)) : null;
            } else {
                sqlSubString = null;
            }
            ExecutionContext ec = tran.getExecutionContext();
            final boolean isDdl = null != ec && null != ec.getDdlContext();

            tran.getConnectionHolder().handleConnIds((group, connId) -> {
                if (fetchSql) {
                    result.addRow(new Object[] {
                        tran.getId(), group, connId, frontendConnId, tran.getStartTimeInMs(), sqlSubString, isDdl});
                } else {
                    result.addRow(
                        new Object[] {tran.getId(), group, connId, frontendConnId, tran.getStartTimeInMs(), isDdl});
                }
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

    public boolean isFetchSql() {
        return fetchSql;
    }

    public void setFetchSql(boolean fetchSql) {
        this.fetchSql = fetchSql;
    }

    @Override
    public String toString() {
        return "FetchAllTransSyncAction(schema = " + schema + ")";
    }
}

