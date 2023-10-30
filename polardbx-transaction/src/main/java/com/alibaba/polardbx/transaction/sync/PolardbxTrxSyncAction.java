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

import com.alibaba.polardbx.common.utils.AddressUtils;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.spi.ITransactionManager;
import com.alibaba.polardbx.executor.sync.ISyncAction;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.utils.ITransaction;
import com.alibaba.polardbx.optimizer.utils.OptimizerHelper;
import com.alibaba.polardbx.optimizer.view.VirtualView;
import com.alibaba.polardbx.stats.TransactionStatistics;

import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;

import static com.alibaba.polardbx.optimizer.view.InformationSchemaPolardbxTrx.COLUMNS;

/**
 * @author yaozhili
 */
public class PolardbxTrxSyncAction implements ISyncAction {

    public PolardbxTrxSyncAction() {
    }

    @Override
    public ResultCursor sync() {
        ArrayResultCursor result = new ArrayResultCursor("POLARDBX_TRX");

        for (VirtualView.ColumnDef column : COLUMNS) {
            switch (column.type) {
            case BIGINT:
                result.addColumn(column.name, DataTypes.LongType);
                break;
            default:
                result.addColumn(column.name, DataTypes.StringType);
            }
        }

        long currentTime = System.nanoTime();
        final SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        for (String schema : OptimizerHelper.getServerConfigManager().getLoadedSchemas()) {
            final ITransactionManager tm = ExecutorContext.getContext(schema).getTransactionManager();
            Collection<ITransaction> transactions = tm.getTransactions().values();
            for (ITransaction tran : transactions) {
                final TransactionStatistics stat = tran.getStat();
                long durationTime = (currentTime - stat.startTime) / 1000;
                // If current transaction is executing sql, add the execution time in active time.
                long activeTime = stat.sqlStartTime > stat.sqlFinishTime ?
                    (currentTime - stat.sqlStartTime + stat.activeTime) / 1000 : stat.activeTime / 1000;

                final long processId;
                if (null == tran.getExecutionContext()
                    || tran.getExecutionContext().getConnection() == null) {
                    processId = -1;
                } else {
                    processId = tran.getExecutionContext().getConnection().getId();
                }

                final String sql = tran.getExecutionContext().getOriginSql();
                final String truncatedSql = (sql == null) ? "" : sql.substring(0, Math.min(sql.length(), 4096));

                result.addRow(new Object[] {
                    Long.toHexString(tran.getId()),
                    schema,
                    processId,
                    tran.getType(),
                    df.format(new Date(stat.startTimeInMs)),
                    durationTime,
                    activeTime,
                    durationTime - activeTime,
                    stat.writeTime / 1000,
                    stat.readTime / 1000,
                    stat.writeAffectRows,
                    stat.readReturnRows,
                    stat.mdlWaitTime / 1000,
                    stat.getTsoTime / 1000,
                    stat.sqlCount,
                    stat.rwSqlCount,
                    AddressUtils.getHostIp(),
                    truncatedSql
                });
            }
        }

        return result;
    }
}
