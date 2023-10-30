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
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.stats.TransStatsColumn;
import com.alibaba.polardbx.executor.statistic.StatisticsUtils;

import static com.alibaba.polardbx.gms.topology.SystemDbHelper.DEFAULT_DB_NAME;

/**
 * @author yaozhili
 */
public class TransactionStatisticsSyncAction implements ISyncAction {

    private String schema;

    public TransactionStatisticsSyncAction() {
    }

    public TransactionStatisticsSyncAction(String schema) {
        this.schema = schema;
    }

    @Override
    public ResultCursor sync() {
        ArrayResultCursor result = new ArrayResultCursor("TRANSACTION_STATISTICS");

        for (TransStatsColumn.ColumnDef column : TransStatsColumn.SORTED_COLUMNS) {
            switch (column.columnType) {
            case LONG:
                result.addColumn(column.name, DataTypes.LongType);
                break;
            case DOUBLE:
                result.addColumn(column.name, DataTypes.DoubleType);
                break;
            default:
                result.addColumn(column.name, DataTypes.StringType);
            }
        }

        if (schema.equalsIgnoreCase(DEFAULT_DB_NAME)) {
            result.addRow(StatisticsUtils.getInstanceStats());
        } else {
            result.addRow(StatisticsUtils.getStats(schema));
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
        return "TransactionStatisticsSyncAction(schema = " + schema + ")";
    }
}

