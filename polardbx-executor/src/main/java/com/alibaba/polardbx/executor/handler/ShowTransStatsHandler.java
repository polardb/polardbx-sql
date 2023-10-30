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

package com.alibaba.polardbx.executor.handler;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.statistic.StatisticsUtils;
import com.alibaba.polardbx.executor.sync.ISyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.stats.TransStatsColumn;
import org.apache.calcite.rel.RelNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.alibaba.polardbx.stats.TransStatsColumn.COLUMNS;
import static com.alibaba.polardbx.stats.TransStatsColumn.NAME;
import static com.alibaba.polardbx.stats.TransStatsColumn.NUM_COLUMN;
import static com.alibaba.polardbx.stats.TransStatsColumn.SORTED_COLUMNS;

/**
 * @author yaozhili
 */
public class ShowTransStatsHandler extends HandlerCommon {
    public ShowTransStatsHandler(IRepository repo) {
        super(repo);
    }

    private static final Class showTransStatsSyncActionClass;

    static {
        try {
            showTransStatsSyncActionClass =
                Class.forName("com.alibaba.polardbx.transaction.sync.TransactionStatisticsSyncAction");
        } catch (ClassNotFoundException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, e, e.getMessage());
        }
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {

        ArrayResultCursor result = new ArrayResultCursor("TRANSACTIONS_STATISTICS");
        for (TransStatsColumn.ColumnDef column : SORTED_COLUMNS) {
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

        ISyncAction syncAction;
        try {
            syncAction = (ISyncAction) showTransStatsSyncActionClass.getConstructor(String.class)
                .newInstance(executionContext.getSchemaName());
        } catch (Exception e) {
            throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, e, e.getMessage());
        }

        final String schema = executionContext.getSchemaName();
        List<List<Map<String, Object>>> results = SyncManagerHelper.sync(syncAction, schema);

        List<Object[]> allStats = new ArrayList<>();

        for (List<Map<String, Object>> rs : results) {
            if (rs == null || rs.isEmpty()) {
                continue;
            }

            final Object[] stat = new Object[NUM_COLUMN];
            for (TransStatsColumn.ColumnDef column : SORTED_COLUMNS) {
                stat[column.index] = rs.get(0).get(column.name);
            }
            allStats.add(stat);
        }

        Object[] mergedStats = StatisticsUtils.mergeStats(allStats);
        mergedStats[COLUMNS.get(NAME).index] = schema;

        result.addRow(mergedStats);

        return result;
    }
}
