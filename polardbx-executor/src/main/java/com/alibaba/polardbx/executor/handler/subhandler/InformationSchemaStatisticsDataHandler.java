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

package com.alibaba.polardbx.executor.handler.subhandler;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.gms.util.StatisticUtils;
import com.alibaba.polardbx.executor.handler.VirtualViewHandler;
import com.alibaba.polardbx.executor.sync.StatisticQuerySyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.optimizer.config.table.statistic.Histogram;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticManager;
import com.alibaba.polardbx.optimizer.config.table.statistic.inf.StatisticResultSource;
import com.alibaba.polardbx.optimizer.config.table.statistic.inf.SystemTableColumnStatistic;
import com.alibaba.polardbx.optimizer.config.table.statistic.inf.SystemTableTableStatistic;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.parse.util.Pair;
import com.alibaba.polardbx.optimizer.view.InformationSchemaStatisticsData;
import com.alibaba.polardbx.optimizer.view.VirtualView;
import com.google.common.collect.Maps;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static com.alibaba.polardbx.optimizer.config.table.statistic.StatisticUtils.buildSketchKey;

/**
 * @author fangwu
 */
public class InformationSchemaStatisticsDataHandler extends BaseVirtualViewSubClassHandler {

    public InformationSchemaStatisticsDataHandler(VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof InformationSchemaStatisticsData;
    }

    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {
        List<List<Map<String, Object>>> results =
            SyncManagerHelper.sync(new StatisticQuerySyncAction(), SystemDbHelper.DEFAULT_DB_NAME, SyncScope.ALL);
        for (List<Map<String, Object>> nodeRows : results) {
            if (nodeRows == null) {
                continue;
            }
            for (Map<String, Object> row : nodeRows) {
                Object[] rowObj = new Object[InformationSchemaStatisticsData.meta.size()];
                for (int i = 0; i < rowObj.length; i++) {
                    Pair<String, SqlTypeName> pair = InformationSchemaStatisticsData.meta.get(i);
                    String colName = pair.getKey();
                    SqlTypeName colType = pair.getValue();
                    DataType dataType = InformationSchemaStatisticsData.transform(colType);
                    rowObj[i] = dataType.convertFrom(row.get(colName));
                }
                cursor.addRow(rowObj);
            }
        }

        // handle statistic data from meta
        Collection<SystemTableTableStatistic.Row> tableRowList =
            StatisticManager.getInstance().getSds().loadAllTableStatistic(0L);
        Collection<SystemTableColumnStatistic.Row> columnRowList =
            StatisticManager.getInstance().getSds().loadAllColumnStatistic(0L);
        Map<String, Long> cardinalitySketch = Maps.newHashMap();
        cardinalitySketch.putAll(StatisticManager.getInstance().getSds().loadAllCardinality());
        Map<String, Long> tableRowMap = Maps.newHashMap();
        for (SystemTableTableStatistic.Row tableRow : tableRowList) {
            String schema = tableRow.getSchema();
            String table = tableRow.getTableName();
            tableRowMap.put(schema + ":" + table, tableRow.getRowCount());
        }
        Object[] rowObj;
        for (SystemTableColumnStatistic.Row colRow : columnRowList) {
            String schema = colRow.getSchema();
            String table = colRow.getTableName();
            String column = colRow.getColumnName();

            // skip oss table cause sample process would do the same
            if (StatisticUtils.isFileStore(schema, table)) {
                continue;
            }

            Long ndv = cardinalitySketch.get(buildSketchKey(schema, table, column));
            String ndvSource = StatisticResultSource.HLL_SKETCH.name();
            if (ndv == null) {
                ndv = colRow.getCardinality();
                ndvSource = StatisticResultSource.CACHE_LINE.name();
            }
            String topN = colRow.getTopN() == null ? "" : colRow.getTopN().manualReading();

            Histogram histogram = colRow.getHistogram();
            rowObj = new Object[] {
                "metadb",
                schema,
                table,
                column,
                tableRowMap.get(schema + ":" + table),
                ndv,
                ndvSource,
                topN == null ? "" : topN,
                histogram == null || histogram.getBuckets().size() == 0 ? "" : histogram.manualReading(),
                colRow.getSampleRate()
            };
            cursor.addRow(rowObj);
        }
        boolean statisticInconsistentTest = InstConfUtil.getBool(ConnectionParams.ALERT_STATISTIC_INCONSISTENT);
        if (statisticInconsistentTest) {
            cursor.addRow(new Object[] {
                "metadb",
                "mock",
                "mock",
                "mock",
                0,
                0,
                "mock",
                "",
                "",
                1.0
            });
        }
        return cursor;
    }
}
