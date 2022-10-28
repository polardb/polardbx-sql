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

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.VirtualViewHandler;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.IndexMeta;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.config.table.statistic.Histogram;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticManager;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticResult;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticUtils;
import com.alibaba.polardbx.optimizer.config.table.statistic.TopN;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.view.VirtualStatistic;
import com.alibaba.polardbx.optimizer.view.VirtualView;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author shengyu
 */
public class VirtualStatisticHandler extends BaseVirtualViewSubClassHandler {

    private static final Logger logger = LoggerFactory.getLogger("statistics");

    public VirtualStatisticHandler(VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof VirtualStatistic;
    }

    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {
        Map<String, Map<String, StatisticManager.CacheLine>> statisticCache =
            StatisticManager.getInstance().getStatisticCache();

        if (statisticCache == null) {
            return cursor;
        }
        for (Map.Entry<String, Map<String, StatisticManager.CacheLine>> entryTmp : statisticCache.entrySet()) {
            if (entryTmp.getKey() == null || entryTmp.getValue() == null) {
                continue;
            }
            String schema = entryTmp.getKey();
            for (Map.Entry<String, StatisticManager.CacheLine> entry : entryTmp.getValue().entrySet()) {
                String tableName = entry.getKey();
                StatisticManager.CacheLine cacheLine = entry.getValue();
                Map<String, TopN> topNMap = cacheLine.getTopNMap();
                Map<String, Histogram> histogramMap = cacheLine.getHistogramMap();

                TableMeta tableMeta;
                try {
                    OptimizerContext op = OptimizerContext.getContext(schema);
                    if (op == null) {
                        continue;
                    }
                    SchemaManager sm = op.getLatestSchemaManager();
                    if (sm == null) {
                        continue;
                    }
                    tableMeta = sm.getTable(tableName);
                } catch (Throwable e) {
                    logger.error(e.getMessage());
                    continue;
                }

                for (ColumnMeta columnMeta : tableMeta.getAllColumns()) {
                    String columnName = columnMeta.getOriginColumnName();
                    Object[] objects = new Object[11];
                    objects[0] = schema;
                    objects[1] = tableName;
                    objects[2] = cacheLine.getRowCount();
                    objects[3] = columnName;

                    StatisticResult statisticResult =
                        StatisticManager.getInstance().getCardinality(schema, tableName, columnName, false);
                    objects[4] = statisticResult.getLongValue();
                    objects[5] = statisticResult.getSource();

                    if (topNMap != null && topNMap.get(columnName) != null) {
                        objects[6] = TopN.serializeToJson(topNMap.get(columnName));
                    } else {
                        objects[7] = null;
                    }
                    if (histogramMap != null && histogramMap.get(columnName) != null) {
                        objects[7] = Histogram.serializeToJson(histogramMap.get(columnName));
                    } else {
                        objects[7] = null;
                    }
                    objects[8] = cacheLine.getSampleRate();
                    objects[9] = cacheLine.getLastModifyTime();
                    objects[10] = cacheLine.getLastAccessTime();
                    cursor.addRow(objects);
                }

                // handle multi columns index
                for (IndexMeta indexMeta : tableMeta.getIndexes()) {
                    Set<String> cols = indexMeta.getKeyColumns().stream().map(ColumnMeta::getOriginColumnName)
                        .collect(Collectors.toSet());
                    if (cols.size() == 1) {
                        continue;
                    }
                    String columnsName = StatisticUtils.buildColumnsName(cols);
                    Object[] objects = new Object[11];
                    objects[0] = schema;
                    objects[1] = tableName;
                    objects[2] = cacheLine.getRowCount();
                    objects[3] = columnsName;

                    StatisticResult statisticResult =
                        StatisticManager.getInstance().getCardinality(schema, tableName, columnsName, false);
                    objects[4] = statisticResult.getLongValue();
                    objects[5] = statisticResult.getSource();

                    if (topNMap != null && topNMap.get(columnsName) != null) {
                        objects[6] = TopN.serializeToJson(topNMap.get(columnsName));
                    } else {
                        objects[6] = null;
                    }
                    if (histogramMap != null && histogramMap.get(columnsName) != null) {
                        objects[7] = Histogram.serializeToJson(histogramMap.get(columnsName));
                    } else {
                        objects[7] = null;
                    }
                    objects[8] = cacheLine.getSampleRate();
                    objects[9] = cacheLine.getLastModifyTime();
                    objects[10] = cacheLine.getLastAccessTime();
                    cursor.addRow(objects);
                }
            }
        }

        return cursor;
    }
}
