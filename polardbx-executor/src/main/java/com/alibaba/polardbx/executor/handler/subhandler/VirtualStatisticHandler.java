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
import com.alibaba.polardbx.optimizer.config.table.IndexMeta;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticManager;
import com.alibaba.polardbx.executor.utils.SchemaMetaUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.config.table.statistic.Histogram;
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

    private static final Logger logger = LoggerFactory.getLogger("statistics view");

    public VirtualStatisticHandler(VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof VirtualStatistic;
    }

    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {
        String schemaName = executionContext.getSchemaName();
        StatisticManager statisticManager = OptimizerContext.getContext(schemaName).getStatisticManager();
        Map<String, StatisticManager.CacheLine> statisticCache = statisticManager.getStatisticCache();

        for (Map.Entry<String, StatisticManager.CacheLine> entry : statisticCache.entrySet()) {
            String tableName = entry.getKey();
            StatisticManager.CacheLine cacheLine = entry.getValue();
            Map<String, TopN> topNMap = cacheLine.getTopNMap();
            Map<String, Histogram> histogramMap = cacheLine.getHistogramMap();

            TableMeta tableMeta;
            try {
                tableMeta = OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(tableName);
            } catch (Throwable e) {
                logger.error(e.getMessage());
                return cursor;
            }

            for (ColumnMeta columnMeta : tableMeta.getAllColumns()) {
                String columnName = columnMeta.getOriginColumnName();
                Object[] objects = new Object[8];
                objects[0] = tableName;
                objects[1] = cacheLine.getRowCount();
                objects[2] = columnName;

                StatisticResult statisticResult = statisticManager.getCardinality(tableName, columnName);
                objects[3] = statisticResult.getLongValue();
                objects[4] = statisticResult.getSource();

                if (topNMap != null && topNMap.get(columnName) != null) {
                    objects[5] = TopN.serializeToJson(topNMap.get(columnName));
                } else {
                    objects[5] = null;
                }
                if (histogramMap != null && histogramMap.get(columnName) != null) {
                    objects[6] = Histogram.serializeToJson(histogramMap.get(columnName));
                } else {
                    objects[6] = null;
                }

                cursor.addRow(objects);
            }

            // handle mutil columns index
            for (IndexMeta indexMeta : tableMeta.getIndexes()) {
                Set<String> cols = indexMeta.getKeyColumns().stream().map(meta -> meta.getOriginColumnName())
                    .collect(Collectors.toSet());
                if (cols.size() == 1) {
                    continue;
                }
                String columnsName = StatisticUtils.buildColumnsName(cols);
                Object[] objects = new Object[8];
                objects[0] = tableName;
                objects[1] = cacheLine.getRowCount();
                objects[2] = columnsName;

                StatisticResult statisticResult = statisticManager.getCardinality(tableName, columnsName);
                objects[3] = statisticResult.getLongValue();
                objects[4] = statisticResult.getSource();

                if (topNMap != null && topNMap.get(columnsName) != null) {
                    objects[5] = TopN.serializeToJson(topNMap.get(columnsName));
                } else {
                    objects[5] = null;
                }
                if (histogramMap != null && histogramMap.get(columnsName) != null) {
                    objects[6] = Histogram.serializeToJson(histogramMap.get(columnsName));
                } else {
                    objects[6] = null;
                }

                cursor.addRow(objects);
            }
        }
        return cursor;
    }
}
