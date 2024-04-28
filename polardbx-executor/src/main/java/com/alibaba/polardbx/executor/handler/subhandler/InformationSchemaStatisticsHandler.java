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
import com.alibaba.polardbx.optimizer.config.schema.InformationSchema;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.IndexMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticManager;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticResult;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.view.InformationSchemaStatistics;
import com.alibaba.polardbx.optimizer.view.InformationSchemaTables;
import com.alibaba.polardbx.optimizer.view.VirtualView;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.common.TddlConstants.IMPLICIT_COL_NAME;

/**
 * @author shengyu
 */
public class InformationSchemaStatisticsHandler extends BaseVirtualViewSubClassHandler {
    private static final Logger logger = LoggerFactory.getLogger(InformationSchemaStatisticsHandler.class);

    public InformationSchemaStatisticsHandler(VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof InformationSchemaStatistics;
    }

    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {
        InformationSchemaStatistics informationSchemaStatistics = (InformationSchemaStatistics) virtualView;
        InformationSchemaTables informationSchemaTables =
            new InformationSchemaTables(informationSchemaStatistics.getCluster(),
                informationSchemaStatistics.getTraitSet());

        informationSchemaTables.copyFilters(informationSchemaStatistics);

        Cursor tablesCursor = null;

        try {
            tablesCursor = virtualViewHandler.handle(informationSchemaTables, executionContext);

            Row row;
            while ((row = tablesCursor.next()) != null) {
                String tableSchema = row.getString(1);
                String tableName = row.getString(2);
                if (InformationSchema.NAME.equalsIgnoreCase(tableSchema)) {
                    continue;
                }

                try {
                    TableMeta tableMeta =
                        OptimizerContext.getContext(tableSchema).getLatestSchemaManager().getTable(tableName);
                    for (ColumnMeta columnMeta : tableMeta.getAllColumns()) {
                        String tableCatalog = "def";
                        int nonUnique = 0;
                        String indexSchema = tableSchema;
                        String indexName = null;
                        int seqInIndex = 0;
                        String columnName = columnMeta.getName();
                        if (columnName.toLowerCase().equals(IMPLICIT_COL_NAME)) {
                            continue;
                        }
                        String collation = "A";
                        StatisticResult statisticResult =
                            StatisticManager.getInstance()
                                .getCardinality(tableSchema, tableName, columnName, false, false);
                        Long cardinality = statisticResult.getLongValue();
                        String subPart = null;
                        String packed = null;
                        String nullable = null;
                        String indexType = null;
                        String comment = null;
                        String indexComment = null;

                        List<IndexMeta> indexMetaList =
                            tableMeta.getIndexes().stream()
                                .filter(x -> x.getKeyColumns().indexOf(tableMeta.getColumn(columnName)) != -1)
                                .collect(
                                    Collectors.toList());

                        if (indexMetaList != null) {
                            for (IndexMeta indexMeta : indexMetaList) {
                                indexName = indexMeta.getPhysicalIndexName();
                                seqInIndex = 1 + indexMeta.getKeyColumns().indexOf(tableMeta.getColumn(columnName));
                                indexType = indexMeta.getIndexType().name();
                                nonUnique = indexMeta.isUniqueIndex() ? 0 : 1;
                                cursor.addRow(new Object[] {
                                    tableCatalog,
                                    tableSchema,
                                    tableName,
                                    nonUnique,
                                    indexSchema,
                                    indexName,
                                    seqInIndex,
                                    columnName,
                                    collation,
                                    cardinality,
                                    subPart,
                                    packed,
                                    nullable,
                                    indexType,
                                    comment,
                                    indexComment});
                            }
                        }
                    }

                } catch (Throwable t) {
                    logger.error(t);
                }
            }
        } finally {
            if (tablesCursor != null) {
                tablesCursor.close(new ArrayList<>());
            }
        }
        return cursor;
    }
}
