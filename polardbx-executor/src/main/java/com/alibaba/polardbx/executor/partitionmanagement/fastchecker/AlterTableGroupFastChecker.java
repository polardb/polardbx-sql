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

package com.alibaba.polardbx.executor.partitionmanagement.fastchecker;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.executor.fastchecker.FastChecker;
import com.alibaba.polardbx.executor.gsi.PhysicalPlanBuilder;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOperation;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class AlterTableGroupFastChecker extends FastChecker {
    public AlterTableGroupFastChecker(String schemaName, String srcLogicalTableName, String dstLogicalTableName,
                                      Map<String, Set<String>> srcPhyDbAndTables,
                                      Map<String, Set<String>> dstPhyDbAndTables,
                                      List<String> srcColumns, List<String> dstColumns, List<String> srcPks,
                                      List<String> dstPks,
                                      PhyTableOperation planSelectHashCheckSrc,
                                      PhyTableOperation planSelectHashCheckWithUpperBoundSrc,
                                      PhyTableOperation planSelectHashCheckWithLowerBoundSrc,
                                      PhyTableOperation planSelectHashCheckWithLowerUpperBoundSrc,
                                      PhyTableOperation planSelectHashCheckDst,
                                      PhyTableOperation planSelectHashCheckWithUpperBoundDst,
                                      PhyTableOperation planSelectHashCheckWithLowerBoundDst,
                                      PhyTableOperation planSelectHashCheckWithLowerUpperBoundDst,
                                      PhyTableOperation planIdleSelectSrc,
                                      PhyTableOperation planIdleSelectDst,
                                      PhyTableOperation planSelectSampleSrc,
                                      PhyTableOperation planSelectSampleDst) {
        super(schemaName, schemaName, srcLogicalTableName, dstLogicalTableName, srcPhyDbAndTables,
            dstPhyDbAndTables,
            srcColumns, dstColumns, srcPks, dstPks,
            planSelectHashCheckSrc,
            planSelectHashCheckWithUpperBoundSrc,
            planSelectHashCheckWithLowerBoundSrc,
            planSelectHashCheckWithLowerUpperBoundSrc,
            planSelectHashCheckDst,
            planSelectHashCheckWithUpperBoundDst,
            planSelectHashCheckWithLowerBoundDst,
            planSelectHashCheckWithLowerUpperBoundDst,
            planIdleSelectSrc,
            planIdleSelectDst,
            planSelectSampleSrc,
            planSelectSampleDst);
    }

    public static FastChecker create(String schemaName, String tableName, Map<String, Set<String>> srcPhyDbAndTables,
                                     Map<String, Set<String>> dstPhyDbAndTables,
                                     ExecutionContext ec) {
        // Build select plan
        final SchemaManager sm = ec.getSchemaManager(schemaName);

        final TableMeta baseTableMeta = sm.getTable(tableName);

        final List<String> baseTableColumns = baseTableMeta.getAllColumns()
            .stream()
            .map(ColumnMeta::getName)
            .collect(Collectors.toList());

        // 重要：构造planSelectSampleSrc 和 planSelectSampleDst时，传入的主键必须按原本的主键顺序!!!
        final List<String> baseTablePks = FastChecker.getorderedPrimaryKeys(baseTableMeta);

        final PhysicalPlanBuilder builder = new PhysicalPlanBuilder(schemaName, ec);

        return new AlterTableGroupFastChecker(
            schemaName, tableName, tableName,
            srcPhyDbAndTables, dstPhyDbAndTables,
            baseTableColumns, baseTableColumns, baseTablePks, baseTablePks,
            builder.buildSelectHashCheckForChecker(baseTableMeta, baseTableColumns, baseTablePks, false, false),
            builder.buildSelectHashCheckForChecker(baseTableMeta, baseTableColumns, baseTablePks, false, true),
            builder.buildSelectHashCheckForChecker(baseTableMeta, baseTableColumns, baseTablePks, true, false),
            builder.buildSelectHashCheckForChecker(baseTableMeta, baseTableColumns, baseTablePks, true, true),

            builder.buildSelectHashCheckForChecker(baseTableMeta, baseTableColumns, baseTablePks, false, false),
            builder.buildSelectHashCheckForChecker(baseTableMeta, baseTableColumns, baseTablePks, false, true),
            builder.buildSelectHashCheckForChecker(baseTableMeta, baseTableColumns, baseTablePks, true, false),
            builder.buildSelectHashCheckForChecker(baseTableMeta, baseTableColumns, baseTablePks, true, true),

            builder.buildIdleSelectForChecker(baseTableMeta, baseTableColumns),
            builder.buildIdleSelectForChecker(baseTableMeta, baseTableColumns),

            builder.buildSqlSelectForSample(baseTableMeta, baseTablePks),
            builder.buildSqlSelectForSample(baseTableMeta, baseTablePks)
        );
    }
}
