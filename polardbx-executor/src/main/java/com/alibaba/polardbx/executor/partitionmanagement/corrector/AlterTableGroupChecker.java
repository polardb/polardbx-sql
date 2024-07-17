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

package com.alibaba.polardbx.executor.partitionmanagement.corrector;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.executor.backfill.Extractor;
import com.alibaba.polardbx.executor.corrector.Checker;
import com.alibaba.polardbx.executor.gsi.PhysicalPlanBuilder;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOperation;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.util.Pair;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class AlterTableGroupChecker extends Checker {
    final Map<String, Set<String>> sourceTargetTables;
    final Map<String, Set<String>> targetTargetTables;

    public AlterTableGroupChecker(String schemaName, String tableName, String indexName,
                                  TableMeta primaryTableMeta,
                                  TableMeta gsiTableMeta, long batchSize,
                                  long speedMin,
                                  long speedLimit,
                                  long parallelism,
                                  boolean useBinary,
                                  SqlSelect.LockMode primaryLock,
                                  SqlSelect.LockMode gsiLock,
                                  PhyTableOperation planSelectWithMaxPrimary,
                                  PhyTableOperation planSelectWithMaxGsi,
                                  PhyTableOperation planSelectWithMinAndMaxPrimary,
                                  PhyTableOperation planSelectWithMinAndMaxGsi,
                                  SqlSelect planSelectWithInTemplate,
                                  PhyTableOperation planSelectWithIn,
                                  PhyTableOperation planSelectMaxPk,
                                  List<String> indexColumns, List<Integer> primaryKeysId,
                                  Comparator<List<Pair<ParameterContext, byte[]>>> rowComparator,
                                  Map<String, Set<String>> sourceTargetTables,
                                  Map<String, Set<String>> targetTargetTables) {
        super(schemaName, tableName, indexName, primaryTableMeta, gsiTableMeta, batchSize, speedMin, speedLimit,
            parallelism, useBinary,
            primaryLock, gsiLock, planSelectWithMaxPrimary, planSelectWithMaxGsi, planSelectWithMinAndMaxPrimary,
            planSelectWithMinAndMaxGsi, planSelectWithInTemplate, planSelectWithIn, planSelectMaxPk, indexColumns,
            primaryKeysId, rowComparator);
        this.isGetShardResultForReplicationTable = true;
        this.sourceTargetTables = sourceTargetTables;
        this.targetTargetTables = targetTargetTables;
    }

    public static Checker create(String schemaName, String tableName, String indexName, long batchSize, long speedMin,
                                 long speedLimit, long parallelism, boolean useBinary,
                                 SqlSelect.LockMode primaryLock, SqlSelect.LockMode gsiLock,
                                 ExecutionContext ec,
                                 Map<String, Set<String>> sourceTargetTables,
                                 Map<String, Set<String>> targetTargetTables) {
        // Build select plan
        final SchemaManager sm = ec.getSchemaManager(schemaName);
        final TableMeta indexTableMeta = sm.getTable(indexName);

        Extractor.ExtractorInfo info = Extractor.buildExtractorInfo(ec, schemaName, tableName, indexName, false);
        final PhysicalPlanBuilder builder = new PhysicalPlanBuilder(schemaName, useBinary, ec);

        final Pair<SqlSelect, PhyTableOperation> selectWithIn = builder
            .buildSelectWithInForChecker(info.getSourceTableMeta(), info.getTargetTableColumns(), info.getPrimaryKeys(),
                indexTableMeta.hasGsiImplicitPrimaryKey() ? null : "PRIMARY");

        final List<DataType> columnTypes = indexTableMeta.getAllColumns()
            .stream()
            .map(ColumnMeta::getDataType)
            .collect(Collectors.toList());
        final Comparator<List<Pair<ParameterContext, byte[]>>> rowComparator = (o1, o2) -> {
            for (int idx : info.getPrimaryKeysId()) {
                int n = ExecUtils
                    .comp(o1.get(idx).getKey().getValue(), o2.get(idx).getKey().getValue(), columnTypes.get(idx), true);
                if (n != 0) {
                    return n;
                }
                ++idx;
            }
            return 0;
        };

        return new AlterTableGroupChecker(schemaName,
            tableName,
            indexName,
            info.getSourceTableMeta(),
            indexTableMeta,
            batchSize,
            speedMin,
            speedLimit,
            parallelism,
            useBinary,
            primaryLock,
            gsiLock,
            builder.buildSelectForBackfill(info.getSourceTableMeta(), info.getTargetTableColumns(),
                info.getPrimaryKeys(),
                false, true, primaryLock),
            builder.buildSelectForBackfill(info.getSourceTableMeta(), info.getTargetTableColumns(),
                info.getPrimaryKeys(),
                false, true, gsiLock),
            builder.buildSelectForBackfill(info.getSourceTableMeta(), info.getTargetTableColumns(),
                info.getPrimaryKeys(),
                true, true, primaryLock),
            builder.buildSelectForBackfill(info.getSourceTableMeta(), info.getTargetTableColumns(),
                info.getPrimaryKeys(),
                true, true, gsiLock),
            selectWithIn.getKey(),
            selectWithIn.getValue(),
            builder.buildSelectMaxPkForBackfill(info.getSourceTableMeta(), info.getPrimaryKeys()),
            info.getTargetTableColumns(),
            info.getPrimaryKeysId(),
            rowComparator,
            sourceTargetTables,
            targetTargetTables);
    }

    @Override
    protected Map<String, Set<String>> getSourcePhysicalTables() {
        return sourceTargetTables;
    }

    @Override
    protected Map<String, Set<String>> getTargetPhysicalTables() {
        return targetTargetTables;
    }

}
