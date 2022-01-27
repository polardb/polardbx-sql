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

package com.alibaba.polardbx.executor.scaleout.corrector;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.executor.backfill.Extractor;
import com.alibaba.polardbx.executor.corrector.Checker;
import com.alibaba.polardbx.executor.gsi.PhysicalPlanBuilder;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.GlobalIndexMeta;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOperation;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.BitSet;
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
public class MoveTableChecker extends Checker {
    final Map<String, Set<String>> sourceTargetTables;
    final Map<String, Set<String>> targetTargetTables;
    final Map<String, String> sourceTargetGroupMap;

    public MoveTableChecker(String schemaName, String tableName, String indexName,
                            TableMeta primaryTableMeta,
                            TableMeta gsiTableMeta, long batchSize,
                            long speedMin,
                            long speedLimit,
                            long parallelism, SqlSelect.LockMode primaryLock,
                            SqlSelect.LockMode gsiLock,
                            PhyTableOperation planSelectWithMaxPrimary,
                            PhyTableOperation planSelectWithMaxGsi,
                            PhyTableOperation planSelectWithMinAndMaxPrimary,
                            PhyTableOperation planSelectWithMinAndMaxGsi,
                            SqlSelect planSelectWithInTemplate,
                            PhyTableOperation planSelectWithIn,
                            PhyTableOperation planSelectMaxPk,
                            List<String> indexColumns, List<Integer> primaryKeys,
                            Comparator<List<Pair<ParameterContext, byte[]>>> rowComparator,
                            Map<String, Set<String>> sourceTargetTables,
                            Map<String, Set<String>> targetTargetTables,
                            Map<String, String> sourceTargetGroupMap) {
        super(schemaName, tableName, indexName, primaryTableMeta, gsiTableMeta, batchSize, speedMin, speedLimit,
            parallelism,
            primaryLock, gsiLock, planSelectWithMaxPrimary, planSelectWithMaxGsi, planSelectWithMinAndMaxPrimary,
            planSelectWithMinAndMaxGsi, planSelectWithInTemplate, planSelectWithIn, planSelectMaxPk, indexColumns,
            primaryKeys, rowComparator);
        this.sourceTargetTables = sourceTargetTables;
        this.targetTargetTables = targetTargetTables;
        this.sourceTargetGroupMap = sourceTargetGroupMap;
    }

    public static Checker create(String schemaName, String tableName, String indexName, long batchSize, long speedMin,
                                 long speedLimit,
                                 long parallelism, SqlSelect.LockMode primaryLock, SqlSelect.LockMode gsiLock,
                                 ExecutionContext ec,
                                 Map<String, Set<String>> sourceTargetTables,
                                 Map<String, Set<String>> targetTargetTables,
                                 Map<String, String> sourceTargetGroupMap) {
        // Build select plan
        final SchemaManager sm = ec.getSchemaManager(schemaName);
        final TableMeta indexTableMeta = sm.getTable(indexName);

        final TableMeta baseTableMeta = sm.getTable(tableName);

        final List<String> indexColumns = indexTableMeta.getAllColumns()
            .stream()
            .map(ColumnMeta::getName)
            .collect(Collectors.toList());

        Extractor.ExtractorInfo info = Extractor.buildExtractorInfo(ec, schemaName, tableName, indexName);
        final PhysicalPlanBuilder builder = new PhysicalPlanBuilder(schemaName, ec);

        final Pair<SqlSelect, PhyTableOperation> selectWithIn = builder
            .buildSelectWithInForChecker(baseTableMeta, indexColumns, info.getPrimaryKeys(), true);

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

        return new MoveTableChecker(schemaName,
            tableName,
            indexName,
            baseTableMeta,
            indexTableMeta,
            batchSize,
            speedMin,
            speedLimit,
            parallelism,
            primaryLock,
            gsiLock,
            builder.buildSelectForBackfill(info.getSourceTableMeta(), info.getTargetTableColumns(),info.getPrimaryKeys(),
                false, true, primaryLock),
            builder.buildSelectForBackfill(info.getSourceTableMeta(), info.getTargetTableColumns(),info.getPrimaryKeys(),
                false, true, gsiLock),
            builder.buildSelectForBackfill(info.getSourceTableMeta(), info.getTargetTableColumns(),info.getPrimaryKeys(),
                true, true, primaryLock),
            builder.buildSelectForBackfill(info.getSourceTableMeta(), info.getTargetTableColumns(),info.getPrimaryKeys(),
                true, true, gsiLock),
            selectWithIn.getKey(),
            selectWithIn.getValue(),
            builder.buildSelectMaxPkForBackfill(baseTableMeta, info.getPrimaryKeys()),
            indexColumns,
            info.getPrimaryKeysId(),
            rowComparator,
            sourceTargetTables,
            targetTargetTables,
            sourceTargetGroupMap);
    }

    @Override
    protected Map<String, Set<String>> getSourcePhysicalTables() {
        return sourceTargetTables;
    }

    @Override
    protected Map<String, Set<String>> getTargetPhysicalTables() {
        return targetTargetTables;
    }

    @Override
    public boolean isMoveTable() {
        return true;
    }

    @Override
    public String getTargetGroup(String baseDbIndex, boolean primaryToGsi) {
        if (primaryToGsi) {
            return sourceTargetGroupMap.get(baseDbIndex);
        } else {
            for (Map.Entry<String, String> entry : sourceTargetGroupMap.entrySet()) {
                if (entry.getValue().equalsIgnoreCase(baseDbIndex)) {
                    return entry.getKey();
                }
            }
        }
        assert false;
        throw new UnsupportedOperationException();
    }

}
