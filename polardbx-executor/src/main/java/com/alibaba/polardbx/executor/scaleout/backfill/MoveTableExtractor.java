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

package com.alibaba.polardbx.executor.scaleout.backfill;

import com.alibaba.polardbx.executor.backfill.Extractor;
import com.alibaba.polardbx.executor.gsi.PhysicalPlanBuilder;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.GlobalIndexMeta;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOperation;
import org.apache.calcite.sql.SqlSelect;

import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class MoveTableExtractor extends com.alibaba.polardbx.executor.backfill.Extractor {
    final Map<String, Set<String>> sourcePhyTables;

    protected MoveTableExtractor(String schemaName, String sourceTableName, String targetTableName,
                                 long batchSize,
                                 long speedMin,
                                 long speedLimit,
                                 long parallelism,
                                 PhyTableOperation planSelectWithMax,
                                 PhyTableOperation planSelectWithMin,
                                 PhyTableOperation planSelectWithMinAndMax,
                                 PhyTableOperation planSelectMaxPk,
                                 BitSet primaryKeys,
                                 Map<String, Set<String>> sourcePhyTables) {
        super(schemaName, sourceTableName, targetTableName, batchSize, speedMin, speedLimit, parallelism,
            planSelectWithMax,
            planSelectWithMin, planSelectWithMinAndMax, planSelectMaxPk, primaryKeys);
        this.sourcePhyTables = sourcePhyTables;
    }

    public static Extractor create(String schemaName, String sourceTableName, long batchSize,
                                   long speedMin, long speedLimit, long parallelism,
                                   Map<String, Set<String>> sourcePhyTables,
                                   ExecutionContext ec) {
        // Build select plan
        final SchemaManager sm = ec.getSchemaManager(schemaName);
        final TableMeta sourceTableMeta = sm.getTable(sourceTableName);
        final TableMeta targetTableMeta = sm.getTable(sourceTableName);
        final List<String> targetTableColumns = targetTableMeta.getWriteColumns()
            .stream()
            .map(ColumnMeta::getName)
            .collect(Collectors.toList());

        List<String> primaryKeys = GlobalIndexMeta.getPrimaryKeys(sourceTableMeta);
        final BitSet primaryKeySet = new BitSet(primaryKeys.size());
        for (String primaryKey : primaryKeys) {
            for (int i = 0; i < targetTableColumns.size(); i++) {
                if (primaryKey.equalsIgnoreCase(targetTableColumns.get(i))) {
                    primaryKeySet.set(i);
                }
            }
        }

        primaryKeys = primaryKeySet.stream().mapToObj(i -> targetTableColumns.get(i)).collect(Collectors.toList());

        final PhysicalPlanBuilder builder = new PhysicalPlanBuilder(schemaName, ec);

        return new MoveTableExtractor(schemaName,
            sourceTableName,
            sourceTableName,
            batchSize,
            speedMin,
            speedLimit,
            parallelism,
            builder.buildSelectForBackfill(sourceTableMeta, targetTableColumns, primaryKeys, false, true,
                SqlSelect.LockMode.SHARED_LOCK),
            builder.buildSelectForBackfill(sourceTableMeta, targetTableColumns, primaryKeys, true, false,
                SqlSelect.LockMode.SHARED_LOCK),
            builder.buildSelectForBackfill(sourceTableMeta, targetTableColumns, primaryKeys, true, true,
                SqlSelect.LockMode.SHARED_LOCK),
            builder.buildSelectMaxPkForBackfill(sourceTableMeta, primaryKeys),
            primaryKeySet,
            sourcePhyTables);
    }

    @Override
    public Map<String, Set<String>> getSourcePhyTables() {
        return sourcePhyTables;
    }
}
