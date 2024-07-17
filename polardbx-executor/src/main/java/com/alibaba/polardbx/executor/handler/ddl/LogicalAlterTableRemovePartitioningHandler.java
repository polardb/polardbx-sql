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

package com.alibaba.polardbx.executor.handler.ddl;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.ddl.job.builder.DdlPhyPlanBuilder;
import com.alibaba.polardbx.executor.ddl.job.builder.gsi.CreateGlobalIndexBuilder;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.factory.gsi.RemovePartitioningJobFactory;
import com.alibaba.polardbx.executor.ddl.job.validator.IndexValidator;
import com.alibaba.polardbx.executor.ddl.job.validator.ddl.RepartitionValidator;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.TransientDdlJob;
import com.alibaba.polardbx.executor.gms.util.AlterRepartitionUtils;
import com.alibaba.polardbx.executor.gsi.GsiUtils;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.IndexMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableRemovePartitioning;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.CreateGlobalIndexPreparedData;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.sql.sql2rel.TddlSqlToRelConverter;
import org.apache.calcite.sql.SqlAddIndex;
import org.apache.calcite.sql.SqlAddUniqueIndex;
import org.apache.calcite.sql.SqlAlterTableRemovePartitioning;
import org.apache.calcite.sql.SqlCreateTable;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIndexDefinition;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.common.TddlConstants.AUTO_LOCAL_INDEX_PREFIX;
import static com.alibaba.polardbx.common.TddlConstants.AUTO_SHARD_KEY_PREFIX;
import static com.alibaba.polardbx.executor.gms.util.AlterRepartitionUtils.genGlobalIndexName;
import static com.alibaba.polardbx.executor.gms.util.AlterRepartitionUtils.getPrimaryKeys;
import static java.lang.String.CASE_INSENSITIVE_ORDER;

public class LogicalAlterTableRemovePartitioningHandler extends LogicalCommonDdlHandler {

    public LogicalAlterTableRemovePartitioningHandler(IRepository repo) {
        super(repo);
    }

    @Override
    protected DdlJob buildDdlJob(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        final LogicalAlterTableRemovePartitioning logicalAlterTableRemovePartitioning =
            (LogicalAlterTableRemovePartitioning) logicalDdlPlan;

        SqlAlterTableRemovePartitioning ast =
            (SqlAlterTableRemovePartitioning) logicalAlterTableRemovePartitioning.relDdl.sqlNode;

        //validate
        boolean isPartitionRuleUnchanged = RepartitionValidator.validateRemovePartitioning(
            ast.getSchemaName(),
            ast.getPrimaryTableName()
        );

        if (isPartitionRuleUnchanged) {
            return new TransientDdlJob();
        }

        Map<String, List<String>> dropGsiColumns = new HashMap<>();
        initPrimaryTableDefinition(logicalAlterTableRemovePartitioning, dropGsiColumns, executionContext);
        logicalAlterTableRemovePartitioning.prepareData();

        List<CreateGlobalIndexPreparedData> globalIndexPreparedDataList =
            logicalAlterTableRemovePartitioning.getCreateGlobalIndexesPreparedData();

        Map<CreateGlobalIndexPreparedData, PhysicalPlanData> globalIndexPrepareData = new HashMap<>();

        Map<String, CreateGlobalIndexPreparedData> indexTablePreparedDataMap =
            new TreeMap<>(String::compareToIgnoreCase);
        for (CreateGlobalIndexPreparedData createGsiPreparedData : globalIndexPreparedDataList) {
            DdlPhyPlanBuilder builder = CreateGlobalIndexBuilder.create(
                logicalAlterTableRemovePartitioning.relDdl,
                createGsiPreparedData,
                indexTablePreparedDataMap,
                executionContext).build();
            indexTablePreparedDataMap.put(createGsiPreparedData.getIndexTableName(), createGsiPreparedData);
            globalIndexPrepareData.put(createGsiPreparedData, builder.genPhysicalPlanData());
        }

        return new RemovePartitioningJobFactory(
            ast.getSchemaName(),
            ast.getPrimaryTableName(),
            ast.getLogicalSecondaryTableName(),
            globalIndexPrepareData,
            dropGsiColumns,
            executionContext
        ).create();
    }

    /**
     * Get table definition from primary table and generate index table definition with it
     */
    private void initPrimaryTableDefinition(BaseDdlOperation logicalDdlPlan,
                                            Map<String, List<String>> dropGsiColumns,
                                            ExecutionContext executionContext) {
        Pair<String, SqlCreateTable> primaryTableInfo = genPrimaryTableInfo(logicalDdlPlan, executionContext);

        SqlAlterTableRemovePartitioning ast =
            (SqlAlterTableRemovePartitioning) logicalDdlPlan.getNativeSqlNode();

        boolean withImplicitTg = StringUtils.isNotEmpty(ast.getTargetImplicitTableGroupName());

        // for primary table
        String primaryTableName = ast.getOriginTableName().getLastName();
        String targetTableName =
            executionContext.getParamManager().getString(ConnectionParams.REPARTITION_FORCE_GSI_NAME);
        if (StringUtils.isEmpty(targetTableName)) {
            targetTableName = GsiUtils.generateRandomGsiName(primaryTableName);
        }
        ast.setLogicalSecondaryTableName(targetTableName);

        List<SqlIndexDefinition> gsiList = new ArrayList<>();
        SqlIndexDefinition repartitionGsi = AlterRepartitionUtils.initIndexInfo(
            ast.getLogicalSecondaryTableName(),
            getPrimaryKeys(ast),
            new ArrayList<>(),
            true,
            false,
            primaryTableInfo.getKey(),
            primaryTableInfo.getValue(),
            withImplicitTg ? new SqlIdentifier(ast.getTargetImplicitTableGroupName(), SqlParserPos.ZERO) : null,
            withImplicitTg
        );

        gsiList.add(repartitionGsi);

        // local index --> gsi
        TableMeta tableMeta = executionContext.getSchemaManager().getTable(primaryTableName);
        Set<String> primaryKey = new TreeSet<>(CASE_INSENSITIVE_ORDER);
        primaryKey.addAll(tableMeta.getPrimaryKey().stream().map(ColumnMeta::getName).collect(Collectors.toList()));

        for (IndexMeta indexMeta : tableMeta.getAllIndexes()) {
            String indexName = indexMeta.getPhysicalIndexName().toLowerCase();
            String schema = executionContext.getSchemaName();

            List<String> indexKeys =
                indexMeta.getKeyColumns().stream().map(ColumnMeta::getName).collect(Collectors.toList());
            List<String> coverKeys =
                indexMeta.getValueColumns().stream().map(ColumnMeta::getName).collect(Collectors.toList());

            if (StringUtils.containsIgnoreCase(indexName, AUTO_SHARD_KEY_PREFIX)) {
                continue;
            }

            if (StringUtils.containsIgnoreCase(indexName, AUTO_LOCAL_INDEX_PREFIX)) {
                indexName = TddlSqlToRelConverter.unwrapLocalIndexName(indexName);
                if (IndexValidator.checkIfIndexExists(schema, primaryTableName, indexName)) {
                    continue;
                }
            }

            if (StringUtils.equalsIgnoreCase(indexName, "primary")) {
                continue;
            }

            String newGsiName = genGlobalIndexName(schema, indexName, executionContext);
            SqlIndexDefinition localIndexGsi = AlterRepartitionUtils.initIndexInfo(
                newGsiName,
                indexKeys,
                coverKeys,
                false,
                indexMeta.isUniqueIndex(),
                primaryTableInfo.getKey(),
                primaryTableInfo.getValue(),
                null,
                false
            );

            // prepare for drop gsi columns
            List<String> originPartitionKey = tableMeta.getPartitionInfo().getPartitionColumnsNotReorder();
            Set<String> indexCols = new TreeSet<>(CASE_INSENSITIVE_ORDER);
            indexCols.addAll(indexKeys);
            List<String> dropColumns = new ArrayList<>();
            for (String col : originPartitionKey) {
                if (!indexCols.contains(col) && !primaryKey.contains(col)) {
                    dropColumns.add(col);
                }
            }

            if (!dropColumns.isEmpty()) {
                dropGsiColumns.put(newGsiName, dropColumns);
            }

            gsiList.add(localIndexGsi);
        }

        List<SqlAddIndex> sqlAddIndexList = new ArrayList<>();
        for (SqlIndexDefinition item : gsiList) {
            if (StringUtils.equalsIgnoreCase(item.getType(), "UNIQUE")) {
                sqlAddIndexList.add(new SqlAddUniqueIndex(SqlParserPos.ZERO, item.getIndexName(), item));
            } else {
                sqlAddIndexList.add(new SqlAddIndex(SqlParserPos.ZERO, item.getIndexName(), item));
            }
        }
        ast.getAlters().addAll(sqlAddIndexList);
    }

}
