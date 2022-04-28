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

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.ddl.job.builder.DdlPhyPlanBuilder;
import com.alibaba.polardbx.executor.ddl.job.builder.gsi.CreateGlobalIndexBuilder;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.factory.gsi.RepartitionJobFactory;
import com.alibaba.polardbx.executor.ddl.job.task.basic.oss.CheckOSSArchiveUtil;
import com.alibaba.polardbx.executor.ddl.job.validator.ddl.RepartitionValidator;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.TransientDdlJob;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.GlobalIndexMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableRepartition;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.RepartitionPrepareData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.AlterTableWithGsiPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.CreateGlobalIndexPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.CreateIndexWithGsiPreparedData;
import org.apache.calcite.sql.SqlAddIndex;
import org.apache.calcite.sql.SqlAlterTableRepartition;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCreateTable;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIndexColumnName;
import org.apache.calcite.sql.SqlIndexDefinition;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlPartitionBy;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.common.ddl.Attribute.RANDOM_SUFFIX_LENGTH_OF_PHYSICAL_TABLE_NAME;

/**
 * @author wumu
 */
public class LogicalAlterTableRepartitionHandler extends LogicalCommonDdlHandler {
    public LogicalAlterTableRepartitionHandler(IRepository repo) {
        super(repo);
    }

    @Override
    protected DdlJob buildDdlJob(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        final LogicalAlterTableRepartition logicalAlterTableRepartition =
            (LogicalAlterTableRepartition) logicalDdlPlan;

        CheckOSSArchiveUtil.checkWithoutOSS(logicalAlterTableRepartition.getSchemaName(), logicalAlterTableRepartition.getTableName());
        // 新建GSI表的 prepareData
        initPrimaryTableDefinition(logicalAlterTableRepartition, executionContext);
        logicalAlterTableRepartition.prepareData();

        AlterTableWithGsiPreparedData alterTableWithGsiPreparedData =
            logicalAlterTableRepartition.getAlterTableWithGsiPreparedData();

        CreateIndexWithGsiPreparedData createIndexWithGsiPreparedData =
            alterTableWithGsiPreparedData.getCreateIndexWithGsiPreparedData();

        CreateGlobalIndexPreparedData globalIndexPreparedData =
            createIndexWithGsiPreparedData.getGlobalIndexPreparedData();

        logicalAlterTableRepartition.prepareLocalIndexData();
        RepartitionPrepareData repartitionPrepareData = logicalAlterTableRepartition.getRepartitionPrepareData();
        globalIndexPreparedData.setRepartitionPrepareData(repartitionPrepareData);

        DdlPhyPlanBuilder builder = CreateGlobalIndexBuilder.create(
            logicalAlterTableRepartition.relDdl,
            globalIndexPreparedData,
            executionContext).build();

        PhysicalPlanData physicalPlanData = builder.genPhysicalPlanData();

        // GSI table prepareData
        logicalAlterTableRepartition.prepareRepartitionData(
            globalIndexPreparedData.getIndexPartitionInfo(),
            physicalPlanData.getSqlTemplate()
        );

        boolean isPartitionRuleUnchanged = RepartitionValidator.checkPartitionInfoUnchanged(
            globalIndexPreparedData.getSchemaName(),
            globalIndexPreparedData.getPrimaryTableName(),
            globalIndexPreparedData.getIndexPartitionInfo()
        );
        //no need to repartition
        if (isPartitionRuleUnchanged) {
            return new TransientDdlJob();
        }

        SqlAlterTableRepartition ast = (SqlAlterTableRepartition) logicalAlterTableRepartition.relDdl.sqlNode;

        //validate
        RepartitionValidator.validate(
            ast.getSchemaName(),
            ast.getPrimaryTableName(),
            ast.getSqlPartition(),
            ast.isBroadcast(),
            ast.isSingle()
        );

        // NewRepartitionJobFactory
        return new RepartitionJobFactory(
            globalIndexPreparedData,
            repartitionPrepareData,
            physicalPlanData,
            executionContext
        ).create();
    }

    /**
     * Get table definition from primary table and generate index table definition with it
     */
    private void initPrimaryTableDefinition(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        Pair<String, SqlCreateTable> primaryTableInfo = genPrimaryTableInfo(logicalDdlPlan, executionContext);

        SqlAlterTableRepartition ast =
            (SqlAlterTableRepartition) logicalDdlPlan.getNativeSqlNode();

        String primaryTableName = ast.getOriginTableName().getLastName();
        String targetTableName =
            executionContext.getParamManager().getString(ConnectionParams.REPARTITION_FORCE_GSI_NAME);
        if (StringUtils.isEmpty(targetTableName)) {
            targetTableName = generateRandomGsiName(primaryTableName);
        }
        ast.setLogicalSecondaryTableName(targetTableName);

        List<SqlIndexDefinition> gsiList = new ArrayList<>();
        SqlIndexDefinition repartitionGsi = initIndexInfo(primaryTableInfo.getValue(), ast, primaryTableInfo.getKey());

        gsiList.add(repartitionGsi);
        List<SqlAddIndex> sqlAddIndexList = gsiList.stream().map(e ->
            new SqlAddIndex(SqlParserPos.ZERO, e.getIndexName(), e)
        ).collect(Collectors.toList());
        ast.getAlters().addAll(sqlAddIndexList);
    }

    public static SqlIndexDefinition initIndexInfo(SqlCreateTable primaryTableNode,
                                                   SqlAlterTableRepartition alterTableNewPartition,
                                                   String primaryTableDefinition) {
        if (StringUtils.isEmpty(alterTableNewPartition.getLogicalSecondaryTableName())) {
            throw new TddlRuntimeException(ErrorCode.ERR_UNKNOWN_TABLE, "partition table name is empty");
        }
        Set<String> partitionColumnSet = new HashSet<>();
        if (!alterTableNewPartition.isBroadcast() && !alterTableNewPartition.isSingle()) {
            SqlPartitionBy sqlPartitionBy = (SqlPartitionBy) alterTableNewPartition.getSqlPartition();
            List<SqlNode> columns = sqlPartitionBy.getColumns();
            for (SqlNode column : columns) {
                if (column instanceof SqlBasicCall) {
                    for (SqlNode col : ((SqlBasicCall) column).operands) {
                        partitionColumnSet.addAll(((SqlIdentifier) col).names);
                    }
                } else {
                    partitionColumnSet.addAll(((SqlIdentifier) column).names);
                }
            }
        } else {
            final String schemaName = alterTableNewPartition.getOriginTableName().getComponent(0).getLastName();
            final String sourceLogicalTable = alterTableNewPartition.getOriginTableName().getComponent(1).getLastName();
            TableMeta tableMeta =
                OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(sourceLogicalTable);
            List<String> primaryKeys =
                GlobalIndexMeta.getPrimaryKeys(tableMeta).stream().map(String::toLowerCase).collect(
                    Collectors.toList());
            partitionColumnSet.addAll(primaryKeys);
        }

        List<SqlIndexColumnName> indexColumns = partitionColumnSet.stream()
            .map(e -> new SqlIndexColumnName(SqlParserPos.ZERO, new SqlIdentifier(e, SqlParserPos.ZERO), null, null))
            .collect(Collectors.toList());

        List<SqlIndexColumnName> coveringColumns = primaryTableNode.getColDefs().stream()
            .filter(e -> partitionColumnSet.stream().noneMatch(e.getKey().getLastName()::equalsIgnoreCase))
            .map(e -> new SqlIndexColumnName(SqlParserPos.ZERO, e.getKey(), null, null))
            .collect(Collectors.toList());

        SqlIndexDefinition indexDef = SqlIndexDefinition.globalIndex(SqlParserPos.ZERO,
            false,
            null,
            null,
            null,
            new SqlIdentifier(alterTableNewPartition.getLogicalSecondaryTableName(), SqlParserPos.ZERO),
            (SqlIdentifier) primaryTableNode.getTargetTable(),
            indexColumns,
            coveringColumns,
            null,
            null,
            null,
            alterTableNewPartition.getSqlPartition(),
            new LinkedList<>());
        indexDef.setBroadcast(alterTableNewPartition.isBroadcast());
        indexDef.setSingle(alterTableNewPartition.isSingle());
        indexDef.setPrimaryTableNode(primaryTableNode);
        indexDef.setPrimaryTableDefinition(primaryTableDefinition);
        return indexDef;
    }

    private static String generateRandomGsiName(String logicalSourceTableName) {
        String randomSuffix =
            RandomStringUtils.randomAlphanumeric(RANDOM_SUFFIX_LENGTH_OF_PHYSICAL_TABLE_NAME).toLowerCase();
        String targetTableName = logicalSourceTableName + "_" + randomSuffix;
        return targetTableName;
    }

    @Override
    protected boolean validatePlan(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        return super.validatePlan(logicalDdlPlan, executionContext);
    }
}
