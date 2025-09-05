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
import com.alibaba.polardbx.executor.ddl.job.task.ttl.TtlJobUtil;
import com.alibaba.polardbx.executor.ddl.job.validator.ddl.RepartitionValidator;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.TransientDdlJob;
import com.alibaba.polardbx.executor.gms.util.AlterRepartitionUtils;
import com.alibaba.polardbx.executor.gsi.GsiUtils;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.utils.DdlUtils;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.archive.CheckOSSArchiveUtil;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.SqlConverter;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableRepartition;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.RepartitionPrepareData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.CreateGlobalIndexPreparedData;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.tablegroup.TableGroupInfoManager;
import org.apache.calcite.rel.ddl.AlterTableRepartition;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAddIndex;
import org.apache.calcite.sql.SqlAlterTableRepartition;
import org.apache.calcite.sql.SqlCreateTable;
import org.apache.calcite.sql.SqlIndexDefinition;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlPartition;
import org.apache.calcite.sql.SqlPartitionBy;
import org.apache.calcite.sql.SqlPartitionByHash;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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

        CheckOSSArchiveUtil.checkWithoutOSS(logicalAlterTableRepartition.getSchemaName(),

            logicalAlterTableRepartition.getTableName());

        SqlAlterTableRepartition ast = (SqlAlterTableRepartition) logicalAlterTableRepartition.relDdl.sqlNode;

        String schemaName = logicalDdlPlan.getSchemaName();
        String tableName = logicalAlterTableRepartition.getTableName();
        TableMeta tableMeta = executionContext.getSchemaManager(schemaName).getTable(tableName);

        boolean repartitionGsi = false;
        if (ast.isAlignToTableGroup()) {
            repartitionGsi = tableMeta.isGsi();
            String tableGroup = ast.getTableGroupName().getLastName();
            TableGroupInfoManager tgInfoManager = OptimizerContext.getContext(schemaName).getTableGroupInfoManager();
            TableGroupConfig tableGroupConfig = tgInfoManager.getTableGroupConfigByName(tableGroup);

            if (tableGroupConfig == null) {
                throw new TddlRuntimeException(ErrorCode.ERR_TABLE_GROUP_NOT_EXISTS,
                    String.format("the tablegroup:[%s] is not exists", tableGroup));
            }
            if (tableGroupConfig.isEmpty()) {
                throw new TddlRuntimeException(ErrorCode.ERR_TABLE_GROUP_IS_EMPTY,
                    String.format("the tablegroup:[%s] is empty, it's not expected", tableGroup));
            }

            String firstTbInTg = tableGroupConfig.getTables().get(0);
            TableMeta refTableMeta = executionContext.getSchemaManager(schemaName).getTable(firstTbInTg);

            SqlPartitionBy sqlPartitionBy = AlterRepartitionUtils.generateSqlPartitionByForAlignToTableGroup(
                tableName, tableGroup, tableMeta.getPartitionInfo(), refTableMeta.getPartitionInfo());

            ast.setSqlPartition(sqlPartitionBy);

            SqlConverter sqlConverter = SqlConverter.getInstance(logicalDdlPlan.getSchemaName(), executionContext);
            PlannerContext plannerContext = PlannerContext.getPlannerContext(logicalDdlPlan.getCluster());

            Map<SqlNode, RexNode> partRexInfoCtx = sqlConverter.convertPartition(sqlPartitionBy, plannerContext);

            ((AlterTableRepartition) (logicalDdlPlan.relDdl)).getAllRexExprInfo().putAll(partRexInfoCtx);
        }

        //validate
        RepartitionValidator.validate(
            ast.getSchemaName(),
            ast.getPrimaryTableName(),
            ast.getSqlPartition(),
            ast.isBroadcast(),
            ast.isSingle(),
            true
        );

        Boolean isBroadcast = ast.isBroadcast();
        Boolean isSingle = ast.isSingle();

        // prepare data for create gsi
        boolean optimizeSingleToPartitions1 = isSingleToPartitions1(logicalAlterTableRepartition, executionContext);
        initPrimaryTableDefinition(logicalAlterTableRepartition, executionContext);
        logicalAlterTableRepartition.prepareData();

        CreateGlobalIndexPreparedData globalIndexPreparedData =
            logicalAlterTableRepartition.getCreateGlobalIndexPreparedData();

        // prepare data for local indexes
        logicalAlterTableRepartition.prepareLocalIndexData();
        RepartitionPrepareData repartitionPrepareData = logicalAlterTableRepartition.getRepartitionPrepareData();
        repartitionPrepareData.setRepartitionGsi(repartitionGsi);
        repartitionPrepareData.setSingleTableToPartitions1(optimizeSingleToPartitions1);
        globalIndexPreparedData.setRepartitionPrepareData(repartitionPrepareData);

        executionContext.setForbidBuildLocalIndexLater(true);
        DdlPhyPlanBuilder builder = CreateGlobalIndexBuilder.create(
            logicalAlterTableRepartition.relDdl,
            globalIndexPreparedData,
            null,
            executionContext).build();

        PhysicalPlanData physicalPlanData = builder.genPhysicalPlanData();
        PhysicalPlanData physicalPlanDataForLocalIndex = null;
        // gsi table prepare data
        logicalAlterTableRepartition.prepareRepartitionData(
            globalIndexPreparedData.getIndexPartitionInfo(),
            physicalPlanData.getSqlTemplate()
        );
        if (!RepartitionJobFactory.expandShardColumnsOnlyWithoutModifyLocality(executionContext,
            repartitionPrepareData.getExpandShardColumnsOnly(),
            repartitionPrepareData.getModifyLocality()) && !optimizeSingleToPartitions1 && !isSingle && !isBroadcast) {
            executionContext.setForbidBuildLocalIndexLater(false);
            builder = CreateGlobalIndexBuilder.create(
                logicalAlterTableRepartition.relDdl,
                globalIndexPreparedData,
                null,
                executionContext).build();
            physicalPlanData = builder.genPhysicalPlanData();
            physicalPlanDataForLocalIndex =
                DdlPhyPlanBuilder.getPhysicalPlanDataForLocalIndex(builder, false);
        }

        boolean isPartitionRuleUnchanged = RepartitionValidator.checkPartitionInfoUnchanged(
            globalIndexPreparedData.getSchemaName(),
            globalIndexPreparedData.getPrimaryTableName(),
            globalIndexPreparedData.getIndexPartitionInfo()
        );
        boolean skipCheck = executionContext.getParamManager().getBoolean(ConnectionParams.REPARTITION_SKIP_CHECK);
        // no need to repartition
        if (!skipCheck && isPartitionRuleUnchanged) {
            return new TransientDdlJob();
        }

        final Long versionId = DdlUtils.generateVersionId(executionContext);
        logicalAlterTableRepartition.setDdlVersionId(versionId);

        // get foreign keys
        logicalAlterTableRepartition.prepareForeignKeyData(tableMeta, ast);

        return new RepartitionJobFactory(
            globalIndexPreparedData,
            repartitionPrepareData,
            physicalPlanData,
            physicalPlanDataForLocalIndex,
            executionContext,
            logicalAlterTableRepartition.getCluster()
        ).create();
    }

    private boolean isSingleToPartitions1(LogicalAlterTableRepartition logicalAlterTableRepartition,
                                          ExecutionContext executionContext) {
        String schemaName = logicalAlterTableRepartition.getSchemaName();
        String tableName = logicalAlterTableRepartition.getTableName();
        TableMeta tableMeta = executionContext.getSchemaManager(schemaName).getTable(tableName);
        PartitionInfo partitionInfo = tableMeta.getPartitionInfo();
        SqlPartitionBy sourceSqlPartitionBy = AlterRepartitionUtils.generateSqlPartitionBy(partitionInfo);

        SqlAlterTableRepartition ast = (SqlAlterTableRepartition) logicalAlterTableRepartition.getNativeSqlNode();
        SqlPartitionBy sqlPartitionBy = (SqlPartitionBy) ast.getSqlPartition();
        if (partitionInfo.isSingleTable() && sqlPartitionBy instanceof SqlPartitionByHash &&
            (((SqlPartitionByHash) sqlPartitionBy).isKey() || sqlPartitionBy.getColumns().size() == 1)
            && sqlPartitionBy.getSubPartitionBy() == null && sqlPartitionBy.getPartitionsCount() != null
            && Integer.parseInt(sqlPartitionBy.getPartitionsCount().toString()) == 1
            && !ast.isAlignToTableGroup() && ast.getLocality() == null
            && StringUtils.isEmpty(ast.getTargetImplicitTableGroupName())) {

            int srcPartColsSize = sqlPartitionBy.getColumns().size();
            int refPartColsSize = sourceSqlPartitionBy.getColumns().size();

            sourceSqlPartitionBy.getColumns().clear();
            sourceSqlPartitionBy.getColumns().addAll(sqlPartitionBy.getColumns());

            String locality = null;
            if (!partitionInfo.getTopology().isEmpty()) {
                String groupName = partitionInfo.getTopology().entrySet().stream().findFirst().get().getKey();
                String dnId = DbTopologyManager.getStorageInstIdByGroupName(schemaName, groupName);
                locality = String.format("dn=%s", dnId);
            }

            for (SqlNode partition : sourceSqlPartitionBy.getPartitions()) {
                SqlPartition sqlPartition = (SqlPartition) partition;
                sqlPartition.setLocality(locality);
                AlterRepartitionUtils.replacePartitionValue(sqlPartition.getValues(),
                    partitionInfo.getPartitionBy().getStrategy(), srcPartColsSize, refPartColsSize);
            }

            ast.setSqlPartition(sourceSqlPartitionBy);

            SqlConverter sqlConverter = SqlConverter.getInstance(schemaName, executionContext);
            PlannerContext plannerContext = PlannerContext.getPlannerContext(logicalAlterTableRepartition.getCluster());

            Map<SqlNode, RexNode> partRexInfoCtx = sqlConverter.convertPartition(sourceSqlPartitionBy, plannerContext);

            ((AlterTableRepartition) (logicalAlterTableRepartition.relDdl)).getAllRexExprInfo().putAll(partRexInfoCtx);

            return true;
        }
        return false;
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
            targetTableName = GsiUtils.generateRandomGsiName(primaryTableName);
        }
        ast.setLogicalSecondaryTableName(targetTableName);

        List<SqlIndexDefinition> gsiList = new ArrayList<>();
        SqlIndexDefinition repartitionGsi =
            AlterRepartitionUtils.initIndexInfo(logicalDdlPlan.getSchemaName(), primaryTableName,
                primaryTableInfo.getValue(), ast, primaryTableInfo.getKey(), executionContext);

        gsiList.add(repartitionGsi);
        List<SqlAddIndex> sqlAddIndexList = gsiList.stream().map(e ->
            new SqlAddIndex(SqlParserPos.ZERO, e.getIndexName(), e)
        ).collect(Collectors.toList());
        ast.getAlters().addAll(sqlAddIndexList);
    }

    @Override
    protected boolean validatePlan(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        return false;
    }
}
