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
import com.alibaba.polardbx.executor.gms.util.AlterRepartitionUtils;
import com.alibaba.polardbx.executor.gsi.GsiUtils;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
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
import org.apache.calcite.sql.SqlPartitionByList;
import org.apache.calcite.sql.SqlPartitionByRange;
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
        if (ast.isAlignToTableGroup()) {
            String schemaName = logicalDdlPlan.getSchemaName();
            String tableName = logicalAlterTableRepartition.getTableName();
            TableMeta tableMeta = executionContext.getSchemaManager(schemaName).getTable(tableName);
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
            String firstTbInTg = tableGroupConfig.getTables().get(0).getTableName();
            TableMeta refTableMeta = executionContext.getSchemaManager(schemaName).getTable(firstTbInTg);
            PartitionInfo refPartitionInfo = refTableMeta.getPartitionInfo();
            SqlPartitionBy sqlPartitionBy = AlterRepartitionUtils.generateSqlPartitionBy(tableMeta, refPartitionInfo);
            ast.setSqlPartition(sqlPartitionBy);
            SqlConverter sqlConverter = SqlConverter.getInstance(logicalDdlPlan.getSchemaName(), executionContext);
            PlannerContext plannerContext = PlannerContext.getPlannerContext(logicalDdlPlan.getCluster());
            Map<SqlNode, RexNode> partRexInfoCtx = sqlConverter.getRexInfoFromPartition(sqlPartitionBy, plannerContext);
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

        // prepare data for create gsi
        initPrimaryTableDefinition(logicalAlterTableRepartition, executionContext);
        logicalAlterTableRepartition.prepareData();

        CreateGlobalIndexPreparedData globalIndexPreparedData =
            logicalAlterTableRepartition.getCreateGlobalIndexPreparedData();

        // prepare data for local indexes
        logicalAlterTableRepartition.prepareLocalIndexData();
        RepartitionPrepareData repartitionPrepareData = logicalAlterTableRepartition.getRepartitionPrepareData();
        globalIndexPreparedData.setRepartitionPrepareData(repartitionPrepareData);

        DdlPhyPlanBuilder builder = CreateGlobalIndexBuilder.create(
            logicalAlterTableRepartition.relDdl,
            globalIndexPreparedData,
            executionContext).build();

        PhysicalPlanData physicalPlanData = builder.genPhysicalPlanData();

        // gsi table prepare data
        logicalAlterTableRepartition.prepareRepartitionData(
            globalIndexPreparedData.getIndexPartitionInfo(),
            physicalPlanData.getSqlTemplate()
        );

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

    private SqlPartitionBy generateSqlPartitionBy(String schemaName, String tableName,
                                                  PartitionInfo referPartitionInfo) {
        SqlPartitionBy sqlPartitionBy;
        switch (referPartitionInfo.getPartitionBy().getStrategy()) {
        case HASH:
            sqlPartitionBy = new SqlPartitionByHash(false, false, SqlParserPos.ZERO);
            break;
        case KEY:
            sqlPartitionBy = new SqlPartitionByHash(true, false, SqlParserPos.ZERO);
            break;
        case RANGE:
        case RANGE_COLUMNS:
            sqlPartitionBy = new SqlPartitionByRange(SqlParserPos.ZERO);
            break;
        case LIST:
        case LIST_COLUMNS:
            sqlPartitionBy = new SqlPartitionByList(SqlParserPos.ZERO);
            break;
        }
        return null;
    }

    @Override
    protected boolean validatePlan(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        return super.validatePlan(logicalDdlPlan, executionContext);
    }
}
