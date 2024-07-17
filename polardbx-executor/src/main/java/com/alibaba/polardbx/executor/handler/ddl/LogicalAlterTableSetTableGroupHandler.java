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
import com.alibaba.polardbx.executor.ddl.job.builder.tablegroup.AlterTableSetTableGroupBuilder;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.factory.AlterTableSetTableGroupJobFactory;
import com.alibaba.polardbx.executor.ddl.job.validator.JoinGroupValidator;
import com.alibaba.polardbx.executor.ddl.job.validator.TableGroupValidator;
import com.alibaba.polardbx.executor.ddl.job.validator.TableValidator;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableSetTableGroup;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableSetTableGroupPreparedData;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import org.apache.calcite.rel.ddl.AlterTableSetTableGroup;
import org.apache.commons.lang.StringUtils;

public class LogicalAlterTableSetTableGroupHandler extends LogicalCommonDdlHandler {

    public LogicalAlterTableSetTableGroupHandler(IRepository repo) {
        super(repo);
    }

    @Override
    protected DdlJob buildDdlJob(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        LogicalAlterTableSetTableGroup logicalAlterTableSetTableGroup =
            (LogicalAlterTableSetTableGroup) logicalDdlPlan;
        if (logicalAlterTableSetTableGroup.getPreparedData() == null) {
            logicalAlterTableSetTableGroup.preparedData(executionContext);
        }
        AlterTableSetTableGroupPreparedData preparedData = logicalAlterTableSetTableGroup.getPreparedData();

        final SchemaManager schemaManager = executionContext.getSchemaManager(preparedData.getSchemaName());
        PartitionInfo sourcePartitionInfo =
            schemaManager.getTable(logicalAlterTableSetTableGroup.getTableName()).getPartitionInfo();
        AlterTableSetTableGroupBuilder builder =
            new AlterTableSetTableGroupBuilder(logicalAlterTableSetTableGroup.relDdl,
                preparedData, executionContext)
                .build();

        preparedData.setRepartition(builder.isRepartition());
        preparedData.setAlignPartitionNameFirst(builder.isAlignPartitionNameFirst());
        preparedData.getPartitionNamesMap().putAll(builder.getPartitionNamesMap());

        PhysicalPlanData physicalPlanData;
        if (!builder.isOnlyChangeSchemaMeta() && !builder.isAlignPartitionNameFirst() && !builder.isRepartition()) {
            builder.getPhysicalPlans().forEach(o -> o.setPartitionInfo(sourcePartitionInfo));
            physicalPlanData = builder.genPhysicalPlanData();
        } else {
            physicalPlanData = null;
        }

        return new AlterTableSetTableGroupJobFactory(logicalAlterTableSetTableGroup.relDdl,
            preparedData, physicalPlanData,
            builder.getSourceTableTopology(),
            builder.getTargetTableTopology(),
            builder.getNewPartitionRecords(),
            executionContext).create();
    }

    @Override
    protected boolean validatePlan(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        LogicalAlterTableSetTableGroup logicalAlterTableSetTableGroup =
            (LogicalAlterTableSetTableGroup) logicalDdlPlan;
        if (logicalAlterTableSetTableGroup.getPreparedData() == null) {
            logicalAlterTableSetTableGroup.preparedData(executionContext);
        }
        AlterTableSetTableGroupPreparedData preparedData = logicalAlterTableSetTableGroup.getPreparedData();
        String tableGroup = ((AlterTableSetTableGroup) (logicalDdlPlan.relDdl)).getTableGroupName();
        if (StringUtils.isNotEmpty(tableGroup) && !preparedData.isImplicit()) {
            TableGroupValidator.validateTableGroupInfo(logicalDdlPlan.getSchemaName(), tableGroup, true,
                executionContext.getParamManager());
            TableGroupConfig tableGroupConfig =
                OptimizerContext.getContext(logicalDdlPlan.getSchemaName()).getTableGroupInfoManager()
                    .getTableGroupConfigByName(tableGroup);
            assert tableGroupConfig != null;
            final boolean onlyManualTableGroupAllowed =
                executionContext.getParamManager().getBoolean(ConnectionParams.ONLY_MANUAL_TABLEGROUP_ALLOW);
            if (!tableGroupConfig.isManuallyCreated() && onlyManualTableGroupAllowed
                && !tableGroupConfig.getLocalityDesc().getBalanceSingleTable()) {
                throw new TddlRuntimeException(ErrorCode.ERR_TABLE_GROUP_IS_AUTO_CREATED,
                    String.format(
                        "only the tablegroup create by user manually could by use explicitly, the table group[%s] is created internally",
                        tableGroup));
            }

        }
        TableValidator
            .validateTableExistence(preparedData.getSchemaName(), logicalDdlPlan.relDdl.getTableName().toString(),
                executionContext);
        String errMsg = String.format(
            "The joinGroup of table:[%s] is not match with the joinGroup of tableGroup[%s]",
            preparedData.getTableName(), preparedData.getTableGroupName());
        JoinGroupValidator.validateJoinGroupInfo(preparedData.getSchemaName(), preparedData.getTableGroupName(),
            preparedData.getOriginalJoinGroup(), errMsg, executionContext, null);
        return false;
    }
}
