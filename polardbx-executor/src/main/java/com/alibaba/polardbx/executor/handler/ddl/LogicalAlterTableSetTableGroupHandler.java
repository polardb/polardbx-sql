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
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.ddl.job.builder.tablegroup.AlterTableSetTableGroupBuilder;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.factory.AlterTableSetTableGroupJobFactory;
import com.alibaba.polardbx.executor.ddl.job.validator.TableGroupValidator;
import com.alibaba.polardbx.executor.ddl.job.validator.TableValidator;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.TransientDdlJob;
import com.alibaba.polardbx.executor.partitionmanagement.AlterTableGroupUtils;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.partition.TablePartRecordInfoContext;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.util.GroupInfoUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableSetTableGroup;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableSetTableGroupPreparedData;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.tablegroup.TableGroupInfoManager;
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
        logicalAlterTableSetTableGroup.preparedData();
        AlterTableSetTableGroupPreparedData preparedData = logicalAlterTableSetTableGroup.getPreparedData();

        boolean isTransientDdlJob =
            onlyChangeMetaInfo(logicalAlterTableSetTableGroup.getPreparedData(), executionContext);
        if (isTransientDdlJob) {
            return new TransientDdlJob();
        } else {
            final SchemaManager schemaManager = executionContext.getSchemaManager();
            PartitionInfo sourcePartitionInfo =
                schemaManager.getTable(logicalAlterTableSetTableGroup.getTableName()).getPartitionInfo();
            AlterTableSetTableGroupBuilder builder =
                new AlterTableSetTableGroupBuilder(logicalAlterTableSetTableGroup.relDdl,
                    preparedData, executionContext)
                    .build();
            builder.getPhysicalPlans().forEach(o -> o.setPartitionInfo(sourcePartitionInfo));
            PhysicalPlanData physicalPlanData = builder.genPhysicalPlanData();
            return new AlterTableSetTableGroupJobFactory(logicalAlterTableSetTableGroup.relDdl,
                preparedData, physicalPlanData,
                builder.getSourceTableTopology(),
                builder.getTargetTableTopology(),
                builder.getNewPartitionRecords(),
                executionContext).create();
        }
    }

    @Override
    protected boolean validatePlan(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        String tableGroup = ((AlterTableSetTableGroup) (logicalDdlPlan.relDdl)).getTableGroupName();
        if (StringUtils.isNotEmpty(tableGroup)) {
            TableGroupValidator.validateTableGroupInfo(executionContext.getSchemaName(), tableGroup, true,
                executionContext.getParamManager());
        }
        TableValidator
            .validateTableExistence(executionContext.getSchemaName(), logicalDdlPlan.relDdl.getTableName().toString(),
                executionContext);
        return false;
    }

    private boolean onlyChangeMetaInfo(AlterTableSetTableGroupPreparedData preparedData,
                                       ExecutionContext executionContext) {
        String schemaName = preparedData.getSchemaName();
        String logicTableName = preparedData.getTableName();
        String targetTableGroup = preparedData.getTableGroupName();
        final SchemaManager schemaManager = executionContext.getSchemaManager();
        PartitionInfo sourcePartitionInfo = schemaManager.getTable(logicTableName).getPartitionInfo();
        if (StringUtils.isEmpty(targetTableGroup)) {
            // 1 create a new tablegroup
            // 2 change the table_partition reference
            AlterTableGroupUtils.addNewPartitionGroupFromPartitionInfo(sourcePartitionInfo, null,
                -1L, false, true);
            return true;
        }
        final TableGroupInfoManager tableGroupInfoManager =
            OptimizerContext.getContext(schemaName).getTableGroupInfoManager();
        TableGroupConfig targetTableGroupConfig = tableGroupInfoManager.getTableGroupConfigByName(targetTableGroup);
        TableGroupConfig sourceTableGroupInfo =
            tableGroupInfoManager.getTableGroupConfigById(sourcePartitionInfo.getTableGroupId());
        if (targetTableGroupConfig == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_TABLE_GROUP_NOT_EXISTS,
                "tablegroup:" + targetTableGroup + " is not exists");
        } else if (sourceTableGroupInfo.getTableGroupRecord().tg_name.equalsIgnoreCase(targetTableGroup)) {
            // do nothing;
        } else if (GeneralUtil.isEmpty(targetTableGroupConfig.getAllTables())) {
            // 1 create the partition group (when partition group is exists, delete if firstly),
            // 2 change the table_group reference
            AlterTableGroupUtils.addNewPartitionGroupFromPartitionInfo(sourcePartitionInfo, null,
                targetTableGroupConfig.getTableGroupRecord().id, true, true);
        } else {
            TablePartRecordInfoContext tablePartRecordInfoContext =
                targetTableGroupConfig.getAllTables().get(0);
            String tableInTbGrp = tablePartRecordInfoContext.getLogTbRec().tableName;
            PartitionInfo targetPartitionInfo = schemaManager.getTable(tableInTbGrp).getPartitionInfo();
            if (!targetPartitionInfo.equals(sourcePartitionInfo)) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                    "the partition policy of tablegroup:" + targetTableGroup + " is not match to table: "
                        + logicTableName);
            }
            for (PartitionSpec partitionSpec : sourcePartitionInfo.getPartitionBy().getPartitions()) {
                PartitionGroupRecord partitionGroupRecord = targetTableGroupConfig.getPartitionGroupRecords().stream()
                    .filter(o -> o.getPartition_name().equalsIgnoreCase(partitionSpec.getName())).findFirst()
                    .orElse(null);
                assert partitionGroupRecord != null;

                if (!partitionSpec.getLocation().getGroupKey()
                    .equalsIgnoreCase(GroupInfoUtil.buildGroupNameFromPhysicalDb(partitionGroupRecord.getPhy_db()))) {
                    return false;
                }
            }
            //the location of target partition group is identical to the table's, just change the groupId for this case
            AlterTableGroupUtils.addNewPartitionGroupFromPartitionInfo(sourcePartitionInfo,
                targetTableGroupConfig.getPartitionGroupRecords(),
                targetTableGroupConfig.getTableGroupRecord().id, true, false);

        }
        return true;
    }

}
