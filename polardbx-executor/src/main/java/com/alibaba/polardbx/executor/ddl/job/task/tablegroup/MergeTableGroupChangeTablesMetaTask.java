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

package com.alibaba.polardbx.executor.ddl.job.task.tablegroup;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.partition.TablePartitionAccessor;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.tablegroup.TableGroupInfoManager;
import lombok.Getter;

import java.sql.Connection;
import java.util.List;
import java.util.Set;

@Getter
@TaskName(name = "MergeTableGroupChangeTablesMetaTask")
public class MergeTableGroupChangeTablesMetaTask extends BaseDdlTask {

    protected Set<String> sourceTableGroups;
    protected String targetTableGroup;
    protected Set<String> primaryTables;

    @JSONCreator
    public MergeTableGroupChangeTablesMetaTask(String schemaName, String targetTableGroup,
                                               Set<String> sourceTableGroups, Set<String> primaryTables) {
        super(schemaName);
        this.targetTableGroup = targetTableGroup;
        this.sourceTableGroups = sourceTableGroups;
        this.primaryTables = primaryTables;
    }

    public void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        changeMeta(metaDbConnection, executionContext);
        updateSupportedCommands(true, false, metaDbConnection);
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        executeImpl(metaDbConnection, executionContext);
    }

    public void changeMeta(Connection metaDbConnection, ExecutionContext executionContext) {

        final TableGroupInfoManager tableGroupInfoManager =
            OptimizerContext.getContext(schemaName).getTableGroupInfoManager();
        TablePartitionAccessor tablePartitionAccessor = new TablePartitionAccessor();
        tablePartitionAccessor.setConnection(metaDbConnection);

        for (String primaryTable : primaryTables) {
            PartitionInfo partitionInfo =
                executionContext.getSchemaManager(schemaName).getTable(primaryTable).getPartitionInfo();
            Long tableGroupId;
            TableGroupConfig targetTableGroupConfig = tableGroupInfoManager.getTableGroupConfigByName(targetTableGroup);
            List<PartitionGroupRecord> partitionGroupRecords = targetTableGroupConfig.getPartitionGroupRecords();

            tableGroupId = targetTableGroupConfig.getTableGroupRecord().id;

            boolean firstPart = true;

            for (PartitionSpec partitionSpec : partitionInfo.getPartitionBy().getPhysicalPartitions()) {
                PartitionGroupRecord partitionGroupRecord = partitionGroupRecords.stream()
                    .filter(o -> o.partition_name.equalsIgnoreCase(partitionSpec.getName())).findFirst()
                    .orElse(null);
                tablePartitionAccessor.updateGroupIdById(partitionGroupRecord.id, partitionSpec.getId());
                if (firstPart) {
                    tablePartitionAccessor.updateGroupIdById(tableGroupId,
                        partitionInfo.getPartitionBy().getNthPartition(1).getParentId());
                }
                firstPart = false;
            }
            try {
                TableInfoManager.updateTableVersion(schemaName, primaryTable, metaDbConnection);
            } catch (Exception e) {
                throw GeneralUtil.nestedException(e);
            }
        }
    }
}
