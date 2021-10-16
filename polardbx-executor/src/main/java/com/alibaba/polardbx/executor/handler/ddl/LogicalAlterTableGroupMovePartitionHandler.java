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

import com.alibaba.polardbx.executor.ddl.job.factory.AlterTableGroupMovePartitionJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.TransientDdlJob;
import com.alibaba.polardbx.executor.partitionmanagement.AlterTableGroupUtils;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import com.alibaba.polardbx.gms.util.GroupInfoUtil;
import com.alibaba.polardbx.gms.util.InstIdUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableGroupMovePartition;
import com.alibaba.polardbx.optimizer.tablegroup.TableGroupInfoManager;
import org.apache.calcite.rel.ddl.AlterTableGroupMovePartition;
import org.apache.calcite.sql.SqlAlterTableGroup;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class LogicalAlterTableGroupMovePartitionHandler extends LogicalCommonDdlHandler {

    public LogicalAlterTableGroupMovePartitionHandler(IRepository repo) {
        super(repo);
    }

    @Override
    protected DdlJob buildDdlJob(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        LogicalAlterTableGroupMovePartition alterTableGroupMovePartition =
            (LogicalAlterTableGroupMovePartition) logicalDdlPlan;
        if (preProcessMovePartitionPlan(alterTableGroupMovePartition)) {
            return new TransientDdlJob();
        }
        alterTableGroupMovePartition.preparedData();
        return AlterTableGroupMovePartitionJobFactory
            .create(alterTableGroupMovePartition.relDdl, alterTableGroupMovePartition.getPreparedData(),
                executionContext);
    }

    @Override
    protected boolean validatePlan(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        AlterTableGroupUtils.alterTableGroupPreCheck(
            (SqlAlterTableGroup) (((LogicalAlterTableGroupMovePartition) logicalDdlPlan).relDdl.getSqlNode()),
            executionContext);
        return false;
    }

    /**
     * @param logicalAlterTableGroupMovePartition table name
     * @return true is not need to do the move partition operation, due to all the partition is in the target dn
     */
    private boolean preProcessMovePartitionPlan(
        LogicalAlterTableGroupMovePartition logicalAlterTableGroupMovePartition) {
        String schemaName = logicalAlterTableGroupMovePartition.getSchemaName();
        AlterTableGroupMovePartition alterTableGroupMovePartition =
            (AlterTableGroupMovePartition) logicalAlterTableGroupMovePartition.relDdl;
        final TableGroupInfoManager tableGroupInfoManager =
            OptimizerContext.getContext(schemaName).getTableGroupInfoManager();
        String tableGroupName = alterTableGroupMovePartition.getTableGroupName();

        TableGroupConfig tableGroupConfig = tableGroupInfoManager.getTableGroupConfigByName(tableGroupName);
        boolean oldPartitionsChange = false;
        Map<String, Set<String>> targetPartitions = new HashMap<>();
        for (Map.Entry<String, Set<String>> item : alterTableGroupMovePartition.getTargetPartitions().entrySet()) {
            String targetInst = item.getKey();
            Set<String> oldPartitions = new HashSet<>();
            for (String oldPartition : item.getValue()) {
                PartitionGroupRecord partitionGroupRecord = tableGroupConfig.getPartitionGroupRecords().stream()
                    .filter(o -> oldPartition.equalsIgnoreCase(o.partition_name)).findFirst().orElse(null);
                String sourceGroupKey = GroupInfoUtil.buildGroupNameFromPhysicalDb(partitionGroupRecord.phy_db);
                final String sourceInstId = DbTopologyManager
                    .getStorageInstIdByGroupName(InstIdUtil.getInstId(),
                        tableGroupConfig.getTableGroupRecord().getSchema(),
                        sourceGroupKey);
                if (!sourceInstId.equalsIgnoreCase(targetInst)) {
                    oldPartitions.add(oldPartition);
                } else {
                    oldPartitionsChange = true;
                }
            }
            if (oldPartitionsChange) {
                if (!oldPartitions.isEmpty()) {
                    targetPartitions.put(item.getKey(), oldPartitions);
                }
            } else {
                targetPartitions.put(item.getKey(), item.getValue());
            }
        }
        if (oldPartitionsChange && targetPartitions.isEmpty()) {
            return true;
        } else {
            alterTableGroupMovePartition.setTargetPartitions(targetPartitions);
        }
        return false;
    }

}
