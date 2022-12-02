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
import com.alibaba.polardbx.executor.ddl.job.factory.AlterTableMovePartitionJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.TransientDdlJob;
import com.alibaba.polardbx.executor.partitionmanagement.AlterTableGroupUtils;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import com.alibaba.polardbx.gms.util.GroupInfoUtil;
import com.alibaba.polardbx.gms.util.InstIdUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableMergePartition;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableMovePartition;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableMovePartitionPreparedData;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.tablegroup.TableGroupInfoManager;
import org.apache.calcite.rel.ddl.AlterTable;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlAlterTableGroup;
import org.apache.calcite.sql.SqlAlterTableMergePartition;
import org.apache.calcite.sql.SqlAlterTableMovePartition;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.util.Util;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class LogicalAlterTableMovePartitionHandler extends LogicalCommonDdlHandler {

    public LogicalAlterTableMovePartitionHandler(IRepository repo) {
        super(repo);
    }

    @Override
    protected DdlJob buildDdlJob(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        LogicalAlterTableMovePartition alterTableMovePartition =
            (LogicalAlterTableMovePartition) logicalDdlPlan;

        String schemaName = logicalDdlPlan.getSchemaName();
        AlterTable alterTable = (AlterTable) alterTableMovePartition.relDdl;
        SqlAlterTable sqlAlterTable = (SqlAlterTable) alterTable.getSqlNode();
        assert sqlAlterTable.getAlters().size() == 1;

        assert sqlAlterTable.getAlters().get(0) instanceof SqlAlterTableMovePartition;
        SqlAlterTableMovePartition sqlAlterTableMovePartition =
            (SqlAlterTableMovePartition) sqlAlterTable.getAlters().get(0);

        String logicalTableName = Util.last(((SqlIdentifier) alterTable.getTableName()).names);
        PartitionInfo curPartitionInfo =
            OptimizerContext.getContext(schemaName).getPartitionInfoManager().getPartitionInfo(logicalTableName);

        OptimizerContext oc =
            Objects.requireNonNull(OptimizerContext.getContext(schemaName), schemaName + " corrupted");
        TableGroupConfig tableGroupConfig =
            oc.getTableGroupInfoManager().getTableGroupConfigById(curPartitionInfo.getTableGroupId());

        if (preProcessMovePartitionPlan(sqlAlterTableMovePartition, tableGroupConfig)) {
            return new TransientDdlJob();
        }
        alterTableMovePartition.preparedData(executionContext);
        return AlterTableMovePartitionJobFactory
            .create(alterTableMovePartition.relDdl,
                (AlterTableMovePartitionPreparedData) alterTableMovePartition.getPreparedData(),
                executionContext);
    }

    @Override
    protected boolean validatePlan(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        LogicalAlterTableMovePartition alterTableMovePartition =
            (LogicalAlterTableMovePartition) logicalDdlPlan;
        AlterTable alterTable = (AlterTable) alterTableMovePartition.relDdl;
        SqlAlterTable sqlAlterTable = (SqlAlterTable) alterTable.getSqlNode();
        SqlAlterTableMovePartition sqlAlterTableMovePartition =
            (SqlAlterTableMovePartition) sqlAlterTable.getAlters().get(0);

        String logicalTableName = Util.last(((SqlIdentifier) alterTable.getTableName()).names);
        String schemaName = alterTableMovePartition.getSchemaName();
        boolean isNewPart = DbInfoManager.getInstance().isNewPartitionDb(schemaName);
        if (!isNewPart) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                "can't execute the merge partition command in drds mode");
        }
        TableMeta tableMeta = executionContext.getSchemaManager(schemaName).getTable(logicalTableName);

        TableGroupInfoManager tableGroupInfoManager =
            OptimizerContext.getContext(schemaName).getTableGroupInfoManager();

        TableGroupConfig tableGroupConfig =
            tableGroupInfoManager.getTableGroupConfigById(tableMeta.getPartitionInfo().getTableGroupId());

        AlterTableGroupUtils.alterTableGroupMovePartitionCheck(sqlAlterTableMovePartition,
            tableGroupConfig,
            executionContext);
        return super.validatePlan(logicalDdlPlan, executionContext);
    }

    /**
     * @return true is not need to do the move partition operation, due to all the partition is in the target dn
     */
    protected boolean preProcessMovePartitionPlan(
        SqlAlterTableMovePartition sqlAlterTableMovePartition,
        TableGroupConfig tableGroupConfig) {

        boolean oldPartitionsChange = false;
        Map<String, Set<String>> targetPartitions = new HashMap<>();
        for (Map.Entry<String, Set<String>> item : sqlAlterTableMovePartition.getTargetPartitions().entrySet()) {
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
            sqlAlterTableMovePartition.setTargetPartitions(targetPartitions);
        }
        return false;
    }

}
