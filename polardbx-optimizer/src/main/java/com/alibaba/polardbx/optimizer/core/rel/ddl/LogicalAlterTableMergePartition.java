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

package com.alibaba.polardbx.optimizer.core.rel.ddl;

import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.tablegroup.TableGroupLocation;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoExRecord;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupMergePartitionPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableMergePartitionPreparedData;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoUtil;
import com.alibaba.polardbx.optimizer.tablegroup.AlterTableGroupSnapShotUtils;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.ddl.AlterTable;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlAlterTableMergePartition;
import org.apache.calcite.sql.SqlAlterTableSplitPartitionByHotValue;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.util.Util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class LogicalAlterTableMergePartition extends BaseDdlOperation {

    protected AlterTableGroupMergePartitionPreparedData preparedData;

    public LogicalAlterTableMergePartition(DDL ddl) {
        super(ddl, ((SqlAlterTable) (ddl.getSqlNode())).getObjectNames());
    }

    public LogicalAlterTableMergePartition(DDL ddl, boolean notIncludeGsiName) {
        super(ddl);
        assert notIncludeGsiName;
    }

    public void preparedData(ExecutionContext ec) {
        AlterTable alterTable = (AlterTable) relDdl;
        SqlAlterTable sqlAlterTable = (SqlAlterTable) alterTable.getSqlNode();
        assert sqlAlterTable.getAlters().size() == 1;

        assert sqlAlterTable.getAlters().get(0) instanceof SqlAlterTableSplitPartitionByHotValue;
        SqlAlterTableMergePartition sqlAlterTableMergePartition =
            (SqlAlterTableMergePartition) sqlAlterTable.getAlters().get(0);

        String logicalTableName = Util.last(((SqlIdentifier) alterTable.getTableName()).names);
        PartitionInfo curPartitionInfo =
            OptimizerContext.getContext(schemaName).getPartitionInfoManager().getPartitionInfo(logicalTableName);

        OptimizerContext oc =
            Objects.requireNonNull(OptimizerContext.getContext(schemaName), schemaName + " corrupted");
        TableGroupConfig tableGroupConfig =
            oc.getTableGroupInfoManager().getTableGroupConfigById(curPartitionInfo.getTableGroupId());
        String tableGroupName = tableGroupConfig.getTableGroupRecord().getTg_name();

        String targetPartitionName =
            Util.last(((SqlIdentifier) (sqlAlterTableMergePartition.getTargetPartitionName())).names);
        List<String> partitionsTobeMerged = new ArrayList<>();
        for (SqlNode oldPartition : sqlAlterTableMergePartition.getOldPartitions()) {
            String oldPartitionName =
                Util.last(((SqlIdentifier) (oldPartition)).names);
            partitionsTobeMerged.add(oldPartitionName);
        }
        Map<String, List<String>> mergePartitions = new HashMap<>();
        mergePartitions.put(targetPartitionName, partitionsTobeMerged);

        List<GroupDetailInfoExRecord> targetGroupDetailInfoExRecords =
            TableGroupLocation.getOrderedGroupList(schemaName);

        preparedData = new AlterTableMergePartitionPreparedData();

        preparedData.setSchemaName(schemaName);
        preparedData.setTableName(logicalTableName);
        preparedData.setWithHint(targetTablesHintCache != null);

        preparedData.setTableGroupName(tableGroupName);
        preparedData.setMergePartitions(mergePartitions);
        preparedData.setTargetGroupDetailInfoExRecords(targetGroupDetailInfoExRecords);
        preparedData.prepareInvisiblePartitionGroup();
        preparedData.setTaskType(ComplexTaskMetaManager.ComplexTaskType.MERGE_PARTITION);
        preparedData.setSourceSql(((SqlAlterTable) alterTable.getSqlNode()).getSourceSql());

        Set<String> mergePartitionsName = partitionsTobeMerged.stream()
            .map(o -> o.toLowerCase()).collect(
                Collectors.toSet());

        List<PartitionGroupRecord> newPartitionGroups = preparedData.getInvisiblePartitionGroups();
        List<Pair<String, String>> mockOrderedTargetTableLocations = new ArrayList<>(newPartitionGroups.size());
        int flag = PartitionInfoUtil.COMPARE_EXISTS_PART_LOCATION;
        int i = 0;
        for (int j = 0; j < newPartitionGroups.size(); j++) {
            GroupDetailInfoExRecord groupDetailInfoExRecord =
                preparedData.getTargetGroupDetailInfoExRecords().get(i++);

            String mockTableName = "";
            mockOrderedTargetTableLocations.add(new Pair<>(mockTableName, groupDetailInfoExRecord.getGroupName()));
            if (i >= preparedData.getTargetGroupDetailInfoExRecords().size()) {
                i = 0;
            }
        }

        PartitionInfo newPartInfo = AlterTableGroupSnapShotUtils.getNewPartitionInfoForMergeType(curPartitionInfo,
            preparedData.getInvisiblePartitionGroups(), tableGroupName,
            mockOrderedTargetTableLocations, mergePartitionsName, preparedData.getNewPartitionNames(), ec);

        preparedData.findCandidateTableGroupAndUpdatePrepareDate(tableGroupConfig, newPartInfo, null,
            null, flag, ec);

    }

    public AlterTableGroupMergePartitionPreparedData getPreparedData() {
        return preparedData;
    }

    public static LogicalAlterTableMergePartition create(DDL ddl) {
        return new LogicalAlterTableMergePartition(ddl);
    }

}
