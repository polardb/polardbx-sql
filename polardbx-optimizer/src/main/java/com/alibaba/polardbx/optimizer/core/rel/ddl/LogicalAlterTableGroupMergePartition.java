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

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.gms.locality.LocalityDesc;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.tablegroup.TableGroupLocation;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoExRecord;
import com.alibaba.polardbx.gms.util.TableGroupNameUtil;
import com.alibaba.polardbx.gms.util.PartitionNameUtil;
import com.alibaba.polardbx.gms.util.TableGroupNameUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.archive.CheckOSSArchiveUtil;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupMergePartitionPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupSplitPartitionPreparedData;
import com.alibaba.polardbx.optimizer.locality.LocalityInfoUtils;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.tablegroup.AlterTablePartitionHelper;
import com.alibaba.polardbx.optimizer.tablegroup.TableGroupInfoManager;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.ddl.AlterTableGroupMergePartition;
import org.apache.calcite.sql.SqlAlterTableGroup;
import org.apache.calcite.sql.SqlAlterTableGroupMergePartition;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import org.apache.calcite.util.Util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

public class LogicalAlterTableGroupMergePartition extends LogicalAlterTableMergePartition {

    public LogicalAlterTableGroupMergePartition(DDL ddl) {
        super(ddl, true);
    }

    @Override
    public void preparedData(ExecutionContext ec) {
        AlterTableGroupMergePartition alterTableGroupMergePartition = (AlterTableGroupMergePartition) relDdl;
        String tableGroupName = alterTableGroupMergePartition.getTableGroupName();
        SqlAlterTableGroup sqlAlterTableGroup = (SqlAlterTableGroup) alterTableGroupMergePartition.getAst();
        assert sqlAlterTableGroup.getAlters().size() == 1;

        assert sqlAlterTableGroup.getAlters().get(0) instanceof SqlAlterTableGroupMergePartition;
        SqlAlterTableGroupMergePartition sqlAlterTableGroupMergePartition =
            (SqlAlterTableGroupMergePartition) sqlAlterTableGroup.getAlters().get(0);
        String targetPartitionName =
            Util.last(((SqlIdentifier) (sqlAlterTableGroupMergePartition.getTargetPartitionName())).names);
        List<String> partitionsTobeMerged = new ArrayList<>();
        for (SqlNode oldPartition : sqlAlterTableGroupMergePartition.getOldPartitions()) {
            String oldPartitionName =
                Util.last(((SqlIdentifier) (oldPartition)).names);
            partitionsTobeMerged.add(oldPartitionName);
        }
        Map<String, List<String>> mergePartitions = new HashMap<>();
        mergePartitions.put(targetPartitionName, partitionsTobeMerged);

        TableGroupConfig tableGroupConfig = OptimizerContext.getContext(schemaName).getTableGroupInfoManager()
            .getTableGroupConfigByName(tableGroupName);

        if (tableGroupConfig == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_TABLE_GROUP_NOT_EXISTS,
                "tablegroup:" + tableGroupName + " is not exists");
        }
        if (tableGroupConfig.isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_TABLE_GROUP_IS_EMPTY,
                "can't modify the empty tablegroup:" + tableGroupName);
        }
        String firstTableInTg = tableGroupConfig.getTables().get(0).getTableName();

        PartitionInfo partitionInfo =
            ec.getSchemaManager(schemaName).getTable(firstTableInTg).getPartitionInfo();

        boolean isUseTemplatePart = partitionInfo.getPartitionBy().getSubPartitionBy() != null ?
            partitionInfo.getPartitionBy().getSubPartitionBy().isUseSubPartTemplate() : false;
        boolean hasSubPartition = partitionInfo.getPartitionBy().getSubPartitionBy() != null;

        preparedData = new AlterTableGroupMergePartitionPreparedData();

        preparedData.setSchemaName(schemaName);
        preparedData.setWithHint(targetTablesHintCache != null);

        preparedData.setMergeSubPartition(sqlAlterTableGroupMergePartition.isSubPartitionsMerge());
        preparedData.setOperateOnSubPartition(sqlAlterTableGroupMergePartition.isSubPartitionsMerge());
        preparedData.setUseTemplatePart(hasSubPartition ?
            partitionInfo.getPartitionBy().getSubPartitionBy().isUseSubPartTemplate() : false);
        preparedData.setHasSubPartition(hasSubPartition);

        if (preparedData.isUseTemplatePart() && preparedData.isMergeSubPartition()) {
            List<String> logicalParts = new ArrayList<>();
            for (PartitionSpec partitionSpec : partitionInfo.getPartitionBy().getPartitions()) {
                assert partitionSpec.isLogical();
                logicalParts.add(partitionSpec.getName());
            }
            preparedData.setLogicalParts(logicalParts);
        }

        Set<String> mergePartitionsName = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        mergePartitionsName.addAll(partitionsTobeMerged.stream()
            .map(o -> o.toLowerCase()).collect(
                Collectors.toSet()));

        List<String> oldPartitionGroups = new ArrayList<>();
        if (!preparedData.isMergeSubPartition() && hasSubPartition) {
            if (preparedData.isUseTemplatePart()) {
                List<String> templatePartNames = new ArrayList<>();
                for (PartitionSpec subPartitionSpec : partitionInfo.getPartitionBy().getSubPartitionBy()
                    .getPartitions()) {
                    templatePartNames.add(subPartitionSpec.getTemplateName());
                }
                preparedData.setTemplatePartNames(templatePartNames);

            } else {
                List<String> newPhyPartNames =
                    PartitionNameUtil.autoGeneratePartitionNames(tableGroupConfig, 1,
                        new TreeSet<>(String::compareToIgnoreCase), true);
                preparedData.setNewPhysicalPartName(newPhyPartNames.get(0));
            }
            for (PartitionSpec partitionSpec : partitionInfo.getPartitionBy().getPartitions()) {
                if (mergePartitionsName.contains(partitionSpec.getName())) {
                    for (PartitionSpec subPartitionSpec : partitionSpec.getSubPartitions()) {
                        oldPartitionGroups.add(subPartitionSpec.getName());
                    }
                }
            }
        } else if (hasSubPartition && preparedData.isUseTemplatePart()) {
            for (PartitionSpec partitionSpec : partitionInfo.getPartitionBy().getPartitions()) {
                for (PartitionSpec subPartitionSpec : partitionSpec.getSubPartitions()) {
                    if (mergePartitionsName.contains(subPartitionSpec.getTemplateName())) {
                        oldPartitionGroups.add(subPartitionSpec.getName());
                    }
                }
            }
        } else {
            oldPartitionGroups.addAll(mergePartitionsName);
        }

        preparedData.setOldPartitionGroupNames(oldPartitionGroups);

        LocalityInfoUtils.CollectAction collectAction = new LocalityInfoUtils.CollectAction();
        LocalityInfoUtils.checkTableGroupLocalityCompatiable(schemaName, tableGroupName,
            preparedData.getOldPartitionGroupNames().stream().collect(
                Collectors.toSet()),
            collectAction);

        List<String> outdatedPartitionGroupLocalities =
            collectAction.getPartitionsLocalityDesc().stream().map(o -> o.toString()).collect(Collectors.toList());
        String firstPartitionLocality = outdatedPartitionGroupLocalities.get(0);
        boolean isIdentical =
            outdatedPartitionGroupLocalities.stream().allMatch(o -> o.equals(firstPartitionLocality));

        final LocalityDesc allowdLocalityDesc =
            isIdentical ? LocalityDesc.parse(firstPartitionLocality) : tableGroupConfig.getLocalityDesc();
        List<GroupDetailInfoExRecord> targetGroupDetailInfoExRecords =
            TableGroupLocation.getOrderedGroupList(schemaName)
                .stream().filter(group -> allowdLocalityDesc.matchStorageInstance(group.storageInstId))
                .collect(Collectors.toList());
        LocalityDesc targetLocalityDesc =
            isIdentical ? LocalityDesc.parse(firstPartitionLocality) : new LocalityDesc();

        preparedData.setTargetLocality(targetLocalityDesc);
        preparedData.setTableGroupName(tableGroupName);
        preparedData.setMergePartitions(mergePartitions);
        preparedData.setTargetGroupDetailInfoExRecords(targetGroupDetailInfoExRecords);
        preparedData.prepareInvisiblePartitionGroup();
        preparedData.setTaskType(ComplexTaskMetaManager.ComplexTaskType.MERGE_PARTITION);

        List<String> newPartitionGroupNames = new ArrayList<>();
        preparedData.getInvisiblePartitionGroups().forEach(o -> newPartitionGroupNames.add(o.getPartition_name()));
        preparedData.setNewPartitionGroupNames(newPartitionGroupNames);

    }

    public static LogicalAlterTableGroupMergePartition create(DDL ddl) {
        return new LogicalAlterTableGroupMergePartition(AlterTablePartitionHelper.fixAlterTableGroupDdlIfNeed(ddl));
    }

    @Override
    public boolean isSupportedByBindFileStorage() {
        AlterTableGroupMergePartition alterTableGroupMergePartition = (AlterTableGroupMergePartition) relDdl;
        String tableGroupName = alterTableGroupMergePartition.getTableGroupName();
        throw new TddlRuntimeException(ErrorCode.ERR_UNARCHIVE_FIRST, "unarchive tablegroup " + tableGroupName);
    }

    @Override
    public boolean checkIfFileStorage(ExecutionContext executionContext) {
        AlterTableGroupMergePartition alterTableGroupMergePartition = (AlterTableGroupMergePartition) relDdl;
        String tableGroupName = alterTableGroupMergePartition.getTableGroupName();
        return TableGroupNameUtil.isOssTg(tableGroupName);
    }

    @Override
    public boolean checkIfBindFileStorage(ExecutionContext executionContext) {
        AlterTableGroupMergePartition alterTableGroupMergePartition = (AlterTableGroupMergePartition) relDdl;
        String tableGroupName = alterTableGroupMergePartition.getTableGroupName();
        return !CheckOSSArchiveUtil.checkTableGroupWithoutOSS(schemaName, tableGroupName);
    }
}
