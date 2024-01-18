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
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.gms.locality.LocalityDesc;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.tablegroup.TableGroupLocation;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoExRecord;
import com.alibaba.polardbx.gms.util.GroupInfoUtil;
import com.alibaba.polardbx.gms.util.PartitionNameUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupMergePartitionPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableMergePartitionPreparedData;
import com.alibaba.polardbx.optimizer.locality.LocalityInfoUtils;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoUtil;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.tablegroup.AlterTableGroupSnapShotUtils;
import com.google.common.collect.ImmutableList;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
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

    @Override
    public boolean isSupportedByFileStorage() {
        return false;
    }

    @Override
    public boolean isSupportedByBindFileStorage() {
        throw new TddlRuntimeException(ErrorCode.ERR_UNARCHIVE_FIRST,
            "unarchive table " + schemaName + "." + tableName);
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

        preparedData = new AlterTableMergePartitionPreparedData();

        preparedData.setSchemaName(schemaName);
        preparedData.setTableName(logicalTableName);
        preparedData.setWithHint(targetTablesHintCache != null);
        preparedData.setMergeSubPartition(sqlAlterTableMergePartition.isSubPartitionsMerge());
        preparedData.setOperateOnSubPartition(sqlAlterTableMergePartition.isSubPartitionsMerge());
        boolean hasSubPartition = curPartitionInfo.getPartitionBy().getSubPartitionBy() != null;
        preparedData.setUseTemplatePart(hasSubPartition ?
            curPartitionInfo.getPartitionBy().getSubPartitionBy().isUseSubPartTemplate() : false);
        preparedData.setHasSubPartition(hasSubPartition);

        if (preparedData.isUseTemplatePart() && preparedData.isMergeSubPartition()) {
            List<String> logicalParts = new ArrayList<>();
            for (PartitionSpec partitionSpec : curPartitionInfo.getPartitionBy().getPartitions()) {
                assert partitionSpec.isLogical();
                logicalParts.add(partitionSpec.getName());
            }
            preparedData.setLogicalParts(logicalParts);
        }

        Set<String> mergePartitionsName = new TreeSet<>(String::compareToIgnoreCase);
        mergePartitionsName.addAll(partitionsTobeMerged);

        List<String> oldPartitionGroups = new ArrayList<>();
        if (!preparedData.isMergeSubPartition() && hasSubPartition) {
            if (preparedData.isUseTemplatePart()) {
                List<String> templatePartNames = new ArrayList<>();
                for (PartitionSpec subPartitionSpec : curPartitionInfo.getPartitionBy().getSubPartitionBy()
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
            for (PartitionSpec partitionSpec : curPartitionInfo.getPartitionBy().getPartitions()) {
                if (mergePartitionsName.contains(partitionSpec.getName())) {
                    for (PartitionSpec subPartitionSpec : partitionSpec.getSubPartitions()) {
                        oldPartitionGroups.add(subPartitionSpec.getName());
                    }
                }
            }
        } else if (hasSubPartition && preparedData.isUseTemplatePart()) {
            for (PartitionSpec partitionSpec : curPartitionInfo.getPartitionBy().getPartitions()) {
                for (PartitionSpec subPartitionSpec : partitionSpec.getSubPartitions()) {
                    if (mergePartitionsName.contains(subPartitionSpec.getTemplateName())) {
                        oldPartitionGroups.add(subPartitionSpec.getName());
                    }
                }
            }
        } else {
            oldPartitionGroups.addAll(mergePartitionsName);
        }

        LocalityInfoUtils.CollectAction collectAction = new LocalityInfoUtils.CollectAction();
        LocalityInfoUtils.checkTableGroupLocalityCompatiable(schemaName, tableGroupName,
            oldPartitionGroups.stream().collect(
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
        preparedData.setOldPartitionGroupNames(oldPartitionGroups);
        preparedData.setTableGroupName(tableGroupName);
        preparedData.setMergePartitions(mergePartitions);
        preparedData.setTargetGroupDetailInfoExRecords(targetGroupDetailInfoExRecords);
        preparedData.prepareInvisiblePartitionGroup();

        List<String> newPartitionGroupNames = new ArrayList<>();
        preparedData.getInvisiblePartitionGroups().forEach(o -> newPartitionGroupNames.add(o.getPartition_name()));
        preparedData.setNewPartitionGroupNames(newPartitionGroupNames);

        preparedData.setTaskType(ComplexTaskMetaManager.ComplexTaskType.MERGE_PARTITION);
        preparedData.setSourceSql(((SqlAlterTable) alterTable.getSqlNode()).getSourceSql());

        List<PartitionGroupRecord> newPartitionGroups = preparedData.getInvisiblePartitionGroups();
        Map<String, Pair<String, String>> mockOrderedTargetTableLocations = new TreeMap<>(String::compareToIgnoreCase);
        int flag = PartitionInfoUtil.COMPARE_EXISTS_PART_LOCATION;
        int i = 0;
        for (int j = 0; j < newPartitionGroups.size(); j++) {
            String mockTableName = "";
            mockOrderedTargetTableLocations.put(newPartitionGroups.get(j).partition_name, new Pair<>(mockTableName,
                GroupInfoUtil.buildGroupNameFromPhysicalDb(newPartitionGroups.get(j).partition_name)));

        }
        if (preparedData.isHasSubPartition() && !preparedData.isUseTemplatePart()
            && !preparedData.isMergeSubPartition()) {
            flag |= PartitionInfoUtil.IGNORE_PARTNAME_LOCALITY;
        }
        PartitionInfo newPartInfo = AlterTableGroupSnapShotUtils
            .getNewPartitionInfo(
                preparedData,
                curPartitionInfo,
                false,
                sqlAlterTableMergePartition,
                preparedData.getOldPartitionNames(),
                ImmutableList.of(targetPartitionName),
                preparedData.getTableGroupName(),
                null,
                preparedData.getInvisiblePartitionGroups(),
                mockOrderedTargetTableLocations,
                ec);

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
