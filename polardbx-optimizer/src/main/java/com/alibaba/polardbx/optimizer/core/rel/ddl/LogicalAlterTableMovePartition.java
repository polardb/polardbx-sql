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
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.gms.locality.LocalityDesc;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.tablegroup.TableGroupLocation;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoExRecord;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupMovePartitionPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableMovePartitionPreparedData;
import com.alibaba.polardbx.optimizer.locality.LocalityInfoUtils;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoUtil;
import com.alibaba.polardbx.optimizer.partition.PartitionLocation;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.tablegroup.AlterTableGroupSnapShotUtils;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.ddl.AlterTable;
import org.apache.calcite.rel.ddl.AlterTableGroupMovePartition;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlAlterTableGroup;
import org.apache.calcite.sql.SqlAlterTableGroupMovePartition;
import org.apache.calcite.sql.SqlAlterTableMovePartition;
import org.apache.calcite.sql.SqlAlterTableSplitPartition;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.util.Util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

public class LogicalAlterTableMovePartition extends BaseDdlOperation {

    protected AlterTableGroupMovePartitionPreparedData preparedData;

    public LogicalAlterTableMovePartition(DDL ddl) {
        super(ddl, ((SqlAlterTable) (ddl.getSqlNode())).getObjectNames());
    }

    public LogicalAlterTableMovePartition(DDL ddl, boolean notIncludeGsiName) {
        super(ddl);
        assert notIncludeGsiName;
    }

    public void preparedData(ExecutionContext ec) {
        AlterTable alterTable = (AlterTable) relDdl;
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
        String tableGroupName = tableGroupConfig.getTableGroupRecord().getTg_name();

        List<GroupDetailInfoExRecord> candidateGroupDetailInfoExRecords =
            TableGroupLocation.getOrderedGroupList(schemaName);

        Map<String, Set<String>> targetPartitions = new HashMap<>();
        for (Map.Entry<SqlNode, List<SqlNode>> entry : sqlAlterTableMovePartition.getInstPartitions().entrySet()) {
            String instId =
                Util.last(((SqlIdentifier) (entry.getKey())).names);
            Set<String> partitionsToBeMoved = entry.getValue().stream()
                .map(o -> Util.last(((SqlIdentifier) (o)).names).toLowerCase()).collect(
                    Collectors.toSet());
            targetPartitions.put(instId, partitionsToBeMoved);
        }

        Set<String> storageInstIds = new TreeSet<>(String::compareToIgnoreCase);

        targetPartitions.entrySet().stream().forEach(o -> storageInstIds.add(o.getKey()));

        //Check accepted dn list for tablegroup retricted by locality.
        //The check is ahead of prepareData because error would happen when prepareData.
        //The tablegroup is not empty.

        Map<String, String> moveOut = new HashMap<>();
        for (Map.Entry<String, Set<String>> targetPartition : targetPartitions.entrySet()) {
            targetPartition.getValue().forEach(
                partition -> moveOut.put(partition, targetPartition.getKey())
            );
        }

        LocalityInfoUtils.CheckAction localityCheckAction = new LocalityInfoUtils.CheckAction() {
            @Override
            public boolean checkPartition(String partition, LocalityDesc localityDesc) {
                return localityDesc.matchStorageInstance(moveOut.get(partition));
            }
        };

        LocalityInfoUtils.checkTableGroupLocalityCompatiable(schemaName, tableGroupName, moveOut.keySet(),
            localityCheckAction);

        preparedData = new AlterTableMovePartitionPreparedData();
        List<GroupDetailInfoExRecord> targetGroupDetailInfoExRecords = new ArrayList<>();
        for (String storageInstId : storageInstIds) {
            List<GroupDetailInfoExRecord> targetGroups =
                candidateGroupDetailInfoExRecords.stream().filter(o -> storageInstIds.contains(o.storageInstId))
                    .collect(
                        Collectors.toList());

            if (GeneralUtil.isEmpty(targetGroups)) {
                candidateGroupDetailInfoExRecords =
                    TableGroupLocation.getOrderedGroupList(schemaName, true);
                targetGroups =
                    candidateGroupDetailInfoExRecords.stream().filter(o -> storageInstIds.contains(o.storageInstId))
                        .collect(
                            Collectors.toList());
                if (GeneralUtil.isEmpty(targetGroups)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_DN_IS_NOT_READY,
                        String.format("the dn[%s] is not ready, please retry this command later",
                            storageInstId));
                } else {
                    throw new TddlRuntimeException(ErrorCode.ERR_PHYSICAL_TOPOLOGY_CHANGING,
                        String.format("the physical group[%s] is changing, please retry this command later",
                            targetGroups.get(0)));
                }
            }
            targetGroupDetailInfoExRecords.addAll(targetGroups);
        }

        preparedData.setTargetGroupDetailInfoExRecords(targetGroupDetailInfoExRecords);
        preparedData.setSchemaName(schemaName);
        preparedData.setWithHint(targetTablesHintCache != null);

        preparedData.setTableGroupName(tableGroupName);
        preparedData.setTargetPartitionsLocation(targetPartitions);

        preparedData.prepareInvisiblePartitionGroup();
        preparedData.setTaskType(ComplexTaskMetaManager.ComplexTaskType.MOVE_PARTITION);
        preparedData.setTableName(logicalTableName);
        preparedData.setSourceSql(((SqlAlterTable) alterTable.getSqlNode()).getSourceSql());

        List<PartitionGroupRecord> newPartitionGroups = preparedData.getInvisiblePartitionGroups();
        List<Pair<String, String>> mockOrderedTargetTableLocations = new ArrayList<>(newPartitionGroups.size());
        int flag = PartitionInfoUtil.COMPARE_EXISTS_PART_LOCATION;
        flag |= PartitionInfoUtil.COMPARE_NEW_PART_LOCATION;
        int i = 0;
        Map<String, String> partitionLocations = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

        for (int j = 0; j < newPartitionGroups.size(); j++) {
            GroupDetailInfoExRecord groupDetailInfoExRecord =
                preparedData.getTargetGroupDetailInfoExRecords().get(i++);

            String mockTableName = "";
            mockOrderedTargetTableLocations.add(new Pair<>(mockTableName, groupDetailInfoExRecord.getGroupName()));
            if (i >= preparedData.getTargetGroupDetailInfoExRecords().size()) {
                i = 0;
            }
            partitionLocations.put(newPartitionGroups.get(j).partition_name, groupDetailInfoExRecord.getGroupName());
        }

        PartitionInfo newPartInfo = AlterTableGroupSnapShotUtils.getNewPartitionInfoForMoveType(curPartitionInfo,
            sqlAlterTableMovePartition.getTargetPartitions(), tableGroupName,
            mockOrderedTargetTableLocations, ec);

        for (PartitionSpec partitionSpec : newPartInfo.getPartitionBy().getPartitions()) {
            PartitionLocation location = partitionSpec.getLocation();
            if (!location.isVisiable()) {
                String groupKey = partitionLocations.get(partitionSpec.getName());
                assert groupKey != null;
                location.setGroupKey(groupKey);
            }
        }

        preparedData.findCandidateTableGroupAndUpdatePrepareDate(tableGroupConfig, newPartInfo, null,
            null, flag, ec);

    }

    public AlterTableGroupMovePartitionPreparedData getPreparedData() {
        return preparedData;
    }

    public static LogicalAlterTableMovePartition create(DDL ddl) {
        return new LogicalAlterTableMovePartition(ddl);
    }

}
