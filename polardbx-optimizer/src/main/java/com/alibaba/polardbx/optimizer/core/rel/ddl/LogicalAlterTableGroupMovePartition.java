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
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.gms.locality.LocalityDesc;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.tablegroup.TableGroupLocation;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoExRecord;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupMovePartitionPreparedData;
import com.alibaba.polardbx.optimizer.locality.LocalityInfoUtils;
import com.alibaba.polardbx.optimizer.tablegroup.TableGroupInfoManager;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.ddl.AlterTableGroupMovePartition;
import org.apache.calcite.sql.SqlAlterTableGroup;
import org.apache.calcite.sql.SqlAlterTableGroupMovePartition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

public class LogicalAlterTableGroupMovePartition extends LogicalAlterTableMovePartition {

    public LogicalAlterTableGroupMovePartition(DDL ddl) {
        super(ddl, true);
    }

    public void preparedData(ExecutionContext ec) {
        AlterTableGroupMovePartition alterTableGroupMovePartition = (AlterTableGroupMovePartition) relDdl;
        String tableGroupName = alterTableGroupMovePartition.getTableGroupName();
        SqlAlterTableGroup sqlAlterTableGroup = (SqlAlterTableGroup) alterTableGroupMovePartition.getAst();
        assert sqlAlterTableGroup.getAlters().size() == 1;

        assert sqlAlterTableGroup.getAlters().get(0) instanceof SqlAlterTableGroupMovePartition;
        SqlAlterTableGroupMovePartition sqlAlterTableGroupMovePartition =
            (SqlAlterTableGroupMovePartition) sqlAlterTableGroup.getAlters().get(0);

        List<GroupDetailInfoExRecord> candidateGroupDetailInfoExRecords =
            TableGroupLocation.getOrderedGroupList(schemaName);

        Set<String> storageInstIds = new TreeSet<>(String::compareToIgnoreCase);

        sqlAlterTableGroupMovePartition.getTargetPartitions().entrySet().stream()
            .forEach(o -> storageInstIds.add(o.getKey()));
        //Check accepted dn list for tablegroup retricted by locality.
        //The check is ahead of prepareData because error would happen when prepareData.
        //The tablegroup is not empty.

        Map<String, String> moveOut = new HashMap<>();
        for (Map.Entry<String, Set<String>> targetPartition : sqlAlterTableGroupMovePartition.getTargetPartitions()
            .entrySet()) {
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
        preparedData = new AlterTableGroupMovePartitionPreparedData();
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
        preparedData.setTargetPartitionsLocation(sqlAlterTableGroupMovePartition.getTargetPartitions());

        preparedData.prepareInvisiblePartitionGroup();
        preparedData.setTaskType(ComplexTaskMetaManager.ComplexTaskType.MOVE_PARTITION);
    }

    public static LogicalAlterTableGroupMovePartition create(DDL ddl) {
        return new LogicalAlterTableGroupMovePartition(ddl);
    }

}
