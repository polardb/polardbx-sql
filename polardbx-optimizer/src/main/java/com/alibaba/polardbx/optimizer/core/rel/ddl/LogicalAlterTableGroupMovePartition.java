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

import com.alibaba.polardbx.gms.tablegroup.TableGroupLocation;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoExRecord;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupMovePartitionPreparedData;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.ddl.AlterTableGroupMovePartition;
import org.apache.calcite.sql.SqlAlterTableGroup;
import org.apache.calcite.sql.SqlAlterTableGroupMovePartition;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class LogicalAlterTableGroupMovePartition extends BaseDdlOperation {

    private AlterTableGroupMovePartitionPreparedData preparedData;

    public LogicalAlterTableGroupMovePartition(DDL ddl) {
        super(ddl);
    }

    public void preparedData() {
        AlterTableGroupMovePartition alterTableGroupMovePartition = (AlterTableGroupMovePartition) relDdl;
        String tableGroupName = alterTableGroupMovePartition.getTableGroupName();
        SqlAlterTableGroup sqlAlterTableGroup = (SqlAlterTableGroup) alterTableGroupMovePartition.getAst();
        assert sqlAlterTableGroup.getAlters().size() == 1;

        assert sqlAlterTableGroup.getAlters().get(0) instanceof SqlAlterTableGroupMovePartition;

        List<GroupDetailInfoExRecord> targetGroupDetailInfoExRecords =
            TableGroupLocation.getOrderedGroupList(schemaName);

        //todo support move multi-groups in one shot
        String storageInstId = alterTableGroupMovePartition.getTargetPartitions().entrySet().iterator().next().getKey();
        preparedData = new AlterTableGroupMovePartitionPreparedData();

        preparedData.setTargetGroupDetailInfoExRecords(
            targetGroupDetailInfoExRecords.stream().filter(o -> o.storageInstId.equalsIgnoreCase(storageInstId))
                .collect(
                    Collectors.toList()));
        preparedData.setSchemaName(schemaName);
        preparedData.setWithHint(targetTablesHintCache != null);

        preparedData.setTableGroupName(tableGroupName);
        preparedData.setTargetPartitionsLocation(alterTableGroupMovePartition.getTargetPartitions());
        List<String> newPartitionNames = new ArrayList<>();
        for (Map.Entry<String, Set<String>> entry : alterTableGroupMovePartition.getTargetPartitions().entrySet()) {
            newPartitionNames.addAll(entry.getValue());
        }
        preparedData.setNewPartitionNames(newPartitionNames);
        preparedData.setOldPartitionNames(newPartitionNames);
        preparedData.prepareInvisiblePartitionGroup();
        preparedData.setTaskType(ComplexTaskMetaManager.ComplexTaskType.MOVE_PARTITION);
    }

    public AlterTableGroupMovePartitionPreparedData getPreparedData() {
        return preparedData;
    }

    public static LogicalAlterTableGroupMovePartition create(DDL ddl) {
        return new LogicalAlterTableGroupMovePartition(ddl);
    }

}
