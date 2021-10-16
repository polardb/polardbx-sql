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
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupDropPartitionPreparedData;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.ddl.AlterTableGroupDropPartition;
import org.apache.calcite.sql.SqlAlterTableDropPartition;
import org.apache.calcite.sql.SqlAlterTableGroup;
import org.apache.calcite.sql.SqlIdentifier;

import java.util.List;
import java.util.stream.Collectors;

public class LogicalAlterTableGroupDropPartition extends BaseDdlOperation {

    private AlterTableGroupDropPartitionPreparedData preparedData;

    public LogicalAlterTableGroupDropPartition(DDL ddl) {
        super(ddl);
    }

    public void preparedData() {
        AlterTableGroupDropPartition alterTableGroupDropPartition = (AlterTableGroupDropPartition) relDdl;
        String tableGroupName = alterTableGroupDropPartition.getTableGroupName();
        String tableName = alterTableGroupDropPartition.getTableName().toString();
        SqlAlterTableGroup sqlAlterTableGroup = (SqlAlterTableGroup) alterTableGroupDropPartition.getAst();
        SqlAlterTableDropPartition sqlAlterTableDropPartition =
            (SqlAlterTableDropPartition) sqlAlterTableGroup.getAlters().get(0);

        List<GroupDetailInfoExRecord> targetGroupDetailInfoExRecords =
            TableGroupLocation.getOrderedGroupList(schemaName);

        preparedData = new AlterTableGroupDropPartitionPreparedData();
        preparedData.setTableGroupName(tableGroupName);
        preparedData.setSchemaName(schemaName);
        preparedData.setTableName(tableName);
        preparedData.setWithHint(targetTablesHintCache != null);
        preparedData.setTargetGroupDetailInfoExRecords(targetGroupDetailInfoExRecords);
        preparedData.setOldPartitionNames(
            sqlAlterTableDropPartition.getPartitionNames().stream().map(o -> ((SqlIdentifier) o).getSimple()).collect(
                Collectors.toList()));

        preparedData.prepareInvisiblePartitionGroup();
        List<String> newPartitionNames =
            preparedData.getInvisiblePartitionGroups().stream().map(o -> o.getPartition_name())
                .collect(Collectors.toList());
        preparedData.setNewPartitionNames(newPartitionNames);

        preparedData.setTaskType(ComplexTaskMetaManager.ComplexTaskType.DROP_PARTITION);

    }

    public AlterTableGroupDropPartitionPreparedData getPreparedData() {
        return preparedData;
    }

    public static LogicalAlterTableGroupDropPartition create(DDL ddl) {
        return new LogicalAlterTableGroupDropPartition(ddl);
    }

}
