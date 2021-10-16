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
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupModifyPartitionPreparedData;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.ddl.AlterTableGroupModifyPartition;
import org.apache.calcite.sql.SqlAlterTableGroup;
import org.apache.calcite.sql.SqlAlterTableModifyPartitionValues;
import org.apache.calcite.sql.SqlIdentifier;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class LogicalAlterTableGroupModifyPartition extends BaseDdlOperation {

    private AlterTableGroupModifyPartitionPreparedData preparedData;

    public LogicalAlterTableGroupModifyPartition(DDL ddl) {
        super(ddl);
    }

    public void preparedData() {
        AlterTableGroupModifyPartition alterTableGroupModifyPartition = (AlterTableGroupModifyPartition) relDdl;
        String tableGroupName = alterTableGroupModifyPartition.getTableGroupName();
        String tableName = alterTableGroupModifyPartition.getTableName().toString();
        SqlAlterTableGroup sqlAlterTableGroup = (SqlAlterTableGroup) alterTableGroupModifyPartition.getAst();
        SqlAlterTableModifyPartitionValues sqlAlterTableModifyPartitionValues =
            (SqlAlterTableModifyPartitionValues) sqlAlterTableGroup.getAlters().get(0);

        boolean isDropVal = sqlAlterTableModifyPartitionValues.isDrop();

        List<GroupDetailInfoExRecord> targetGroupDetailInfoExRecords =
            TableGroupLocation.getOrderedGroupList(schemaName);

        preparedData = new AlterTableGroupModifyPartitionPreparedData();
        preparedData.setTableGroupName(tableGroupName);
        preparedData.setSchemaName(schemaName);
        preparedData.setTableName(tableName);
        preparedData.setWithHint(targetTablesHintCache != null);
        preparedData.setTargetGroupDetailInfoExRecords(targetGroupDetailInfoExRecords);
        List<String> oldPartition = new ArrayList<>();
        oldPartition.add(((SqlIdentifier) sqlAlterTableModifyPartitionValues.getPartition().getName()).getLastName());
        preparedData.setOldPartitionNames(oldPartition);
        preparedData.setDropVal(isDropVal);

        preparedData.prepareInvisiblePartitionGroup();
        List<String> newPartitionNames =
            preparedData.getInvisiblePartitionGroups().stream().map(o -> o.getPartition_name())
                .collect(Collectors.toList());
        preparedData.setNewPartitionNames(newPartitionNames);

        preparedData.setTaskType(ComplexTaskMetaManager.ComplexTaskType.MODIFY_PARTITION);

    }

    public AlterTableGroupModifyPartitionPreparedData getPreparedData() {
        return preparedData;
    }

    public static LogicalAlterTableGroupModifyPartition create(DDL ddl) {
        return new LogicalAlterTableGroupModifyPartition(ddl);
    }

}
