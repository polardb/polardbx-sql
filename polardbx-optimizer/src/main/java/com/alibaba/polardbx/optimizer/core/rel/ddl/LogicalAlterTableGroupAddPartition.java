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

import com.google.common.collect.ImmutableList;
import com.alibaba.polardbx.gms.tablegroup.TableGroupLocation;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoExRecord;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupAddPartitionPreparedData;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.ddl.AlterTableGroupAddPartition;
import org.apache.calcite.rel.ddl.AlterTableGroupSplitPartition;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAlterTableAddPartition;
import org.apache.calcite.sql.SqlAlterTableGroup;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlPartition;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class LogicalAlterTableGroupAddPartition extends BaseDdlOperation {

    private AlterTableGroupAddPartitionPreparedData preparedData;

    public LogicalAlterTableGroupAddPartition(DDL ddl) {
        super(ddl);
    }

    public void preparedData() {
        AlterTableGroupAddPartition alterTableGroupAddPartition = (AlterTableGroupAddPartition) relDdl;
        String tableGroupName = alterTableGroupAddPartition.getTableGroupName();
        String tableName = alterTableGroupAddPartition.getTableName().toString();
        Map<SqlNode, RexNode> partBoundExprInfo = alterTableGroupAddPartition.getPartBoundExprInfo();
        SqlAlterTableGroup sqlAlterTableGroup = (SqlAlterTableGroup) alterTableGroupAddPartition.getAst();
        SqlAlterTableAddPartition sqlAlterTableAddPartition =
            (SqlAlterTableAddPartition) sqlAlterTableGroup.getAlters().get(0);

        List<GroupDetailInfoExRecord> targetGroupDetailInfoExRecords =
            TableGroupLocation.getOrderedGroupList(schemaName);

        preparedData = new AlterTableGroupAddPartitionPreparedData();
        preparedData.setTableGroupName(tableGroupName);
        preparedData.setSchemaName(schemaName);
        preparedData.setTableName(tableName);
        preparedData.setWithHint(targetTablesHintCache != null);
        preparedData.setTargetGroupDetailInfoExRecords(targetGroupDetailInfoExRecords);
        preparedData
            .setNewPartitions(sqlAlterTableAddPartition.getPartitions().stream().map(o -> (SqlPartition) o).collect(
                Collectors.toList()));
        preparedData.setOldPartitionNames(ImmutableList.of());

        preparedData.prepareInvisiblePartitionGroup();

        preparedData.setTaskType(ComplexTaskMetaManager.ComplexTaskType.ADD_PARTITION);

    }

    public AlterTableGroupAddPartitionPreparedData getPreparedData() {
        return preparedData;
    }

    public static LogicalAlterTableGroupAddPartition create(DDL ddl) {
        return new LogicalAlterTableGroupAddPartition(ddl);
    }

}
