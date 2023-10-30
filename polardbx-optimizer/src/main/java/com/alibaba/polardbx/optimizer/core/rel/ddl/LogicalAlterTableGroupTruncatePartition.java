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

import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoExRecord;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupTruncatePartitionPreparedData;
import com.alibaba.polardbx.optimizer.locality.LocalityInfoUtils;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.tablegroup.AlterTablePartitionHelper;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.ddl.AlterTableGroupTruncatePartition;
import org.apache.calcite.sql.SqlAlterTableGroup;
import org.apache.calcite.sql.SqlAlterTableTruncatePartition;

import java.util.List;
import java.util.Set;

public class LogicalAlterTableGroupTruncatePartition extends LogicalAlterTableTruncatePartition {

    public LogicalAlterTableGroupTruncatePartition(DDL ddl) {
        super(ddl, true);
    }

    public void prepareData(ExecutionContext executionContext) {
        AlterTableGroupTruncatePartition alterTableGroupTruncatePartition = (AlterTableGroupTruncatePartition) relDdl;
        SqlAlterTableGroup sqlAlterTableGroup = (SqlAlterTableGroup) alterTableGroupTruncatePartition.getAst();

        SqlAlterTableTruncatePartition sqlAlterTableTruncatePartition =
            (SqlAlterTableTruncatePartition) sqlAlterTableGroup.getAlters().get(0);

        String tableGroupName = alterTableGroupTruncatePartition.getTableGroupName();

        TableGroupConfig tableGroupConfig =
            OptimizerContext.getContext(null).getTableGroupInfoManager().getTableGroupConfigByName(tableGroupName);
        String firstTableInGroup = tableGroupConfig.getAllTables().get(0).getLogTbRec().tableName;
        PartitionInfo partitionInfo =
            executionContext.getSchemaManager().getTable(firstTableInGroup).getPartitionInfo();

        List<GroupDetailInfoExRecord> targetGroupDetailInfoExRecords =
            LocalityInfoUtils.getAllowedGroupInfoOfTableGroup(schemaName, tableGroupName);

        Set<String> actualPartitionNames =
            getTruncatingPartitionNames(sqlAlterTableTruncatePartition, partitionInfo, tableGroupConfig);

        preparedData = new AlterTableGroupTruncatePartitionPreparedData();
        preparedData.setSchemaName(schemaName);
        preparedData.setTableGroupName(tableGroupName);
        preparedData.setWithHint(targetTablesHintCache != null);
        preparedData.setTargetGroupDetailInfoExRecords(targetGroupDetailInfoExRecords);
        preparedData.setTruncatePartitionNames(actualPartitionNames);
    }

    public static LogicalAlterTableGroupTruncatePartition create(DDL ddl) {
        return new LogicalAlterTableGroupTruncatePartition(AlterTablePartitionHelper.fixAlterTableGroupDdlIfNeed(ddl));
    }

}
