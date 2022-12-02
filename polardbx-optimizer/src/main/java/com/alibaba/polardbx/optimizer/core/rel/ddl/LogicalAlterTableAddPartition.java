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
import com.alibaba.polardbx.gms.topology.GroupDetailInfoExRecord;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableAddPartitionPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupAddPartitionPreparedData;
import com.alibaba.polardbx.optimizer.locality.LocalityInfoUtils;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoUtil;
import com.alibaba.polardbx.optimizer.tablegroup.AlterTableGroupSnapShotUtils;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.ddl.AlterTable;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlAlterTableAddPartition;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlPartition;
import org.apache.calcite.util.Util;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class LogicalAlterTableAddPartition extends BaseDdlOperation {

    protected AlterTableGroupAddPartitionPreparedData preparedData;

    public LogicalAlterTableAddPartition(DDL ddl) {
        super(ddl, ((SqlAlterTable) (ddl.getSqlNode())).getObjectNames());
    }

    public LogicalAlterTableAddPartition(DDL ddl, boolean notIncludeGsiName) {
        super(ddl);
        assert notIncludeGsiName;
    }

    public void preparedData(ExecutionContext ec) {
        AlterTable alterTable = (AlterTable) relDdl;
        SqlAlterTable sqlAlterTable = (SqlAlterTable) alterTable.getSqlNode();
        assert sqlAlterTable.getAlters().size() == 1;

        assert sqlAlterTable.getAlters().get(0) instanceof SqlAlterTableAddPartition;
        SqlAlterTableAddPartition sqlAlterTableAddPartition =
            (SqlAlterTableAddPartition) sqlAlterTable.getAlters().get(0);

        String logicalTableName = Util.last(((SqlIdentifier) alterTable.getTableName()).names);
        PartitionInfo partitionInfo =
            OptimizerContext.getContext(schemaName).getPartitionInfoManager().getPartitionInfo(logicalTableName);

        TableGroupConfig tableGroupConfig =
            OptimizerContext.getContext(schemaName).getTableGroupInfoManager().getTableGroupConfigById(
                partitionInfo.getTableGroupId());

        String tableGroupName = tableGroupConfig.getTableGroupRecord().getTg_name();

        List<GroupDetailInfoExRecord> targetGroupDetailInfoExRecords =
            LocalityInfoUtils.getAllowedGroupInfoOfTableGroup(schemaName, tableGroupName);

        preparedData = new AlterTableAddPartitionPreparedData();
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
        preparedData.setPartBoundExprInfo(alterTable.getAllRexExprInfo());
        preparedData.setSourceSql(((SqlAlterTable) alterTable.getSqlNode()).getSourceSql());
        preparedData.setTableName(logicalTableName);
        preparedData.setTaskType(ComplexTaskMetaManager.ComplexTaskType.ADD_PARTITION);

        List<PartitionGroupRecord> newPartitionGroups = preparedData.getInvisiblePartitionGroups();
        List<Pair<String, String>> mockOrderedTargetTableLocations = new ArrayList<>(newPartitionGroups.size());
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

        PartitionInfo newPartInfo = AlterTableGroupSnapShotUtils
            .getNewPartitionInfoForAddPartition(partitionInfo, preparedData.getInvisiblePartitionGroups(),
                sqlAlterTableAddPartition,
                mockOrderedTargetTableLocations, preparedData.getPartBoundExprInfo(), ec);
        List<SqlPartition> sqlPartitions =
            sqlAlterTableAddPartition.getPartitions().stream().map(o -> (SqlPartition) o).collect(Collectors.toList());
        int flag = PartitionInfoUtil.COMPARE_EXISTS_PART_LOCATION;
        preparedData.findCandidateTableGroupAndUpdatePrepareDate(tableGroupConfig, newPartInfo,
            sqlPartitions, null, flag, ec);

    }

    public AlterTableGroupAddPartitionPreparedData getPreparedData() {
        return preparedData;
    }

    public static LogicalAlterTableAddPartition create(DDL ddl) {
        return new LogicalAlterTableAddPartition(ddl);
    }

}
