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
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupModifyPartitionPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableModifyPartitionPreparedData;
import com.alibaba.polardbx.optimizer.locality.LocalityInfoUtils;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoUtil;
import com.alibaba.polardbx.optimizer.tablegroup.AlterTableGroupSnapShotUtils;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.ddl.AlterTable;
import org.apache.calcite.rel.ddl.AlterTableGroupModifyPartition;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlAlterTableGroup;
import org.apache.calcite.sql.SqlAlterTableModifyPartitionValues;
import org.apache.calcite.sql.SqlAlterTableMovePartition;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.util.Util;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class LogicalAlterTableModifyPartition extends BaseDdlOperation {

    protected AlterTableGroupModifyPartitionPreparedData preparedData;

    public LogicalAlterTableModifyPartition(DDL ddl) {
        super(ddl, ((SqlAlterTable) (ddl.getSqlNode())).getObjectNames());
    }

    public LogicalAlterTableModifyPartition(DDL ddl, boolean notIncludeGsiName) {
        super(ddl);
        assert notIncludeGsiName;
    }

    public void preparedData(ExecutionContext ec) {
        AlterTable alterTable = (AlterTable) relDdl;
        SqlAlterTable sqlAlterTable = (SqlAlterTable) alterTable.getSqlNode();
        assert sqlAlterTable.getAlters().size() == 1;

        assert sqlAlterTable.getAlters().get(0) instanceof SqlAlterTableMovePartition;
        SqlAlterTableModifyPartitionValues sqlAlterTableModifyPartitionValues =
            (SqlAlterTableModifyPartitionValues) sqlAlterTable.getAlters().get(0);

        String logicalTableName = Util.last(((SqlIdentifier) alterTable.getTableName()).names);
        PartitionInfo curPartitionInfo =
            OptimizerContext.getContext(schemaName).getPartitionInfoManager().getPartitionInfo(logicalTableName);

        OptimizerContext oc =
            Objects.requireNonNull(OptimizerContext.getContext(schemaName), schemaName + " corrupted");
        TableGroupConfig tableGroupConfig =
            oc.getTableGroupInfoManager().getTableGroupConfigById(curPartitionInfo.getTableGroupId());
        String tableGroupName = tableGroupConfig.getTableGroupRecord().getTg_name();

        boolean isDropVal = sqlAlterTableModifyPartitionValues.isDrop();

        preparedData = new AlterTableModifyPartitionPreparedData();
        preparedData.setTableGroupName(tableGroupName);
        preparedData.setSchemaName(schemaName);
        preparedData.setTableName(tableName);
        preparedData.setWithHint(targetTablesHintCache != null);
        List<String> oldPartition = new ArrayList<>();
        oldPartition.add(((SqlIdentifier) sqlAlterTableModifyPartitionValues.getPartition().getName()).getLastName());
        preparedData.setOldPartitionNames(oldPartition);
        preparedData.setDropVal(isDropVal);
        List<GroupDetailInfoExRecord> targetGroupDetailInfoExRecords =
            LocalityInfoUtils.getAllowedGroupInfoOfPartitionGroup(schemaName, tableGroupName, oldPartition.get(0));
        preparedData.setTargetGroupDetailInfoExRecords(targetGroupDetailInfoExRecords);
        preparedData.prepareInvisiblePartitionGroup();
        List<String> newPartitionNames =
            preparedData.getInvisiblePartitionGroups().stream().map(o -> o.getPartition_name())
                .collect(Collectors.toList());
        preparedData.setNewPartitionNames(newPartitionNames);

        preparedData.setSourceSql(((SqlAlterTable) alterTable.getSqlNode()).getSourceSql());
        preparedData.setTableName(logicalTableName);
        preparedData.setTaskType(ComplexTaskMetaManager.ComplexTaskType.MODIFY_PARTITION);
        preparedData.setPartBoundExprInfo(alterTable.getAllRexExprInfo());

        List<PartitionGroupRecord> newPartitionGroups = preparedData.getInvisiblePartitionGroups();
        List<Pair<String, String>> mockOrderedTargetTableLocations = new ArrayList<>(newPartitionGroups.size());
        int flag = PartitionInfoUtil.COMPARE_EXISTS_PART_LOCATION;
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

        PartitionInfo newPartInfo = AlterTableGroupSnapShotUtils.getNewPartitionInfoForModifyPartition(curPartitionInfo,
            sqlAlterTableModifyPartitionValues,
            preparedData.getPartBoundExprInfo(), mockOrderedTargetTableLocations, ec);

        preparedData.findCandidateTableGroupAndUpdatePrepareDate(tableGroupConfig, newPartInfo, null,
            null, flag, ec);
    }

    public AlterTableGroupModifyPartitionPreparedData getPreparedData() {
        return preparedData;
    }

    public static LogicalAlterTableModifyPartition create(DDL ddl) {
        return new LogicalAlterTableModifyPartition(ddl);
    }

}
