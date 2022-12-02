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
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoExRecord;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableDropPartitionPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupDropPartitionPreparedData;
import com.alibaba.polardbx.optimizer.locality.LocalityInfoUtils;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoUtil;
import com.alibaba.polardbx.optimizer.tablegroup.AlterTableGroupSnapShotUtils;
import groovy.sql.Sql;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.ddl.AlterTable;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlAlterTableDropPartition;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlPartition;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.Util;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class LogicalAlterTableDropPartition extends BaseDdlOperation {

    protected AlterTableGroupDropPartitionPreparedData preparedData;

    public LogicalAlterTableDropPartition(DDL ddl) {
        super(ddl, ((SqlAlterTable) (ddl.getSqlNode())).getObjectNames());
    }

    public LogicalAlterTableDropPartition(DDL ddl, boolean notIncludeGsiName) {
        super(ddl);
        assert notIncludeGsiName;
    }

    public void preparedData(ExecutionContext ec) {
        AlterTable alterTable = (AlterTable) relDdl;
        SqlAlterTable sqlAlterTable = (SqlAlterTable) alterTable.getSqlNode();
        assert sqlAlterTable.getAlters().size() == 1;

        assert sqlAlterTable.getAlters().get(0) instanceof SqlAlterTableDropPartition;
        SqlAlterTableDropPartition sqlAlterTableDropPartition =
            (SqlAlterTableDropPartition) sqlAlterTable.getAlters().get(0);

        String logicalTableName = Util.last(((SqlIdentifier) alterTable.getTableName()).names);
        PartitionInfo partitionInfo =
            OptimizerContext.getContext(schemaName).getPartitionInfoManager().getPartitionInfo(logicalTableName);

        TableGroupConfig tableGroupConfig =
            OptimizerContext.getContext(schemaName).getTableGroupInfoManager().getTableGroupConfigById(
                partitionInfo.getTableGroupId());

        String tableGroupName = tableGroupConfig.getTableGroupRecord().getTg_name();

        List<GroupDetailInfoExRecord> targetGroupDetailInfoExRecords =
            LocalityInfoUtils.getAllowedGroupInfoOfTableGroup(schemaName, tableGroupName);

        preparedData = new AlterTableDropPartitionPreparedData();
        preparedData.setTableGroupName(tableGroupName);
        preparedData.setSchemaName(schemaName);
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
        preparedData.setSourceSql(((SqlAlterTable) alterTable.getSqlNode()).getSourceSql());
        preparedData.setTableName(logicalTableName);

        List<Pair<String, String>> mockOrderedTargetTableLocations = new ArrayList<>(newPartitionNames.size());
        int i = 0;
        for (int j = 0; j < newPartitionNames.size(); j++) {
            GroupDetailInfoExRecord groupDetailInfoExRecord =
                preparedData.getTargetGroupDetailInfoExRecords().get(i++);

            String mockTableName = "";
            mockOrderedTargetTableLocations.add(new Pair<>(mockTableName, groupDetailInfoExRecord.getGroupName()));
            if (i >= preparedData.getTargetGroupDetailInfoExRecords().size()) {
                i = 0;
            }
        }

        PartitionInfo newPartInfo = AlterTableGroupSnapShotUtils
            .getNewPartitionInfoForDropPartition(partitionInfo, preparedData.getInvisiblePartitionGroups(),
                sqlAlterTableDropPartition,
                mockOrderedTargetTableLocations,
                ec);
        int flag = PartitionInfoUtil.COMPARE_EXISTS_PART_LOCATION;
        preparedData.findCandidateTableGroupAndUpdatePrepareDate(tableGroupConfig, newPartInfo,
            null, null, flag, ec);
    }

    public AlterTableGroupDropPartitionPreparedData getPreparedData() {
        return preparedData;
    }

    public static LogicalAlterTableDropPartition create(DDL ddl) {
        return new LogicalAlterTableDropPartition(ddl);
    }

}
