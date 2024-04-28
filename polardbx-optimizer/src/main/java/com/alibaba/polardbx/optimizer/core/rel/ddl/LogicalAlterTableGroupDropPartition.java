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

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoExRecord;
import com.alibaba.polardbx.gms.util.TableGroupNameUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupDropPartitionPreparedData;
import com.alibaba.polardbx.optimizer.archive.CheckOSSArchiveUtil;
import com.alibaba.polardbx.optimizer.locality.LocalityInfoUtils;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.tablegroup.AlterTablePartitionHelper;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.ddl.AlterTableGroupDropPartition;
import org.apache.calcite.sql.SqlAlterTableDropPartition;
import org.apache.calcite.sql.SqlAlterTableGroup;

import java.util.List;
import java.util.stream.Collectors;

public class LogicalAlterTableGroupDropPartition extends LogicalAlterTableDropPartition {

    public LogicalAlterTableGroupDropPartition(DDL ddl) {
        super(ddl, true);
    }

    public void preparedData(ExecutionContext ec) {
        AlterTableGroupDropPartition alterTableGroupDropPartition = (AlterTableGroupDropPartition) relDdl;
        String tableGroupName = alterTableGroupDropPartition.getTableGroupName();
        String tableName = alterTableGroupDropPartition.getTableName().toString();
        SqlAlterTableGroup sqlAlterTableGroup = (SqlAlterTableGroup) alterTableGroupDropPartition.getAst();
        SqlAlterTableDropPartition sqlAlterTableDropPartition =
            (SqlAlterTableDropPartition) sqlAlterTableGroup.getAlters().get(0);

        TableGroupConfig tableGroupConfig =
            OptimizerContext.getContext(schemaName).getTableGroupInfoManager()
                .getTableGroupConfigByName(tableGroupName);
        String firstTableInGroup = tableGroupConfig.getAllTables().get(0);
        PartitionInfo partitionInfo = ec.getSchemaManager(schemaName).getTable(firstTableInGroup).getPartitionInfo();

        List<GroupDetailInfoExRecord> targetGroupDetailInfoExRecords =
            LocalityInfoUtils.getAllowedGroupInfoOfTableGroup(schemaName, tableGroupName);

        preparedData = new AlterTableGroupDropPartitionPreparedData();
        preparedData.setTableGroupName(tableGroupName);
        preparedData.setSchemaName(schemaName);
        preparedData.setTableName(tableName);
        preparedData.setWithHint(targetTablesHintCache != null);
        preparedData.setTargetGroupDetailInfoExRecords(targetGroupDetailInfoExRecords);

        List<String> oldPartitionNames =
            getDroppingPartitionNames(sqlAlterTableDropPartition, partitionInfo, preparedData);

        preparedData.setOldPartitionNames(oldPartitionNames);

        preparedData.prepareInvisiblePartitionGroup(sqlAlterTableDropPartition.isSubPartition());

        List<String> newPartitionNames =
            preparedData.getInvisiblePartitionGroups().stream().map(o -> o.getPartition_name())
                .collect(Collectors.toList());

        preparedData.setNewPartitionNames(newPartitionNames);

        preparedData.setTaskType(ComplexTaskMetaManager.ComplexTaskType.DROP_PARTITION);

    }

    public static LogicalAlterTableGroupDropPartition create(DDL ddl) {
        return new LogicalAlterTableGroupDropPartition(AlterTablePartitionHelper.fixAlterTableGroupDdlIfNeed(ddl));
    }

    @Override
    public boolean isSupportedByBindFileStorage() {
        AlterTableGroupDropPartition alterTableGroupAddPartition = (AlterTableGroupDropPartition) relDdl;
        String tableGroup = alterTableGroupAddPartition.getTableGroupName();
        throw new TddlRuntimeException(ErrorCode.ERR_UNARCHIVE_FIRST, "unarchive tablegroup " + tableGroup);
    }

    @Override
    public boolean checkIfFileStorage(ExecutionContext executionContext) {
        AlterTableGroupDropPartition alterTableGroupDropPartition = (AlterTableGroupDropPartition) relDdl;
        String tableGroupName = alterTableGroupDropPartition.getTableGroupName();
        /* check table group */
        if (TableGroupNameUtil.isFileStorageTg(tableGroupName)) {
            return true;
        }

        /* check table/partition */
        String tableName = alterTableGroupDropPartition.getTableName().toString();
        TableMeta tableMeta = executionContext.getSchemaManager(schemaName).getTableWithNull(tableName);
        if (tableMeta == null) {
            return false;
        }
        return Engine.isFileStore(tableMeta.getEngine());
    }

    @Override
    public boolean checkIfBindFileStorage(ExecutionContext executionContext) {
        AlterTableGroupDropPartition alterTableGroupDropPartition = (AlterTableGroupDropPartition) relDdl;
        String tableGroupName = alterTableGroupDropPartition.getTableGroupName();
        return !CheckOSSArchiveUtil.checkTableGroupWithoutOSS(schemaName, tableGroupName);
    }
}
