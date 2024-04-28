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
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupAddPartitionPreparedData;
import com.alibaba.polardbx.optimizer.archive.CheckOSSArchiveUtil;
import com.alibaba.polardbx.optimizer.locality.LocalityInfoUtils;
import com.google.common.collect.ImmutableList;
import com.alibaba.polardbx.optimizer.tablegroup.AlterTablePartitionHelper;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.ddl.AlterTableGroupAddPartition;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAlterTableAddPartition;
import org.apache.calcite.sql.SqlAlterTableGroup;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlPartition;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class LogicalAlterTableGroupAddPartition extends LogicalAlterTableAddPartition {

    public LogicalAlterTableGroupAddPartition(DDL ddl) {
        super(ddl, true);
    }

    @Override
    public void preparedData(ExecutionContext ec) {
        AlterTableGroupAddPartition alterTableGroupAddPartition = (AlterTableGroupAddPartition) relDdl;
        String tableGroupName = alterTableGroupAddPartition.getTableGroupName();
        String tableName = alterTableGroupAddPartition.getTableName().toString();
        Map<Integer, Map<SqlNode, RexNode>> partBoundExprInfoByLevel =
            alterTableGroupAddPartition.getPartBoundExprInfoByLevel();
        SqlAlterTableGroup sqlAlterTableGroup = (SqlAlterTableGroup) alterTableGroupAddPartition.getAst();
        SqlAlterTableAddPartition sqlAlterTableAddPartition =
            (SqlAlterTableAddPartition) sqlAlterTableGroup.getAlters().get(0);

        List<GroupDetailInfoExRecord> targetGroupDetailInfoExRecords =
            LocalityInfoUtils.getAllowedGroupInfoOfTableGroup(schemaName, tableGroupName);

        TableGroupConfig tableGroupConfig = OptimizerContext.getContext(schemaName).getTableGroupInfoManager()
            .getTableGroupConfigByName(tableGroupName);
        String firstTableName = tableGroupConfig.getAllTables().get(0);
        PartitionInfo partitionInfo =
            OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(firstTableName)
                .getPartitionInfo();

        preparedData = new AlterTableGroupAddPartitionPreparedData();
        preparedData.setTableGroupName(tableGroupName);
        preparedData.setSchemaName(schemaName);
        preparedData.setTableName(tableName);
        preparedData.setWithHint(targetTablesHintCache != null);
        preparedData.setTargetGroupDetailInfoExRecords(targetGroupDetailInfoExRecords);

        preparedData.setPartBoundExprInfoByLevel(partBoundExprInfoByLevel);

        preparedData.setNewPartitions(
            sqlAlterTableAddPartition.getPartitions().stream().map(o -> (SqlPartition) o).collect(Collectors.toList()),
            partitionInfo.getPartitionBy(), tableGroupConfig, sqlAlterTableAddPartition.isSubPartition());

        preparedData.setOldPartitionNames(ImmutableList.of());

        preparedData.prepareInvisiblePartitionGroup();

        preparedData.setTaskType(ComplexTaskMetaManager.ComplexTaskType.ADD_PARTITION);

    }

    public static LogicalAlterTableGroupAddPartition create(DDL ddl) {
        return new LogicalAlterTableGroupAddPartition(AlterTablePartitionHelper.fixAlterTableGroupDdlIfNeed(ddl));
    }

    @Override
    public boolean isSupportedByBindFileStorage() {
        AlterTableGroupAddPartition alterTableGroupAddPartition = (AlterTableGroupAddPartition) relDdl;
        String tableGroup = alterTableGroupAddPartition.getTableGroupName();
        throw new TddlRuntimeException(ErrorCode.ERR_UNARCHIVE_FIRST, "unarchive tablegroup " + tableGroup);
    }

    @Override
    public boolean checkIfFileStorage(ExecutionContext executionContext) {
        AlterTableGroupAddPartition alterTableGroupAddPartition = (AlterTableGroupAddPartition) relDdl;
        String tableGroupName = alterTableGroupAddPartition.getTableGroupName();
        /* check table group */
        if (TableGroupNameUtil.isFileStorageTg(tableGroupName)) {
            return true;
        }

        /* check table/partition */
        String tableName = alterTableGroupAddPartition.getTableName().toString();
        TableMeta tableMeta = executionContext.getSchemaManager(schemaName).getTableWithNull(tableName);
        if (tableMeta == null) {
            return false;
        }
        return Engine.isFileStore(tableMeta.getEngine());
    }

    @Override
    public boolean checkIfBindFileStorage(ExecutionContext executionContext) {
        AlterTableGroupAddPartition alterTableGroupAddPartition = (AlterTableGroupAddPartition) relDdl;
        String tableGroupName = alterTableGroupAddPartition.getTableGroupName();
        return !CheckOSSArchiveUtil.checkTableGroupWithoutOSS(schemaName, tableGroupName);
    }
}
