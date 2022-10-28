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

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoExRecord;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupExtractPartitionPreparedData;
import com.alibaba.polardbx.optimizer.locality.LocalityManager;
import com.alibaba.polardbx.optimizer.locality.LocalityInfoUtils;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPrunerUtils;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionTupleRouteInfoBuilder;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.ddl.AlterTableGroupExtractPartition;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAlterTableGroup;
import org.apache.calcite.sql.SqlAlterTableGroupExtractPartition;
import org.apache.calcite.sql.SqlNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class LogicalAlterTableGroupExtractPartition extends LogicalAlterTableExtractPartition {

    public LogicalAlterTableGroupExtractPartition(DDL ddl) {
        super(ddl, true);
    }

    public void preparedData(ExecutionContext executionContext) {
        AlterTableGroupExtractPartition alterTableGroupExtractPartition = (AlterTableGroupExtractPartition) relDdl;
        String tableGroupName = alterTableGroupExtractPartition.getTableGroupName();
        Map<SqlNode, RexNode> partBoundExprInfo = alterTableGroupExtractPartition.getPartBoundExprInfo();
        SqlAlterTableGroup sqlAlterTableGroup = (SqlAlterTableGroup) alterTableGroupExtractPartition.getAst();
        assert sqlAlterTableGroup.getAlters().size() == 1;

        TableGroupConfig tableGroupConfig = OptimizerContext.getContext(schemaName).getTableGroupInfoManager()
            .getTableGroupConfigByName(tableGroupName);
        if (tableGroupConfig == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_TABLE_GROUP_NOT_EXISTS,
                "tablegroup:" + tableGroupName + " doesn't exists");
        }
        if (tableGroupConfig.getTableCount() == 0) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                "can't modify the tablegroup:" + tableGroupName + " when it's empty");
        }
        String firstTableInTableGroup = tableGroupConfig.getTables().get(0).getTableName();

        assert sqlAlterTableGroup.getAlters().get(0) instanceof SqlAlterTableGroupExtractPartition;
        SqlAlterTableGroupExtractPartition sqlAlterTableGroupExtractPartition =
            (SqlAlterTableGroupExtractPartition) sqlAlterTableGroup.getAlters().get(0);

        List<Long[]> splitPoints =
            normalizeSqlExtractPartition(sqlAlterTableGroupExtractPartition, firstTableInTableGroup,
                alterTableGroupExtractPartition.getPartBoundExprInfo(), executionContext);

        String extractPartitionName = sqlAlterTableGroupExtractPartition.getExtractPartitionName();
        List<String> splitPartitions = new ArrayList<>();
        splitPartitions.add(extractPartitionName);

        List<GroupDetailInfoExRecord> targetGroupDetailInfoExRecords =
            LocalityInfoUtils.getAllowedGroupInfoOfTableGroup(schemaName, tableGroupName);
        preparedData = new AlterTableGroupExtractPartitionPreparedData();

        preparedData.setSchemaName(schemaName);
        preparedData.setWithHint(targetTablesHintCache != null);

        preparedData.setTableGroupName(tableGroupName);
        preparedData.setSplitPartitions(splitPartitions);
        preparedData.setNewPartitions(sqlAlterTableGroupExtractPartition.getNewPartitions());
        preparedData.setTargetGroupDetailInfoExRecords(targetGroupDetailInfoExRecords);
        preparedData.setPartBoundExprInfo(partBoundExprInfo);
        preparedData.setHotKeys(sqlAlterTableGroupExtractPartition.getHotKeys());
        preparedData.prepareInvisiblePartitionGroup();
        preparedData.setTaskType(ComplexTaskMetaManager.ComplexTaskType.EXTRACT_PARTITION);
        preparedData.setSplitPoints(splitPoints);
    }

    public static LogicalAlterTableGroupExtractPartition create(DDL ddl) {
        return new LogicalAlterTableGroupExtractPartition(ddl);
    }

}
