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

import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPrunerUtils;
import com.google.common.collect.ImmutableList;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.tablegroup.TableGroupLocation;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoExRecord;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupExtractPartitionPreparedData;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPruner;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionTupleRouteInfoBuilder;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.ddl.AlterTableGroupExtractPartition;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAlterTableGroup;
import org.apache.calcite.sql.SqlAlterTableGroupExtractPartition;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlPartition;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class LogicalAlterTableGroupExtractPartition extends BaseDdlOperation {

    private AlterTableGroupExtractPartitionPreparedData preparedData;
    private AlterTableGroupExtractPartition alterTableGroupExtractPartition;

    public LogicalAlterTableGroupExtractPartition(DDL ddl) {
        super(ddl);
    }

    public void preparedData(ExecutionContext executionContext) {
        alterTableGroupExtractPartition = (AlterTableGroupExtractPartition) relDdl;
        String tableGroupName = alterTableGroupExtractPartition.getTableGroupName();
        Map<SqlNode, RexNode> partBoundExprInfo = alterTableGroupExtractPartition.getPartBoundExprInfo();
        SqlAlterTableGroup sqlAlterTableGroup = (SqlAlterTableGroup) alterTableGroupExtractPartition.getAst();
        assert sqlAlterTableGroup.getAlters().size() == 1;

        assert sqlAlterTableGroup.getAlters().get(0) instanceof SqlAlterTableGroupExtractPartition;
        SqlAlterTableGroupExtractPartition sqlAlterTableGroupExtractPartition =
            (SqlAlterTableGroupExtractPartition) sqlAlterTableGroup.getAlters().get(0);
        normalizeSqlExtractPartition(sqlAlterTableGroupExtractPartition, tableGroupName, executionContext);

        String extractPartitionName = sqlAlterTableGroupExtractPartition.getExtractPartitionName();
        List<String> splitPartitions = new ArrayList<>();
        splitPartitions.add(extractPartitionName);

        List<GroupDetailInfoExRecord> targetGroupDetailInfoExRecords =
            TableGroupLocation.getOrderedGroupList(schemaName);

        preparedData = new AlterTableGroupExtractPartitionPreparedData();

        preparedData.setSchemaName(schemaName);
        preparedData.setWithHint(targetTablesHintCache != null);

        preparedData.setTableGroupName(tableGroupName);
        preparedData.setSplitPartitions(splitPartitions);
        preparedData.setNewPartitions(sqlAlterTableGroupExtractPartition.getNewPartitions());
        preparedData.setTargetGroupDetailInfoExRecords(targetGroupDetailInfoExRecords);
        preparedData.setPartBoundExprInfo(partBoundExprInfo);
        preparedData.setHotKey(sqlAlterTableGroupExtractPartition.getHotKey());
        preparedData.prepareInvisiblePartitionGroup();
        preparedData.setTaskType(ComplexTaskMetaManager.ComplexTaskType.EXTRACT_PARTITION);
    }

    private void normalizeSqlExtractPartition(SqlAlterTableGroupExtractPartition sqlAlterTableGroupExtractPartition,
                                              String tableGroupName,
                                              ExecutionContext executionContext) {
        TableGroupConfig tableGroupConfig = OptimizerContext.getContext(schemaName).getTableGroupInfoManager()
            .getTableGroupConfigByName(tableGroupName);
        RexNode rexNode = alterTableGroupExtractPartition.getPartBoundExprInfo()
            .get(sqlAlterTableGroupExtractPartition.getHotKey());

        String tableInCurrentGroup = tableGroupConfig.getAllTables().get(0).getLogTbRec().tableName;
        PartitionInfo partitionInfo =
            OptimizerContext.getContext(schemaName).getPartitionInfoManager().getPartitionInfo(tableInCurrentGroup);

        PartitionSpec partitionSpec = PartitionTupleRouteInfoBuilder
            .getPartitionSpecByExprValues(partitionInfo, ImmutableList.of(rexNode), executionContext);
        assert partitionSpec != null;
        PartitionSpec prevPartitionSpec = partitionInfo.getPartitionBy().getPartitions().stream()
            .filter(o -> o.getPosition().longValue() == partitionSpec.getPosition().longValue() - 1).findFirst()
            .orElse(null);
        PartitionGroupRecord splitPartitionGroupRecord = tableGroupConfig.getPartitionGroupRecords().stream()
            .filter(o -> o.id.longValue() == partitionSpec.getLocation().getPartitionGroupId().longValue()).findFirst()
            .orElse(null);
        assert splitPartitionGroupRecord != null;

        sqlAlterTableGroupExtractPartition.setExtractPartitionName(splitPartitionGroupRecord.partition_name);
        Long atVal = PartitionTupleRouteInfoBuilder
            .computeExprValuesHashCode(partitionInfo, ImmutableList.of(rexNode), executionContext);
        int flag = PartitionPrunerUtils.getExtractPosition(partitionSpec, prevPartitionSpec, atVal);
        if (flag == -2) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                "the hot key:" + atVal.toString() + " is already in a exclusive partition");
        }
        int maxIndex = 0;
        for (PartitionGroupRecord record : tableGroupConfig.getPartitionGroupRecords()) {
            int curIndex = Integer.valueOf(record.partition_name.substring(1));
            if (curIndex > maxIndex) {
                maxIndex = curIndex;
            }
        }
        SqlIdentifier name1 = new SqlIdentifier("p" + String.valueOf(maxIndex + 1), SqlParserPos.ZERO);
        SqlPartition sqlPartition1 = new SqlPartition(name1, null, SqlParserPos.ZERO);
        sqlAlterTableGroupExtractPartition.getNewPartitions().add(sqlPartition1);

        SqlIdentifier name2 = new SqlIdentifier("p" + String.valueOf(maxIndex + 2), SqlParserPos.ZERO);
        SqlPartition sqlPartition2 = new SqlPartition(name2, null, SqlParserPos.ZERO);
        sqlAlterTableGroupExtractPartition.getNewPartitions().add(sqlPartition2);
        if (flag == 0) {
            SqlIdentifier name3 = new SqlIdentifier("p" + String.valueOf(maxIndex + 3), SqlParserPos.ZERO);
            SqlPartition sqlPartition3 = new SqlPartition(name3, null, SqlParserPos.ZERO);
            sqlAlterTableGroupExtractPartition.getNewPartitions().add(sqlPartition3);
        }

    }

    public AlterTableGroupExtractPartitionPreparedData getPreparedData() {
        return preparedData;
    }

    public static LogicalAlterTableGroupExtractPartition create(DDL ddl) {
        return new LogicalAlterTableGroupExtractPartition(ddl);
    }

}
