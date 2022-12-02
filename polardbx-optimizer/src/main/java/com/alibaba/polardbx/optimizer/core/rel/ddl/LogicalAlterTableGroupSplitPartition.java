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

import com.alibaba.polardbx.gms.locality.LocalityDesc;
import com.alibaba.polardbx.gms.tablegroup.TableGroupLocation;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoExRecord;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupSplitPartitionPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableSplitPartitionPreparedData;
import com.alibaba.polardbx.optimizer.locality.LocalityInfoUtils;
import com.alibaba.polardbx.optimizer.tablegroup.TableGroupInfoManager;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.ddl.AlterTableGroupSplitPartition;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAlterTableGroup;
import org.apache.calcite.sql.SqlAlterTableGroupSplitPartition;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.util.Util;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class LogicalAlterTableGroupSplitPartition extends LogicalAlterTableSplitPartition {

    public LogicalAlterTableGroupSplitPartition(DDL ddl) {
        super(ddl, true);
    }

    public void preparedData(ExecutionContext ec) {
        AlterTableGroupSplitPartition alterTableGroupSplitPartition = (AlterTableGroupSplitPartition) relDdl;
        String tableGroupName = alterTableGroupSplitPartition.getTableGroupName();
        Map<SqlNode, RexNode> partBoundExprInfo = alterTableGroupSplitPartition.getPartBoundExprInfo();
        SqlAlterTableGroup sqlAlterTableGroup = (SqlAlterTableGroup) alterTableGroupSplitPartition.getAst();
        assert sqlAlterTableGroup.getAlters().size() == 1;

        assert sqlAlterTableGroup.getAlters().get(0) instanceof SqlAlterTableGroupSplitPartition;
        SqlAlterTableGroupSplitPartition sqlAlterTableGroupSplitPartition =
            (SqlAlterTableGroupSplitPartition) sqlAlterTableGroup.getAlters().get(0);
        normalizeSqlSplitPartition(sqlAlterTableGroupSplitPartition, tableGroupName);
        String splitPartitionName =
            Util.last(((SqlIdentifier) (sqlAlterTableGroupSplitPartition.getSplitPartitionName())).names);
        List<String> splitPartitions = new ArrayList<>();
        splitPartitions.add(splitPartitionName);

        List<GroupDetailInfoExRecord> targetGroupDetailInfoExRecords =
            LocalityInfoUtils.getAllowedGroupInfoOfPartitionGroup(schemaName, tableGroupName, splitPartitionName);

        preparedData = new AlterTableGroupSplitPartitionPreparedData();

        preparedData.setSchemaName(schemaName);
        preparedData.setWithHint(targetTablesHintCache != null);

        preparedData.setTableGroupName(tableGroupName);
        preparedData.setSplitPartitions(splitPartitions);
        preparedData.setNewPartitions(sqlAlterTableGroupSplitPartition.getNewPartitions());
        preparedData.setIncludeFullPartitionDefinition(sqlAlterTableGroupSplitPartition.getAtValue() == null);
        preparedData.setTargetStorageInstIds(null);
        preparedData.setTargetGroupDetailInfoExRecords(targetGroupDetailInfoExRecords);
        preparedData.setPartBoundExprInfo(partBoundExprInfo);
        preparedData.setAtVal(sqlAlterTableGroupSplitPartition.getAtValue());
        preparedData.prepareInvisiblePartitionGroup();
        preparedData.setTaskType(ComplexTaskMetaManager.ComplexTaskType.SPLIT_PARTITION);
    }

    public static LogicalAlterTableGroupSplitPartition create(DDL ddl) {
        return new LogicalAlterTableGroupSplitPartition(ddl);
    }

}
