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

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.tablegroup.TableGroupLocation;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoExRecord;
import com.alibaba.polardbx.gms.util.PartitionNameUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupMergePartitionPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupSplitPartitionPreparedData;
import com.alibaba.polardbx.optimizer.tablegroup.TableGroupInfoManager;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.ddl.AlterTableGroupMergePartition;
import org.apache.calcite.rel.ddl.AlterTableGroupSplitPartition;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAlterTableGroup;
import org.apache.calcite.sql.SqlAlterTableGroupMergePartition;
import org.apache.calcite.sql.SqlAlterTableGroupSplitPartition;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlPartition;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.Util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LogicalAlterTableGroupMergePartition extends BaseDdlOperation {

    private AlterTableGroupMergePartitionPreparedData preparedData;

    public LogicalAlterTableGroupMergePartition(DDL ddl) {
        super(ddl);
    }

    public void preparedData() {
        AlterTableGroupMergePartition alterTableGroupMergePartition = (AlterTableGroupMergePartition) relDdl;
        String tableGroupName = alterTableGroupMergePartition.getTableGroupName();
        SqlAlterTableGroup sqlAlterTableGroup = (SqlAlterTableGroup) alterTableGroupMergePartition.getAst();
        assert sqlAlterTableGroup.getAlters().size() == 1;

        assert sqlAlterTableGroup.getAlters().get(0) instanceof SqlAlterTableGroupMergePartition;
        SqlAlterTableGroupMergePartition sqlAlterTableGroupMergePartition =
            (SqlAlterTableGroupMergePartition) sqlAlterTableGroup.getAlters().get(0);
        String targetPartitionName =
            Util.last(((SqlIdentifier) (sqlAlterTableGroupMergePartition.getTargetPartitionName())).names);
        List<String> partitionsTobeMerged = new ArrayList<>();
        for (SqlNode oldPartition : sqlAlterTableGroupMergePartition.getOldPartitions()) {
            String oldPartitionName =
                Util.last(((SqlIdentifier) (oldPartition)).names);
            partitionsTobeMerged.add(oldPartitionName);
        }
        Map<String, List<String>> mergePartitions = new HashMap<>();
        mergePartitions.put(targetPartitionName, partitionsTobeMerged);

        List<GroupDetailInfoExRecord> targetGroupDetailInfoExRecords =
            TableGroupLocation.getOrderedGroupList(schemaName);

        preparedData = new AlterTableGroupMergePartitionPreparedData();

        preparedData.setSchemaName(schemaName);
        preparedData.setWithHint(targetTablesHintCache != null);

        preparedData.setTableGroupName(tableGroupName);
        preparedData.setMergePartitions(mergePartitions);
        preparedData.setTargetGroupDetailInfoExRecords(targetGroupDetailInfoExRecords);
        preparedData.prepareInvisiblePartitionGroup();
        preparedData.setTaskType(ComplexTaskMetaManager.ComplexTaskType.MERGE_PARTITION);

    }

    public AlterTableGroupMergePartitionPreparedData getPreparedData() {
        return preparedData;
    }

    public static LogicalAlterTableGroupMergePartition create(DDL ddl) {
        return new LogicalAlterTableGroupMergePartition(ddl);
    }

}
