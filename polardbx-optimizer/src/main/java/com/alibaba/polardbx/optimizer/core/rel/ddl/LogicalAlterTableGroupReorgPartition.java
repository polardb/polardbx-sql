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
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.SqlConverter;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupReorgPartitionPreparedData;
import com.alibaba.polardbx.optimizer.partition.PartitionByDefinition;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoUtil;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.tablegroup.AlterTablePartitionHelper;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.ddl.AlterTableGroupReorgPartition;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAlterTableGroup;
import org.apache.calcite.sql.SqlAlterTableReorgPartition;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlPartition;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.gms.partition.TablePartitionRecord.PARTITION_LEVEL_PARTITION;

public class LogicalAlterTableGroupReorgPartition extends LogicalAlterTableReorgPartition {

    public LogicalAlterTableGroupReorgPartition(DDL ddl) {
        super(ddl, true);
    }

    @Override
    public void prepareData(ExecutionContext executionContext) {
        AlterTableGroupReorgPartition alterTableGroupReorgPartition =
            (AlterTableGroupReorgPartition) relDdl;
        SqlAlterTableGroup sqlAlterTableGroup = (SqlAlterTableGroup) alterTableGroupReorgPartition.getAst();

        // Basic information

        SqlAlterTableReorgPartition sqlAlterTableReorgPartition =
            (SqlAlterTableReorgPartition) sqlAlterTableGroup.getAlters().get(0);

        boolean isSubPartition = sqlAlterTableReorgPartition.isSubPartition();

        String tableGroupName = alterTableGroupReorgPartition.getTableGroupName();

        OptimizerContext oc = OptimizerContext.getContext(schemaName);
        TableGroupConfig tableGroupConfig = oc.getTableGroupInfoManager().getTableGroupConfigByName(tableGroupName);
        String firstTableName = tableGroupConfig.getAllTables().get(0).getTableName();
        PartitionInfo partitionInfo = oc.getPartitionInfoManager().getPartitionInfo(firstTableName);

        PartitionByDefinition partByDef = partitionInfo.getPartitionBy();
        PartitionByDefinition subPartByDef = partByDef.getSubPartitionBy();

        // Old partition info

        Set<String> oldPartNames = sqlAlterTableReorgPartition.getNames().stream()
            .map(n -> ((SqlIdentifier) n).getLastName().toLowerCase()).collect(Collectors.toSet());

        Set<String> oldPartGroupNames =
            PartitionInfoUtil.checkAndExpandPartitions(partitionInfo, tableGroupConfig, oldPartNames,
                sqlAlterTableReorgPartition.isSubPartition());

        // New partition info

        List<SqlPartition> newPartDefs =
            sqlAlterTableReorgPartition.getPartitions().stream().map(p -> (SqlPartition) p)
                .collect(Collectors.toList());

        if (!isSubPartition && subPartByDef != null) {
            normalizeNewPartitionDefs(partitionInfo, newPartDefs);
        }

        // Group detail info by locality

        List<GroupDetailInfoExRecord> targetGroupDetailInfoExRecords =
            getGroupDetailInfoByLocality(tableGroupName, tableGroupConfig, oldPartGroupNames);

        // Ready for prepared data

        preparedData = new AlterTableGroupReorgPartitionPreparedData();

        preparedData.setSchemaName(schemaName);
        preparedData.setTableGroupName(tableGroupName);
        preparedData.setWithHint(targetTablesHintCache != null);
        preparedData.setTargetGroupDetailInfoExRecords(targetGroupDetailInfoExRecords);

        preparedData.setOldPartitionNames(oldPartNames.stream().collect(Collectors.toList()));
        preparedData.setOldPartGroupNames(oldPartGroupNames);

        preparedData.setNewPartitions(newPartDefs);

        preparedData.setHasSubPartition(subPartByDef != null);
        preparedData.setUseTemplatePart(subPartByDef != null && subPartByDef.isUseSubPartTemplate());

        preparedData.setReorgSubPartition(isSubPartition);
        preparedData.setOperateOnSubPartition(isSubPartition);

        if (preparedData.isUseTemplatePart() && isSubPartition) {
            List<String> logicalParts = new ArrayList<>();
            for (PartitionSpec partSpec : partByDef.getPartitions()) {
                assert partSpec.isLogical();
                logicalParts.add(partSpec.getName().toLowerCase());
            }
            preparedData.setLogicalParts(logicalParts);
        }

        preparedData.setPartRexInfoCtx(
            alterTableGroupReorgPartition.getPartRexInfoCtxByLevel().get(PARTITION_LEVEL_PARTITION));

        preparedData.setTaskType(ComplexTaskMetaManager.ComplexTaskType.REORGANIZE_PARTITION);

        preparedData.prepareInvisiblePartitionGroup();

        List<String> newPartGroupNames = new ArrayList<>();
        preparedData.getInvisiblePartitionGroups().forEach(p -> newPartGroupNames.add(p.getPartition_name()));

        preparedData.setNewPartitionNames(newPartGroupNames);

        SqlConverter sqlConverter = SqlConverter.getInstance(schemaName, executionContext);
        PlannerContext plannerContext = PlannerContext.getPlannerContext(this.getCluster());
        Map<SqlNode, RexNode> partRexInfoCtx =
            sqlConverter.getRexInfoFromSqlAlterSpec(sqlAlterTableGroup, ImmutableList.of(sqlAlterTableReorgPartition),
                plannerContext);
        preparedData.getPartRexInfoCtx().putAll(partRexInfoCtx);
    }

    public static LogicalAlterTableGroupReorgPartition create(DDL ddl) {
        return new LogicalAlterTableGroupReorgPartition(AlterTablePartitionHelper.fixAlterTableGroupDdlIfNeed(ddl));
    }

}
