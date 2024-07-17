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
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoExRecord;
import com.alibaba.polardbx.gms.util.TableGroupNameUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.archive.CheckOSSArchiveUtil;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.SqlConverter;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupSplitPartitionPreparedData;
import com.alibaba.polardbx.optimizer.locality.LocalityInfoUtils;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.tablegroup.AlterTablePartitionHelper;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.ddl.AlterTableGroupSplitPartition;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAlterTableGroup;
import org.apache.calcite.sql.SqlAlterTableGroupSplitPartition;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlPartition;
import org.apache.calcite.util.Util;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

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

        TableGroupConfig tableGroupConfig =
            OptimizerContext.getContext(schemaName).getTableGroupInfoManager()
                .getTableGroupConfigByName(tableGroupName);

        if (tableGroupConfig == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_TABLE_GROUP_NOT_EXISTS,
                "tablegroup:" + tableGroupName + " is not exists");
        }
        if (tableGroupConfig.isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_TABLE_GROUP_IS_EMPTY,
                "can't modify the empty tablegroup:" + tableGroupName);
        }
        String firstTableInTg = tableGroupConfig.getTables().get(0);
        assert sqlAlterTableGroup.getAlters().get(0) instanceof SqlAlterTableGroupSplitPartition;
        SqlAlterTableGroupSplitPartition sqlAlterTableGroupSplitPartition =
            (SqlAlterTableGroupSplitPartition) sqlAlterTableGroup.getAlters().get(0);
        String splitPartitionName =
            Util.last(((SqlIdentifier) (sqlAlterTableGroupSplitPartition.getSplitPartitionName())).names);
        List<String> splitPartitions = new ArrayList<>();
        splitPartitions.add(splitPartitionName);
        boolean ignorePartGroupLocality = false;
        Set<String> phyPartitionNames = new TreeSet<>(String::compareToIgnoreCase);
        PartitionInfo partitionInfo =
            ec.getSchemaManager(schemaName).getTable(firstTableInTg).getPartitionInfo();

        boolean isUseTemplatePart = partitionInfo.getPartitionBy().getSubPartitionBy() != null ?
            partitionInfo.getPartitionBy().getSubPartitionBy().isUseSubPartTemplate() : false;
        if (!sqlAlterTableGroupSplitPartition.isSubPartitionsSplit()) {
            if (GeneralUtil.isNotEmpty(sqlAlterTableGroupSplitPartition.getNewPartitions())) {
                for (SqlPartition sqlPartition : sqlAlterTableGroupSplitPartition.getNewPartitions()) {
                    if (GeneralUtil.isNotEmpty(sqlPartition.getSubPartitions())) {
                        ignorePartGroupLocality = true;
                        break;
                    }
                }
            } else {
                if (partitionInfo.getPartitionBy().getSubPartitionBy() != null) {
                    //key/hash split without any new partition info
                    // i.e. split partition p1
                    ignorePartGroupLocality = true;
                }
            }
            if (!ignorePartGroupLocality) {
                for (PartitionSpec partitionSpec : partitionInfo.getPartitionBy().getPartitions()) {
                    if (partitionSpec.getName().equalsIgnoreCase(splitPartitionName) && partitionSpec.isLogical()) {
                        for (PartitionSpec subPartSpec : GeneralUtil.emptyIfNull(partitionSpec.getSubPartitions())) {
                            phyPartitionNames.add(subPartSpec.getName());
                        }
                        break;
                    }
                }
            }
        } else if (isUseTemplatePart) {
            ignorePartGroupLocality = true;
        }
        if (GeneralUtil.isEmpty(phyPartitionNames)) {
            phyPartitionNames.add(splitPartitionName);
        }
        List<GroupDetailInfoExRecord> targetGroupDetailInfoExRecords =
            LocalityInfoUtils.getAllowedGroupInfoOfPartitionGroup(schemaName, tableGroupName, splitPartitionName,
                phyPartitionNames, ignorePartGroupLocality);

        normalizeSqlSplitPartition(sqlAlterTableGroupSplitPartition, tableGroupName, firstTableInTg, splitPartitionName,
            ec);

        preparedData = new AlterTableGroupSplitPartitionPreparedData();

        preparedData.setSchemaName(schemaName);
        preparedData.setWithHint(targetTablesHintCache != null);
        preparedData.setUseTemplatePart(ignorePartGroupLocality);
        preparedData.setSplitSubPartition(sqlAlterTableGroupSplitPartition.isSubPartitionsSplit());
        preparedData.setOperateOnSubPartition(sqlAlterTableGroupSplitPartition.isSubPartitionsSplit());
        preparedData.setTableGroupName(tableGroupName);
        preparedData.setSplitPartitions(splitPartitions);
        preparedData.setNewPartitions(sqlAlterTableGroupSplitPartition.getNewPartitions());
        preparedData.setIncludeFullPartitionDefinition(sqlAlterTableGroupSplitPartition.getAtValue() == null);
        preparedData.setTargetStorageInstIds(null);
        preparedData.setTargetGroupDetailInfoExRecords(targetGroupDetailInfoExRecords);
        preparedData.setPartBoundExprInfo(partBoundExprInfo);
        preparedData.setAtVal(sqlAlterTableGroupSplitPartition.getAtValue());
        preparedData.setTaskType(ComplexTaskMetaManager.ComplexTaskType.SPLIT_PARTITION);
        SqlConverter sqlConverter = SqlConverter.getInstance(schemaName, ec);
        PlannerContext plannerContext = PlannerContext.getPlannerContext(this.getCluster());
        Map<SqlNode, RexNode> partRexInfoCtx =
            sqlConverter.getRexInfoFromSqlAlterSpec(sqlAlterTableGroup,
                ImmutableList.of(sqlAlterTableGroupSplitPartition), plannerContext);
        preparedData.getPartBoundExprInfo().putAll(partRexInfoCtx);

        if (preparedData.isUseTemplatePart() && preparedData.isSplitSubPartition()) {
            List<String> logicalParts = new ArrayList<>();
            for (PartitionSpec partitionSpec : partitionInfo.getPartitionBy().getPartitions()) {
                assert partitionSpec.isLogical();
                logicalParts.add(partitionSpec.getName());
            }
            preparedData.setLogicalParts(logicalParts);
        }
        Boolean hasSubPartition = (partitionInfo.getPartitionBy().getSubPartitionBy() != null);
        preparedData.prepareInvisiblePartitionGroup(hasSubPartition);
    }

    public static LogicalAlterTableGroupSplitPartition create(DDL ddl) {
        return new LogicalAlterTableGroupSplitPartition(AlterTablePartitionHelper.fixAlterTableGroupDdlIfNeed(ddl));
    }

    @Override
    public boolean isSupportedByBindFileStorage() {
        AlterTableGroupSplitPartition alterTableGroupAddPartition = (AlterTableGroupSplitPartition) relDdl;
        String tableGroup = alterTableGroupAddPartition.getTableGroupName();
        throw new TddlRuntimeException(ErrorCode.ERR_UNARCHIVE_FIRST, "unarchive tablegroup " + tableGroup);
    }

    @Override
    public boolean checkIfBindFileStorage(ExecutionContext executionContext) {
        AlterTableGroupSplitPartition alterTableGroupAddPartition = (AlterTableGroupSplitPartition) relDdl;
        String tableGroupName = alterTableGroupAddPartition.getTableGroupName();
        return !CheckOSSArchiveUtil.checkTableGroupWithoutOSS(schemaName, tableGroupName);
    }

    @Override
    public boolean checkIfFileStorage(ExecutionContext executionContext) {
        final AlterTableGroupSplitPartition alterTableGroupSplitPartition = (AlterTableGroupSplitPartition) relDdl;
        final String tableGroupName = alterTableGroupSplitPartition.getTableGroupName();

        return TableGroupNameUtil.isFileStorageTg(tableGroupName);
    }
}
