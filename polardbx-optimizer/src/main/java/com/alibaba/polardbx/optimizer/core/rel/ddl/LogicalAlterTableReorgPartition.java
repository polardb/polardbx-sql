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
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.gms.locality.LocalityDesc;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.tablegroup.TableGroupLocation;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoExRecord;
import com.alibaba.polardbx.gms.util.GroupInfoUtil;
import com.alibaba.polardbx.gms.util.PartitionNameUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.SqlConverter;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupReorgPartitionPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableReorgPartitionPreparedData;
import com.alibaba.polardbx.optimizer.locality.LocalityInfoUtils;
import com.alibaba.polardbx.optimizer.partition.PartitionByDefinition;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoUtil;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.partition.common.PartitionStrategy;
import com.alibaba.polardbx.optimizer.tablegroup.AlterTableGroupSnapShotUtils;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.ddl.AlterTable;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlAlterTableReorgPartition;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlPartition;
import org.apache.calcite.sql.SqlPartitionValue;
import org.apache.calcite.sql.SqlSubPartition;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.Util;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.gms.partition.TablePartitionRecord.PARTITION_LEVEL_PARTITION;

public class LogicalAlterTableReorgPartition extends BaseDdlOperation {

    protected AlterTableGroupReorgPartitionPreparedData preparedData;

    public LogicalAlterTableReorgPartition(DDL ddl) {
        super(ddl, ((SqlAlterTable) (ddl.getSqlNode())).getObjectNames());
    }

    public LogicalAlterTableReorgPartition(DDL ddl, boolean notIncludeGsiName) {
        super(ddl);
        assert notIncludeGsiName;
    }

    public void prepareData(ExecutionContext executionContext) {
        AlterTable alterTable = (AlterTable) relDdl;
        SqlAlterTable sqlAlterTable = (SqlAlterTable) alterTable.getSqlNode();

        // Basic information

        SqlAlterTableReorgPartition sqlAlterTableReorgPartition =
            (SqlAlterTableReorgPartition) sqlAlterTable.getAlters().get(0);

        boolean isSubPartition = sqlAlterTableReorgPartition.isSubPartition();

        String logicalTableName = Util.last(((SqlIdentifier) alterTable.getTableName()).names);

        OptimizerContext optimizerContext = OptimizerContext.getContext(schemaName);
        PartitionInfo partitionInfo = optimizerContext.getPartitionInfoManager().getPartitionInfo(logicalTableName);
        TableGroupConfig tableGroupConfig =
            optimizerContext.getTableGroupInfoManager().getTableGroupConfigById(partitionInfo.getTableGroupId());

        String tableGroupName = tableGroupConfig.getTableGroupRecord().getTg_name();

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
            TableGroupLocation.getOrderedGroupList(schemaName);

        // Ready for prepared data

        preparedData = new AlterTableReorgPartitionPreparedData();

        preparedData.setSchemaName(schemaName);
        preparedData.setTableGroupName(tableGroupName);
        preparedData.setTableName(logicalTableName);
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

        preparedData.setPartRexInfoCtx(sqlAlterTable.getPartRexInfoCtxByLevel().get(PARTITION_LEVEL_PARTITION));

        preparedData.setTaskType(ComplexTaskMetaManager.ComplexTaskType.REORGANIZE_PARTITION);

        preparedData.prepareInvisiblePartitionGroup(preparedData.isHasSubPartition());

        List<String> newPartGroupNames = new ArrayList<>();
        preparedData.getInvisiblePartitionGroups().forEach(p -> newPartGroupNames.add(p.getPartition_name()));

        preparedData.setNewPartitionNames(newPartGroupNames);

        SqlConverter sqlConverter = SqlConverter.getInstance(schemaName, executionContext);
        PlannerContext plannerContext = PlannerContext.getPlannerContext(this.getCluster());
        Map<SqlNode, RexNode> partRexInfoCtx =
            sqlConverter.getRexInfoFromSqlAlterSpec(sqlAlterTable, ImmutableList.of(sqlAlterTableReorgPartition),
                plannerContext);
        preparedData.getPartRexInfoCtx().putAll(partRexInfoCtx);
        preparedData.setTargetImplicitTableGroupName(sqlAlterTable.getTargetImplicitTableGroupName());
        preparedData.setSourceSql(((SqlAlterTable) alterTable.getSqlNode()).getSourceSql());

        if (preparedData.needFindCandidateTableGroup()) {
            List<PartitionGroupRecord> newPartitionGroups = preparedData.getInvisiblePartitionGroups();
            Map<String, Pair<String, String>> mockOrderedTargetTableLocations =
                new TreeMap<>(String::compareToIgnoreCase);
            int flag = PartitionInfoUtil.COMPARE_EXISTS_PART_LOCATION;

            for (int i = 0; i < newPartitionGroups.size(); i++) {
                String mockTableName = "";
                mockOrderedTargetTableLocations.put(newPartitionGroups.get(i).partition_name, new Pair<>(mockTableName,
                    GroupInfoUtil.buildGroupNameFromPhysicalDb(newPartitionGroups.get(i).partition_name)));
            }

            boolean isAlterTableGroup = this instanceof LogicalAlterTableGroupReorgPartition;

            PartitionInfo newPartInfo = AlterTableGroupSnapShotUtils
                .getNewPartitionInfo(
                    preparedData,
                    partitionInfo,
                    isAlterTableGroup,
                    sqlAlterTableReorgPartition,
                    preparedData.getOldPartitionNames(),
                    preparedData.getNewPartitionNames(),
                    preparedData.getTableGroupName(),
                    null,
                    preparedData.getInvisiblePartitionGroups(),
                    mockOrderedTargetTableLocations,
                    executionContext);

            preparedData.findCandidateTableGroupAndUpdatePrepareDate(tableGroupConfig, newPartInfo,
                preparedData.getNewPartitions(), null, flag, true, executionContext);
        }
    }

    protected void normalizeNewPartitionDefs(PartitionInfo partitionInfo,
                                             List<SqlPartition> newPartDefs) {
        PartitionByDefinition partByDef = partitionInfo.getPartitionBy();
        PartitionByDefinition subPartByDef = partByDef.getSubPartitionBy();

        if (subPartByDef.isUseSubPartTemplate()) {
            for (SqlPartition sqlPartition : newPartDefs) {
                String partName = ((SqlIdentifier) (sqlPartition.getName())).getLastName();

                for (PartitionSpec subPartSpecTemplate : subPartByDef.getPartitions()) {
                    SqlIdentifier subPartName = new SqlIdentifier(
                        PartitionNameUtil.autoBuildSubPartitionName(partName, subPartSpecTemplate.getName()),
                        SqlParserPos.ZERO);

                    SqlPartitionValue subPartValue =
                        (SqlPartitionValue) subPartSpecTemplate.getBoundSpec().getBoundRawValue()
                            .clone(SqlParserPos.ZERO);

                    SqlSubPartition sqlSubPartition = new SqlSubPartition(SqlParserPos.ZERO, subPartName, subPartValue);

                    sqlPartition.getSubPartitions().add(sqlSubPartition);
                }
            }
        } else {
            PartitionStrategy subPartStrategy = subPartByDef.getStrategy();
            for (SqlPartition sqlPartition : newPartDefs) {
                if (GeneralUtil.isEmpty(sqlPartition.getSubPartitions())) {
                    // We don't know which old partition should be referred to
                    // add subpartitions to a new partition that has no subpartition
                    // specified, so have to use default subpartition.
                    String partName = ((SqlIdentifier) sqlPartition.getName()).getLastName();

                    if (subPartStrategy.isRange()) {
                        int subPartColCount = subPartByDef.getPartitionFieldList().size();
                        SqlSubPartition subPartition =
                            PartitionInfoUtil.buildDefaultSubPartitionForRange(partName, 1, subPartColCount);
                        sqlPartition.getSubPartitions().add(subPartition);
                    } else if (subPartStrategy.isList()) {
                        SqlSubPartition subPartition =
                            PartitionInfoUtil.buildDefaultSubPartitionForList(partName, 1);
                        sqlPartition.getSubPartitions().add(subPartition);
                    } else if (subPartStrategy.isHashed()) {
                        SqlSubPartition subPartition = PartitionInfoUtil.buildDefaultSubPartitionForKey(partName, 1);
                        sqlPartition.getSubPartitions().add(subPartition);
                    } else {
                        throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                            String.format("unsupported subpartition strategy %s for reorganization", subPartStrategy));
                    }
                }
            }
        }
    }

    protected List<GroupDetailInfoExRecord> getGroupDetailInfoByLocality(String tableGroupName,
                                                                         TableGroupConfig tableGroupConfig,
                                                                         Set<String> oldPartGroupNames) {
        LocalityInfoUtils.CollectAction collectAction = new LocalityInfoUtils.CollectAction();
        LocalityInfoUtils.checkTableGroupLocalityCompatiable(schemaName, tableGroupName, oldPartGroupNames,
            collectAction);

        List<String> outdatedPartitionGroupLocalities =
            collectAction.getPartitionsLocalityDesc().stream().map(o -> o.toString()).collect(Collectors.toList());
        String firstPartitionLocality = outdatedPartitionGroupLocalities.get(0);

        boolean isIdentical = outdatedPartitionGroupLocalities.stream().allMatch(o -> o.equals(firstPartitionLocality));

        LocalityDesc targetLocalityDesc =
            isIdentical ? LocalityDesc.parse(firstPartitionLocality) : tableGroupConfig.getLocalityDesc();

        List<GroupDetailInfoExRecord> targetGroupDetailInfoExRecords =
            TableGroupLocation.getOrderedGroupList(schemaName).stream()
                .filter(group -> targetLocalityDesc.matchStorageInstance(group.storageInstId))
                .collect(Collectors.toList());

        return targetGroupDetailInfoExRecords;
    }

    public AlterTableGroupReorgPartitionPreparedData getPreparedData() {
        return preparedData;
    }

    public static LogicalAlterTableReorgPartition create(DDL ddl) {
        return new LogicalAlterTableReorgPartition(ddl);
    }

}
