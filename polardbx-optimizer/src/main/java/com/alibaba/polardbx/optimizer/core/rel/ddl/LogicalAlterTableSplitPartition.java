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
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.tablegroup.TableGroupLocation;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoExRecord;
import com.alibaba.polardbx.gms.util.GroupInfoUtil;
import com.alibaba.polardbx.gms.util.PartitionNameUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.SqlConverter;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupSplitPartitionPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableSplitPartitionPreparedData;
import com.alibaba.polardbx.optimizer.partition.PartitionByDefinition;
import com.alibaba.polardbx.optimizer.locality.LocalityInfoUtils;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoUtil;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.tablegroup.AlterTableGroupSnapShotUtils;
import com.alibaba.polardbx.optimizer.tablegroup.TableGroupInfoManager;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.ddl.AlterTable;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlAlterTableSplitPartition;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlPartition;
import org.apache.calcite.sql.SqlPartitionValue;
import org.apache.calcite.sql.SqlSubPartition;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.Util;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import static com.alibaba.polardbx.optimizer.partition.PartitionInfoUtil.IGNORE_PARTNAME_LOCALITY;

public class LogicalAlterTableSplitPartition extends BaseDdlOperation {

    protected AlterTableGroupSplitPartitionPreparedData preparedData;

    public LogicalAlterTableSplitPartition(DDL ddl) {
        super(ddl, ((SqlAlterTable) (ddl.getSqlNode())).getObjectNames());
    }

    public LogicalAlterTableSplitPartition(DDL ddl, boolean notIncludeGsiName) {
        super(ddl);
        assert notIncludeGsiName;
    }

    @Override
    public boolean isSupportedByFileStorage() {
        return false;
    }

    @Override
    public boolean isSupportedByBindFileStorage() {
        throw new TddlRuntimeException(ErrorCode.ERR_UNARCHIVE_FIRST,
            "unarchive table " + schemaName + "." + tableName);
    }

    public void preparedData(ExecutionContext ec) {
        AlterTable alterTable = (AlterTable) relDdl;
        SqlAlterTable sqlAlterTable = (SqlAlterTable) alterTable.getSqlNode();
        assert sqlAlterTable.getAlters().size() == 1;

        assert sqlAlterTable.getAlters().get(0) instanceof SqlAlterTableSplitPartition;
        SqlAlterTableSplitPartition sqlAlterTableSplitPartition =
            (SqlAlterTableSplitPartition) sqlAlterTable.getAlters().get(0);

        int flag = PartitionInfoUtil.COMPARE_EXISTS_PART_LOCATION;
        boolean ignoreNameAndLocality = GeneralUtil.isEmpty(sqlAlterTableSplitPartition.getNewPartitions());
        boolean hasSubPartDef = false;
        if (!ignoreNameAndLocality) {
            hasSubPartDef =
                GeneralUtil.isNotEmpty(sqlAlterTableSplitPartition.getNewPartitions().get(0).getSubPartitions());
        }

        String logicalTableName = Util.last(((SqlIdentifier) alterTable.getTableName()).names);
        PartitionInfo curPartitionInfo =
            OptimizerContext.getContext(schemaName).getPartitionInfoManager().getPartitionInfo(logicalTableName);

        OptimizerContext oc =
            Objects.requireNonNull(OptimizerContext.getContext(schemaName), schemaName + " corrupted");
        TableGroupConfig tableGroupConfig =
            oc.getTableGroupInfoManager().getTableGroupConfigById(curPartitionInfo.getTableGroupId());
        String tableGroupName = tableGroupConfig.getTableGroupRecord().getTg_name();

        String partNamePrefix = StringUtils.EMPTY;
        if (sqlAlterTableSplitPartition.getNewPartitionPrefix() != null) {
            partNamePrefix =
                SQLUtils.normalizeNoTrim(sqlAlterTableSplitPartition.getNewPartitionPrefix().toString());
        }
        String splitPartitionName =
            Util.last(((SqlIdentifier) (sqlAlterTableSplitPartition.getSplitPartitionName())).names);
        List<String> splitPartitions = new ArrayList<>();
        splitPartitions.add(splitPartitionName);

        normalizeSqlSplitPartition(sqlAlterTableSplitPartition, tableGroupName, logicalTableName, splitPartitionName,
            ec);

        List<GroupDetailInfoExRecord> targetGroupDetailInfoExRecords =
            LocalityInfoUtils.getAllowedGroupInfoOfTableGroup(schemaName, tableGroupName);

        preparedData = new AlterTableSplitPartitionPreparedData();

        preparedData.setSchemaName(schemaName);
        preparedData.setTableName(logicalTableName);
        preparedData.setWithHint(targetTablesHintCache != null);
        preparedData.setSplitSubPartition(sqlAlterTableSplitPartition.isSubPartitionsSplit());
        preparedData.setOperateOnSubPartition(sqlAlterTableSplitPartition.isSubPartitionsSplit());
        preparedData.setUseTemplatePart(curPartitionInfo.getPartitionBy().getSubPartitionBy() != null ?
            curPartitionInfo.getPartitionBy().getSubPartitionBy().isUseSubPartTemplate() : false);

        preparedData.setTableGroupName(tableGroupName);
        preparedData.setSplitPartitions(splitPartitions);
        preparedData.setNewPartitions(sqlAlterTableSplitPartition.getNewPartitions());
        preparedData.setIncludeFullPartitionDefinition(sqlAlterTableSplitPartition.getAtValue() == null);
        preparedData.setTargetStorageInstIds(null);
        preparedData.setTargetGroupDetailInfoExRecords(targetGroupDetailInfoExRecords);
        preparedData.setPartBoundExprInfo(alterTable.getAllRexExprInfo());
        preparedData.setAtVal(sqlAlterTableSplitPartition.getAtValue());
        preparedData.setTaskType(ComplexTaskMetaManager.ComplexTaskType.SPLIT_PARTITION);
        preparedData.setSourceSql(((SqlAlterTable) alterTable.getSqlNode()).getSourceSql());

        if (preparedData.isUseTemplatePart() && preparedData.isSplitSubPartition()) {
            List<String> logicalParts = new ArrayList<>();
            for (PartitionSpec partitionSpec : curPartitionInfo.getPartitionBy().getPartitions()) {
                assert partitionSpec.isLogical();
                logicalParts.add(partitionSpec.getName());
            }
            preparedData.setLogicalParts(logicalParts);
        }

        preparedData.prepareInvisiblePartitionGroup();

        SqlConverter sqlConverter = SqlConverter.getInstance(curPartitionInfo.getTableSchema(), ec);
        PlannerContext plannerContext = PlannerContext.getPlannerContext(this.getCluster());
        Map<SqlNode, RexNode> partRexInfoCtx =
            sqlConverter.getRexInfoFromSqlAlterSpec(sqlAlterTable, ImmutableList.of(sqlAlterTableSplitPartition),
                plannerContext);
        preparedData.getPartBoundExprInfo().putAll(partRexInfoCtx);

        List<PartitionGroupRecord> newPartitionGroups = preparedData.getInvisiblePartitionGroups();
        Map<String, Pair<String, String>> mockOrderedTargetTableLocations = new TreeMap<>(String::compareToIgnoreCase);
        int newPartCount = newPartitionGroups.size();
        if (ignoreNameAndLocality) {
            for (int j = 0; j < newPartCount; j++) {
                Pair<String, String> pair = new Pair<>("", "");
                mockOrderedTargetTableLocations.put(newPartitionGroups.get(j).partition_name, pair);
            }
            flag |= PartitionInfoUtil.IGNORE_PARTNAME_LOCALITY;
        } else {
            for (int j = 0; j < newPartCount; j++) {
                String mockTableName = "";
                mockOrderedTargetTableLocations.put(newPartitionGroups.get(j).partition_name, new Pair<>(mockTableName,
                    GroupInfoUtil.buildGroupNameFromPhysicalDb(newPartitionGroups.get(j).partition_name)));
            }
        }

        boolean changeFlag =
            curPartitionInfo.getPartitionBy().getSubPartitionBy() != null && !preparedData.isUseTemplatePart()
                && !preparedData.isSplitSubPartition() && ((flag & IGNORE_PARTNAME_LOCALITY)
                != IGNORE_PARTNAME_LOCALITY) && !hasSubPartDef;

        if (changeFlag) {
            flag |= PartitionInfoUtil.IGNORE_PARTNAME_LOCALITY;
        }

        PartitionInfo newPartInfo = AlterTableGroupSnapShotUtils
            .getNewPartitionInfo(
                preparedData,
                curPartitionInfo,
                false,
                sqlAlterTableSplitPartition,
                preparedData.getOldPartitionNames(),
                preparedData.getNewPartitionNames(),
                preparedData.getTableGroupName(),
                splitPartitionName,
                preparedData.getInvisiblePartitionGroups(),
                mockOrderedTargetTableLocations,
                ec);

        preparedData.findCandidateTableGroupAndUpdatePrepareDate(tableGroupConfig, newPartInfo,
            sqlAlterTableSplitPartition.getNewPartitions(), partNamePrefix, flag, ec);
    }

    public void normalizeSqlSplitPartition(SqlAlterTableSplitPartition sqlAlterTableGroupSplitPartition,
                                           String tableGroupName,
                                           String logicalTableName,
                                           String splitPartitionName,
                                           ExecutionContext ec) {
        final TableGroupInfoManager tableGroupInfoManager =
            OptimizerContext.getContext(schemaName).getTableGroupInfoManager();

        TableGroupConfig tableGroupConfig = tableGroupInfoManager.getTableGroupConfigByName(tableGroupName);
        //for example, for key hash/key strategy partition split, alter table t1 split p1
        if (GeneralUtil.isEmpty(sqlAlterTableGroupSplitPartition.getNewPartitions())) {
            int newNameCount = 2;
            if (sqlAlterTableGroupSplitPartition.getNewPartitionNum() != null) {
                newNameCount =
                    ((SqlNumericLiteral) (sqlAlterTableGroupSplitPartition.getNewPartitionNum())).intValue(true);
            }
            List<String> newPartitionNames;
            if (sqlAlterTableGroupSplitPartition.getNewPartitionPrefix() != null) {
                String partNamePrefix =
                    SQLUtils.normalizeNoTrim(sqlAlterTableGroupSplitPartition.getNewPartitionPrefix().toString());
                newPartitionNames =
                    PartitionNameUtil.autoGeneratePartitionNamesWithUserDefPrefix(partNamePrefix, newNameCount);
            } else {
                newPartitionNames =
                    PartitionNameUtil.autoGeneratePartitionNames(tableGroupConfig, newNameCount,
                        new TreeSet<>(String::compareToIgnoreCase),
                        sqlAlterTableGroupSplitPartition.isSubPartitionsSplit());
            }

            assert newPartitionNames.size() == newNameCount;
            for (int i = 0; i < newNameCount; i++) {
                SqlIdentifier name = new SqlIdentifier(newPartitionNames.get(i), SqlParserPos.ZERO);
                SqlPartition sqlPartition = new SqlPartition(name, null, SqlParserPos.ZERO);
                sqlAlterTableGroupSplitPartition.getNewPartitions().add(sqlPartition);
            }

        }
        if (!sqlAlterTableGroupSplitPartition.isSubPartitionsSplit() && StringUtils.isNotEmpty(logicalTableName)) {
            TableMeta tableMeta = ec.getSchemaManager(schemaName).getTable(logicalTableName);
            PartitionInfo partitionInfo = tableMeta.getPartitionInfo();
            PartitionByDefinition subPartBy = partitionInfo.getPartitionBy().getSubPartitionBy();
            if (subPartBy == null) {
                return;
            }

            //split logical partition, will inherit the same subPartition definition as the source logical partition
            if (!subPartBy.isUseSubPartTemplate()) {
                for (PartitionSpec partitionSpec : partitionInfo.getPartitionBy().getPartitions()) {
                    if (partitionSpec.getName().equalsIgnoreCase(splitPartitionName)) {
                        Set<String> existsNames = new TreeSet<>(String::compareToIgnoreCase);
                        int hasSubPartDefinition = 0;
                        for (SqlPartition sqlPartition : sqlAlterTableGroupSplitPartition.getNewPartitions()) {
                            for (SqlNode subPart : GeneralUtil.emptyIfNull(sqlPartition.getSubPartitions())) {
                                existsNames.add(((SqlIdentifier) ((SqlSubPartition) subPart).getName()).getSimple());
                            }
                            if (GeneralUtil.isNotEmpty(sqlPartition.getSubPartitions())) {
                                hasSubPartDefinition++;
                            }
                        }
                        if (hasSubPartDefinition < sqlAlterTableGroupSplitPartition.getNewPartitions().size()) {
                            int subPartitionSize = partitionSpec.getSubPartitions().size();
                            List<String> newSubPartitionNames =
                                PartitionNameUtil.autoGeneratePartitionNames(tableGroupConfig,
                                    (sqlAlterTableGroupSplitPartition.getNewPartitions().size() - hasSubPartDefinition)
                                        * subPartitionSize, existsNames, true);
                            int j = 0;
                            for (SqlPartition sqlPartition : sqlAlterTableGroupSplitPartition.getNewPartitions()) {
                                if (GeneralUtil.isEmpty(sqlPartition.getSubPartitions())) {
                                    for (int i = 0; i < subPartitionSize; i++) {
                                        PartitionSpec subPartitionSpec = partitionSpec.getSubPartitions().get(i);
                                        SqlSubPartition sqlSubPartition = new SqlSubPartition(SqlParserPos.ZERO,
                                            new SqlIdentifier(newSubPartitionNames.get(j), SqlParserPos.ZERO),
                                            (SqlPartitionValue) subPartitionSpec.getBoundSpec().getBoundRawValue()
                                                .clone(SqlParserPos.ZERO));
                                        sqlPartition.getSubPartitions().add(sqlSubPartition);
                                        j++;
                                    }
                                }
                            }
                        }
                        break;
                    }
                }
            } else {
                PartitionSpec partitionSpec = partitionInfo.getPartitionBy().getPartitions().get(0);
                for (SqlPartition sqlPartition : sqlAlterTableGroupSplitPartition.getNewPartitions()) {
                    String logicalPartName = ((SqlIdentifier) (sqlPartition.getName())).getSimple();
                    for (PartitionSpec subPartitionSpec : partitionSpec.getSubPartitions()) {
                        SqlSubPartition sqlSubPartition = new SqlSubPartition(SqlParserPos.ZERO,
                            new SqlIdentifier(PartitionNameUtil.autoBuildSubPartitionName(logicalPartName,
                                subPartitionSpec.getTemplateName()), SqlParserPos.ZERO),
                            (SqlPartitionValue) subPartitionSpec.getBoundSpec().getBoundRawValue()
                                .clone(SqlParserPos.ZERO));
                        sqlPartition.getSubPartitions().add(sqlSubPartition);
                    }
                }
            }
        }

    }

    public AlterTableGroupSplitPartitionPreparedData getPreparedData() {
        return preparedData;
    }

    public static LogicalAlterTableSplitPartition create(DDL ddl) {
        return new LogicalAlterTableSplitPartition(ddl);
    }

}
