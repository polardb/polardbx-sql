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

package com.alibaba.polardbx.executor.partitionmanagement;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.executor.ExecutorHelper;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.ddl.job.validator.JoinGroupValidator;
import com.alibaba.polardbx.gms.partition.TablePartRecordInfoContext;
import com.alibaba.polardbx.gms.partition.TablePartitionRecord;
import com.alibaba.polardbx.gms.tablegroup.JoinGroupInfoRecord;
import com.alibaba.polardbx.gms.tablegroup.JoinGroupUtils;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.tablegroup.TableGroupRecord;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import com.alibaba.polardbx.gms.util.GroupInfoUtil;
import com.alibaba.polardbx.gms.util.InstIdUtil;
import com.alibaba.polardbx.gms.util.PartitionNameUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.ScaleOutPlanUtil;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.CharType;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.rel.dal.PhyShow;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableAddPartition;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableDropPartition;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableExtractPartition;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableModifyPartition;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupAddTablePreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.MergeTableGroupPreparedData;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.partition.PartitionByDefinition;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoBuilder;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoManager;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoUtil;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.partition.boundspec.MultiValuePartitionBoundSpec;
import com.alibaba.polardbx.optimizer.partition.boundspec.PartitionBoundSpec;
import com.alibaba.polardbx.optimizer.partition.boundspec.PartitionBoundVal;
import com.alibaba.polardbx.optimizer.partition.boundspec.RangeBoundSpec;
import com.alibaba.polardbx.optimizer.partition.common.BuildPartSpecFromAstParams;
import com.alibaba.polardbx.optimizer.partition.common.PartKeyLevel;
import com.alibaba.polardbx.optimizer.partition.common.PartitionStrategy;
import com.alibaba.polardbx.optimizer.partition.datatype.PartitionField;
import com.alibaba.polardbx.optimizer.partition.datatype.PartitionFieldBuilder;
import com.alibaba.polardbx.optimizer.partition.datatype.function.PartitionIntFunction;
import com.alibaba.polardbx.optimizer.partition.pruning.PartFieldAccessType;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPrunerUtils;
import com.alibaba.polardbx.optimizer.partition.pruning.SearchDatumComparator;
import com.alibaba.polardbx.optimizer.partition.pruning.SearchDatumInfo;
import com.alibaba.polardbx.optimizer.tablegroup.TableGroupInfoManager;
import com.alibaba.polardbx.optimizer.utils.KeyWordsUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.ddl.AlterTable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAlterSpecification;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlAlterTableAddPartition;
import org.apache.calcite.sql.SqlAlterTableDropPartition;
import org.apache.calcite.sql.SqlAlterTableExtractPartition;
import org.apache.calcite.sql.SqlAlterTableGroup;
import org.apache.calcite.sql.SqlAlterTableGroupExtractPartition;
import org.apache.calcite.sql.SqlAlterTableGroupMergePartition;
import org.apache.calcite.sql.SqlAlterTableGroupMovePartition;
import org.apache.calcite.sql.SqlAlterTableGroupRenamePartition;
import org.apache.calcite.sql.SqlAlterTableGroupSetLocality;
import org.apache.calcite.sql.SqlAlterTableGroupSetPartitionsLocality;
import org.apache.calcite.sql.SqlAlterTableGroupSplitPartition;
import org.apache.calcite.sql.SqlAlterTableGroupSplitPartitionByHotValue;
import org.apache.calcite.sql.SqlAlterTableMergePartition;
import org.apache.calcite.sql.SqlAlterTableModifyPartitionValues;
import org.apache.calcite.sql.SqlAlterTableMovePartition;
import org.apache.calcite.sql.SqlAlterTableRenamePartition;
import org.apache.calcite.sql.SqlAlterTableReorgPartition;
import org.apache.calcite.sql.SqlAlterTableSplitPartition;
import org.apache.calcite.sql.SqlAlterTableSplitPartitionByHotValue;
import org.apache.calcite.sql.SqlAlterTableTruncatePartition;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlPartition;
import org.apache.calcite.sql.SqlPartitionValue;
import org.apache.calcite.sql.SqlPartitionValueItem;
import org.apache.calcite.sql.SqlShowCreateTable;
import org.apache.calcite.sql.SqlSubPartition;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.Util;
import org.apache.commons.lang3.StringUtils;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.gms.partition.TablePartitionRecord.PARTITION_LEVEL_PARTITION;
import static com.alibaba.polardbx.gms.partition.TablePartitionRecord.PARTITION_LEVEL_SUBPARTITION;

/**
 * Created by luoyanxin
 *
 * @author luoyanxin
 */
public class AlterTableGroupUtils {
    public static void alterTableGroupPreCheck(SqlAlterTableGroup sqlAlterTableGroup, String schemaName,
                                               ExecutionContext executionContext) {
        List<SqlAlterSpecification> alterSpecifications = sqlAlterTableGroup.getAlters();
        SqlIdentifier original = (SqlIdentifier) sqlAlterTableGroup.getTableGroupName();
        String tableGroupName = Util.last(original.names);
        if (GeneralUtil.isNotEmpty(alterSpecifications)) {
            assert alterSpecifications.size() == 1;

            final TableGroupInfoManager tableGroupInfoManager =
                OptimizerContext.getContext(schemaName).getTableGroupInfoManager();

            TableGroupConfig tableGroupConfig = tableGroupInfoManager.getTableGroupConfigByName(tableGroupName);
            if (tableGroupConfig == null) {
                throw new TddlRuntimeException(ErrorCode.ERR_TABLE_GROUP_NOT_EXISTS,
                    "tablegroup:" + tableGroupName + " doesn't exists");
            }
            if (tableGroupConfig.getTableCount() == 0) {
                throw new TddlRuntimeException(ErrorCode.ERR_TABLE_GROUP_IS_EMPTY,
                    "can't modify the empty tablegroup:" + tableGroupName);
            }
            if (alterSpecifications.get(0) instanceof SqlAlterTableGroupSplitPartition) {
                final SqlAlterTableGroupSplitPartition sqlAlterTableGroupSplitPartition =
                    (SqlAlterTableGroupSplitPartition) alterSpecifications.get(0);
                sqlAlterTableGroupSplitPartition.setParent(sqlAlterTableGroup);
                alterTableGroupSplitPartitionCheck(sqlAlterTableGroupSplitPartition, tableGroupConfig,
                    sqlAlterTableGroupSplitPartition.getParent().getPartRexInfoCtxByLevel()
                        .get(PARTITION_LEVEL_PARTITION), true, true, executionContext, schemaName);
            } else if (alterSpecifications.get(0) instanceof SqlAlterTableGroupMergePartition) {
                final SqlAlterTableGroupMergePartition sqlAlterTableGroupMergePartition =
                    (SqlAlterTableGroupMergePartition) alterSpecifications.get(0);
                sqlAlterTableGroupMergePartition.setParent(sqlAlterTableGroup);
                String targetPartitionName =
                    Util.last(((SqlIdentifier) (sqlAlterTableGroupMergePartition.getTargetPartitionName())).names);
                Set<String> partitionsToBeMerged = new TreeSet(String.CASE_INSENSITIVE_ORDER);
                partitionsToBeMerged.addAll(sqlAlterTableGroupMergePartition.getOldPartitions().stream()
                    .map(o -> Util.last(((SqlIdentifier) (o)).names).toLowerCase()).collect(Collectors.toSet()));

                alterTableGroupMergePartitionCheck(sqlAlterTableGroupMergePartition, tableGroupConfig,
                    targetPartitionName, partitionsToBeMerged, executionContext);
            } else if (alterSpecifications.get(0) instanceof SqlAlterTableReorgPartition) {
                SqlAlterTableReorgPartition sqlAlterTableReorgPartition =
                    (SqlAlterTableReorgPartition) alterSpecifications.get(0);
                sqlAlterTableReorgPartition.setParent(sqlAlterTableGroup);
                alterTableGroupReorgPartitionCheck(sqlAlterTableReorgPartition, tableGroupConfig,
                    sqlAlterTableGroup.getPartRexInfoCtxByLevel().get(PARTITION_LEVEL_PARTITION), true,
                    executionContext);

            } else if (alterSpecifications.get(0) instanceof SqlAlterTableGroupMovePartition) {
                alterTableGroupMovePartitionCheck((SqlAlterTableGroupMovePartition) alterSpecifications.get(0),
                    tableGroupConfig, schemaName);

            } else if (alterSpecifications.get(0) instanceof SqlAlterTableGroupExtractPartition) {
                final SqlAlterTableGroupExtractPartition sqlAlterTableGroupExtractPartition =
                    (SqlAlterTableGroupExtractPartition) alterSpecifications.get(0);
                sqlAlterTableGroupExtractPartition.setParent(sqlAlterTableGroup);
                alterTableGroupExtractPartitionCheck(sqlAlterTableGroup, tableGroupConfig, executionContext);

            } else if (alterSpecifications.get(0) instanceof SqlAlterTableAddPartition) {
                alterTableGroupAddPartitionCheck(tableGroupConfig, executionContext.getSchemaName(),
                    (SqlAlterTableAddPartition) alterSpecifications.get(0));

            } else if (alterSpecifications.get(0) instanceof SqlAlterTableDropPartition) {
                alterTableGroupDropPartitionCheck((SqlAlterTableDropPartition) alterSpecifications.get(0),
                    tableGroupConfig, executionContext);

            } else if (alterSpecifications.get(0) instanceof SqlAlterTableTruncatePartition) {
                alterTableGroupTruncatePartitionCheck((SqlAlterTableTruncatePartition) alterSpecifications.get(0),
                    tableGroupConfig, executionContext);

            } else if (alterSpecifications.get(0) instanceof SqlAlterTableGroupSetPartitionsLocality) {

            } else if (alterSpecifications.get(0) instanceof SqlAlterTableModifyPartitionValues) {

                final SqlAlterTableModifyPartitionValues sqlModifyListPartitionValues =
                    (SqlAlterTableModifyPartitionValues) alterSpecifications.get(0);
                alterTableModifyListPartitionValuesCheck(sqlAlterTableGroup, tableGroupConfig,
                    sqlModifyListPartitionValues, executionContext);

            } else if (alterSpecifications.get(0) instanceof SqlAlterTableGroupRenamePartition) {
                final SqlAlterTableGroupRenamePartition sqlAlterTableGroupRenamePartition =
                    (SqlAlterTableGroupRenamePartition) alterSpecifications.get(0);
                sqlAlterTableGroupRenamePartition.setParent(sqlAlterTableGroup);
                alterTableGroupRenamePartitionCheck(sqlAlterTableGroupRenamePartition, tableGroupConfig,
                    executionContext);
            } else if (alterSpecifications.get(0) instanceof SqlAlterTableGroupSplitPartitionByHotValue) {
                final SqlAlterTableGroupSplitPartitionByHotValue sqlAlterTableGroupSplitPartitionByHotValue =
                    (SqlAlterTableGroupSplitPartitionByHotValue) alterSpecifications.get(0);
                sqlAlterTableGroupSplitPartitionByHotValue.setParent(sqlAlterTableGroup);
                alterTableGroupSplitPartitionByHotValueCheck(sqlAlterTableGroup, tableGroupConfig, executionContext);
            } else if (alterSpecifications.get(0) instanceof SqlAlterTableGroupSetLocality) {

            } else {
                throw new UnsupportedOperationException(alterSpecifications.get(0).getClass().toString());
            }

        }
    }

    public static void alterTableGroupSplitPartitionCheck(SqlAlterTableSplitPartition sqlAlterTableSplitPartition,
                                                          TableGroupConfig tableGroupConfig,
                                                          Map<SqlNode, RexNode> partRexInfoCtx,
                                                          boolean checkPartitionColumn,
                                                          boolean isAlterTableGroup,
                                                          ExecutionContext executionContext, String schemaName) {

        boolean alterTableOnly = !(sqlAlterTableSplitPartition instanceof SqlAlterTableGroupSplitPartition);
        String tgSchema = tableGroupConfig.getTableGroupRecord().getSchema();
        final SchemaManager schemaManager = OptimizerContext.getContext(tgSchema).getLatestSchemaManager();
        String tableInCurrentGroup = tableGroupConfig.getAllTables().get(0).getLogTbRec().tableName;
        TableMeta tableMeta = schemaManager.getTable(tableInCurrentGroup);
        TableGroupRecord tableGroupRecord = tableGroupConfig.getTableGroupRecord();
        String tgName = tableGroupRecord.getTg_name();

        PartitionInfo partitionInfo = tableMeta.getPartitionInfo();

        if (partitionInfo.isSingleTable() || partitionInfo.isBroadcastTable()) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                "can't split the partition group for single/broadcast tables");
        }

        String mayTempPartitionName =
            Util.last(((SqlIdentifier) (sqlAlterTableSplitPartition.getSplitPartitionName())).names);
        PartitionGroupRecord splitPartRecord = tableGroupConfig.getPartitionGroupRecords().stream()
            .filter(o -> o.partition_name.equalsIgnoreCase(mayTempPartitionName)).findFirst().orElse(null);

        if (partitionInfo.getSpTemplateFlag() == TablePartitionRecord.SUBPARTITION_TEMPLATE_USING
            && sqlAlterTableSplitPartition.isSubPartitionsSplit()) {
            if (splitPartRecord != null) {
                throw new TddlRuntimeException(ErrorCode.ERR_TEMPLATE_SUBPARTITION_PARTITION_NOT_EXIST,
                    "the template subpartition:" + mayTempPartitionName + " is not exists");
            }
            for (PartitionSpec partitionSpec : partitionInfo.getPartitionBy().getPhysicalPartitions()) {
                if (partitionSpec.getTemplateName().equalsIgnoreCase(mayTempPartitionName)) {
                    splitPartRecord = tableGroupConfig.getPartitionGroupRecords().stream()
                        .filter(o -> o.partition_name.equalsIgnoreCase(partitionSpec.getName())).findFirst()
                        .orElse(null);
                    //select one partition to check is enough
                    break;
                }
            }
            if (splitPartRecord == null) {
                throw new TddlRuntimeException(ErrorCode.ERR_TEMPLATE_SUBPARTITION_PARTITION_NOT_EXIST,
                    "the template subpartition:" + mayTempPartitionName + " is not exists");
            }
        }
        String splitPartitionName = splitPartRecord == null ? mayTempPartitionName : splitPartRecord.partition_name;

        PartitionSpec splitPartitionSpec = null;
        PartitionSpec previousPartitionSpec = null;
        boolean isSplitLogicaPart = false;
        PartitionSpec parentPartitionSpec = null;

        PartitionStrategy strategy = partitionInfo.getPartitionBy().getStrategy();

        if (splitPartRecord == null) {
            for (PartitionSpec partitionSpec : partitionInfo.getPartitionBy().getPartitions()) {
                if (!partitionSpec.isLogical()) {
                    break;
                } else if (partitionSpec.getName().equalsIgnoreCase(splitPartitionName)) {
                    isSplitLogicaPart = true;
                    splitPartitionSpec = partitionSpec;
                    break;
                } else {
                    previousPartitionSpec = partitionSpec;
                }
            }
            if (!isSplitLogicaPart) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_NAME_NOT_EXISTS,
                    "the partition:" + splitPartitionName + " is not exists");
            }
        } else {
            List<PartitionSpec> partitionSpecs = partitionInfo.getPartitionBy().getOrderedPartitionSpecs();

            for (PartitionSpec spec : partitionSpecs) {
                if (spec.isLogical()) {
                    previousPartitionSpec = null;
                    if (sqlAlterTableSplitPartition.isSubPartitionsSplit()) {
                        for (PartitionSpec subSpec : spec.getSubPartitions()) {
                            if (subSpec.getLocation().getPartitionGroupId().longValue()
                                == splitPartRecord.id.longValue()) {
                                splitPartitionSpec = subSpec;
                                parentPartitionSpec = spec;
                                strategy = subSpec.getStrategy();
                                break;
                            } else {
                                previousPartitionSpec = subSpec;
                            }
                        }
                        if (splitPartitionSpec != null) {
                            break;
                        }
                    }
                } else {
                    if (spec.getLocation().getPartitionGroupId().longValue() == splitPartRecord.id.longValue()) {
                        splitPartitionSpec = spec;
                        break;
                    } else {
                        previousPartitionSpec = spec;
                    }
                }
            }
        }

        PartitionField atVal =
            calculateSplitAtVal(tableGroupConfig, partitionInfo, sqlAlterTableSplitPartition, strategy);

        if (splitPartitionSpec == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_NAME_NOT_EXISTS,
                String.format("the %spartition:[%s] is not exists this current table group",
                    sqlAlterTableSplitPartition.isSubPartitionsSplit() ? "" : "logical ", splitPartitionName));
        }
        if (strategy == PartitionStrategy.RANGE && atVal != null) {
            RangeBoundSpec rangeBoundSpec = ((RangeBoundSpec) splitPartitionSpec.getBoundSpec());
            PartitionField maxVal = rangeBoundSpec.getBoundValue().getValue();

            if (!rangeBoundSpec.getBoundValue().isMaxValue() && atVal.compareTo(maxVal) >= 0) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                    "the atValue should less than " + maxVal.longValue());
            }
            if (previousPartitionSpec != null) {
                PartitionField minVal =
                    ((RangeBoundSpec) previousPartitionSpec.getBoundSpec()).getBoundValue().getValue();
                if (atVal != null && atVal.compareTo(minVal) != 1) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                        "the atValue should great than " + minVal.longValue());
                }
            }
        } else if (strategy != PartitionStrategy.HASH) {
            if (atVal != null) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                    "not support split with at for:" + strategy.toString());
            }
            List<ColumnMeta> partColMetaList;
            SearchDatumComparator comparator;
            PartitionIntFunction partIntFunc;
            List<Integer> allLevelFullPartColCnts;
            List<Integer> allLevelActPartColCnts;
            if (sqlAlterTableSplitPartition.isSubPartitionsSplit()) {
                partColMetaList = partitionInfo.getPartitionBy().getSubPartitionBy().getPartitionFieldList();
                comparator = partitionInfo.getPartitionBy().getSubPartitionBy().getPruningSpaceComparator();
                partIntFunc = partitionInfo.getPartitionBy().getSubPartitionBy().getPartIntFunc();
            } else {
                partColMetaList = partitionInfo.getPartitionBy().getPartitionFieldList();
                comparator = partitionInfo.getPartitionBy().getPruningSpaceComparator();
                partIntFunc = partitionInfo.getPartitionBy().getPartIntFunc();
            }

            allLevelFullPartColCnts = partitionInfo.getAllLevelFullPartColCounts();
            allLevelActPartColCnts = partitionInfo.getAllLevelActualPartColCounts();

            int pos = 0;
            List<SqlPartition> newPartitions = sqlAlterTableSplitPartition.getNewPartitions();
            List<PartitionSpec> newPartitionSpecs = new ArrayList<>();
            List<Integer> allLevelNewPrefixPartColCnts = new ArrayList<>();

//            int actualPartColCnt =
//                PartitionInfoUtil.getAllLevelActualPartColumnMetasInfoForTableGroup(schemaName, tgName).get(0).size();
//            int newPrefixPartColCnt =
//                PartitionInfoUtil.getNewPrefixPartColCntBySqlPartitionAst(Integer.MAX_VALUE/*ignore*/, actualPartColCnt,
//                    strategy, newPartitions);
//            GetNewActPartColCntFromAstParams params = new GetNewActPartColCntFromAstParams();

            List<SqlPartition> newPartitionsAst = sqlAlterTableSplitPartition.getNewPartitions();
            checkPartitionsDef(newPartitionsAst);

//            List<PartitionStrategy> allLevelPartStrategies = partitionInfo.getAllLevelPartitionStrategies();
//            params.setAllLevelFullPartColCnts(allLevelFullPartColCnts);
//            params.setAllLevelActualPartColCnts(allLevelActPartColCnts);
//            params.setAllLevelStrategies(allLevelPartStrategies);
//            params.setNewPartitions(newPartitionsAst);
//            params.setChangeSubpartition(sqlAlterTableSplitPartition.isSubPartitionsSplit());
//            allLevelNewPrefixPartColCnts = PartitionInfoUtil.getNewAllLevelPrefixPartColCntBySqlPartitionAst(params);

            allLevelNewPrefixPartColCnts =
                PartitionInfoUtil.getNewAllLevelPrefixPartColCntByPartInfoAndNewParts(isAlterTableGroup, partitionInfo,
                    newPartitionsAst, sqlAlterTableSplitPartition.isSubPartitionsSplit());

            AtomicInteger phyPartCounter = new AtomicInteger(pos);

            PartitionByDefinition partByDef = partitionInfo.getPartitionBy();
            PartitionByDefinition subPartByDef = partByDef.getSubPartitionBy();
            if (checkPartitionColumn) {
                if (strategy == PartitionStrategy.RANGE_COLUMNS || strategy == PartitionStrategy.KEY) {
                    /**
                     * Check if is allowed to use newPrefixPartColCnt as new partColCnt for all tables in table group
                     */
                    if (!newPartitions.isEmpty() && !alterTableOnly) {
                        alterTableGroupCheckPartitionColumn(schemaName, tgName, allLevelNewPrefixPartColCnts);
                    }
                }
            }

            for (SqlPartition sqlPartition : sqlAlterTableSplitPartition.getNewPartitions()) {
                if (sqlPartition.getValues() == null && isSplitLogicaPart) {
                    continue;
                }
                if (sqlPartition.getValues() == null) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                        "the new partition value is not specified");
                }
                BuildPartSpecFromAstParams partSpecAstParams = new BuildPartSpecFromAstParams();
                partSpecAstParams.setContext(executionContext);
                partSpecAstParams.setPartColMetaList(partColMetaList);
                partSpecAstParams.setPartKeyLevel(
                    sqlAlterTableSplitPartition.isSubPartitionsSplit() ? PartKeyLevel.SUBPARTITION_KEY :
                        PartKeyLevel.PARTITION_KEY);
                partSpecAstParams.setPartIntFunc(partIntFunc);
                partSpecAstParams.setPruningComparator(comparator);
                partSpecAstParams.setPartBoundExprInfo(partRexInfoCtx);
                partSpecAstParams.setPartBoundValBuilder(null);
                partSpecAstParams.setStrategy(strategy);
                partSpecAstParams.setPartPosition(pos++);
                partSpecAstParams.setAllLevelPrefixPartColCnts(allLevelNewPrefixPartColCnts);
                partSpecAstParams.setPartNameAst(sqlPartition.getName());
                partSpecAstParams.setPartComment(sqlPartition.getComment());
                partSpecAstParams.setPartLocality(sqlPartition.getLocality());
                partSpecAstParams.setPartBndValuesAst(sqlPartition.getValues());

                partSpecAstParams.setLogical(false);
                partSpecAstParams.setSpecTemplate(false);
                partSpecAstParams.setSubPartSpec(false);
                partSpecAstParams.setParentPartSpec(null);
                partSpecAstParams.setPhySpecCounter(phyPartCounter);
                PartitionSpec newSpec = PartitionInfoBuilder.buildPartSpecByAstParams(partSpecAstParams);

//                PartitionSpec newSpec = PartitionInfoBuilder
//                    .buildPartitionSpecByPartSpecAst(
//                        executionContext,
//                        partColMetaList,
//                        partIntFunc,
//                        comparator,
//                        sqlAlterTableGroupSplitPartition.getParent().getPartRexInfoCtx(),
//                        null,
//                        sqlPartition,
//                        partitionInfo.getPartitionBy().getStrategy(),
//                        pos++,
//                        newPrefixPartColCnt);

                newPartitionSpecs.add(newSpec);
            }
            if (strategy == PartitionStrategy.RANGE || strategy == PartitionStrategy.RANGE_COLUMNS) {

                SearchDatumInfo maxVal = splitPartitionSpec.getBoundSpec().getSingleDatum();
                SearchDatumInfo minVal = SearchDatumInfo.createMinValDatumInfo(
                    partitionInfo.getPartitionBy().getPartitionFieldList().size());

                if (previousPartitionSpec != null) {
                    minVal = previousPartitionSpec.getBoundSpec().getSingleDatum();
                    SearchDatumInfo firstPartitionVal = newPartitionSpecs.get(0).getBoundSpec().getSingleDatum();
                    if (splitPartitionSpec.getBoundSpaceComparator().compare(minVal, firstPartitionVal) != -1) {
                        throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                            "the first partition should great than " + minVal.toString());
                    }
                }
                int i = 0;
                for (PartitionSpec partitionSpec : newPartitionSpecs) {
                    SearchDatumInfo curPartitionVal = partitionSpec.getBoundSpec().getSingleDatum();
                    if (previousPartitionSpec != null && i == 0) {
                        minVal = previousPartitionSpec.getBoundSpec().getSingleDatum();
                    }
                    i++;
                    if (partitionSpec.getBoundSpaceComparator().compare(minVal, curPartitionVal) != -1) {
                        throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                            String.format("the partition bound of %s is (%s), should greater than %s",
                                partitionSpec.getName(), curPartitionVal, minVal));
                    }
                    if (partitionSpec.getBoundSpaceComparator().compare(maxVal, curPartitionVal) == -1) {
                        throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                            String.format("the partition bound of %s is %s, should not greater than %s",
                                partitionSpec.getName(), curPartitionVal, maxVal));
                    }
                    if (i == newPartitionSpecs.size()
                        && partitionSpec.getBoundSpaceComparator().compare(maxVal, curPartitionVal) != 0) {
                        throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                            String.format("the partition bound of %s is %s, should equal to %s",
                                partitionSpec.getName(), curPartitionVal, maxVal));
                    }
                    minVal = curPartitionVal;
                }
            } else if (strategy == PartitionStrategy.LIST || strategy == PartitionStrategy.LIST_COLUMNS) {
                Set<String> newPartsValSet = new HashSet<>();
                Set<String> oldPartsValSet = new HashSet<>();

                for (PartitionSpec partitionSpec : newPartitionSpecs) {
                    Set<String> newPartValSet = new HashSet<>();

                    for (SearchDatumInfo datum : partitionSpec.getBoundSpec().getMultiDatums()) {
                        if (newPartValSet.contains(datum.toString())) {
                            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                                "duplicate values:" + datum.toString());
                        }
                        newPartValSet.add(datum.toString());
                        if (newPartsValSet.contains(datum.toString())) {
                            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                                "duplicate values:" + datum.toString());
                        } else {
                            newPartsValSet.add(datum.toString());
                        }
                    }
                }

                /**
                 * check if the old partition is the summation of the new partitions.
                 * e.g.
                 * 1. if the split partition is non-default partition, we check whether p0 == p00 + p01 + p02 ...
                 * 2. if the split partition is default partition, we check whether default_part == default_part + p_new0 + p_new1 ...
                 *      and p_new shouldn't be the existing partition
                 * */
                if (!splitPartitionSpec.isDefaultPartition()) {
                    MultiValuePartitionBoundSpec newListBoundSpec =
                        (MultiValuePartitionBoundSpec) splitPartitionSpec.getBoundSpec();
                    for (SearchDatumInfo oldDatum : newListBoundSpec.getMultiDatums()) {
                        oldPartsValSet.add(oldDatum.toString());
                        if (!newPartsValSet.contains(oldDatum.toString())) {
                            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                                "missing " + oldDatum.toString() + " in the new Partition spec");
                        }
                    }
                    for (String val : newPartsValSet) {
                        if (!oldPartsValSet.contains(val)) {
                            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                                "orphan " + val.toString() + " in the new Partition spec");
                        }
                    }
                } else {
                    SearchDatumInfo defaultDatum = splitPartitionSpec.getBoundSpec().getMultiDatums().get(0);
                    if (!newPartsValSet.contains(defaultDatum.toString())) {
                        throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                            "missing " + defaultDatum.toString() + " in the new Partition spec");
                    }

                    Set<String> allPartsVal = new HashSet<>();
                    List<PartitionSpec> brotherParts;

                    if (sqlAlterTableSplitPartition.isSubPartitionsSplit()) {
                        brotherParts = parentPartitionSpec.getSubPartitions();
                    } else {
                        brotherParts = partitionInfo.getPartitionBy().getOrderedPartitionSpecs();
                    }
                    for (PartitionSpec spec : brotherParts) {
                        MultiValuePartitionBoundSpec listBoundSpec = (MultiValuePartitionBoundSpec) spec.getBoundSpec();
                        for (SearchDatumInfo datumInfo : listBoundSpec.getMultiDatums()) {
                            if (datumInfo.containDefaultValue()) {
                                continue;
                            }
                            allPartsVal.add(datumInfo.toString());
                        }
                    }

                    for (String val : newPartsValSet) {
                        if (allPartsVal.contains(val)) {
                            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                                val + " has already been in other partition");
                        }
                    }
                }

                /**
                 * if we split a default partition, we need check new partition's order
                 * in other words, the new default partition must be the last partition.
                 * e.g.
                 * split default_partition into p00, default_partition, p11 is not allowed
                 * */
                int newDefaultPartitionPosition = -1;
                if (splitPartitionSpec.isDefaultPartition()) {
                    for (int index = 0; index < newPartitionSpecs.size(); index++) {
                        if (newPartitionSpecs.get(index).isDefaultPartition()) {
                            newDefaultPartitionPosition = index;
                            break;
                        }
                    }
                    if (newDefaultPartitionPosition != newPartitionSpecs.size() - 1) {
                        throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                            "default partition must be the last partition");
                    }
                }
            }
        } else {
            //hash partitions
            if (atVal != null) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                    "not support split with at for:" + strategy.toString());
            }
        }
    }

    // all the partitions in the same level should have partitionVal
    // or haven't partitionVal, can't some has but some hasn't
    private static void checkPartitionsDef(List<SqlPartition> newPartitionsAst) {
        boolean hasPartitionVal = false;
        int i = 0;
        for (SqlPartition sqlPartition : GeneralUtil.emptyIfNull(newPartitionsAst)) {
            if (i == 0) {
                hasPartitionVal = sqlPartition.getValues() != null;
            } else if ((sqlPartition.getValues() != null) != hasPartitionVal) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                    "all the partitions in the same level should (have/or haven't) partitionVal");
            }
            int j = 0;
            boolean hasSubPartitionVal = false;
            for (SqlNode sqlNode : GeneralUtil.emptyIfNull(sqlPartition.getSubPartitions())) {
                SqlSubPartition sqlSubPartition = (SqlSubPartition) sqlNode;
                if (j == 0) {
                    hasSubPartitionVal = sqlSubPartition.getValues() != null;
                } else if ((sqlSubPartition.getValues() != null) != hasSubPartitionVal) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                        "all the partitions in the same level should (have/or haven't) partitionVal");
                }
                j++;
            }
            i++;
        }
    }

    /**
     * Calculate split-at value, if not specified
     */
    private static PartitionField calculateSplitAtVal(TableGroupConfig tableGroupConfig, PartitionInfo partitionInfo,
                                                      SqlAlterTableSplitPartition sqlAlter,
                                                      PartitionStrategy strategy) {
        PartitionField atVal = null;
        if (sqlAlter.getAtValue() != null) {
            if (strategy != PartitionStrategy.RANGE) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                    "Only support range partition to split with at value");
            }
            RelDataType type;
            if (sqlAlter.isSubPartitionsSplit()) {
                type = partitionInfo.getPartitionBy().getSubPartitionBy().getPartitionExprTypeList().get(0);
            } else {
                type = partitionInfo.getPartitionBy().getPartitionExprTypeList().get(0);
            }
            DataType atValDataType = DataTypeUtil.calciteToDrdsType(type);
            atVal = PartitionFieldBuilder.createField(atValDataType);
            SqlLiteral constLiteral = ((SqlLiteral) sqlAlter.getAtValue());
            String constStr = constLiteral.getValueAs(String.class);
            atVal.store(constStr, new CharType());
        } else if (strategy != PartitionStrategy.HASH && strategy != PartitionStrategy.KEY && GeneralUtil.isEmpty(
            sqlAlter.getNewPartitions())) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT, "Missing the new partitions spec");
        }
        if (GeneralUtil.isNotEmpty(sqlAlter.getNewPartitions())) {
            assert sqlAlter.getNewPartitions().size() >= 2;
            for (SqlPartition sqlPartition : sqlAlter.getNewPartitions()) {
                String partitionName = Util.last(((SqlIdentifier) (sqlPartition.getName())).names);
                PartitionGroupRecord partitionGroupRecord = tableGroupConfig.getPartitionGroupRecords().stream()
                    .filter(o -> o.partition_name.equalsIgnoreCase(partitionName)).findFirst().orElse(null);
                //allowed: split p1 into (p1, p10)
                /**
                 * For split partition: new partition name can be same as the splited one.
                 * eg. "split p1 into(p1, p10)" is valid
                 * */
                if (partitionGroupRecord != null && !partitionName.equals(
                    sqlAlter.getSplitPartitionName().toString())) {
                    throw new TddlRuntimeException(ErrorCode.ERR_DUPLICATED_PARTITION_NAME,
                        "partition name:" + partitionName + " is exists");
                }
            }
        }
        return atVal;
    }

    public static void alterTableGroupMergePartitionCheck(SqlAlterTableMergePartition sqlAlterTableMergePartition,
                                                          TableGroupConfig tableGroupConfig, String targetPartitionName,
                                                          Set<String> partitionsToBeMerged,
                                                          ExecutionContext executionContext) {
        String schemaName = tableGroupConfig.getTableGroupRecord().getSchema();
        final SchemaManager schemaManager = executionContext.getSchemaManager(schemaName);

        String tableInCurrentGroup = tableGroupConfig.getAllTables().get(0).getLogTbRec().tableName;
        TableMeta tableMeta = schemaManager.getTable(tableInCurrentGroup);
        PartitionInfo partitionInfo = tableMeta.getPartitionInfo();

        PartitionGroupRecord partRecord = tableGroupConfig.getPartitionGroupRecords().stream()
            .filter(o -> o.partition_name.equalsIgnoreCase(targetPartitionName)).findFirst().orElse(null);

        if (partitionsToBeMerged.size() < 2) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT, "merge single partition is meaningless");
        }

        //Map<PartitionName, isDefaultPartition>
        Map<String, Boolean> allExistingPartitions = new HashMap<>();

        PartitionByDefinition subPartBy = partitionInfo.getPartitionBy().getSubPartitionBy();
        boolean mergeSubPartBy = sqlAlterTableMergePartition.isSubPartitionsMerge();
        if (!mergeSubPartBy) {
            partitionInfo.getPartitionBy().getPartitions().stream()
                .forEach(o -> allExistingPartitions.put(o.getName().toLowerCase(), o.isDefaultPartition()));
        } else {
            if (subPartBy == null) {
                throw new TddlRuntimeException(ErrorCode.ERR_SUBPARTITION_NOT_EXISTS,
                    "the sub-partitioning is not exists");
            }
            if (subPartBy.isUseSubPartTemplate()) {
                subPartBy.getPartitions().stream()
                    .forEach(o -> allExistingPartitions.put(o.getName().toLowerCase(), o.isDefaultPartition()));
            } else {
                for (PartitionSpec partitionSpec : partitionInfo.getPartitionBy().getPartitions()) {
                    partitionSpec.getSubPartitions().stream()
                        .forEach(o -> allExistingPartitions.put(o.getName().toLowerCase(), o.isDefaultPartition()));
                }
            }
        }
        /**
         * 1. all the partition to be merged should be existing
         * 2. default partition can be merged to
         *    e.g.
         *    merge p1(1,2,3), pd(default) to pd_new(default)
         * */
        for (String toBeMerged : partitionsToBeMerged) {
            if (!allExistingPartitions.containsKey(toBeMerged)) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_NAME_NOT_EXISTS,
                    String.format("partition [%s] does not exist", toBeMerged));
            }
        }

        if (partitionInfo.isBroadcastTable()) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                "can't merge the partition group for broadcast tables");
        }

        if (partRecord != null && !partitionsToBeMerged.contains(partRecord.partition_name)) {
            throw new TddlRuntimeException(ErrorCode.ERR_DUPLICATED_PARTITION_NAME,
                "the partition group:" + targetPartitionName + " is exists");
        }

        // check whether partitions to be merged are contiguous
        List<PartitionSpec> mergePartSpecs = new ArrayList<>();
        PartitionStrategy strategyOfPartitionsToBeMerge;
        if (mergeSubPartBy && subPartBy.isUseSubPartTemplate()) {
            PartitionSpec firstPartitionSpec = partitionInfo.getPartitionBy().getNthPartition(1);
            for (PartitionSpec subPartSpec : firstPartitionSpec.getSubPartitions()) {
                if (partitionsToBeMerged.contains(subPartSpec.getTemplateName())) {
                    mergePartSpecs.add(subPartSpec);
                }
            }
            strategyOfPartitionsToBeMerge = firstPartitionSpec.getSubPartitions().get(0).getStrategy();
        } else {
            List<PartitionSpec> partitionSpecs;
            if (mergeSubPartBy) {
                partitionSpecs = partitionInfo.getPartitionBy().getPhysicalPartitions();
                strategyOfPartitionsToBeMerge = partitionSpecs.get(0).getStrategy();
            } else {
                partitionSpecs = partitionInfo.getPartitionBy().getPartitions();
                strategyOfPartitionsToBeMerge = partitionInfo.getPartitionBy().getStrategy();
            }
            for (PartitionSpec partitionSpec : partitionSpecs) {
                if (partitionsToBeMerged.contains(partitionSpec.getName())) {
                    mergePartSpecs.add(partitionSpec);
                }
            }
        }

        mergePartSpecs.sort(Comparator.comparing(PartitionSpec::getPosition));
        for (int i = 1; i < mergePartSpecs.size(); i++) {
            Long prev = mergePartSpecs.get(i - 1).getPosition();
            Long curr = mergePartSpecs.get(i).getPosition();
            Long prevParentId = mergePartSpecs.get(i - 1).getParentId();
            Long currParentId = mergePartSpecs.get(i).getParentId();

            if (strategyOfPartitionsToBeMerge.isList()) {
                if (mergeSubPartBy) {
                    if (prevParentId.longValue() != currParentId.longValue()) {
                        throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                            String.format("subpartitions to be merged should be in the same parent partition",
                                mergePartSpecs.get(i - 1).getName(), mergePartSpecs.get(i).getName()));
                    }
                }
            } else {
                if (prev + 1 != curr || prevParentId.longValue() != currParentId.longValue()) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                        String.format("partitions to be merged must be continuous, but %s and %s is not",
                            mergePartSpecs.get(i - 1).getName(), mergePartSpecs.get(i).getName()));
                }
            }
        }
    }

    public static void alterTableGroupReorgPartitionCheck(SqlAlterTableReorgPartition sqlAlterTableReorgPartition,
                                                          TableGroupConfig tableGroupConfig,
                                                          Map<SqlNode, RexNode> partRexInfoCtx,
                                                          boolean isAlterTableGroup,
                                                          ExecutionContext executionContext) {

        String schemaName = tableGroupConfig.getTableGroupRecord().getSchema();
        final SchemaManager schemaManager = executionContext.getSchemaManager(schemaName);
        String firstTableName = tableGroupConfig.getAllTables().get(0).getTableName();
        PartitionInfo partitionInfo = schemaManager.getTable(firstTableName).getPartitionInfo();

        PartitionByDefinition partByDef = partitionInfo.getPartitionBy();
        PartitionByDefinition subPartByDef = partByDef.getSubPartitionBy();

        boolean isSubPartition = sqlAlterTableReorgPartition.isSubPartition();

        PartitionStrategy strategy = isSubPartition ? subPartByDef.getStrategy() : partByDef.getStrategy();

        if (isSubPartition) {
            if (subPartByDef != null) {
                if (strategy.isHashed()) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                        "Don't allow to reorganize subpartitions for hash/key subpartition strategy tables");
                }
            } else {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                    "Don't allow to reorganize subpartitions from one-level partitioned table");
            }
        } else if (strategy.isHashed()) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                "Don't allow to reorganize partitions for hash/key partition strategy tables");
        }

        Set<String> oldPartNames =
            sqlAlterTableReorgPartition.getNames().stream().map(n -> ((SqlIdentifier) n).getLastName().toLowerCase())
                .collect(Collectors.toSet());

        List<SqlPartition> newPartDefs = sqlAlterTableReorgPartition.getPartitions().stream().map(p -> (SqlPartition) p)
            .collect(Collectors.toList());

        if (oldPartNames.size() == 1 && newPartDefs.size() == 1) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                "The source and target have only one partition respectively "
                    + "in which scenario a reorganization isn't necessary, "
                    + "so please use an ADD/DROP/RENAME/MODIFY operation instead");

        }

        PartitionInfoUtil.checkAndExpandPartitions(partitionInfo, tableGroupConfig, oldPartNames, isSubPartition);

        checkPartitionsDef(newPartDefs);

        List<PartitionSpec> oldPartSpecs =
            checkIfPartitionsContinuousForReorg(partByDef, subPartByDef, oldPartNames, isSubPartition);

        checkIfNewPartitionsValidForReorg(partitionInfo, tableGroupConfig, partRexInfoCtx, oldPartNames, oldPartSpecs,
            newPartDefs, isSubPartition, isAlterTableGroup, executionContext);
    }

    private static void checkIfNewPartitionsValidForReorg(PartitionInfo partitionInfo,
                                                          TableGroupConfig tableGroupConfig,
                                                          Map<SqlNode, RexNode> partRexInfoCtx,
                                                          Set<String> oldPartNames, List<PartitionSpec> oldPartSpecs,
                                                          List<SqlPartition> newPartDefs, boolean isSubPartition,
                                                          boolean isAlterTableGroup,
                                                          ExecutionContext executionContext) {
        PartitionByDefinition partByDef = partitionInfo.getPartitionBy();
        PartitionByDefinition subPartByDef = partByDef.getSubPartitionBy();

        PartitionStrategy strategy;
        PartitionSpec previousOldPartSpec = null;
        PartitionSpec parentOldPartSpec = null;

        PartitionSpec firstOldPartSpec = oldPartSpecs.get(0);

        if (isSubPartition) {
            strategy = subPartByDef.getStrategy();
            if (subPartByDef.isUseSubPartTemplate()) {
                for (PartitionSpec subPartSpecTemplate : subPartByDef.getPartitions()) {
                    if (subPartSpecTemplate.getName().equalsIgnoreCase(firstOldPartSpec.getName())) {
                        break;
                    } else {
                        previousOldPartSpec = subPartSpecTemplate;
                    }
                }
            } else {
                boolean matched = false;
                for (PartitionSpec partSpec : partByDef.getPartitions()) {
                    for (PartitionSpec subPartSpec : partSpec.getSubPartitions()) {
                        if (subPartSpec.getName().equalsIgnoreCase(firstOldPartSpec.getName())) {
                            matched = true;
                            parentOldPartSpec = partSpec;
                            break;
                        } else {
                            previousOldPartSpec = subPartSpec;
                        }
                    }
                    if (matched) {
                        break;
                    }
                    previousOldPartSpec = null;
                }
            }
        } else {
            strategy = partByDef.getStrategy();
            for (PartitionSpec partSpec : partByDef.getPartitions()) {
                if (partSpec.getName().equalsIgnoreCase(firstOldPartSpec.getName())) {
                    break;
                } else {
                    previousOldPartSpec = partSpec;
                }
            }
        }

        List<String> newPartNames =
            newPartDefs.stream().map(p -> ((SqlIdentifier) p.getName()).getLastName()).collect(Collectors.toList());

        Set<String> distinctPartNames = new TreeSet<>(String::compareToIgnoreCase);

        for (String newPartName : newPartNames) {
            if (!distinctPartNames.contains(newPartName)) {
                distinctPartNames.add(newPartName);
            } else {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                    String.format("duplicate new partition name '%s'", newPartName));
            }
        }

        // Check if existing partition names have contained any new partition name.
        Set<String> existingPartNames = new TreeSet<>(String::compareToIgnoreCase);
        if (isSubPartition) {
            if (subPartByDef.isUseSubPartTemplate()) {
                subPartByDef.getPartitions().forEach(p -> existingPartNames.add(p.getName()));
            } else {
                partByDef.getPhysicalPartitions().forEach(p -> existingPartNames.add(p.getName()));
            }
        } else {
            partByDef.getPartitions().forEach(p -> existingPartNames.add(p.getName()));
        }

        // Remove old partition names that are being reorganized.
        oldPartNames.forEach(p -> existingPartNames.remove(p));

        for (String newPartName : newPartNames) {
            if (existingPartNames.contains(newPartName)) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                    String.format("the partition name '%s' already exists (case-insensitive)", newPartName));
            }
        }
        // End of checking.

        List<PartitionSpec> newPartSpecs =
            buildNewPartitionSpecsForCheck(partitionInfo, tableGroupConfig, partRexInfoCtx, newPartDefs, isSubPartition,
                isAlterTableGroup, executionContext);

        if (strategy.isRange()) {
            PartitionSpec firstNewPartSpec = newPartSpecs.get(0);
            PartitionSpec lastOldPartSpec = oldPartSpecs.get(oldPartSpecs.size() - 1);

            SearchDatumInfo maxValue = lastOldPartSpec.getBoundSpec().getSingleDatum();
            SearchDatumInfo minValue = SearchDatumInfo.createMinValDatumInfo(partByDef.getPartitionFieldList().size());

            if (previousOldPartSpec != null) {
                minValue = previousOldPartSpec.getBoundSpec().getSingleDatum();
                SearchDatumInfo minNewValue = firstNewPartSpec.getBoundSpec().getSingleDatum();
                if (firstOldPartSpec.getBoundSpaceComparator().isLessThanOrEqualTo(minNewValue, minValue)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                        String.format("the bound %s of first new partition '%s' should be greater than %s", minNewValue,
                            firstNewPartSpec.getName(), minValue));
                }
            }

            for (int i = 0; i < newPartSpecs.size(); i++) {
                PartitionSpec newPartSpec = newPartSpecs.get(i);

                SearchDatumInfo newPartValue = newPartSpec.getBoundSpec().getSingleDatum();
                SearchDatumComparator comparator = newPartSpec.getBoundSpaceComparator();

                if (comparator.isLessThanOrEqualTo(newPartValue, minValue)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                        String.format("the bound %s of partition '%s' should be greater than %s", newPartValue,
                            newPartSpec.getName(), minValue));
                }

                if (comparator.isGreaterThan(newPartValue, maxValue)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                        String.format("the bound %s of partition %s should be less than or equal to %s", newPartValue,
                            newPartSpec.getName(), maxValue));
                }

                if (i == (newPartSpecs.size() - 1) && !comparator.isEqualTo(newPartValue, maxValue)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                        String.format("the bound %s of partition %s should be equal to %s", newPartValue,
                            newPartSpec.getName(), maxValue));
                }

                minValue = newPartValue;
            }
        } else if (strategy.isList()) {
            PartitionSpec defaultOldPartSpec = null;
            Set<String> oldPartValues = new HashSet<>();

            for (PartitionSpec oldPartSpec : oldPartSpecs) {
                if (oldPartSpec.isDefaultPartition()) {
                    defaultOldPartSpec = oldPartSpec;
                } else {
                    for (SearchDatumInfo oldPartValue : oldPartSpec.getBoundSpec().getMultiDatums()) {
                        oldPartValues.add(oldPartValue.toString());
                    }
                }
            }

            PartitionSpec defaultNewPartSpec = null;
            Set<String> newPartValues = new HashSet<>();

            for (PartitionSpec newPartSpec : newPartSpecs) {
                if (newPartSpec.isDefaultPartition()) {
                    if (defaultNewPartSpec != null) {
                        throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                            "duplicate default partitions");
                    } else {
                        defaultNewPartSpec = newPartSpec;
                    }
                } else {
                    for (SearchDatumInfo newPartValue : newPartSpec.getBoundSpec().getMultiDatums()) {
                        if (newPartValues.contains(newPartValue.toString())) {
                            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                                String.format("duplicate value ", newPartValue));
                        } else {
                            newPartValues.add(newPartValue.toString());
                        }
                    }
                }
            }

            /**
             * Refer to the SPLIT's logic:
             * check if the old partition is the summation of the new partitions.
             * e.g.
             * 1. if the reorg partition is non-default partition, we check whether p0 == p00 + p01 + p02 ...
             * 2. if the reorg partition is default partition, we check whether default_part == default_part + p_new0 + p_new1 ...
             *      and p_new shouldn't be the existing partition
             * */
            if (defaultOldPartSpec == null && defaultNewPartSpec == null) {
                for (String oldPartValue : oldPartValues) {
                    if (!newPartValues.contains(oldPartValue)) {
                        throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                            String.format("missing value %s in new partition definitions", oldPartValue));
                    }
                }
                for (String newPartValue : newPartValues) {
                    if (!oldPartValues.contains(newPartValue)) {
                        throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                            String.format("orphan value %s in new partition definitions", newPartValue));
                    }
                }
            } else if (defaultOldPartSpec != null && defaultNewPartSpec != null) {
                List<PartitionSpec> allPartSpecs;
                if (isSubPartition) {
                    if (subPartByDef.isUseSubPartTemplate()) {
                        allPartSpecs = subPartByDef.getPartitions();
                    } else {
                        allPartSpecs = parentOldPartSpec.getSubPartitions();
                    }
                } else {
                    allPartSpecs = partByDef.getPartitions();
                }

                Set<String> allNotReorgPartValues = new HashSet<>();
                for (PartitionSpec partSpec : allPartSpecs) {
                    if (partSpec.isDefaultPartition() || oldPartNames.contains(partSpec.getName().toLowerCase())) {
                        continue;
                    }
                    for (SearchDatumInfo partValue : partSpec.getBoundSpec().getMultiDatums()) {
                        allNotReorgPartValues.add(partValue.toString());
                    }
                }

                for (String newPartValue : newPartValues) {
                    if (allNotReorgPartValues.contains(newPartValue)) {
                        throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                            String.format("value %s is already in other existing partitions", newPartValue));
                    }
                }
            } else if (defaultOldPartSpec != null && defaultNewPartSpec == null) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                    "New partition definitions must contain a default partition");
            } else {
                // defaultOldPartSpec == null && defaultNewPartSpec != null
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                    "New partition definitions can't contain any default partition");
            }
        } else {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                String.format("Unsupported partition strategy %s for reorganization", strategy));
        }

        boolean isNonTemplateSubPart = subPartByDef != null && !subPartByDef.isUseSubPartTemplate();
        boolean isRangeOrListStrategy =
            subPartByDef != null && (subPartByDef.getStrategy().isRange() || subPartByDef.getStrategy().isList());
        if (!isSubPartition && isNonTemplateSubPart && isRangeOrListStrategy) {
            for (int i = 0; i < newPartDefs.size(); i++) {
                SqlPartition newPartDef = newPartDefs.get(i);
                if (GeneralUtil.isNotEmpty(newPartDef.getSubPartitions())) {
                    List<SqlPartition> mockDefForNewSubPart = new ArrayList<>();
                    for (SqlNode sqlNode : newPartDef.getSubPartitions()) {
                        SqlSubPartition newSubPartDef = (SqlSubPartition) sqlNode;
                        mockDefForNewSubPart.add(new SqlPartition(newSubPartDef.getName(), newSubPartDef.getValues(),
                            newSubPartDef.getParserPosition()));
                    }
                    List<PartitionSpec> newSubPartSpecs =
                        buildNewPartitionSpecsForCheck(partitionInfo, tableGroupConfig, partRexInfoCtx,
                            mockDefForNewSubPart, true, isAlterTableGroup, executionContext);
                    newPartSpecs.get(i).getSubPartitions().addAll(newSubPartSpecs);
                }
            }
            checkNewSubPartDefs(oldPartSpecs, newPartSpecs, subPartByDef.getStrategy());
        }
    }

    private static void checkNewSubPartDefs(List<PartitionSpec> oldPartSpecs, List<PartitionSpec> newPartSpecs,
                                            PartitionStrategy subPartStrategy) {
        SearchDatumInfo maxOldSubPartData = null;
        Set<String> oldListValues = new TreeSet<>(String::compareToIgnoreCase);

        for (PartitionSpec oldPartSpec : oldPartSpecs) {
            for (PartitionSpec oldSubPartSpec : oldPartSpec.getSubPartitions()) {
                if (subPartStrategy.isRange()) {
                    SearchDatumComparator comparator = oldSubPartSpec.getBoundSpaceComparator();
                    SearchDatumInfo oldSubPartData = oldSubPartSpec.getBoundSpec().getSingleDatum();
                    if (maxOldSubPartData == null) {
                        maxOldSubPartData = oldSubPartData;
                    } else if (comparator.isGreaterThan(oldSubPartData, maxOldSubPartData)) {
                        maxOldSubPartData = oldSubPartData;
                    }
                } else if (subPartStrategy.isList()) {
                    oldSubPartSpec.getBoundSpec().getMultiDatums().forEach(d -> oldListValues.add(d.toString()));
                }
            }
        }

        for (PartitionSpec newPartSpec : newPartSpecs) {
            if (GeneralUtil.isNotEmpty(newPartSpec.getSubPartitions())) {
                boolean hasCatchAllSubPart = false;
                SearchDatumInfo maxNewSubPartData = null;
                Set<String> newListValues = new TreeSet<>(String::compareToIgnoreCase);

                for (PartitionSpec newSubPartSpec : newPartSpec.getSubPartitions()) {
                    PartitionBoundSpec newPartBoundSpec = newSubPartSpec.getBoundSpec();

                    if (subPartStrategy.isRange()) {
                        SearchDatumComparator comparator = newSubPartSpec.getBoundSpaceComparator();
                        SearchDatumInfo newSubPartData = newPartBoundSpec.getSingleDatum();
                        if (maxNewSubPartData == null) {
                            maxNewSubPartData = newSubPartData;
                        } else if (comparator.isGreaterThan(newSubPartData, maxNewSubPartData)) {
                            maxNewSubPartData = newSubPartData;
                        }
                    } else if (subPartStrategy.isList()) {
                        newSubPartSpec.getBoundSpec().getMultiDatums().forEach(d -> newListValues.add(d.toString()));
                    }

                    if (newPartBoundSpec.containMaxValues() || newPartBoundSpec.isDefaultPartSpec()) {
                        hasCatchAllSubPart = true;
                    }
                }

                SearchDatumComparator comparator = newPartSpec.getBoundSpaceComparator();

                if (subPartStrategy.isRange() && comparator.isGreaterThanOrEqualTo(maxNewSubPartData,
                    maxOldSubPartData)) {
                    continue;
                }

                if (subPartStrategy.isList() && newListValues.containsAll(oldListValues)) {
                    continue;
                }

                if (!hasCatchAllSubPart) {
                    String value = subPartStrategy.isRange() ? "MAXVALUE" : subPartStrategy.isList() ? "DEFAULT" : "";
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT, String.format(
                        "Newly defined subpartitions for each reorganized partition must contain a "
                            + "catch-all subpartition, i.e. %s for the %s strategy", value, subPartStrategy));
                }
            }
        }
    }

    private static List<PartitionSpec> buildNewPartitionSpecsForCheck(PartitionInfo partitionInfo,
                                                                      TableGroupConfig tableGroupConfig,
                                                                      Map<SqlNode, RexNode> partRexInfoCtx,
                                                                      List<SqlPartition> newPartDefs,
                                                                      boolean isSubPartition,
                                                                      boolean isAlterTableGroup,
                                                                      ExecutionContext executionContext) {
        String schemaName = tableGroupConfig.getTableGroupRecord().getSchema();
        String tableGroupName = tableGroupConfig.getTableGroupRecord().getTg_name();

        PartitionByDefinition partByDef = partitionInfo.getPartitionBy();
        PartitionByDefinition subPartByDef = partByDef.getSubPartitionBy();

        List<PartitionSpec> newPartSpecs = new ArrayList<>();

        PartitionStrategy strategy;
        List<ColumnMeta> partColMetaList;
        SearchDatumComparator comparator;
        PartitionIntFunction partIntFunc;

        if (isSubPartition) {
            strategy = subPartByDef.getStrategy();
            partColMetaList = subPartByDef.getPartitionFieldList();
            comparator = subPartByDef.getPruningSpaceComparator();
            partIntFunc = subPartByDef.getPartIntFunc();
        } else {
            strategy = partByDef.getStrategy();
            partColMetaList = partByDef.getPartitionFieldList();
            comparator = partByDef.getPruningSpaceComparator();
            partIntFunc = partByDef.getPartIntFunc();
        }

//        GetNewActPartColCntFromAstParams params = new GetNewActPartColCntFromAstParams();
//        params.setAllLevelFullPartColCnts(partitionInfo.getAllLevelFullPartColCounts());
//        params.setAllLevelActualPartColCnts(partitionInfo.getAllLevelActualPartColCounts());
//        params.setAllLevelStrategies(partitionInfo.getAllLevelPartitionStrategies());
//        params.setNewPartitions(newPartDefs);
//        params.setChangeSubpartition(isSubPartition);
//        List<Integer> allLevelNewPrefixPartColCnts =
//            PartitionInfoUtil.getNewAllLevelPrefixPartColCntBySqlPartitionAst(params);
        List<Integer> allLevelNewPrefixPartColCnts =
            PartitionInfoUtil.getNewAllLevelPrefixPartColCntByPartInfoAndNewParts(isAlterTableGroup, partitionInfo,
                newPartDefs,
                isSubPartition);

        if (strategy == PartitionStrategy.RANGE_COLUMNS && isAlterTableGroup) {
            /**
             * Check if is allowed to use newPrefixPartColCnt as new partColCnt for all tables in table group
             */
            alterTableGroupCheckPartitionColumn(schemaName, tableGroupName, allLevelNewPrefixPartColCnts);
        }

        int pos = 0;
        AtomicInteger phyPartCounter = new AtomicInteger(pos);

        for (SqlPartition sqlPartition : newPartDefs) {
            if (sqlPartition.getValues() == null) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                    String.format("the new partition value is not specified for '%s'",
                        ((SqlIdentifier) sqlPartition.getName()).getLastName()));
            }

            BuildPartSpecFromAstParams partSpecAstParams = new BuildPartSpecFromAstParams();

            partSpecAstParams.setContext(executionContext);
            partSpecAstParams.setPartColMetaList(partColMetaList);
            partSpecAstParams.setPartKeyLevel(
                isSubPartition ? PartKeyLevel.SUBPARTITION_KEY : PartKeyLevel.PARTITION_KEY);
            partSpecAstParams.setPartIntFunc(partIntFunc);
            partSpecAstParams.setPruningComparator(comparator);
            partSpecAstParams.setPartBoundExprInfo(partRexInfoCtx);
            partSpecAstParams.setPartBoundValBuilder(null);
            partSpecAstParams.setStrategy(strategy);
            partSpecAstParams.setPartPosition(pos++);
            partSpecAstParams.setAllLevelPrefixPartColCnts(allLevelNewPrefixPartColCnts);

            partSpecAstParams.setPartNameAst(sqlPartition.getName());
            partSpecAstParams.setPartComment(sqlPartition.getComment());
            partSpecAstParams.setPartLocality(sqlPartition.getLocality());
            partSpecAstParams.setPartBndValuesAst(sqlPartition.getValues());

            partSpecAstParams.setLogical(false);
            partSpecAstParams.setSpecTemplate(false);
            partSpecAstParams.setSubPartSpec(false);
            partSpecAstParams.setParentPartSpec(null);
            partSpecAstParams.setPhySpecCounter(phyPartCounter);

            PartitionSpec newPartSpec = PartitionInfoBuilder.buildPartSpecByAstParams(partSpecAstParams);

            newPartSpecs.add(newPartSpec);
        }

        return newPartSpecs;
    }

    private static List<PartitionSpec> checkIfPartitionsContinuousForReorg(PartitionByDefinition partByDef,
                                                                           PartitionByDefinition subPartByDef,
                                                                           Set<String> oldPartNames,
                                                                           boolean isSubPartition) {
        List<PartitionSpec> allPartSpecs = new ArrayList<>();
        List<PartitionSpec> oldPartSpecs = new ArrayList<>();
        PartitionStrategy strategy;

        if (isSubPartition) {
            strategy = subPartByDef.getStrategy();
            if (subPartByDef.isUseSubPartTemplate()) {
                allPartSpecs = subPartByDef.getPartitions();
            } else {
                for (PartitionSpec partSpec : partByDef.getPartitions()) {
                    allPartSpecs.addAll(partSpec.getSubPartitions());
                }
            }
        } else {
            strategy = partByDef.getStrategy();
            allPartSpecs = partByDef.getPartitions();
        }

        for (PartitionSpec partSpec : allPartSpecs) {
            if (oldPartNames.contains(partSpec.getName().toLowerCase())) {
                oldPartSpecs.add(partSpec);
            }
        }

        if (oldPartNames.size() != oldPartSpecs.size()) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT, String.format(
                "Old partition names (size %s) aren't matched to existing partition specifications (size %s)",
                oldPartNames.size(), oldPartSpecs.size()));
        }

        oldPartSpecs.sort(Comparator.comparing(PartitionSpec::getPosition));

        for (int i = 1; i < oldPartSpecs.size(); i++) {
            PartitionSpec prevPartSpec = oldPartSpecs.get(i - 1);
            PartitionSpec currPartSpec = oldPartSpecs.get(i);

            Long prevPos = prevPartSpec.getPosition();
            Long currPos = currPartSpec.getPosition();

            Long prevParentId = prevPartSpec.getParentId();
            Long currParentId = currPartSpec.getParentId();

            if (prevParentId.longValue() != currParentId.longValue()) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                    String.format("subpartitions to be reorganized should be in the same parent partition: %s, %s",
                        prevPartSpec.getName(), currPartSpec.getName()));
            }

            if (strategy.isRange() && (prevPos + 1 != currPos)) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                    String.format("partitions to be reorganized must be continuous, but %s and %s are not",
                        prevPartSpec.getName(), currPartSpec.getName()));
            }
        }

        return oldPartSpecs;
    }

    public static void alterTableGroupMovePartitionCheck(SqlAlterTableMovePartition sqlAlterTableMovePartition,
                                                         TableGroupConfig tableGroupConfig, String schemaName) {
        if (tableGroupConfig.getTableGroupRecord().isBroadCastTableGroup()) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                "can't move the partition for broadcast tables");
        }

        String tgSchema = tableGroupConfig.getTableGroupRecord().getSchema();
        String firstTableName = tableGroupConfig.getAllTables().get(0).getTableName();
        PartitionInfo partitionInfo =
            OptimizerContext.getContext(tgSchema).getPartitionInfoManager().getPartitionInfo(firstTableName);

        final String metaDbStorageInstId = ScaleOutPlanUtil.getMetaDbStorageInstId();

        Map<String, Set<String>> newTargetPartitions = new HashMap<>();

        for (Map.Entry<String, Set<String>> entry : sqlAlterTableMovePartition.getTargetPartitions().entrySet()) {
            String targetInstId = entry.getKey();
            Set<String> partitionsToMove = entry.getValue();

            if (!ScaleOutPlanUtil.storageInstIsReady(targetInstId.toLowerCase())) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                    targetInstId + " is not a valid storage instance id");
            }

            if (targetInstId.equalsIgnoreCase(metaDbStorageInstId)) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                    " it's not allow to move normal partitions to the storage instance:" + metaDbStorageInstId
                        + ", which is only for hosting the metaDb");
            }

            Set<String> newPartitions = new TreeSet<>(String::compareToIgnoreCase);

            Set<String> actualPartitionsToMove =
                PartitionInfoUtil.checkAndExpandPartitions(partitionInfo, tableGroupConfig, partitionsToMove,
                    sqlAlterTableMovePartition.isSubPartitionsMoved());

            for (String partition : actualPartitionsToMove) {
                PartitionGroupRecord partitionGroupRecord = tableGroupConfig.getPartitionGroupByName(partition);

                String sourceGroupKey = GroupInfoUtil.buildGroupNameFromPhysicalDb(partitionGroupRecord.phy_db);
                final String sourceInstId = DbTopologyManager.getStorageInstIdByGroupName(InstIdUtil.getInstId(),
                    tableGroupConfig.getTableGroupRecord().getSchema(), sourceGroupKey);

                if (!sourceInstId.equalsIgnoreCase(targetInstId)) {
                    newPartitions.add(partition);
                }
            }

            if (GeneralUtil.isNotEmpty(newPartitions)) {
                newTargetPartitions.put(targetInstId, newPartitions);
            }
        }

        sqlAlterTableMovePartition.setTargetPartitions(newTargetPartitions);
    }

    private static void alterTableGroupExtractPartitionCheck(SqlAlterTableGroup sqlAlterTableGroup,
                                                             TableGroupConfig tableGroupConfig,
                                                             ExecutionContext executionContext) {
        String schemaName = tableGroupConfig.getAllTables().get(0).getLogTbRec().tableSchema;
        String tableInCurrentGroup = tableGroupConfig.getAllTables().get(0).getLogTbRec().tableName;

        final SchemaManager schemaManager = executionContext.getSchemaManager(schemaName);

        TableMeta tableMeta = schemaManager.getTable(tableInCurrentGroup);
        PartitionInfo partitionInfo = tableMeta.getPartitionInfo();
        assert GeneralUtil.isNotEmpty(sqlAlterTableGroup.getPartRexInfoCtxByLevel());

        if (partitionInfo.isSingleTable() || partitionInfo.isBroadcastTable()) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                "can't execute the extract partition command for single/broadcast tables");
        }

        if (partitionInfo.getPartitionBy().getStrategy() != PartitionStrategy.HASH
            && partitionInfo.getPartitionBy().getStrategy() != PartitionStrategy.KEY
            && partitionInfo.getPartitionBy().getStrategy() != PartitionStrategy.LIST
            && partitionInfo.getPartitionBy().getStrategy() != PartitionStrategy.LIST_COLUMNS) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                "it's not allow to extract to partition by hot value for non-hash partition table");
        }
        TableGroupRecord tableGroupRecord = tableGroupConfig.getTableGroupRecord();
        alterTableGroupCheckPartitionColumn(tableGroupRecord.getSchema(), tableGroupRecord.getTg_name(),
            PartitionInfoUtil.ALL_LEVEL_FULL_PART_COL_COUNT_LIST);
    }

    public static void alterTableExtractPartitionCheck(String schemaName, String logicalTableName,
                                                       Map<SqlNode, RexNode> rexExprInfo,
                                                       ExecutionContext executionContext) {
        final SchemaManager schemaManager = executionContext.getSchemaManager(schemaName);

        TableMeta tableMeta = schemaManager.getTable(logicalTableName);
        PartitionInfo partitionInfo = tableMeta.getPartitionInfo();
        assert GeneralUtil.isNotEmpty(rexExprInfo);

        if (partitionInfo.isSingleTable() || partitionInfo.isBroadcastTable()) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                "can't execute the extract partition command for single/broadcast tables");
        }
        if (partitionInfo.getPartitionBy().getStrategy() != PartitionStrategy.HASH
            && partitionInfo.getPartitionBy().getStrategy() != PartitionStrategy.KEY
            && partitionInfo.getPartitionBy().getStrategy() != PartitionStrategy.LIST
            && partitionInfo.getPartitionBy().getStrategy() != PartitionStrategy.LIST_COLUMNS) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                "it's not allow to extract to partition by hot value for non-hash partition table");
        }
    }

    private static void alterTableGroupSplitPartitionByHotValueCheck(SqlAlterTableGroup sqlAlterTableGroup,
                                                                     TableGroupConfig tableGroupConfig,
                                                                     ExecutionContext executionContext) {
//        final SchemaManager schemaManager = executionContext.getSchemaManager();
        SqlAlterTableGroupSplitPartitionByHotValue sqlAlterTableSplitPartitionByHotValue =
            (SqlAlterTableGroupSplitPartitionByHotValue) sqlAlterTableGroup.getAlters().get(0);

        boolean subPartitionSplit = sqlAlterTableSplitPartitionByHotValue.isSubPartitionsSplit();
        boolean modifyNonTemplateSubPartition =
            subPartitionSplit && sqlAlterTableSplitPartitionByHotValue.getModifyPartitionName() != null;

        String schemaName = tableGroupConfig.getAllTables().get(0).getLogTbRec().tableSchema;
        String tableInCurrentGroup = tableGroupConfig.getAllTables().get(0).getLogTbRec().tableName;

        final SchemaManager schemaManager = executionContext.getSchemaManager(schemaName);

        TableMeta tableMeta = schemaManager.getTable(tableInCurrentGroup);
        PartitionInfo partitionInfo = tableMeta.getPartitionInfo();
        assert GeneralUtil.isNotEmpty(sqlAlterTableGroup.getPartRexInfoCtxByLevel());

        if (partitionInfo.isSingleTable() || partitionInfo.isBroadcastTable()) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                "can't split the partition group for single/broadcast tables");
        }
        if (!subPartitionSplit && partitionInfo.getPartitionBy().getStrategy() != PartitionStrategy.KEY) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                "it's not allow to split partition by hot value for non-key partition table");
        }
        if (modifyNonTemplateSubPartition) {
            String parentPartName = SQLUtils.normalizeNoTrim(
                sqlAlterTableSplitPartitionByHotValue.getModifyPartitionName().getSimpleName());

            PartitionSpec parentPartitionSpec = partitionInfo.getPartitionBy().getPartitionByPartName(parentPartName);

            if (parentPartitionSpec == null) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_NAME_NOT_EXISTS,
                    parentPartName + " is not exists");
            }
        }

        PartitionByDefinition subPartByDef = partitionInfo.getPartitionBy().getSubPartitionBy();

        if (subPartitionSplit && subPartByDef == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                "it's not allow to split subpartition by hot value because tables haven't subpartition");
        }

        if (subPartitionSplit && subPartByDef.getStrategy() != PartitionStrategy.KEY) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                "it's not allow to split subpartition by hot value for non-key subpartition table");
        }

        if (modifyNonTemplateSubPartition && subPartByDef.isUseSubPartTemplate()) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                "it's not allow to split partial subpartition by hot value for template subpartition table");
        }
        if (subPartitionSplit && !modifyNonTemplateSubPartition && !subPartByDef.isUseSubPartTemplate()) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                "it's only allow to split subpartition of specified logical partition by hot value for non-template subpartition table");
        }

        SqlNode partitions = sqlAlterTableSplitPartitionByHotValue.getPartitions();
        int splitIntoParts = ((SqlNumericLiteral) (partitions)).intValue(true);
        if (splitIntoParts <= 0) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT, "partitions must greater than 0");
        }
        TableGroupRecord tableGroupRecord = tableGroupConfig.getTableGroupRecord();
        int newPrefixPartColCnt = sqlAlterTableSplitPartitionByHotValue.getHotKeys().size();
        if (splitIntoParts > 1) {
            newPrefixPartColCnt += 1;
        }

        List<Integer> allLevelPrefixPartColCnts = new ArrayList<>();
        allLevelPrefixPartColCnts.add(newPrefixPartColCnt);
        allLevelPrefixPartColCnts.add(0);

        alterTableGroupCheckPartitionColumn(tableGroupRecord.getSchema(), tableGroupRecord.getTg_name(),
            allLevelPrefixPartColCnts);

    }

    public static void alterTableSplitPartitionByHotValueCheck(String schemaName, String logicalTableName,
                                                               SqlAlterTableSplitPartitionByHotValue sqlAlterTableSplitPartitionByHotValue,
                                                               Map<SqlNode, RexNode> rexExprInfo,
                                                               ExecutionContext executionContext) {
        final SchemaManager schemaManager = executionContext.getSchemaManager(schemaName);

        TableMeta tableMeta = schemaManager.getTable(logicalTableName);
        PartitionInfo partitionInfo = tableMeta.getPartitionInfo();
        assert GeneralUtil.isNotEmpty(rexExprInfo);
        boolean subPartitionSplit = sqlAlterTableSplitPartitionByHotValue.isSubPartitionsSplit();
        boolean modifyNonTemplateSubPartition =
            subPartitionSplit && sqlAlterTableSplitPartitionByHotValue.getModifyPartitionName() != null;

        if (partitionInfo.isSingleTable() || partitionInfo.isBroadcastTable()) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                "can't split the partition for single/broadcast tables");
        }

        if (!subPartitionSplit && partitionInfo.getPartitionBy().getStrategy() != PartitionStrategy.KEY) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                "it's not allow to split partition by hot value for non-key partition table");
        }

        PartitionByDefinition subPartByDef = partitionInfo.getPartitionBy().getSubPartitionBy();

        if (subPartitionSplit && subPartByDef == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                "it's not allow to split subpartition by hot value because tables haven't subpartition");
        }

        if (subPartitionSplit && subPartByDef.getStrategy() != PartitionStrategy.KEY) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                "it's not allow to split subpartition by hot value for non-key subpartition table");
        }

        if (modifyNonTemplateSubPartition && subPartByDef.isUseSubPartTemplate()) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                "it's not allow to split partial subpartition by hot value for template subpartition table");
        }
        if (subPartitionSplit && !modifyNonTemplateSubPartition && !subPartByDef.isUseSubPartTemplate()) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                "it's only allow to split subpartition of specified logical partition by hot value for non-template subpartition table");
        }

        if (modifyNonTemplateSubPartition) {
            String parentPartName = SQLUtils.normalizeNoTrim(
                sqlAlterTableSplitPartitionByHotValue.getModifyPartitionName().getSimpleName());

            PartitionSpec parentPartitionSpec = partitionInfo.getPartitionBy().getPartitionByPartName(parentPartName);

            if (parentPartitionSpec == null) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_NAME_NOT_EXISTS,
                    parentPartName + " is not exists");
            }
        }

        SqlNode partitions = sqlAlterTableSplitPartitionByHotValue.getPartitions();
        int splitIntoParts = ((SqlNumericLiteral) (partitions)).intValue(true);
        if (splitIntoParts <= 0) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT, "partitions must greater than 0");
        }

    }

    public static void alterTableGroupDropPartitionCheck(SqlAlterTableDropPartition sqlAlterTableDropPartition,
                                                         TableGroupConfig tableGroupConfig,
                                                         ExecutionContext executionContext) {

        String schemaName = tableGroupConfig.getTableGroupRecord().getSchema();
        final SchemaManager schemaManager = executionContext.getSchemaManager(schemaName);
        String tableInCurrentGroup = tableGroupConfig.getAllTables().get(0).getTableName();
        PartitionInfo partitionInfo = schemaManager.getTable(tableInCurrentGroup).getPartitionInfo();

        PartitionByDefinition partByDef = partitionInfo.getPartitionBy();
        PartitionByDefinition subPartByDef = partByDef.getSubPartitionBy();

        if (sqlAlterTableDropPartition.isSubPartition()) {
            // DROP SUBPARTITION
            if (subPartByDef != null) {
                // Two-level Partitioned Table
                if (subPartByDef.getStrategy() == PartitionStrategy.HASH
                    || subPartByDef.getStrategy() == PartitionStrategy.KEY) {
                    throw new TddlRuntimeException(ErrorCode.ERR_DROP_SUBPARTITION,
                        "Don't allow to drop subpartitions for hash/key subpartition strategy tables");
                }
                if (subPartByDef.isUseSubPartTemplate()) {
                    // Templated Subpartition
                    if (sqlAlterTableDropPartition.getPartitionName() != null) {
                        // MODIFY PARTITION DROP SUBPARTITION
                        throw new TddlRuntimeException(ErrorCode.ERR_MODIFY_PARTITION,
                            "Don't allow to drop subpartitions from a particular partition for templated subpartition");
                    } else {
                        // DROP SUBPARTITION
                        if (subPartByDef.getPartitions().size() <= 1) {
                            throw new TddlRuntimeException(ErrorCode.ERR_DROP_SUBPARTITION,
                                "Don't allow to drop subpartitions for table partition that only contains one subpartition");
                        }
                    }
                } else {
                    // Non-Templated Subpartition
                    if (sqlAlterTableDropPartition.getPartitionName() != null) {
                        // MODIFY PARTITION DROP SUBPARTITION
                        for (PartitionSpec partSpec : partByDef.getPartitions()) {
                            if (TStringUtil.equalsIgnoreCase(partSpec.getName(),
                                sqlAlterTableDropPartition.getPartitionName().toString())) {
                                if (partSpec.getSubPartitions().size() <= 1) {
                                    throw new TddlRuntimeException(ErrorCode.ERR_MODIFY_PARTITION,
                                        "Don't allow to drop subpartitions for table partition '" + partSpec.getName()
                                            + "' that only contains one subpartition");
                                }
                                break;
                            }
                        }
                    }
                }
            } else {
                // One-level Partitioned Table
                throw new TddlRuntimeException(ErrorCode.ERR_DROP_SUBPARTITION,
                    "Don't allow to drop subpartitions from one-level partitioned table");
            }
        } else {
            // DROP PARTITION
            if (partByDef.getStrategy() == PartitionStrategy.HASH || partByDef.getStrategy() == PartitionStrategy.KEY) {
                throw new TddlRuntimeException(ErrorCode.ERR_DROP_PARTITION,
                    "Don't allow to drop partitions for hash/key partition strategy tables");
            }
            if (partByDef.getPartitions().size() <= 1) {
                throw new TddlRuntimeException(ErrorCode.ERR_DROP_PARTITION,
                    "Don't allow to drop partition when the table only has one partition");
            }
        }

        Set<String> allPartitionGroupNames = PartitionInfoUtil.getAllPartitionGroupNames(tableGroupConfig);

        List<String> droppingPartNames =
            LogicalAlterTableDropPartition.getDroppingPartitionNames(sqlAlterTableDropPartition, partitionInfo, null);

        for (String partName : droppingPartNames) {
            if (!allPartitionGroupNames.contains(partName.toLowerCase())) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                    "Partition group '" + partName + "' doesn't exist");
            }
        }

        Set<String> droppingPartNameSet = droppingPartNames.stream().collect(Collectors.toSet());

        boolean allExistingPartGroupsDropped = true;
        for (String partGroupName : allPartitionGroupNames) {
            if (!droppingPartNameSet.contains(partGroupName)) {
                allExistingPartGroupsDropped = false;
                break;
            }
        }

        if (allExistingPartGroupsDropped) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                "Don't allow to drop all the partitions");
        }

        for (TablePartRecordInfoContext record : tableGroupConfig.getAllTables()) {
            TableMeta tbMeta = schemaManager.getTable(record.getTableName());
            if (tbMeta.withGsi()) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                    String.format("it's not support to drop partition/subpartition when table[%s] with GSI",
                        record.getTableName()));
            }
            if (tbMeta.isGsi()) {
                throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_DROP_PARTITION,
                    String.format("it's not support to drop global index[%s]'s partition/subpartition",
                        record.getTableName()));
            }
        }
    }

    public static void alterTableGroupTruncatePartitionCheck(
        SqlAlterTableTruncatePartition sqlAlterTableTruncatePartition, TableGroupConfig tableGroupConfig,
        ExecutionContext executionContext) {
        String schemaName = tableGroupConfig.getTableGroupRecord().getSchema();
        final SchemaManager schemaManager = executionContext.getSchemaManager(schemaName);

        String tableInCurrentGroup = tableGroupConfig.getAllTables().get(0).getLogTbRec().tableName;
        PartitionInfo partitionInfo = schemaManager.getTable(tableInCurrentGroup).getPartitionInfo();

        PartitionByDefinition subPartByDef = partitionInfo.getPartitionBy().getSubPartitionBy();

        if (subPartByDef == null && sqlAlterTableTruncatePartition.isSubPartition()) {
            // TRUNCATE SUBPARTITION
            throw new TddlRuntimeException(ErrorCode.ERR_DROP_SUBPARTITION,
                "Don't allow to drop subpartitions from one-level partitioned table");
        }

        for (TablePartRecordInfoContext record : tableGroupConfig.getAllTables()) {
            TableMeta tbMeta = schemaManager.getTable(record.getTableName());
            if (tbMeta.withGsi()) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                    String.format("it's not support to truncate partition/subpartition when table[%s] with GSI",
                        record.getTableName()));
            }
            if (tbMeta.isGsi()) {
                throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_DROP_PARTITION,
                    String.format("it's not support to truncate global index[%s]'s partition/subpartition",
                        record.getTableName()));
            }
        }
    }

    private static void alterTableModifyListPartitionValuesCheck(SqlAlterTableGroup sqlAlterTableGroup,
                                                                 TableGroupConfig tableGroupConfig,
                                                                 SqlAlterTableModifyPartitionValues sqlModifyListPartitionValues,
                                                                 ExecutionContext executionContext) {

        TablePartitionRecord tablePartitionRecord = tableGroupConfig.getAllTables().get(0).getLogTbRec();
        SchemaManager schemaManager =
            OptimizerContext.getContext(tablePartitionRecord.getTableSchema()).getLatestSchemaManager();
        PartitionInfo partitionInfo =
            OptimizerContext.getContext(tablePartitionRecord.getTableSchema()).getPartitionInfoManager()
                .getPartitionInfo(tablePartitionRecord.getTableName());

        SqlPartition partSpecAst = sqlModifyListPartitionValues.getPartition();
        SqlSubPartition subpartSpecAst = null;
        SqlPartitionValue targetPartSpecBndVal = null;
        boolean isSubPart = sqlModifyListPartitionValues.isSubPartition();
        PartitionByDefinition tarPartDef = partitionInfo.getPartitionBy();
        boolean useSubPart = partitionInfo.getPartitionBy().getSubPartitionBy() != null;
        SqlIdentifier targetPartName = (SqlIdentifier) partSpecAst.getName();
        String targetPartNameStr = null;
        boolean useSubPartTemp = false;
        if (isSubPart) {
            tarPartDef = tarPartDef.getSubPartitionBy();
            subpartSpecAst = (SqlSubPartition) partSpecAst.getSubPartitions().get(0);
            targetPartName = (SqlIdentifier) subpartSpecAst.getName();
            targetPartSpecBndVal = subpartSpecAst.getValues();
            useSubPartTemp = tarPartDef.isUseSubPartTemplate();
        } else {
            targetPartSpecBndVal = partSpecAst.getValues();
        }
        targetPartNameStr = SQLUtils.normalizeNoTrim(targetPartName.getLastName());

        /**
         * Target PartSpec To be check
         */
        List<PartitionSpec> targetPartSpecList = null;
        if (isSubPart) {
            /**
             * targetPartName maybe real subpartName or subpartTemplateName
             */
            if (!useSubPartTemp) {
                /**
                 * Find the 2nd-level subpartSpecs with the same parent 1st-level part
                 */
                targetPartSpecList =
                    PartitionInfoUtil.getAllPhyPartsWithSameParentByPhyPartName(partitionInfo, targetPartNameStr);
            } else {
                /**
                 * here tarPartDef must be the SubPartitionBy, so
                 * for the modification of templated subpart, use its templated subpart specs to do validation
                 */
                targetPartSpecList = tarPartDef.getPartitions();
            }
        } else {
            /**
             * here tarPartDef must be the PartitionBy, so
             * for the modification of 1st-level part, use its part specs to do validation
             */
            targetPartSpecList = tarPartDef.getPartitions();
        }

        if (tarPartDef.getStrategy() != PartitionStrategy.LIST
            && tarPartDef.getStrategy() != PartitionStrategy.LIST_COLUMNS) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                String.format("it's not allow to add/drop values for tableGroup[%s], which partition strategy is [%s]",
                    tableGroupConfig.getTableGroupRecord().getTg_name(), tarPartDef.getStrategy().toString()));
        }

//        for (PartitionSpec spec : targetPartSpecList) {
//            if (spec.isDefaultPartition()) {
//                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
//                    String.format("It's not allowed to modify partition when table group contain default partition"));
//            }
//        }

        for (PartitionSpec spec : targetPartSpecList) {
            if (spec.getName().equalsIgnoreCase(targetPartNameStr) && spec.isDefaultPartition()) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                    String.format("It's not allowed to modify default partition [%s]", targetPartNameStr));
            }
        }

        if (sqlModifyListPartitionValues.isAdd()) {
            PartitionSpec targetPartSpec = null;
            if (!useSubPartTemp) {
                targetPartSpec = partitionInfo.getPartSpecSearcher().getPartSpecByPartName(targetPartNameStr);
            } else {
                /**
                 * For the modification of templated subpart,
                 * use the subpart spec as target subpart spec to do validation
                 */
                List<PartitionSpec> subPartTempSpecs =
                    partitionInfo.getPartitionBy().getSubPartitionBy().getPartitions();
                for (int i = 0; i < subPartTempSpecs.size(); i++) {
                    PartitionSpec subPartTemp = subPartTempSpecs.get(i);
                    if (subPartTemp.getName().equalsIgnoreCase(targetPartNameStr)) {
                        targetPartSpec = subPartTemp;
                        break;
                    }
                }
            }
            if (targetPartSpec == null) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                    String.format("partition[%s] is not exists", targetPartNameStr));
            }

        } else if (sqlModifyListPartitionValues.isDrop()) {

            PartitionSpec targetPartSpec = null;
            if (!useSubPartTemp) {
                targetPartSpec = partitionInfo.getPartSpecSearcher().getPartSpecByPartName(targetPartNameStr);
            } else {
                /**
                 * For the modification of templated subpart,
                 * use the subpart spec as target subpart spec to do validation
                 */
                List<PartitionSpec> subPartTempSpecs =
                    partitionInfo.getPartitionBy().getSubPartitionBy().getPartitions();
                for (int i = 0; i < subPartTempSpecs.size(); i++) {
                    PartitionSpec subPartTemp = subPartTempSpecs.get(i);
                    if (subPartTemp.getName().equalsIgnoreCase(targetPartNameStr)) {
                        targetPartSpec = subPartTemp;
                        break;
                    }
                }
            }

            if (targetPartSpec == null) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                    String.format("partition[%s] is not exists", targetPartNameStr));
            }
            List<SearchDatumInfo> originalDatums = targetPartSpec.getBoundSpec().getMultiDatums();

            if (originalDatums.size() == 1) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                    String.format("partition[%s] has only one value now, can't drop value any more",
                        targetPartNameStr));
            }
            if (originalDatums.size() <= targetPartSpecBndVal.getItems().size()) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT, String.format(
                    "the number of drop values should less than the number of values contain by partition[%s]",
                    targetPartNameStr));
            }
            for (TablePartRecordInfoContext record : tableGroupConfig.getAllTables()) {
                TableMeta tbMeta = schemaManager.getTable(record.getTableName());
                if (tbMeta.withGsi()) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                        String.format("it's not support to drop value when table[%s] with GSI", record.getTableName()));
                }
                if (tbMeta.isGsi()) {
                    throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_MODIFY_PARTITION_DROP_VALUE,
                        String.format("it's not support to drop value for global index[%s]", record.getTableName()));
                }
            }
        }
    }

    public static void alterTableGroupRenamePartitionCheck(
        SqlAlterTableRenamePartition sqlAlterTableGroupRenamePartition, TableGroupConfig tableGroupConfig,
        ExecutionContext executionContext) {

        final String schemaName = tableGroupConfig.getTableGroupRecord().getSchema();
        final SchemaManager schemaManager = executionContext.getSchemaManager(schemaName);

        String firstTableInCurrentGroup = tableGroupConfig.getAllTables().get(0).getLogTbRec().tableName;
        TableMeta tableMeta = schemaManager.getTable(firstTableInCurrentGroup);

        Set<String> oldPartitionNames = new HashSet<>();
        Set<String> newPartitionNames = new HashSet<>();
        final Set<String> allLevelPartitionNameExists = new TreeSet(String.CASE_INSENSITIVE_ORDER);
        final Set<String> level1PartitionNameExists = new TreeSet(String.CASE_INSENSITIVE_ORDER);
        final Set<String> level2PartitionNameExists = new TreeSet(String.CASE_INSENSITIVE_ORDER);

        boolean useSubPart = false;
        boolean useSubPartTemplate = false;
        boolean renameSubPartition = sqlAlterTableGroupRenamePartition.isSubPartitionsRename();
        PartitionByDefinition partBy = tableMeta.getPartitionInfo().getPartitionBy();
        PartitionByDefinition subPartBy = partBy.getSubPartitionBy();

        if (subPartBy != null) {
            useSubPart = true;
            useSubPartTemplate = subPartBy.isUseSubPartTemplate();
        }

        partBy.getPartitions().stream().forEach(o -> level1PartitionNameExists.add(o.getName()));
        if (!useSubPartTemplate) {
            if (subPartBy != null) {
                tableGroupConfig.getPartitionGroupRecords().stream()
                    .forEach(o -> level2PartitionNameExists.add(o.partition_name));
            }
        } else {
            for (PartitionSpec subPartSpec : subPartBy.getPartitions()) {
                level2PartitionNameExists.add(subPartSpec.getTemplateName());
            }
        }

        allLevelPartitionNameExists.addAll(level1PartitionNameExists);
        allLevelPartitionNameExists.addAll(level2PartitionNameExists);

        List<Pair<String, String>> partitionNamesPair = sqlAlterTableGroupRenamePartition.getChangePartitionsPair();
        if (tableGroupConfig.getTableGroupRecord().isBroadCastTableGroup() || tableGroupConfig.getTableGroupRecord()
            .isSingleTableGroup()) {
            throw new TddlRuntimeException(ErrorCode.ERR_RENAME_BROADCAST_OR_SINGLE_TABLE,
                "rename partition of broadcast/single table is not allow");
        }
        for (Pair<String, String> pair : partitionNamesPair) {
            if (oldPartitionNames.contains(pair.getKey().toLowerCase())) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                    "reChange partition:" + pair.getKey() + " in the same statement is not allow");
            } else {
                oldPartitionNames.add(pair.getKey().toLowerCase());
            }
            if (newPartitionNames.contains(pair.getValue().toLowerCase())) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                    "duplicate partition name:" + pair.getValue());
            } else {
                newPartitionNames.add(pair.getValue().toLowerCase());
            }
            if (renameSubPartition) {
                if (!level2PartitionNameExists.contains(pair.getKey().toLowerCase())) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                        "subpartition name:" + pair.getKey() + " is not exists");
                }
            } else {
                if (!level1PartitionNameExists.contains(pair.getKey().toLowerCase())) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                        "partition name:" + pair.getKey() + " is not exists");
                }
            }
            if (KeyWordsUtil.isKeyWord(pair.getValue())) {
                throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                    String.format("Failed to execute this command because the string of partName[%s] is a keyword",
                        pair.getValue()));
            }
            if (pair.getValue().length() > pair.getKey().length()) {
                String nameVal = pair.getValue();
                if (!useSubPartTemplate) {
                    if (renameSubPartition) {
                        PartitionNameUtil.validatePartName(nameVal, KeyWordsUtil.isKeyWord(nameVal), true);
                    } else {
                        PartitionNameUtil.validatePartName(nameVal, KeyWordsUtil.isKeyWord(nameVal), false);
                    }
                } else {
                    if (renameSubPartition) {
                        for (String existName : level1PartitionNameExists) {
                            String newPartName = existName + pair.getValue();
                            PartitionNameUtil.validatePartName(newPartName, KeyWordsUtil.isKeyWord(newPartName), true);
                        }
                    } else {
                        for (String existName : level2PartitionNameExists) {
                            String newPartName = pair.getValue() + existName;
                            PartitionNameUtil.validatePartName(newPartName, KeyWordsUtil.isKeyWord(newPartName), false);
                        }
                    }
                }
            }
        }
        boolean renameSubTempPartition = renameSubPartition && useSubPartTemplate;
        for (Pair<String, String> pair : partitionNamesPair) {
            if (!oldPartitionNames.contains(pair.getValue())) {
                if (!renameSubTempPartition) {
                    if (allLevelPartitionNameExists.contains(pair.getValue().toLowerCase())) {
                        throw new TddlRuntimeException(ErrorCode.ERR_DUPLICATED_PARTITION_NAME,
                            "partition name:" + pair.getValue() + " is exists");
                    }
                } else {
                    for (String l1Name : level1PartitionNameExists) {
                        if (level1PartitionNameExists.contains(l1Name + pair.getValue())) {
                            throw new TddlRuntimeException(ErrorCode.ERR_DUPLICATED_PARTITION_NAME,
                                "partition name:" + l1Name + pair.getValue() + " is exists");
                        }
                        if (level2PartitionNameExists.contains(l1Name + pair.getValue())) {
                            throw new TddlRuntimeException(ErrorCode.ERR_DUPLICATED_PARTITION_NAME,
                                "template partition name:" + l1Name + pair.getValue() + " is exists");
                        }
                    }
                    if (level2PartitionNameExists.contains(pair.getValue())) {
                        throw new TddlRuntimeException(ErrorCode.ERR_DUPLICATED_PARTITION_NAME,
                            "partition name:" + pair.getValue() + " is exists");
                    }
                }
            }
        }
    }

    private static void alterTableGroupAddPartitionCheck(TableGroupConfig tableGroupConfig, String schemaName,
                                                         SqlAlterTableAddPartition sqlAlterTableAddPartition) {
        String tgSchema = tableGroupConfig.getTableGroupRecord().getSchema();
        final SchemaManager schemaManager = OptimizerContext.getContext(tgSchema).getLatestSchemaManager();
        String tgName = tableGroupConfig.getTableGroupRecord().getTg_name();
        String firstTableInCurrentGroup = tableGroupConfig.getAllTables().get(0).getLogTbRec().tableName;
        PartitionInfo partitionInfo = schemaManager.getTable(firstTableInCurrentGroup).getPartitionInfo();
        if (partitionInfo.isSingleTable() || partitionInfo.isBroadcastTable()) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                "can't add the partition group for single/broadcast tables");
        }
        PartitionStrategy strategy = partitionInfo.getPartitionBy().getStrategy();

        List<SqlPartition> newPartitions = new ArrayList<>();
        List<SqlNode> newPartAst = sqlAlterTableAddPartition.getPartitions();
        for (int i = 0; i < newPartAst.size(); i++) {
            newPartitions.add((SqlPartition) newPartAst.get(i));
        }

        //to check: default partition must be the last partition
        int defaultPartitionPosition = -1;
        boolean containDefaultPartition = false;
        for (int index = 0; index < newPartitions.size(); index++) {
            SqlPartition sqlPartition = newPartitions.get(index);
            if (sqlPartition.getValues() != null && sqlPartition.getValues().getItems().size() != 0) {
                List<SqlPartitionValueItem> items = sqlPartition.getValues().getItems();
                for (SqlPartitionValueItem sqlPartitionValueItem : items) {
                    if (sqlPartitionValueItem.getValue() != null
                        && sqlPartitionValueItem.getValue().getKind() == SqlKind.DEFAULT) {
                        containDefaultPartition = true;
                        defaultPartitionPosition = index;
                        break;
                    }
                }
            }
            if (containDefaultPartition) {
                break;
            }
        }
        if (containDefaultPartition && defaultPartitionPosition != newPartitions.size() - 1) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                "default partition must be the last partition");
        }

//        GetNewActPartColCntFromAstParams params = new GetNewActPartColCntFromAstParams();
//        List<Integer> allLevelFullPartColCnts = partitionInfo.getAllLevelFullPartColCounts();
//        List<Integer> allLevelActPartColCnts = partitionInfo.getAllLevelActualPartColCounts();
//        List<PartitionStrategy> allLevelPartStrategies = partitionInfo.getAllLevelPartitionStrategies();
//        params.setAllLevelFullPartColCnts(allLevelFullPartColCnts);
//        params.setAllLevelActualPartColCnts(allLevelActPartColCnts);
//        params.setAllLevelStrategies(allLevelPartStrategies);
//        params.setNewPartitions(newPartitions);
//        List<Integer> newActualPartColCnts = PartitionInfoUtil.getNewAllLevelPrefixPartColCntBySqlPartitionAst(params);

        List<Integer> newActualPartColCnts =
            PartitionInfoUtil.getNewAllLevelPrefixPartColCntByPartInfoAndNewParts(true, partitionInfo, newPartitions,
                false);

        if (strategy == PartitionStrategy.RANGE_COLUMNS || strategy == PartitionStrategy.KEY) {
            /**
             * Check if is allowed to use newPrefixPartColCnt as new partColCnt for all tables in table group
             */
            alterTableGroupCheckPartitionColumn(schemaName, tgName, newActualPartColCnts);
        } else {
            alterTableGroupCheckPartitionColumn(schemaName, tgName,
                PartitionInfoUtil.ALL_LEVEL_FULL_PART_COL_COUNT_LIST);
        }
    }

    private static void alterTableGroupCheckPartitionColumn(String schemaName, String tableGroup,
                                                            List<Integer> allLevelActualPartColCnts) {
        boolean identical =
            PartitionInfoUtil.allTablesWithIdenticalPartitionColumns(schemaName, tableGroup, allLevelActualPartColCnts);
        if (!identical) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                String.format("not all the tables in tablegroup[%s] has identical partition columns", tableGroup));
        }
    }

    public static void mergeTableGroupCheck(MergeTableGroupPreparedData preparedData, ExecutionContext ec) {
        String targetTableGroup = preparedData.getTargetTableGroupName();
        PartitionInfoManager partitionInfoManager =
            OptimizerContext.getContext(preparedData.getSchemaName()).getPartitionInfoManager();
        TableGroupConfig tableGroupConfig = preparedData.getTableGroupConfigMap().get(targetTableGroup);
        if (tableGroupConfig == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_TABLE_GROUP_NOT_EXISTS,
                "tablegroup:[" + targetTableGroup + "] is not exists");
        }
        if (GeneralUtil.isEmpty(tableGroupConfig.getAllTables())) {
            throw new TddlRuntimeException(ErrorCode.ERR_TABLE_GROUP_IS_EMPTY,
                "it not allow to merge tables into empty tablegroup");
        }
        String firstTable = tableGroupConfig.getAllTables().get(0).getTableName();
        PartitionInfo partitionInfo = partitionInfoManager.getPartitionInfo(firstTable);
        TableMeta tableMeta = ec.getSchemaManager(preparedData.getSchemaName()).getTable(firstTable);
        if (tableMeta.isGsi()) {
            String pt = tableMeta.getGsiTableMetaBean().gsiMetaBean.tableName;
            tableMeta = ec.getSchemaManager(preparedData.getSchemaName()).getTable(pt);
        }
        JoinGroupInfoRecord joinGroupInfoRecord =
            JoinGroupUtils.getJoinGroupInfoByTable(preparedData.getSchemaName(), tableMeta.getTableName(), null);
        String joinGroupName = joinGroupInfoRecord == null ? "" : joinGroupInfoRecord.joinGroupName;

        for (String tableGroup : preparedData.getSourceTableGroups()) {
            tableGroupConfig = preparedData.getTableGroupConfigMap().get(tableGroup);
            if (tableGroupConfig == null) {
                throw new TddlRuntimeException(ErrorCode.ERR_TABLE_GROUP_NOT_EXISTS,
                    "tablegroup:[" + tableGroup + "] is not exists");
            }
            String errMsg =
                String.format("The joinGroup of tableGroup:[%s] is not match with the joinGroup of tableGroup[%s]",
                    tableGroup, targetTableGroup);
            if (GeneralUtil.isNotEmpty(tableGroupConfig.getAllTables())) {
                String sourceTable = tableGroupConfig.getAllTables().get(0).getTableName();
                PartitionInfo sourcePartitionInfo = partitionInfoManager.getPartitionInfo(sourceTable);
                if (!sourcePartitionInfo.equals(partitionInfo)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                        String.format("the partition definition of the tableGroup:[%s] mismatch the tableGroup[%s]",
                            tableGroup, targetTableGroup));
                }
                boolean allPartLocIsSame = true;
                for (int i = 0; i < partitionInfo.getPartitionBy().getPartitions().size(); i++) {
                    PartitionSpec sourcePartSpec = sourcePartitionInfo.getPartitionBy().getPartitions().get(i);
                    PartitionSpec targetPartSpec = partitionInfo.getPartitionBy().getPartitions().get(i);
                    if (!sourcePartSpec.getLocation().getGroupKey()
                        .equalsIgnoreCase(targetPartSpec.getLocation().getGroupKey())) {
                        allPartLocIsSame = false;
                        break;
                    }
                }
                if (!allPartLocIsSame && !preparedData.isForce()) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                        String.format("the physical location of %s is not the same as %s", tableGroup,
                            targetTableGroup));
                }
                JoinGroupValidator.validateJoinGroupInfo(preparedData.getSchemaName(), tableGroup, joinGroupName,
                    errMsg, ec, null);
            }
        }
    }

    public static void alterTableGroupAddTableCheck(AlterTableGroupAddTablePreparedData preparedData,
                                                    ExecutionContext ec) {
        String targetTableGroup = preparedData.getTableGroupName();
        PartitionInfoManager partitionInfoManager =
            OptimizerContext.getContext(preparedData.getSchemaName()).getPartitionInfoManager();
        TableGroupConfig tableGroupConfig = preparedData.getTableGroupConfig();
        if (tableGroupConfig == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_TABLE_GROUP_NOT_EXISTS,
                "tablegroup:[" + targetTableGroup + "] is not exists");
        }
        PartitionInfo partitionInfo = null;
        if (GeneralUtil.isNotEmpty(tableGroupConfig.getAllTables())) {
            String firstTable = tableGroupConfig.getAllTables().get(0).getTableName();
            partitionInfo = partitionInfoManager.getPartitionInfo(firstTable);
        } else {
            partitionInfo = partitionInfoManager.getPartitionInfo(preparedData.getReferenceTable());
        }

        Set<String> tableToBeExcluded = new TreeSet<>(String::compareToIgnoreCase);
        boolean emptyGroup = preparedData.getReferenceTable() != null;

        for (String tableName : preparedData.getTables()) {
            PartitionInfo sourcePartitionInfo = partitionInfoManager.getPartitionInfo(tableName);
            if (sourcePartitionInfo == null) {
                throw new TddlRuntimeException(ErrorCode.ERR_UNKNOWN_TABLE,
                    String.format("the table:[%s] not exists", tableName));
            }
            if (emptyGroup && tableName.equalsIgnoreCase(preparedData.getReferenceTable())) {
                continue;
            }
            if (!emptyGroup && sourcePartitionInfo.getTableGroupId().longValue() == partitionInfo.getTableGroupId()
                .longValue()) {
                //remove those tables already in target tableGroup yet
                tableToBeExcluded.add(tableName);
                continue;
            }
            boolean match = sourcePartitionInfo.equals(partitionInfo);

            if (!match && !preparedData.isForce()) {
                if (GeneralUtil.isNotEmpty(tableGroupConfig.getAllTables())) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                        String.format("the partition definition of the table:[%s] does not match the tableGroup[%s]",
                            tableName, targetTableGroup));
                } else {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                        String.format("the partition definition of the table:[%s] is different from table:[%s]",
                            tableName, partitionInfo.getTableName()));
                }
            } else if (match) {
                boolean allPartLocIsSame = true;
                for (int i = 0; i < partitionInfo.getPartitionBy().getPartitions().size(); i++) {
                    PartitionSpec sourcePartSpec = sourcePartitionInfo.getPartitionBy().getPartitions().get(i);
                    PartitionSpec targetPartSpec = partitionInfo.getPartitionBy().getPartitions().get(i);
                    if (!sourcePartSpec.getLocation().getGroupKey()
                        .equalsIgnoreCase(targetPartSpec.getLocation().getGroupKey())) {
                        allPartLocIsSame = false;
                        break;
                    }
                }
                if (!allPartLocIsSame && !preparedData.isForce()) {
                    if (emptyGroup) {
                        throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                            String.format("the physical location of %s is not match with %s", tableName,
                                partitionInfo.getTableName()));
                    } else {
                        throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                            String.format("the physical location of %s is not match with tables in %s", tableName,
                                targetTableGroup));
                    }
                }
            }
        }
        if (GeneralUtil.isNotEmpty(tableToBeExcluded)) {
            preparedData.getTables().removeAll(tableToBeExcluded);
        }
        TableMeta tableMeta = ec.getSchemaManager(preparedData.getSchemaName()).getTable(partitionInfo.getTableName());
        if (tableMeta.isGsi()) {
            String pt = tableMeta.getGsiTableMetaBean().gsiMetaBean.tableName;
            tableMeta = ec.getSchemaManager(preparedData.getSchemaName()).getTable(pt);
        }
        JoinGroupInfoRecord joinGroupInfoRecord =
            JoinGroupUtils.getJoinGroupInfoByTable(preparedData.getSchemaName(), tableMeta.getTableName(), null);
        String targetJoinGroupName = joinGroupInfoRecord == null ? "" : joinGroupInfoRecord.joinGroupName;
        if (GeneralUtil.isNotEmpty(preparedData.getTables())) {
            for (Map.Entry<String, Long> entry : preparedData.getTableVersions().entrySet()) {
                String tableName = entry.getKey();
                joinGroupInfoRecord =
                    JoinGroupUtils.getJoinGroupInfoByTable(preparedData.getSchemaName(), tableName, null);
                String sourceJoinGroupName = joinGroupInfoRecord == null ? "" : joinGroupInfoRecord.joinGroupName;
                boolean isValid = targetJoinGroupName.equalsIgnoreCase(sourceJoinGroupName);
                if (!isValid) {
                    String errMsg =
                        String.format("The joinGroup of table:[%s] is not match with the joinGroup of tableGroup[%s]",
                            tableName, targetTableGroup);
                    throw new TddlRuntimeException(ErrorCode.ERR_JOIN_GROUP_NOT_MATCH, errMsg);
                }
            }
        }
    }

    public static SqlNode getSqlTemplate(String schemaName, String logicalTableName, String sqlTemplateStr,
                                         ExecutionContext executionContext) {
        return PartitionUtils.getSqlTemplate(schemaName, logicalTableName, sqlTemplateStr, executionContext);
    }

    public static String fetchCreateTableDefinition(RelNode relNode, ExecutionContext executionContext, String groupKey,
                                                    String phyTableName, String schemaName) {
        Cursor cursor = null;
        String primaryTableDefinition = null;
        try {
            cursor = ExecutorHelper.execute(new PhyShow(relNode.getCluster(), relNode.getTraitSet(),
                SqlShowCreateTable.create(SqlParserPos.ZERO, new SqlIdentifier(phyTableName, SqlParserPos.ZERO)),
                relNode.getRowType(), groupKey, phyTableName, schemaName), executionContext);
            Row row = null;
            if (cursor != null && (row = cursor.next()) != null) {
                primaryTableDefinition = row.getString(1);
                assert primaryTableDefinition.substring(0, 13).trim().equalsIgnoreCase("CREATE TABLE");
            }
        } finally {
            if (null != cursor) {
                cursor.close(new ArrayList<>());
            }
        }
        return primaryTableDefinition;
    }

    public static PartitionInfo getPartitionInfo(String tableGroupName, String schemaName) {
        final SchemaManager schemaManager = OptimizerContext.getContext(schemaName).getLatestSchemaManager();
        final TableGroupInfoManager tableGroupInfoManager =
            OptimizerContext.getContext(schemaName).getTableGroupInfoManager();
        TableGroupConfig tableGroupConfig = tableGroupInfoManager.getTableGroupConfigByName(tableGroupName);

        if (tableGroupConfig == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_TABLE_GROUP_NOT_EXISTS,
                "tablegroup:" + tableGroupName + " doesn't exists");
        }

        if (tableGroupConfig.getTableCount() == 0) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                "can't modify the tablegroup:" + tableGroupName + " when it's empty");
        }

        String tableInCurrentGroup = tableGroupConfig.getAllTables().get(0).getLogTbRec().tableName;
        TableMeta tableMeta = schemaManager.getTable(tableInCurrentGroup);
        PartitionInfo partitionInfo = tableMeta.getPartitionInfo();

        return partitionInfo;
    }

    /**
     * convert
     * sql 'ALTER TABLEGROUP/TABLE [tgName] extract to PARTITION [newPartitionName] by hot value ([hotValue]);'
     * to
     * sql 'ALTER TABLEGROUP/TABLE [tgName] split into [splitPartition] partitions 1 by hot value ([hotValue]);'
     * <p>
     * [splitPartition]. Is the partition that hot key belongs to.
     * [newPartitionName]. Is the new partition name from user input.
     * [hotValue]. Is the hot key(user input).
     * [restValues]. Is the split partition's rest values after we eliminate the hot key.
     * the target partition to be changed of this TABLEGROUP/TABLE IS NOT list strategy.
     * <p>
     */
    public static String convertExtractPartitionToSplitPartitionSql(
        LogicalAlterTableExtractPartition extractPartitionPlan, boolean isAlterTable,
        ExecutionContext executionContext) {
        final String SPLIT_SQL = isAlterTable ? "ALTER TABLE {0} SPLIT INTO {1} PARTITIONS 1 BY HOT VALUE({2});" :
            "ALTER TABLEGROUP {0} SPLIT INTO {1} PARTITIONS 1 BY HOT VALUE({2})";
        PartitionInfo partitionInfo;
        SqlAlterTableExtractPartition sqlAlterTableExtractPartition;
        Map<SqlNode, RexNode> partBoundExprInfo;
        String objectName;
        String hint = StringUtils.EMPTY;
        if (isAlterTable) {
            AlterTable alterTable = (AlterTable) extractPartitionPlan.relDdl;
            objectName = Util.last(((SqlIdentifier) alterTable.getTableName()).names);
            partitionInfo = executionContext.getSchemaManager(extractPartitionPlan.getSchemaName()).getTddlRuleManager()
                .getPartitionInfoManager().getPartitionInfo(objectName);
            SqlAlterTable sqlAlterTable = (SqlAlterTable) alterTable.getSqlNode();
            sqlAlterTableExtractPartition = (SqlAlterTableExtractPartition) sqlAlterTable.getAlters().get(0);
        } else {
            objectName = extractPartitionPlan.getTableName();
            partitionInfo = getPartitionInfo(objectName, extractPartitionPlan.getSchemaName());
            SqlAlterTableGroup sqlAlterTableGroup = (SqlAlterTableGroup) ((extractPartitionPlan).relDdl.getSqlNode());
            sqlAlterTableExtractPartition = (SqlAlterTableExtractPartition) sqlAlterTableGroup.getAlters().get(0);
        }

        PartitionStrategy strategy = partitionInfo.getPartitionBy().getStrategy();
        if (strategy == PartitionStrategy.LIST_COLUMNS || strategy == PartitionStrategy.LIST) {
            throw new UnsupportedOperationException("unknown unexpected strategy: " + strategy);
        }

        String newPartitionName = sqlAlterTableExtractPartition.getHotKeyPartitionName() != null ?
            sqlAlterTableExtractPartition.getHotKeyPartitionName().toString() : StringUtils.EMPTY;
        String splitSql = MessageFormat.format(SPLIT_SQL, objectName, newPartitionName,
            sqlAlterTableExtractPartition.getHotKeys().stream().map(o -> o.toString())
                .collect(Collectors.joining(", ")));

        return splitSql;
    }

    public static String convertExtractToSplitSqlForSubpartition(LogicalAlterTableExtractPartition extractPartitionPlan,
                                                                 boolean isAlterTable,
                                                                 ExecutionContext executionContext) {
        final String SPLIT_SQL_FOR_TEMPLATE =
            isAlterTable ? "alter table {0} split into {1} subpartitions 1 by hot value{2}"
                : "alter tablegroup {0} split into {1} subpartitions 1 by hot value{2}";
        final String SPLIT_SQL_FOR_NON_TEMPLATE =
            isAlterTable ? "alter table {0} modify partition {1} split into {2} subpartitions 1 by hot value{3}"
                : "alter tablegroup {0} modify partition {1} split into {2} subpartitions 1 by hot value{3}";

        PartitionInfo partitionInfo;
        SqlAlterTableExtractPartition sqlAlterTableExtractPartition;
        Map<SqlNode, RexNode> partBoundExprInfo;
        String objectName;
        String hint = StringUtils.EMPTY;
        if (isAlterTable) {
            AlterTable alterTable = (AlterTable) extractPartitionPlan.relDdl;
            objectName = Util.last(((SqlIdentifier) alterTable.getTableName()).names);
            partitionInfo = executionContext.getSchemaManager(extractPartitionPlan.getSchemaName()).getTddlRuleManager()
                .getPartitionInfoManager().getPartitionInfo(objectName);
            SqlAlterTable sqlAlterTable = (SqlAlterTable) alterTable.getSqlNode();
            sqlAlterTableExtractPartition = (SqlAlterTableExtractPartition) sqlAlterTable.getAlters().get(0);
        } else {
            objectName = extractPartitionPlan.getTableName();
            partitionInfo = getPartitionInfo(objectName, extractPartitionPlan.getSchemaName());
            SqlAlterTableGroup sqlAlterTableGroup = (SqlAlterTableGroup) ((extractPartitionPlan).relDdl.getSqlNode());
            sqlAlterTableExtractPartition = (SqlAlterTableExtractPartition) sqlAlterTableGroup.getAlters().get(0);
        }

        PartitionStrategy strategy = partitionInfo.getPartitionBy().getSubPartitionBy().getStrategy();
        if (strategy == PartitionStrategy.LIST_COLUMNS || strategy == PartitionStrategy.LIST) {
            throw new UnsupportedOperationException("unknown unexpected strategy: " + strategy);
        }

        boolean isTemplateSubpartition = partitionInfo.getPartitionBy().getSubPartitionBy().isUseSubPartTemplate();

        String newSubPartitionName = sqlAlterTableExtractPartition.getHotKeyPartitionName() != null ?
            sqlAlterTableExtractPartition.getHotKeyPartitionName().toString() : StringUtils.EMPTY;

        String firstLvlPartitionTobeModified = null;
        if (!isTemplateSubpartition) {
            if (sqlAlterTableExtractPartition.getParentPartitions().isEmpty()) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                    "please specify the logical partition name");
            }
            SqlPartition parentPartition = (SqlPartition) sqlAlterTableExtractPartition.getParentPartitions().get(0);
            firstLvlPartitionTobeModified = parentPartition.getName().toString();
        }

        if (isTemplateSubpartition) {
            return MessageFormat.format(SPLIT_SQL_FOR_TEMPLATE, objectName, newSubPartitionName,
                sqlAlterTableExtractPartition.getHotKeys().stream().map(o -> o.toString())
                    .collect(Collectors.joining(", ")));
        } else {
            return MessageFormat.format(SPLIT_SQL_FOR_NON_TEMPLATE, objectName, firstLvlPartitionTobeModified,
                newSubPartitionName,
                sqlAlterTableExtractPartition.getHotKeys().stream().map(o -> o.toString())
                    .collect(Collectors.joining(", "))
            );
        }
    }

    /**
     * convert
     * sql 'ALTER TABLEGROUP/TABLE [tgName] extract to PARTITION [newPartitionName] by hot value ([hotValue]);'
     * to
     * sql 'ALTER TABLEGROUP/TABLE [tgName] split PARTITION [splitPartition] into (PARTITION [splitPartition] VALUES IN([restValues]), PARTITION [newPartitionName] VALUES IN([hotValue]))'
     * <p>
     * [splitPartition]. Is the partition that hot key belongs to.
     * [newPartitionName]. Is the new partition name from user input.
     * [hotValue]. Is the hot key(user input).
     * [restValues]. Is the split partition's rest values after we eliminate the hot key.
     * the target partition to be changed of this TABLEGROUP/TABLE IS list strategy.
     * <p>
     * need to check:
     * 1. we use hot key to find the split partition, so, hot key must exist in one of partitions.
     * if hot key not found in each partition,
     * - if we have default partition, the split partition is it.
     * - if we don't have default partition, throw exception.
     * 2. new partition's name can't be same as existing partition name.
     */
    public static String convertExtractListRelToSplitListSql(LogicalAlterTableExtractPartition extractPartitionPlan,
                                                             boolean isAlterTable, ExecutionContext executionContext) {
        final String SPLIT_SQL = isAlterTable ? "ALTER TABLE {0} split PARTITION {1} into ({2});" :
            "ALTER TABLEGROUP {0} split PARTITION {1} into ({2});";
        final String PARTITION_DEF = "PARTITION {0} VALUES IN({1})";
        PartitionInfo partitionInfo;
        SqlAlterTableExtractPartition sqlAlterTableExtractPartition;
        Map<SqlNode, RexNode> partBoundExprInfo;
        String objectName;
        if (isAlterTable) {
            AlterTable alterTable = (AlterTable) extractPartitionPlan.relDdl;
            objectName = Util.last(((SqlIdentifier) alterTable.getTableName()).names);
            partitionInfo = executionContext.getSchemaManager(extractPartitionPlan.getSchemaName()).getTddlRuleManager()
                .getPartitionInfoManager().getPartitionInfo(objectName);
            partBoundExprInfo = alterTable.getAllRexExprInfo();
            SqlAlterTable sqlAlterTable = (SqlAlterTable) alterTable.getSqlNode();
            sqlAlterTableExtractPartition = (SqlAlterTableExtractPartition) sqlAlterTable.getAlters().get(0);
        } else {
            objectName = extractPartitionPlan.getTableName();
            partitionInfo = getPartitionInfo(objectName, extractPartitionPlan.getSchemaName());
            SqlAlterTableGroup sqlAlterTableGroup = (SqlAlterTableGroup) ((extractPartitionPlan).relDdl.getSqlNode());
            partBoundExprInfo = sqlAlterTableGroup.getPartRexInfoCtxByLevel().get(PARTITION_LEVEL_PARTITION);
            sqlAlterTableExtractPartition = (SqlAlterTableExtractPartition) sqlAlterTableGroup.getAlters().get(0);
        }

        PartitionStrategy strategy = partitionInfo.getPartitionBy().getStrategy();
        int partitionColumnCnt = partitionInfo.getPartitionBy().getPartitionExprTypeList().size();
        boolean isMultiColumns = (partitionColumnCnt > 1);

        //convert hotKey sqlNode to SearchDatumInfo
        SearchDatumComparator comparator = partitionInfo.getPartitionBy().getPruningSpaceComparator();
        PartitionIntFunction partIntFunc = partitionInfo.getPartitionBy().getPartIntFunc();

        if (sqlAlterTableExtractPartition.getHotKeys() == null
            || sqlAlterTableExtractPartition.getHotKeys().size() != 1) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                "only allows to extract one key each time");
        }

//        SqlAlterTableGroup sqlNode =
//            (SqlAlterTableGroup) (((LogicalAlterTableGroupExtractPartition) extractPartitionPlan).relDdl.getSqlNode());
//        SqlAlterTableGroupExtractPartition sqlAlterTableGroupExtractPartition =
//            (SqlAlterTableGroupExtractPartition) sqlNode.getAlters().get(0);
//        if (sqlAlterTableGroupExtractPartition.getHotKeys() == null
//            || sqlAlterTableGroupExtractPartition.getHotKeys().size() != 1) {
//            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
//                "only allows to extract one key each time");
//        }

        SqlNode hotKeyItem = sqlAlterTableExtractPartition.getHotKeys().get(0);
        List<PartitionBoundVal> oneBndVal;
        SearchDatumInfo hotKeyDatum;
        //todo: List and single List Columns are same?
        if (strategy == PartitionStrategy.LIST || strategy == PartitionStrategy.LIST_COLUMNS && !isMultiColumns) {
            RexNode bndExprRex = partBoundExprInfo.get(hotKeyItem);
            RelDataType bndValDt = comparator.getDatumRelDataTypes()[0];
            PartitionInfoUtil.validateBoundValueExpr(bndExprRex, bndValDt, partIntFunc,
                partitionInfo.getPartitionBy().getStrategy());
            PartitionBoundVal bndVal =
                PartitionPrunerUtils.getBoundValByRexExpr(bndExprRex, bndValDt, PartFieldAccessType.DDL_EXECUTION,
                    executionContext);
            oneBndVal = Collections.singletonList(bndVal);
            hotKeyDatum = new SearchDatumInfo(oneBndVal);
        } else {
            //for list columns
            if (!(hotKeyItem instanceof SqlCall) || hotKeyItem.getKind() != SqlKind.ROW) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                    "key " + hotKeyItem.toString() + " is not row type");
            }
            RexNode bndExprRex = partBoundExprInfo.get(hotKeyItem);
            oneBndVal = new ArrayList<>();
            for (int i = 0; i < ((RexCall) bndExprRex).getOperands().size(); i++) {
                RexNode rexOperand = ((RexCall) bndExprRex).getOperands().get(i);
                RelDataType bndValDt = comparator.getDatumRelDataTypes()[i];
                PartitionInfoUtil.validateBoundValueExpr(rexOperand, bndValDt, partIntFunc,
                    partitionInfo.getPartitionBy().getStrategy());
                PartitionBoundVal bndVal =
                    PartitionPrunerUtils.getBoundValByRexExpr(rexOperand, bndValDt, PartFieldAccessType.DDL_EXECUTION,
                        executionContext);
                oneBndVal.add(bndVal);
            }
            hotKeyDatum = new SearchDatumInfo(oneBndVal);
        }

        //get new partition's name(convert to lower case)
        String newPartitionName =
            PartitionNameUtil.toLowerCase(sqlAlterTableExtractPartition.getHotKeyPartitionName().getSimpleName());
//        String newPartitionName =
//            PartitionNameUtil.toLowerCase(sqlAlterTableGroupExtractPartition.getHotKeyPartitionName().getSimpleName());

        //use hot key to find split partition
        boolean hasDefaultPartition = false;
        PartitionSpec splitPartitionSpec = null, defaultPartitionSpec = null;
        for (PartitionSpec spec : partitionInfo.getPartitionBy().getOrderedPartitionSpecs()) {
            if (spec.isDefaultPartition()) {
                hasDefaultPartition = true;
                defaultPartitionSpec = spec;
            }
            if (spec.getName().equals(newPartitionName)) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                    "new partition name [" + newPartitionName + "] duplicate with existing partition");
            }
            MultiValuePartitionBoundSpec listBoundSpec = (MultiValuePartitionBoundSpec) spec.getBoundSpec();
            for (SearchDatumInfo datumInfo : listBoundSpec.getMultiDatums()) {
                if (datumInfo.equals(hotKeyDatum)) {
                    splitPartitionSpec = spec;
                }
            }
        }
        if (splitPartitionSpec == null) {
            if (hasDefaultPartition) {
                splitPartitionSpec = defaultPartitionSpec;
            } else {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                    "key " + hotKeyDatum.toString() + " is not contained in any partition");
            }
        }

        //eliminate hot key from split partition
        List<SearchDatumInfo> newDatumInfos = new ArrayList<>();
        PartitionSpec tobePrintSplitPartitionSpec = splitPartitionSpec.copy();
        tobePrintSplitPartitionSpec.setLogical(false);
        MultiValuePartitionBoundSpec listBoundSpec =
            (MultiValuePartitionBoundSpec) tobePrintSplitPartitionSpec.getBoundSpec();
        for (SearchDatumInfo datumInfo : listBoundSpec.getMultiDatums()) {
            if (datumInfo.equals(hotKeyDatum)) {
                continue;
            }
            newDatumInfos.add(datumInfo);
        }
        MultiValuePartitionBoundSpec newListBoundSpec = new MultiValuePartitionBoundSpec();
        newListBoundSpec.setMultiDatums(newDatumInfos);
        tobePrintSplitPartitionSpec.setBoundSpec(newListBoundSpec);

        String newVal = newListBoundSpec.toString();
        String newPartition = MessageFormat.format(PARTITION_DEF, newPartitionName, hotKeyDatum.toString());
        String oldPartition =
            newVal.isEmpty() ? null : tobePrintSplitPartitionSpec.toString();
        String partitions = oldPartition == null ? newPartition : newPartition + " , " + oldPartition;

        String splitSql =
            MessageFormat.format(SPLIT_SQL, objectName, tobePrintSplitPartitionSpec.getName(), partitions);
        return splitSql;
    }

    public static String convertExtractListToSplitListForSubpartition(
        LogicalAlterTableExtractPartition extractPartitionPlan,
        boolean isAlterTable, ExecutionContext executionContext) {
        final String SPLIT_SQL = isAlterTable ? "alter table {0} split subpartition {1} into ({2})"
            : "alter tablegroup {0} split subpartition {1} into ({2})";

        final String SUBPARTITION_DEF = "subpartition {0} values in({1})";
        PartitionInfo partitionInfo;
        SqlAlterTableExtractPartition sqlAlterTableExtractPartition;
        Map<SqlNode, RexNode> partBoundExprInfo;
        String objectName;
        if (isAlterTable) {
            AlterTable alterTable = (AlterTable) extractPartitionPlan.relDdl;
            objectName = Util.last(((SqlIdentifier) alterTable.getTableName()).names);
            partitionInfo = executionContext.getSchemaManager(extractPartitionPlan.getSchemaName()).getTddlRuleManager()
                .getPartitionInfoManager().getPartitionInfo(objectName);
            partBoundExprInfo = alterTable.getAllRexExprInfo();
            SqlAlterTable sqlAlterTable = (SqlAlterTable) alterTable.getSqlNode();
            sqlAlterTableExtractPartition = (SqlAlterTableExtractPartition) sqlAlterTable.getAlters().get(0);
        } else {
            objectName = extractPartitionPlan.getTableName();
            partitionInfo = getPartitionInfo(objectName, extractPartitionPlan.getSchemaName());
            SqlAlterTableGroup sqlAlterTableGroup = (SqlAlterTableGroup) ((extractPartitionPlan).relDdl.getSqlNode());
            partBoundExprInfo = sqlAlterTableGroup.getPartRexInfoCtxByLevel().get(PARTITION_LEVEL_PARTITION);
            sqlAlterTableExtractPartition = (SqlAlterTableExtractPartition) sqlAlterTableGroup.getAlters().get(0);
        }

        PartitionStrategy strategy = partitionInfo.getPartitionBy().getSubPartitionBy().getStrategy();
        int partitionColumnCnt = partitionInfo.getPartitionBy().getSubPartitionBy().getPartitionExprTypeList().size();
        boolean isMultiColumns = (partitionColumnCnt > 1);

        SearchDatumComparator comparator =
            partitionInfo.getPartitionBy().getSubPartitionBy().getPruningSpaceComparator();
        PartitionIntFunction partIntFunc = partitionInfo.getPartitionBy().getSubPartitionBy().getPartIntFunc();

        boolean isTemplateSubpartition = partitionInfo.getPartitionBy().getSubPartitionBy().isUseSubPartTemplate();

        if (sqlAlterTableExtractPartition.getHotKeys() == null
            || sqlAlterTableExtractPartition.getHotKeys().size() != 1) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                "only allows to extract one key each time");
        }

        SqlNode hotKeyItem = sqlAlterTableExtractPartition.getHotKeys().get(0);
        List<PartitionBoundVal> oneBndVal;
        SearchDatumInfo hotKeyDatum;
        if (strategy == PartitionStrategy.LIST || strategy == PartitionStrategy.LIST_COLUMNS && !isMultiColumns) {
            RexNode bndExprRex = partBoundExprInfo.get(hotKeyItem);
            RelDataType bndValDt = comparator.getDatumRelDataTypes()[0];
            PartitionInfoUtil.validateBoundValueExpr(bndExprRex, bndValDt, partIntFunc,
                partitionInfo.getPartitionBy().getSubPartitionBy().getStrategy());
            PartitionBoundVal bndVal =
                PartitionPrunerUtils.getBoundValByRexExpr(bndExprRex, bndValDt, PartFieldAccessType.DDL_EXECUTION,
                    executionContext);
            oneBndVal = Collections.singletonList(bndVal);
            hotKeyDatum = new SearchDatumInfo(oneBndVal);
        } else {
            //for list columns
            if (!(hotKeyItem instanceof SqlCall) || hotKeyItem.getKind() != SqlKind.ROW) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                    "key " + hotKeyItem.toString() + " is not row type");
            }
            RexNode bndExprRex = partBoundExprInfo.get(hotKeyItem);
            oneBndVal = new ArrayList<>();
            for (int i = 0; i < ((RexCall) bndExprRex).getOperands().size(); i++) {
                RexNode rexOperand = ((RexCall) bndExprRex).getOperands().get(i);
                RelDataType bndValDt = comparator.getDatumRelDataTypes()[i];
                PartitionInfoUtil.validateBoundValueExpr(rexOperand, bndValDt, partIntFunc,
                    partitionInfo.getPartitionBy().getSubPartitionBy().getStrategy());
                PartitionBoundVal bndVal =
                    PartitionPrunerUtils.getBoundValByRexExpr(rexOperand, bndValDt, PartFieldAccessType.DDL_EXECUTION,
                        executionContext);
                oneBndVal.add(bndVal);
            }
            hotKeyDatum = new SearchDatumInfo(oneBndVal);
        }

        String newPartitionName =
            PartitionNameUtil.toLowerCase(sqlAlterTableExtractPartition.getHotKeyPartitionName().getSimpleName());
        String firstLvlPartitionName = null;
        if (!isTemplateSubpartition) {
            if (sqlAlterTableExtractPartition.getParentPartitions().isEmpty()) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                    "please specify the logical partition name");
            }
            SqlPartition parentPartition = (SqlPartition) sqlAlterTableExtractPartition.getParentPartitions().get(0);
            firstLvlPartitionName = parentPartition.getName().toString();
        }

        boolean hasSubDefaultPartition = false;
        PartitionSpec splitSubPartitionSpec = null, defaultSubPartitionSpec = null;

        if (isTemplateSubpartition) {
            for (PartitionSpec subSpec : partitionInfo.getPartitionBy().getSubPartitionBy().getPartitions()) {
                if (subSpec.isDefaultPartition()) {
                    defaultSubPartitionSpec = subSpec;
                    hasSubDefaultPartition = true;
                }
                if (subSpec.getName().equalsIgnoreCase(newPartitionName)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                        String.format("new subpartition name [%s] duplicate with existing subpartition",
                            newPartitionName));
                }

                MultiValuePartitionBoundSpec listBoundSpec = (MultiValuePartitionBoundSpec) subSpec.getBoundSpec();
                for (SearchDatumInfo datumInfo : listBoundSpec.getMultiDatums()) {
                    if (datumInfo.equals(hotKeyDatum)) {
                        splitSubPartitionSpec = subSpec;
                    }
                }
            }
        } else {
            PartitionSpec parentSpec = partitionInfo.getPartitionBy().getPartitionByPartName(firstLvlPartitionName);
            if (parentSpec == null) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                    String.format("partition %s not found", newPartitionName));
            }
            for (PartitionSpec subSpec : parentSpec.getSubPartitions()) {
                if (subSpec.isDefaultPartition()) {
                    defaultSubPartitionSpec = subSpec;
                    hasSubDefaultPartition = true;
                }
                if (subSpec.getName().equalsIgnoreCase(newPartitionName)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                        String.format("new subpartition name [%s] duplicate with existing subpartition",
                            newPartitionName));
                }

                MultiValuePartitionBoundSpec listBoundSpec = (MultiValuePartitionBoundSpec) subSpec.getBoundSpec();
                for (SearchDatumInfo datumInfo : listBoundSpec.getMultiDatums()) {
                    if (datumInfo.equals(hotKeyDatum)) {
                        splitSubPartitionSpec = subSpec;
                    }
                }
            }
        }

        if (splitSubPartitionSpec == null) {
            if (hasSubDefaultPartition) {
                splitSubPartitionSpec = defaultSubPartitionSpec;
            } else {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                    "key " + hotKeyDatum.toString() + " is not contained in any partition");
            }
        }

        PartitionSpec tobePrintSplitSubPartitionSpec = splitSubPartitionSpec.copy();
        MultiValuePartitionBoundSpec listBoundSpec =
            (MultiValuePartitionBoundSpec) tobePrintSplitSubPartitionSpec.getBoundSpec();
        List<SearchDatumInfo> newDatumInfos = listBoundSpec.getMultiDatums()
            .stream()
            .filter(datum -> !datum.equals(hotKeyDatum))
            .collect(Collectors.toList());

        MultiValuePartitionBoundSpec newListBoundSpec = new MultiValuePartitionBoundSpec();
        newListBoundSpec.setMultiDatums(newDatumInfos);
        tobePrintSplitSubPartitionSpec.setBoundSpec(newListBoundSpec);

        String newVal = newListBoundSpec.toString();
        String newPartition = MessageFormat.format(SUBPARTITION_DEF, newPartitionName, hotKeyDatum.toString());
        String oldPartition =
            newVal.isEmpty() ? null : tobePrintSplitSubPartitionSpec.toString();
        String partitions = oldPartition == null ? newPartition : newPartition + " , " + oldPartition;

        if (isTemplateSubpartition) {
            return MessageFormat.format(SPLIT_SQL, objectName, tobePrintSplitSubPartitionSpec.getName(), partitions);
        } else {
            return MessageFormat.format(SPLIT_SQL, objectName, tobePrintSplitSubPartitionSpec.getName(), partitions);
        }
    }

    /**
     * need to check:
     * 1. new partitions should not have intersection with old partitions
     * 2. new partitions should not contain default, and new partitions should not intersection with each other.
     */
    public static String convertAddListRelToSplitListSql(LogicalAlterTableAddPartition addPartitionPlan,
                                                         boolean isAlterTable, ExecutionContext executionContext) {
        final String SPLIT_TABLEGROUP_SQL = isAlterTable ? "ALTER TABLE {0} split PARTITION {1} into ({2});" :
            "ALTER TABLEGROUP {0} split PARTITION {1} into ({2});";

        PartitionInfo partitionInfo;
        SqlAlterTableAddPartition sqlAlterTableAddPartition;
        Map<SqlNode, RexNode> partBoundExprInfo;
        String objectName;
        if (isAlterTable) {
            AlterTable alterTable = (AlterTable) addPartitionPlan.relDdl;
            objectName = Util.last(((SqlIdentifier) alterTable.getTableName()).names);
            partitionInfo = executionContext.getSchemaManager(addPartitionPlan.getSchemaName()).getTddlRuleManager()
                .getPartitionInfoManager().getPartitionInfo(objectName);
            partBoundExprInfo = alterTable.getAllRexExprInfo();
            SqlAlterTable sqlAlterTable = (SqlAlterTable) alterTable.getSqlNode();
            sqlAlterTableAddPartition = (SqlAlterTableAddPartition) sqlAlterTable.getAlters().get(0);
        } else {
            objectName = addPartitionPlan.getTableName();
            partitionInfo = getPartitionInfo(objectName, addPartitionPlan.getSchemaName());
            SqlAlterTableGroup sqlAlterTableGroup = (SqlAlterTableGroup) ((addPartitionPlan).relDdl.getSqlNode());
            partBoundExprInfo = sqlAlterTableGroup.getPartRexInfoCtxByLevel().get(PARTITION_LEVEL_PARTITION);
            sqlAlterTableAddPartition = (SqlAlterTableAddPartition) sqlAlterTableGroup.getAlters().get(0);
        }

        PartitionStrategy strategy = partitionInfo.getPartitionBy().getStrategy();
        int partitionColumnCnt = partitionInfo.getPartitionBy().getPartitionExprTypeList().size();
        boolean isMultiColumns = (partitionColumnCnt > 1);
        SearchDatumComparator comparator = partitionInfo.getPartitionBy().getPruningSpaceComparator();
        PartitionIntFunction partIntFunc = partitionInfo.getPartitionBy().getPartIntFunc();

        PartitionSpec defaultSpec = null;
        Set<String> allPartsValSet = new HashSet<>();
        for (PartitionSpec spec : partitionInfo.getPartitionBy().getOrderedPartitionSpecs()) {
            if (spec.isDefaultPartition()) {
                defaultSpec = spec;
            }
            MultiValuePartitionBoundSpec listBoundSpec = (MultiValuePartitionBoundSpec) spec.getBoundSpec();
            for (SearchDatumInfo datumInfo : listBoundSpec.getMultiDatums()) {
                if (datumInfo.containDefaultValue()) {
                    continue;
                }
                allPartsValSet.add(datumInfo.toString());
            }
        }

        if (defaultSpec == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT, "default partition not found");
        }

        List<SqlPartition> newPartitions = new ArrayList<>();
        sqlAlterTableAddPartition.getPartitions().stream().forEach(o -> newPartitions.add((SqlPartition) o));

        //List<Pair<partitionName, List<values>>>
        Set<String> newPartsValueSet = new TreeSet<>(String::compareToIgnoreCase);

        for (SqlPartition sqlPartition : newPartitions) {
            String name = sqlPartition.getName().toString();
            Pair<String, List<String>> nameAndValue = Pair.of(name, new ArrayList<>());
            List<SqlPartitionValueItem> values = sqlPartition.getValues().getItems();
            for (SqlPartitionValueItem sqlPartitionValueItem : values) {
                if (strategy == PartitionStrategy.LIST
                    || strategy == PartitionStrategy.LIST_COLUMNS && !isMultiColumns) {
                    SqlNode valueItem = sqlPartitionValueItem.getValue();
                    if (valueItem instanceof SqlBasicCall && "default".equalsIgnoreCase(
                        ((SqlBasicCall) valueItem).getOperator().getName())) {
                        throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                            "only allowed one default partition");
                    }
                    RexNode bndExprRex = partBoundExprInfo.get(valueItem);
                    RelDataType bndValDt = comparator.getDatumRelDataTypes()[0];
                    PartitionInfoUtil.validateBoundValueExpr(bndExprRex, bndValDt, partIntFunc,
                        partitionInfo.getPartitionBy().getStrategy());
                    PartitionBoundVal bndVal =
                        PartitionPrunerUtils.getBoundValByRexExpr(bndExprRex, bndValDt,
                            PartFieldAccessType.DDL_EXECUTION,
                            executionContext);
                    List<PartitionBoundVal> oneBndVal = Collections.singletonList(bndVal);
                    SearchDatumInfo datumInfo = new SearchDatumInfo(oneBndVal);
                    nameAndValue.getValue().add(datumInfo.toString());
                    if (newPartsValueSet.contains(datumInfo.toString())) {
                        throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                            String.format("partition[%s] value [%s] duplicated with new partition", name,
                                datumInfo));
                    }

                    if (allPartsValSet.contains(datumInfo.toString())) {
                        throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                            String.format("partition[%s] value [%s] duplicated with old partition", name,
                                datumInfo));
                    }
                    newPartsValueSet.add(datumInfo.toString());
                } else {
                    SqlNode valueItem = sqlPartitionValueItem.getValue();
                    //for multi columns
                    if (!(valueItem instanceof SqlCall) || valueItem.getKind() != SqlKind.ROW) {
                        throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                            "key " + valueItem.toString() + " is not row type");
                    }
                    if (valueItem instanceof SqlBasicCall && "default".equalsIgnoreCase(
                        ((SqlBasicCall) valueItem).getOperator().getName())) {
                        throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                            "only allowed one default partition");
                    }
                    RexNode bndExprRex = partBoundExprInfo.get(valueItem);
                    List<PartitionBoundVal> oneBndVal = new ArrayList<>();
                    for (int i = 0; i < ((RexCall) bndExprRex).getOperands().size(); i++) {
                        RexNode rexOperand = ((RexCall) bndExprRex).getOperands().get(i);
                        RelDataType bndValDt = comparator.getDatumRelDataTypes()[i];
                        PartitionInfoUtil.validateBoundValueExpr(rexOperand, bndValDt, partIntFunc,
                            partitionInfo.getPartitionBy().getStrategy());
                        PartitionBoundVal bndVal =
                            PartitionPrunerUtils.getBoundValByRexExpr(rexOperand, bndValDt,
                                PartFieldAccessType.DDL_EXECUTION,
                                executionContext);
                        oneBndVal.add(bndVal);
                    }

                    SearchDatumInfo datumInfo = new SearchDatumInfo(oneBndVal);
                    nameAndValue.getValue().add(datumInfo.toString());
                    if (newPartsValueSet.contains(datumInfo.toString())) {
                        throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                            String.format("partition[%s] value [%s] duplicated with new partition", name,
                                datumInfo));
                    }
                    if (allPartsValSet.contains(datumInfo.toString())) {
                        throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                            String.format("partition[%s] value [%s] duplicated with old partition", name,
                                datumInfo));
                    }
                    newPartsValueSet.add(datumInfo.toString());
                }
            }
        }

        List<String> newPartitionsStr = new ArrayList<>();
        for (SqlPartition newSqlPartition : newPartitions) {
            newPartitionsStr.add(newSqlPartition.toString());
        }

        PartitionSpec tobePrintDefaultSpec = defaultSpec.copy();
        tobePrintDefaultSpec.setLogical(false);
        newPartitionsStr.add(
            tobePrintDefaultSpec.toString()
        );
        String finalNewpartsExpr = String.join(", ", newPartitionsStr);
        return MessageFormat.format(SPLIT_TABLEGROUP_SQL, objectName, defaultSpec.getName(), finalNewpartsExpr);
    }

    public static String convertAddListRelToSplitListSqlForSubPartition(LogicalAlterTableAddPartition addPartitionPlan,
                                                                        boolean isAlterTable,
                                                                        ExecutionContext executionContext) {
        final String SPLIT_SQL = isAlterTable ? "alter table {0} split subpartition {1} into ({2});" :
            "alter tablegroup {0} split subpartition {1} into ({2})";

        PartitionInfo partitionInfo;
        Map<SqlNode, RexNode> partBoundExprInfo;
        SqlAlterTableAddPartition sqlAlterTableAddPartition;
        String objectName;
        if (isAlterTable) {
            AlterTable alterTable = (AlterTable) addPartitionPlan.relDdl;
            objectName = Util.last(((SqlIdentifier) alterTable.getTableName()).names);
            partitionInfo = executionContext.getSchemaManager(addPartitionPlan.getSchemaName()).getTddlRuleManager()
                .getPartitionInfoManager().getPartitionInfo(objectName);
            SqlAlterTable sqlAlterTable = (SqlAlterTable) alterTable.getSqlNode();
            partBoundExprInfo = sqlAlterTable.getPartRexInfoCtxByLevel().get(PARTITION_LEVEL_SUBPARTITION);
            sqlAlterTableAddPartition = (SqlAlterTableAddPartition) sqlAlterTable.getAlters().get(0);
        } else {
            objectName = addPartitionPlan.getTableName();
            partitionInfo = getPartitionInfo(objectName, addPartitionPlan.getSchemaName());
            SqlAlterTableGroup sqlAlterTableGroup = (SqlAlterTableGroup) ((addPartitionPlan).relDdl.getSqlNode());
            partBoundExprInfo = sqlAlterTableGroup.getPartRexInfoCtxByLevel().get(PARTITION_LEVEL_SUBPARTITION);
            sqlAlterTableAddPartition = (SqlAlterTableAddPartition) sqlAlterTableGroup.getAlters().get(0);
        }

        PartitionStrategy strategy = partitionInfo.getPartitionBy().getSubPartitionBy().getStrategy();
        int partitionColumnCnt = partitionInfo.getPartitionBy().getSubPartitionBy().getPartitionExprTypeList().size();
        boolean isMultiColumns = (partitionColumnCnt > 1);
        boolean isTemplateSubPartition = partitionInfo.getPartitionBy().getSubPartitionBy().isUseSubPartTemplate();
        SearchDatumComparator comparator =
            partitionInfo.getPartitionBy().getSubPartitionBy().getPruningSpaceComparator();
        PartitionIntFunction partIntFunc = partitionInfo.getPartitionBy().getSubPartitionBy().getPartIntFunc();

        PartitionSpec defaultSubSpec = null;
        Set<String> allPartsValSetInSubpartition = new HashSet<>();
        PartitionSpec parentSpec = null;  //only used for non-template subpartition
        if (isTemplateSubPartition) {
            for (PartitionSpec subSpec : partitionInfo.getPartitionBy().getSubPartitionBy().getPartitions()) {
                if (subSpec.isDefaultPartition()) {
                    defaultSubSpec = subSpec;
                }
                MultiValuePartitionBoundSpec listBoundSpec = (MultiValuePartitionBoundSpec) subSpec.getBoundSpec();
                for (SearchDatumInfo datumInfo : listBoundSpec.getMultiDatums()) {
                    if (datumInfo.containDefaultValue()) {
                        continue;
                    }
                    allPartsValSetInSubpartition.add(datumInfo.toString());
                }
            }
        } else {
            String parentPartitionName =
                ((SqlPartition) sqlAlterTableAddPartition.getPartitions().get(0)).getName().toString();
            parentSpec = partitionInfo.getPartitionBy().getPartitionByPartName(parentPartitionName);
            for (PartitionSpec subSpec : parentSpec.getSubPartitions()) {
                if (subSpec.isDefaultPartition()) {
                    defaultSubSpec = subSpec;
                }
                MultiValuePartitionBoundSpec listBoundSpec = (MultiValuePartitionBoundSpec) subSpec.getBoundSpec();
                for (SearchDatumInfo datumInfo : listBoundSpec.getMultiDatums()) {
                    if (datumInfo.containDefaultValue()) {
                        continue;
                    }
                    allPartsValSetInSubpartition.add(datumInfo.toString());
                }
            }
        }
        if (defaultSubSpec == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT, "default partition not found");
        }
        if (!isTemplateSubPartition && parentSpec == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT, "1st level partition not found");
        }

        List<SqlSubPartition> newSubPartitions = new ArrayList<>();
        SqlPartition parentPartition = (SqlPartition) sqlAlterTableAddPartition.getPartitions().get(0);
        parentPartition.getSubPartitions().forEach(o -> newSubPartitions.add((SqlSubPartition) o));

        //List<Pair<partitionName, List<values>>>
        Set<String> newPartsValueSetInSubpartition = new TreeSet<>(String::compareToIgnoreCase);
        for (SqlSubPartition subPartition : newSubPartitions) {
            String name = subPartition.getName().toString();
            Pair<String, List<String>> nameAndValue = Pair.of(name, new ArrayList<>());
            List<SqlPartitionValueItem> values = subPartition.getValues().getItems();
            for (SqlPartitionValueItem sqlPartitionValueItem : values) {
                if (strategy == PartitionStrategy.LIST
                    || strategy == PartitionStrategy.LIST_COLUMNS && !isMultiColumns) {
                    SqlNode valueItem = sqlPartitionValueItem.getValue();
                    if (valueItem instanceof SqlBasicCall && "default".equalsIgnoreCase(
                        ((SqlBasicCall) valueItem).getOperator().getName())) {
                        throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                            "only allowed one default partition");
                    }
                    RexNode bndExprRex = partBoundExprInfo.get(valueItem);
                    RelDataType bndValDt = comparator.getDatumRelDataTypes()[0];
                    PartitionInfoUtil.validateBoundValueExpr(bndExprRex, bndValDt, partIntFunc,
                        partitionInfo.getPartitionBy().getSubPartitionBy().getStrategy());
                    PartitionBoundVal bndVal =
                        PartitionPrunerUtils.getBoundValByRexExpr(bndExprRex, bndValDt,
                            PartFieldAccessType.DDL_EXECUTION,
                            executionContext);
                    List<PartitionBoundVal> oneBndVal = Collections.singletonList(bndVal);
                    SearchDatumInfo datumInfo = new SearchDatumInfo(oneBndVal);
                    nameAndValue.getValue().add(datumInfo.toString());
                    if (newPartsValueSetInSubpartition.contains(datumInfo.toString())) {
                        throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                            String.format("partition[%s] value [%s] duplicated with new partition", name,
                                datumInfo));
                    }

                    if (allPartsValSetInSubpartition.contains(datumInfo.toString())) {
                        throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                            String.format("partition[%s] value [%s] duplicated with old partition", name,
                                datumInfo));
                    }
                    newPartsValueSetInSubpartition.add(datumInfo.toString());
                } else {
                    SqlNode valueItem = sqlPartitionValueItem.getValue();
                    //for multi columns
                    if (!(valueItem instanceof SqlCall) || valueItem.getKind() != SqlKind.ROW) {
                        throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                            "key " + valueItem.toString() + " is not row type");
                    }
                    if (valueItem instanceof SqlBasicCall && "default".equalsIgnoreCase(
                        ((SqlBasicCall) valueItem).getOperator().getName())) {
                        throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                            "only allowed one default partition");
                    }
                    RexNode bndExprRex = partBoundExprInfo.get(valueItem);
                    List<PartitionBoundVal> oneBndVal = new ArrayList<>();
                    for (int i = 0; i < ((RexCall) bndExprRex).getOperands().size(); i++) {
                        RexNode rexOperand = ((RexCall) bndExprRex).getOperands().get(i);
                        RelDataType bndValDt = comparator.getDatumRelDataTypes()[i];
                        PartitionInfoUtil.validateBoundValueExpr(rexOperand, bndValDt, partIntFunc,
                            partitionInfo.getPartitionBy().getSubPartitionBy().getStrategy());
                        PartitionBoundVal bndVal =
                            PartitionPrunerUtils.getBoundValByRexExpr(rexOperand, bndValDt,
                                PartFieldAccessType.DDL_EXECUTION,
                                executionContext);
                        oneBndVal.add(bndVal);
                    }
                    SearchDatumInfo datumInfo = new SearchDatumInfo(oneBndVal);
                    nameAndValue.getValue().add(datumInfo.toString());
                    if (newPartsValueSetInSubpartition.contains(datumInfo.toString())) {
                        throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                            String.format("partition[%s] value [%s] duplicated with new partition", name,
                                datumInfo));
                    }
                    if (allPartsValSetInSubpartition.contains(datumInfo.toString())) {
                        throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                            String.format("partition[%s] value [%s] duplicated with old partition", name,
                                datumInfo));
                    }
                    newPartsValueSetInSubpartition.add(datumInfo.toString());
                }
            }
        }

        List<String> newPartitionsStr = new ArrayList<>();
        for (SqlSubPartition sqlSubPartition : newSubPartitions) {
            newPartitionsStr.add(sqlSubPartition.toString());
        }
        newPartitionsStr.add(
            defaultSubSpec.toString()
        );

        String finalNewpartsExpr = String.join(", ", newPartitionsStr);
        return MessageFormat.format(SPLIT_SQL, objectName, defaultSubSpec.getName(), finalNewpartsExpr);
    }

    //isDropValue:  true(means drop value), false(means add value)
    public static String convertModifyListPartitionValueRelToReorganizePartitionSql(
        LogicalAlterTableModifyPartition modifyPartition,
        boolean isAlterTable, ExecutionContext executionContext, boolean isDropValue) {
        final String REORGANIZE_SQL = isAlterTable ? "alter table {0} reorganize partition {1}, {2} into"
            + "({3}, {4})" :
            "alter tablegroup {0} reorganize partition {1}, {2} into"
                + "({3}, {4})";

        PartitionInfo partitionInfo;
        Map<SqlNode, RexNode> partBoundExprInfo;
        SqlAlterTableModifyPartitionValues sqlAlterTableModifyPartitionValues;
        String objectName;
        if (isAlterTable) {
            AlterTable alterTable = (AlterTable) modifyPartition.relDdl;
            objectName = Util.last(((SqlIdentifier) alterTable.getTableName()).names);
            partitionInfo = executionContext.getSchemaManager(modifyPartition.getSchemaName()).getTddlRuleManager()
                .getPartitionInfoManager().getPartitionInfo(objectName);
            partBoundExprInfo = alterTable.getAllRexExprInfo();
            SqlAlterTable sqlAlterTable = (SqlAlterTable) alterTable.getSqlNode();
            sqlAlterTableModifyPartitionValues = (SqlAlterTableModifyPartitionValues) sqlAlterTable.getAlters().get(0);
        } else {
            objectName = modifyPartition.getTableName();
            partitionInfo = getPartitionInfo(objectName, modifyPartition.getSchemaName());
            SqlAlterTableGroup sqlAlterTableGroup = (SqlAlterTableGroup) ((modifyPartition).relDdl.getSqlNode());
            partBoundExprInfo = sqlAlterTableGroup.getPartRexInfoCtxByLevel().get(PARTITION_LEVEL_PARTITION);
            sqlAlterTableModifyPartitionValues =
                (SqlAlterTableModifyPartitionValues) sqlAlterTableGroup.getAlters().get(0);
        }

        PartitionStrategy strategy = partitionInfo.getPartitionBy().getStrategy();
        int partitionColumnCnt = partitionInfo.getPartitionBy().getPartitionExprTypeList().size();
        boolean isMultiColumns = (partitionColumnCnt > 1);

        SearchDatumComparator comparator = partitionInfo.getPartitionBy().getPruningSpaceComparator();
        PartitionIntFunction partIntFunc = partitionInfo.getPartitionBy().getPartIntFunc();

        PartitionSpec defaultSpec = null;
        PartitionSpec tobeModifyValueSpec = null;

        Set<String> allPartsVals = new TreeSet<>(String::compareToIgnoreCase);
        for (PartitionSpec spec : partitionInfo.getPartitionBy().getOrderedPartitionSpecs()) {
            if (spec.isDefaultPartition()) {
                defaultSpec = spec;
            }
            if (spec.getName()
                .equalsIgnoreCase(sqlAlterTableModifyPartitionValues.getPartition().getName().toString())) {
                tobeModifyValueSpec = spec;
            }
            MultiValuePartitionBoundSpec listBoundSpec = (MultiValuePartitionBoundSpec) spec.getBoundSpec();
            for (SearchDatumInfo datumInfo : listBoundSpec.getMultiDatums()) {
                if (datumInfo.containDefaultValue()) {
                    continue;
                }
                allPartsVals.add(datumInfo.toString());
            }
        }
        if (defaultSpec == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT, "default partition not found");
        }
        if (tobeModifyValueSpec == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                String.format("src partition [%s] not found",
                    sqlAlterTableModifyPartitionValues.getPartition().getName().toString()));
        }

        if (tobeModifyValueSpec == defaultSpec) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                String.format("It's not allowed to modify default partition [%s]", defaultSpec.getName()));
        }

        PartitionSpec tobeModifyValueSpecNew = tobeModifyValueSpec.copy();
        PartitionSpec defaultSpecNew = defaultSpec.copy();

        //add new values to tobeAddValueSpecNew
        List<SqlPartitionValueItem> values = sqlAlterTableModifyPartitionValues.getPartition().getValues().getItems();
        List<SearchDatumInfo> valuesDatum = new ArrayList<>();

        for (SqlPartitionValueItem sqlPartitionValueItem : values) {
            //single column
            if (strategy == PartitionStrategy.LIST || strategy == PartitionStrategy.LIST_COLUMNS && !isMultiColumns) {
                SqlNode valueItem = sqlPartitionValueItem.getValue();
                RexNode bndExprRex = partBoundExprInfo.get(valueItem);
                RelDataType bndValDt = comparator.getDatumRelDataTypes()[0];
                PartitionInfoUtil.validateBoundValueExpr(bndExprRex, bndValDt, partIntFunc,
                    partitionInfo.getPartitionBy().getStrategy());
                PartitionBoundVal bndVal =
                    PartitionPrunerUtils.getBoundValByRexExpr(bndExprRex, bndValDt, PartFieldAccessType.DDL_EXECUTION,
                        executionContext);
                List<PartitionBoundVal> oneBndVal = Collections.singletonList(bndVal);
                valuesDatum.add(new SearchDatumInfo(oneBndVal));
            } else {
                SqlNode valueItem = sqlPartitionValueItem.getValue();
                //for multi columns
                if (!(valueItem instanceof SqlCall) || valueItem.getKind() != SqlKind.ROW) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                        "key " + valueItem.toString() + " is not row type");
                }
                RexNode bndExprRex = partBoundExprInfo.get(valueItem);
                List<PartitionBoundVal> oneBndVal = new ArrayList<>();
                for (int i = 0; i < ((RexCall) bndExprRex).getOperands().size(); i++) {
                    RexNode rexOperand = ((RexCall) bndExprRex).getOperands().get(i);
                    RelDataType bndValDt = comparator.getDatumRelDataTypes()[i];
                    PartitionInfoUtil.validateBoundValueExpr(rexOperand, bndValDt, partIntFunc,
                        partitionInfo.getPartitionBy().getStrategy());
                    PartitionBoundVal bndVal =
                        PartitionPrunerUtils.getBoundValByRexExpr(rexOperand, bndValDt,
                            PartFieldAccessType.DDL_EXECUTION,
                            executionContext);
                    oneBndVal.add(bndVal);
                }
                valuesDatum.add(new SearchDatumInfo(oneBndVal));
            }
        }

        //for add value: newly added value should not exist in existing partition
        if (!isDropValue) {
            for (SearchDatumInfo datumInfo : valuesDatum) {
                if (allPartsVals.contains(datumInfo.toString())) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                        "key " + datumInfo.toString() + " already exists in other partition");
                }
            }
        } else {
            //for drop value: old value should exist in src partition
            Set<String> tobeModifyPartitionValues = new TreeSet<>(String::compareToIgnoreCase);
            tobeModifyValueSpecNew.getBoundSpec().getMultiDatums()
                .forEach(datum -> tobeModifyPartitionValues.add(datum.toString()));
            for (SearchDatumInfo datumInfo : valuesDatum) {
                if (!tobeModifyPartitionValues.contains(datumInfo.toString())) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                        "key " + datumInfo.toString() + " not exists in partition " + tobeModifyValueSpec.getName());
                }
            }

        }

        if (!isDropValue) {
            tobeModifyValueSpecNew.getBoundSpec().getMultiDatums().addAll(valuesDatum);
        } else {
            Set<String> valuesToBeDrop = new TreeSet<>(String::compareToIgnoreCase);
            valuesDatum.forEach(v -> valuesToBeDrop.add(v.toString()));

            tobeModifyValueSpecNew.getBoundSpec().getMultiDatums().clear();

            tobeModifyValueSpecNew.getBoundSpec().getMultiDatums().addAll(
                tobeModifyValueSpec.getBoundSpec()
                    .getMultiDatums()
                    .stream()
                    .filter(datum -> !valuesToBeDrop.contains(datum.toString()))
                    .collect(Collectors.toList())
            );
        }

        tobeModifyValueSpecNew.setLogical(false);
        defaultSpecNew.setLogical(false);
        return MessageFormat.format(
            REORGANIZE_SQL, objectName, tobeModifyValueSpec.getName(), defaultSpec.getName(),
            tobeModifyValueSpecNew.toString(),
            defaultSpecNew.toString()
        );
    }

    //isDropValue: true(means drop value), false(means add value)
    public static String convertModifyListPartitionValueRelToReorganizeForSubpartition(
        LogicalAlterTableModifyPartition modifyPartition,
        boolean isAlterTable,
        ExecutionContext executionContext,
        boolean isDropValue) {
        final String REORGANIZE_SQL = isAlterTable ? "alter table {0} reorganize subpartition {1}, {2} into"
            + "({3}, {4})" :
            "alter tablegroup {0} reorganize subpartition {1}, {2} into"
                + "({3}, {4})";

        PartitionInfo partitionInfo;
        Map<SqlNode, RexNode> partBoundExprInfo;
        SqlAlterTableModifyPartitionValues sqlAlterTableModifyPartitionValues;
        String objectName;
        if (isAlterTable) {
            AlterTable alterTable = (AlterTable) modifyPartition.relDdl;
            objectName = Util.last(((SqlIdentifier) alterTable.getTableName()).names);
            partitionInfo = executionContext.getSchemaManager(modifyPartition.getSchemaName()).getTddlRuleManager()
                .getPartitionInfoManager().getPartitionInfo(objectName);
            partBoundExprInfo = alterTable.getAllRexExprInfo();
            SqlAlterTable sqlAlterTable = (SqlAlterTable) alterTable.getSqlNode();
            sqlAlterTableModifyPartitionValues = (SqlAlterTableModifyPartitionValues) sqlAlterTable.getAlters().get(0);
        } else {
            objectName = modifyPartition.getTableName();
            partitionInfo = getPartitionInfo(objectName, modifyPartition.getSchemaName());
            SqlAlterTableGroup sqlAlterTableGroup = (SqlAlterTableGroup) ((modifyPartition).relDdl.getSqlNode());
            partBoundExprInfo = sqlAlterTableGroup.getPartRexInfoCtxByLevel().get(PARTITION_LEVEL_PARTITION);
            sqlAlterTableModifyPartitionValues =
                (SqlAlterTableModifyPartitionValues) sqlAlterTableGroup.getAlters().get(0);
        }

        PartitionStrategy strategy = partitionInfo.getPartitionBy().getSubPartitionBy().getStrategy();
        int partitionColumnCnt = partitionInfo.getPartitionBy().getSubPartitionBy().getPartitionExprTypeList().size();
        boolean isMultiColumns = (partitionColumnCnt > 1);
        boolean isTemplateSubPartition = partitionInfo.getPartitionBy().getSubPartitionBy().isUseSubPartTemplate();

        SearchDatumComparator comparator =
            partitionInfo.getPartitionBy().getSubPartitionBy().getPruningSpaceComparator();
        PartitionIntFunction partIntFunc = partitionInfo.getPartitionBy().getSubPartitionBy().getPartIntFunc();

        PartitionSpec defaultSpec = null;
        PartitionSpec tobeModifyValueSpec = null;
        Set<String> allPartsVals = new TreeSet<>(String::compareToIgnoreCase);
        //for template subpartition
        if (isTemplateSubPartition) {
            String subPartitionName =
                ((SqlSubPartition) sqlAlterTableModifyPartitionValues.getPartition().getSubPartitions()
                    .get(0)).getName().toString();
            for (PartitionSpec subSpec : partitionInfo.getPartitionBy().getSubPartitionBy().getPartitions()) {
                if (subSpec.isDefaultPartition()) {
                    defaultSpec = subSpec;
                }
                if (subSpec.getName()
                    .equalsIgnoreCase(subPartitionName)) {
                    tobeModifyValueSpec = subSpec;
                }
                MultiValuePartitionBoundSpec listBoundSpec = (MultiValuePartitionBoundSpec) subSpec.getBoundSpec();
                for (SearchDatumInfo datumInfo : listBoundSpec.getMultiDatums()) {
                    if (datumInfo.containDefaultValue()) {
                        continue;
                    }
                    allPartsVals.add(datumInfo.toString());
                }
            }
        } else {
            //for non-template subpartition
            String subPartitionName =
                ((SqlSubPartition) sqlAlterTableModifyPartitionValues.getPartition().getSubPartitions()
                    .get(0)).getName().toString();
            PartitionSpec parentSpec = null;

            //find parent parent
            for (PartitionSpec firstLevelSpec : partitionInfo.getPartitionBy().getPartitions()) {
                for (PartitionSpec subSpec : firstLevelSpec.getSubPartitions()) {
                    if (subSpec.getName().equalsIgnoreCase(subPartitionName)) {
                        parentSpec = firstLevelSpec;
                        tobeModifyValueSpec = subSpec;
                        break;
                    }
                }
            }

            if (parentSpec == null) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                    "1st level partition not found");
            }

            for (PartitionSpec subSpec : parentSpec.getSubPartitions()) {
                if (subSpec.isDefaultPartition()) {
                    defaultSpec = subSpec;
                    continue;
                }
                MultiValuePartitionBoundSpec listBoundSpec = (MultiValuePartitionBoundSpec) subSpec.getBoundSpec();
                for (SearchDatumInfo datumInfo : listBoundSpec.getMultiDatums()) {
                    if (datumInfo.containDefaultValue()) {
                        continue;
                    }
                    allPartsVals.add(datumInfo.toString());
                }
            }
        }

        if (defaultSpec == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT, "default partition not found");
        }
        if (tobeModifyValueSpec == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                String.format("src partition [%s] not found",
                    sqlAlterTableModifyPartitionValues.getPartition().getName().toString()));
        }

        if (tobeModifyValueSpec == defaultSpec) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                String.format("It's not allowed to modify default subpartition [%s]", defaultSpec.getName()));
        }

        PartitionSpec tobeModifyValueSpecNew = tobeModifyValueSpec.copy();
        PartitionSpec defaultSpecNew = defaultSpec.copy();
        //add new values to tobeAddValueSpecNew
        List<SqlPartitionValueItem> values =
            ((SqlSubPartition) sqlAlterTableModifyPartitionValues.getPartition().getSubPartitions().get(0)).getValues()
                .getItems();
        List<SearchDatumInfo> valuesDatum = new ArrayList<>();

        for (SqlPartitionValueItem sqlPartitionValueItem : values) {
            //single column
            if (strategy == PartitionStrategy.LIST || strategy == PartitionStrategy.LIST_COLUMNS && !isMultiColumns) {
                SqlNode valueItem = sqlPartitionValueItem.getValue();
                RexNode bndExprRex = partBoundExprInfo.get(valueItem);
                RelDataType bndValDt = comparator.getDatumRelDataTypes()[0];
                PartitionInfoUtil.validateBoundValueExpr(bndExprRex, bndValDt, partIntFunc,
                    partitionInfo.getPartitionBy().getSubPartitionBy().getStrategy());
                PartitionBoundVal bndVal =
                    PartitionPrunerUtils.getBoundValByRexExpr(bndExprRex, bndValDt, PartFieldAccessType.DDL_EXECUTION,
                        executionContext);
                List<PartitionBoundVal> oneBndVal = Collections.singletonList(bndVal);
                valuesDatum.add(new SearchDatumInfo(oneBndVal));
            } else {
                SqlNode valueItem = sqlPartitionValueItem.getValue();
                //for multi columns
                if (!(valueItem instanceof SqlCall) || valueItem.getKind() != SqlKind.ROW) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                        "key " + valueItem.toString() + " is not row type");
                }
                RexNode bndExprRex = partBoundExprInfo.get(valueItem);
                List<PartitionBoundVal> oneBndVal = new ArrayList<>();
                for (int i = 0; i < ((RexCall) bndExprRex).getOperands().size(); i++) {
                    RexNode rexOperand = ((RexCall) bndExprRex).getOperands().get(i);
                    RelDataType bndValDt = comparator.getDatumRelDataTypes()[i];
                    PartitionInfoUtil.validateBoundValueExpr(rexOperand, bndValDt, partIntFunc,
                        partitionInfo.getPartitionBy().getSubPartitionBy().getStrategy());
                    PartitionBoundVal bndVal =
                        PartitionPrunerUtils.getBoundValByRexExpr(rexOperand, bndValDt,
                            PartFieldAccessType.DDL_EXECUTION,
                            executionContext);
                    oneBndVal.add(bndVal);
                }
                valuesDatum.add(new SearchDatumInfo(oneBndVal));
            }
        }

        //for add value: newly added value should not exist in existing partition
        if (!isDropValue) {
            for (SearchDatumInfo datumInfo : valuesDatum) {
                if (allPartsVals.contains(datumInfo.toString())) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                        "key " + datumInfo.toString() + " already exists in other partition");
                }
            }
        } else {
            //for drop value: old value should exist in src partition
            Set<String> tobeModifyPartitionValues = new TreeSet<>(String::compareToIgnoreCase);
            tobeModifyValueSpecNew.getBoundSpec().getMultiDatums()
                .forEach(datum -> tobeModifyPartitionValues.add(datum.toString()));
            for (SearchDatumInfo datumInfo : valuesDatum) {
                if (!tobeModifyPartitionValues.contains(datumInfo.toString())) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                        "key " + datumInfo.toString() + " not exists in partition " + tobeModifyValueSpec.getName());
                }
            }

        }

        if (!isDropValue) {
            tobeModifyValueSpecNew.getBoundSpec().getMultiDatums().addAll(valuesDatum);
        } else {
            Set<String> valuesToBeDrop = new TreeSet<>(String::compareToIgnoreCase);
            valuesDatum.forEach(v -> valuesToBeDrop.add(v.toString()));

            tobeModifyValueSpecNew.getBoundSpec().getMultiDatums().clear();

            tobeModifyValueSpecNew.getBoundSpec().getMultiDatums().addAll(
                tobeModifyValueSpec.getBoundSpec()
                    .getMultiDatums()
                    .stream()
                    .filter(datum -> !valuesToBeDrop.contains(datum.toString()))
                    .collect(Collectors.toList())
            );
        }

        return MessageFormat.format(
            REORGANIZE_SQL, objectName, tobeModifyValueSpec.getName(), defaultSpec.getName(),
            tobeModifyValueSpecNew.toString(),
            defaultSpecNew.toString()
        );
    }

    public static PartitionSpec findPartitionSpec(PartitionInfo partitionInfo,
                                                  PartitionGroupRecord partitionGroupRecord) {
        PartitionSpec partitionSpec = partitionInfo.getPartitionBy().getPartitions().stream()
            .filter(p -> p.getName().equalsIgnoreCase(partitionGroupRecord.getPartition_name())).findFirst()
            .orElse(null);

        if (partitionSpec == null && partitionInfo.getPartitionBy().getSubPartitionBy() != null) {
            for (PartitionSpec partSpec : partitionInfo.getPartitionBy().getPartitions()) {
                partitionSpec = partSpec.getSubPartitions().stream()
                    .filter(sp -> sp.getName().equalsIgnoreCase(partitionGroupRecord.getPartition_name())).findFirst()
                    .orElse(null);
                if (partitionSpec != null) {
                    break;
                }
            }
        }

        return partitionSpec;
    }
}

