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
import com.alibaba.polardbx.optimizer.core.TddlRelDataTypeSystemImpl;
import com.alibaba.polardbx.optimizer.core.TddlTypeFactoryImpl;
import com.alibaba.polardbx.optimizer.core.datatype.CharType;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.rel.dal.PhyShow;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableAddPartition;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableExtractPartition;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableGroupAddPartition;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableGroupExtractPartition;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupAddTablePreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.MergeTableGroupPreparedData;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.partition.MultiValuePartitionBoundSpec;
import com.alibaba.polardbx.optimizer.partition.PartitionBoundVal;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoBuilder;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoManager;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoUtil;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.partition.PartitionStrategy;
import com.alibaba.polardbx.optimizer.partition.RangeBoundSpec;
import com.alibaba.polardbx.optimizer.partition.datatype.PartitionField;
import com.alibaba.polardbx.optimizer.partition.datatype.PartitionFieldBuilder;
import com.alibaba.polardbx.optimizer.partition.datatype.function.PartitionIntFunction;
import com.alibaba.polardbx.optimizer.partition.pruning.PartFieldAccessType;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPrunerUtils;
import com.alibaba.polardbx.optimizer.partition.pruning.SearchDatumComparator;
import com.alibaba.polardbx.optimizer.partition.pruning.SearchDatumInfo;
import com.alibaba.polardbx.optimizer.tablegroup.TableGroupInfoManager;
import com.alibaba.polardbx.optimizer.utils.KeyWordsUtil;
import it.unimi.dsi.fastutil.Hash;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.ddl.AlterTable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
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
import org.apache.calcite.sql.SqlAlterTableModifyPartitionValues;
import org.apache.calcite.sql.SqlAlterTableMovePartition;
import org.apache.calcite.sql.SqlAlterTableRenamePartition;
import org.apache.calcite.sql.SqlAlterTableSplitPartition;
import org.apache.calcite.sql.SqlAlterTableSplitPartitionByHotValue;
import org.apache.calcite.sql.SqlAlterTableTruncatePartition;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlPartition;
import org.apache.calcite.sql.SqlPartitionValue;
import org.apache.calcite.sql.SqlPartitionValueItem;
import org.apache.calcite.sql.SqlShowCreateTable;
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
import java.util.stream.Collectors;

/**
 * Created by luoyanxin
 *
 * @author luoyanxin
 */
public class AlterTableGroupUtils {
    public static void alterTableGroupPreCheck(SqlAlterTableGroup sqlAlterTableGroup,
                                               String schemaName,
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
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                    "can't modify the tablegroup:" + tableGroupName + " when it's empty");
            }
            if (alterSpecifications.get(0) instanceof SqlAlterTableGroupSplitPartition) {
                final SqlAlterTableGroupSplitPartition sqlAlterTableGroupSplitPartition =
                    (SqlAlterTableGroupSplitPartition) alterSpecifications.get(0);
                sqlAlterTableGroupSplitPartition.setParent(sqlAlterTableGroup);
                alterTableGroupSplitPartitionCheck(sqlAlterTableGroupSplitPartition, tableGroupConfig,
                    sqlAlterTableGroupSplitPartition.getParent().getPartRexInfoCtx(), true, executionContext,
                    schemaName);
            } else if (alterSpecifications.get(0) instanceof SqlAlterTableGroupMergePartition) {
                final SqlAlterTableGroupMergePartition sqlAlterTableGroupMergePartition =
                    (SqlAlterTableGroupMergePartition) alterSpecifications.get(0);
                sqlAlterTableGroupMergePartition.setParent(sqlAlterTableGroup);
                String targetPartitionName =
                    Util.last(((SqlIdentifier) (sqlAlterTableGroupMergePartition.getTargetPartitionName())).names);
                Set<String> partitionsToBeMerged = sqlAlterTableGroupMergePartition.getOldPartitions().stream()
                    .map(o -> Util.last(((SqlIdentifier) (o)).names).toLowerCase()).collect(
                        Collectors.toSet());

                alterTableGroupMergePartitionCheck(tableGroupConfig, targetPartitionName, partitionsToBeMerged,
                    executionContext);
            } else if (alterSpecifications.get(0) instanceof SqlAlterTableGroupMovePartition) {
                final SqlAlterTableGroupMovePartition sqlAlterTableGroupMovePartition =
                    (SqlAlterTableGroupMovePartition) alterSpecifications.get(0);
                sqlAlterTableGroupMovePartition.setParent(sqlAlterTableGroup);

                alterTableGroupMovePartitionCheck(sqlAlterTableGroupMovePartition, tableGroupConfig, executionContext);
            } else if (alterSpecifications.get(0) instanceof SqlAlterTableGroupExtractPartition) {
                final SqlAlterTableGroupExtractPartition sqlAlterTableGroupExtractPartition =
                    (SqlAlterTableGroupExtractPartition) alterSpecifications.get(0);
                sqlAlterTableGroupExtractPartition.setParent(sqlAlterTableGroup);
                alterTableGroupExtractPartitionCheck(sqlAlterTableGroup, tableGroupConfig, executionContext);

            } else if (alterSpecifications.get(0) instanceof SqlAlterTableAddPartition) {
                alterTableGroupAddPartitionCheck(tableGroupConfig, schemaName,
                    (SqlAlterTableAddPartition) alterSpecifications.get(0));
            } else if (alterSpecifications.get(0) instanceof SqlAlterTableDropPartition) {
                SqlAlterTableDropPartition sqlAlterTableDropPartition =
                    (SqlAlterTableDropPartition) sqlAlterTableGroup.getAlters().get(0);
                alterTableGroupDropPartitionCheck(sqlAlterTableDropPartition, tableGroupConfig, executionContext);
            } else if (alterSpecifications.get(0) instanceof SqlAlterTableTruncatePartition) {
                // ignore
            } else if (alterSpecifications.get(0) instanceof SqlAlterTableGroupSetPartitionsLocality) {

            } else if (alterSpecifications.get(0) instanceof SqlAlterTableModifyPartitionValues) {

                final SqlAlterTableModifyPartitionValues sqlModifyListPartitionValues =
                    (SqlAlterTableModifyPartitionValues) alterSpecifications.get(0);
                alterTableModifyListPartitionValuesCheck(sqlAlterTableGroup,
                    tableGroupConfig,
                    sqlModifyListPartitionValues,
                    executionContext);

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
                alterTableGroupSplitPartitionByHotValueCheck(sqlAlterTableGroup,
                    tableGroupConfig,
                    executionContext);
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
                                                          ExecutionContext executionContext,
                                                          String schemaName) {

        boolean alterTableOnly = !(sqlAlterTableSplitPartition instanceof SqlAlterTableGroupSplitPartition);
        final SchemaManager schemaManager = OptimizerContext.getContext(schemaName).getLatestSchemaManager();
        String tableInCurrentGroup = tableGroupConfig.getAllTables().get(0).getLogTbRec().tableName;
        TableMeta tableMeta = schemaManager.getTable(tableInCurrentGroup);
        TableGroupRecord tableGroupRecord = tableGroupConfig.getTableGroupRecord();
        String tgName = tableGroupRecord.getTg_name();

        PartitionInfo partitionInfo = tableMeta.getPartitionInfo();

        if (partitionInfo.isSingleTable() || partitionInfo.isBroadcastTable()) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                "can't split the partition group for single/broadcast tables");
        }

        String splitPartitionName =
            Util.last(((SqlIdentifier) (sqlAlterTableSplitPartition.getSplitPartitionName())).names);
        PartitionGroupRecord splitPartRecord = tableGroupConfig.getPartitionGroupRecords().stream()
            .filter(o -> o.partition_name.equalsIgnoreCase(splitPartitionName)).findFirst().orElse(null);
        if (splitPartRecord == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_NAME_NOT_EXISTS,
                "the partition:" + splitPartitionName + " is not exists this current table group");
        }

        PartitionStrategy strategy = partitionInfo.getPartitionBy().getStrategy();
        PartitionField atVal = calculateSplitAtVal(tableGroupConfig, partitionInfo, sqlAlterTableSplitPartition);

        List<PartitionSpec> partitionSpecs = partitionInfo.getPartitionBy().getOrderedPartitionSpec();
        PartitionSpec splitPartitionSpec = null;
        PartitionSpec previousPartitionSpec = null;
        for (PartitionSpec spec : partitionSpecs) {
            if (spec.getLocation().getPartitionGroupId().longValue() == splitPartRecord.id.longValue()) {
                splitPartitionSpec = spec;
                break;
            } else {
                previousPartitionSpec = spec;
            }
        }
        if (splitPartitionSpec == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                "the metadata of table :" + tableInCurrentGroup + " is not correct");
        }
        if (strategy == PartitionStrategy.RANGE && atVal != null) {
            PartitionField maxVal = ((RangeBoundSpec) splitPartitionSpec.getBoundSpec()).getBoundValue().getValue();

            if (atVal.compareTo(maxVal) >= 0) {
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
            List<ColumnMeta> partColMetaList = partitionInfo.getPartitionBy().getPartitionFieldList();
            SearchDatumComparator comparator = partitionInfo.getPartitionBy().getPruningSpaceComparator();
            PartitionIntFunction partIntFunc = partitionInfo.getPartitionBy().getPartIntFunc();
            int pos = 0;
            List<SqlPartition> newPartitions = sqlAlterTableSplitPartition.getNewPartitions();
            List<PartitionSpec> newPartitionSpecs = new ArrayList<>();
            int newPrefixPartColCnt;
            if (checkPartitionColumn) {
                int actualPartColCnt =
                    PartitionInfoUtil.getActualPartColumnMetasInfoForTableGroup(schemaName, tgName).size();
                newPrefixPartColCnt =
                    PartitionInfoUtil.getNewPrefixPartColCntBySqlPartitionAst(Integer.MAX_VALUE/*ignore*/,
                        actualPartColCnt, strategy, newPartitions);
                if (strategy == PartitionStrategy.RANGE_COLUMNS || strategy == PartitionStrategy.KEY) {
                    /**
                     * Check if is allowed to use newPrefixPartColCnt as neew partColCnt for all tables in table group
                     */
                    if (!newPartitions.isEmpty() && !alterTableOnly) {
                        alterTableGroupCheckPartitionColumn(schemaName, tgName, newPrefixPartColCnt);
                    }
                }
            } else {
                int actualPartColCnt = PartitionInfoUtil.getActualPartitionColumns(partitionInfo).size();
                newPrefixPartColCnt =
                    PartitionInfoUtil.getNewPrefixPartColCntBySqlPartitionAst(Integer.MAX_VALUE/*ignore*/,
                        actualPartColCnt, strategy,
                        newPartitions);
            }

            for (SqlPartition sqlPartition : sqlAlterTableSplitPartition.getNewPartitions()) {
                if (sqlPartition.getValues() == null) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                        "the new partition value is not specified");
                }
                PartitionSpec newSpec = PartitionInfoBuilder
                    .buildPartitionSpecByPartSpecAst(
                        executionContext,
                        partColMetaList,
                        partIntFunc,
                        comparator,
                        partRexInfoCtx,
                        null,
                        sqlPartition,
                        partitionInfo.getPartitionBy().getStrategy(),
                        pos++,
                        newPrefixPartColCnt);

                newPartitionSpecs.add(newSpec);
            }
            if (strategy == PartitionStrategy.RANGE || strategy == PartitionStrategy.RANGE_COLUMNS) {

                SearchDatumInfo maxVal = splitPartitionSpec.getBoundSpec().getSingleDatum();
                SearchDatumInfo minVal = SearchDatumInfo
                    .createMinValDatumInfo(partitionInfo.getPartitionBy().getPartitionFieldList().size());

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
                            String.format("the partition bound of %s is %s, should greater than %s",
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
                if (!splitPartitionSpec.getIsDefaultPartition()) {
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
                    for (PartitionSpec spec : partitionInfo.getPartitionBy().getOrderedPartitionSpec()) {
                        MultiValuePartitionBoundSpec listBoundSpec =
                            (MultiValuePartitionBoundSpec) spec.getBoundSpec();
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
            }
        } else {
            //hash partitions
            if (atVal != null) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                    "not support split with at for:" + strategy.toString());
            }
        }
    }

    /**
     * Calculate split-at value, if not specified
     */
    private static PartitionField calculateSplitAtVal(TableGroupConfig tableGroupConfig, PartitionInfo partitionInfo,
                                                      SqlAlterTableSplitPartition sqlAlter) {
        PartitionField atVal = null;
        if (sqlAlter.getAtValue() != null) {
            if (partitionInfo.getPartitionBy().getStrategy() != PartitionStrategy.RANGE) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                    "Only support range partition to split with at value");
            }
            DataType atValDataType = DataTypeUtil
                .calciteToDrdsType(partitionInfo.getPartitionBy().getPartitionExprTypeList().get(0));
            atVal = PartitionFieldBuilder.createField(atValDataType);
            SqlLiteral constLiteral = ((SqlLiteral) sqlAlter.getAtValue());
            String constStr = constLiteral.getValueAs(String.class);
            atVal.store(constStr, new CharType());
        } else if (partitionInfo.getPartitionBy().getStrategy() != PartitionStrategy.HASH &&
            partitionInfo.getPartitionBy().getStrategy() != PartitionStrategy.KEY &&
            GeneralUtil.isEmpty(sqlAlter.getNewPartitions())) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                "Missing the new partitions spec");
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

    public static void alterTableGroupMergePartitionCheck(TableGroupConfig tableGroupConfig,
                                                          String targetPartitionName,
                                                          Set<String> partitionsToBeMerged,
                                                          ExecutionContext executionContext) {
        final SchemaManager schemaManager = executionContext.getSchemaManager();

        String tableInCurrentGroup = tableGroupConfig.getAllTables().get(0).getLogTbRec().tableName;
        TableMeta tableMeta = schemaManager.getTable(tableInCurrentGroup);
        PartitionInfo partitionInfo = tableMeta.getPartitionInfo();

        PartitionGroupRecord partRecord = tableGroupConfig.getPartitionGroupRecords().stream()
            .filter(o -> o.partition_name.equalsIgnoreCase(targetPartitionName)).findFirst().orElse(null);

        if (partitionsToBeMerged.size() < 2) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                "merge single partition is meaningless");
        }

        //Map<PartitionName, isDefaultPartition>
        Map<String, Boolean> allExistingPartitions = new HashMap<>();
        partitionInfo.getPartitionBy().getPartitions().stream()
            .forEach(o -> allExistingPartitions.put(o.getName().toLowerCase(), o.getIsDefaultPartition()));
        /**
         * 1. all the partition to be merged should be existing
         * 2. default partition should not be merged
         * */
        PartitionStrategy strategy = partitionInfo.getPartitionBy().getStrategy();
        for (String toBeMerged : partitionsToBeMerged) {
            if (!allExistingPartitions.containsKey(toBeMerged)) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                    String.format("partition [%s] does not exist", toBeMerged));
            }
            if (strategy == PartitionStrategy.LIST || strategy == PartitionStrategy.LIST_COLUMNS) {
                if (allExistingPartitions.get(toBeMerged) == true) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                        String.format("partition [%s] is not allowed to be merged, because it is default partition",
                            toBeMerged));
                }
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

        if (partitionInfo.getPartitionBy().getStrategy() != PartitionStrategy.LIST
            && partitionInfo.getPartitionBy().getStrategy() != PartitionStrategy.LIST_COLUMNS) {
            // check whether partitions to be merged are contiguous
            List<PartitionGroupRecord> mergeRecords = tableGroupConfig.getPartitionGroupRecords().stream()
                .filter(o -> partitionsToBeMerged.contains(o.partition_name.toLowerCase()))
                .collect(Collectors.toList());
            assert GeneralUtil.isNotEmpty(mergeRecords);
            Set<Long> partGroupIds = mergeRecords.stream().map(o -> o.id).collect(Collectors.toSet());

            List<PartitionSpec> mergePartSpecs = partitionInfo.getPartitionBy().getPartitions().stream()
                .filter(o -> partGroupIds.contains(o.getLocation().getPartitionGroupId())).collect(
                    Collectors.toList());
            mergePartSpecs.sort(Comparator.comparing(PartitionSpec::getPosition));
            for (int i = 1; i < mergePartSpecs.size(); i++) {
                Long prev = mergePartSpecs.get(i - 1).getPosition();
                Long curr = mergePartSpecs.get(i).getPosition();
                if (prev + 1 != curr) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                        String.format("partitions to be merged must be continuous, but %s and %s is not",
                            mergePartSpecs.get(i - 1).getName(), mergePartSpecs.get(i).getName()));
                }
            }
        }
    }

    public static void alterTableGroupMovePartitionCheck(SqlAlterTableMovePartition sqlAlterTableGroup,
                                                         TableGroupConfig tableGroupConfig,
                                                         ExecutionContext executionContext) {

        final String metadbStorageInstId = ScaleOutPlanUtil.getMetaDbStorageInstId();
        boolean oldPartitionsChange = false;

        Map<SqlNode, List<SqlNode>> newInstPartitions = new HashMap<>();

        if (tableGroupConfig.getTableGroupRecord().isBroadCastTableGroup()) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                "can't move the partition for broadcast tables");
        }
        for (Map.Entry<SqlNode, List<SqlNode>> entry : sqlAlterTableGroup.getInstPartitions().entrySet()) {
            String targetInstId = Util.last(((SqlIdentifier) (entry.getKey())).names);
            Set<String> partitionsToBeMoved = new TreeSet<>(String::compareToIgnoreCase);
            entry.getValue().stream().forEach(o -> partitionsToBeMoved.add(Util.last(((SqlIdentifier) (o)).names)));
            List<SqlNode> oldPartitions = new ArrayList<>();
            if (!ScaleOutPlanUtil.storageInstIsReady(targetInstId)) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                    targetInstId
                        + " is not a valid storage instance id");
            }

            if (targetInstId.equalsIgnoreCase(metadbStorageInstId)) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                    " it's not allow to move normal partitions to the storage instance:" + metadbStorageInstId
                        + ", which is only for hosting the metaDb");
            }
            for (String partitionToBeMoved : partitionsToBeMoved) {
                PartitionGroupRecord partitionGroupRecord = tableGroupConfig.getPartitionGroupRecords().stream()
                    .filter(o -> partitionToBeMoved.equalsIgnoreCase(o.partition_name)).findFirst().orElse(null);
                if (partitionGroupRecord == null) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_NAME_NOT_EXISTS,
                        "the partition group:" + partitionToBeMoved + " is not exists");
                }
                String sourceGroupKey = GroupInfoUtil.buildGroupNameFromPhysicalDb(partitionGroupRecord.phy_db);
                final String sourceInstId = DbTopologyManager
                    .getStorageInstIdByGroupName(InstIdUtil.getInstId(),
                        tableGroupConfig.getTableGroupRecord().getSchema(),
                        sourceGroupKey);
                if (!sourceInstId.equalsIgnoreCase(targetInstId)) {
                    oldPartitions.add(new SqlIdentifier(partitionToBeMoved, SqlParserPos.ZERO));
                } else {
                    oldPartitionsChange = true;
                }
            }
            if (GeneralUtil.isNotEmpty(oldPartitions)) {
                newInstPartitions.put(new SqlIdentifier(targetInstId, SqlParserPos.ZERO), oldPartitions);
            }
        }

        if (oldPartitionsChange) {
            sqlAlterTableGroup.getInstPartitions().clear();
            sqlAlterTableGroup.getInstPartitions().putAll(newInstPartitions);
        }

    }

    private static void alterTableGroupExtractPartitionCheck(SqlAlterTableGroup sqlAlterTableGroup,
                                                             TableGroupConfig tableGroupConfig,
                                                             ExecutionContext executionContext) {
        final SchemaManager schemaManager = executionContext.getSchemaManager();

        String tableInCurrentGroup = tableGroupConfig.getAllTables().get(0).getLogTbRec().tableName;
        TableMeta tableMeta = schemaManager.getTable(tableInCurrentGroup);
        PartitionInfo partitionInfo = tableMeta.getPartitionInfo();
        assert GeneralUtil.isNotEmpty(sqlAlterTableGroup.getPartRexInfoCtx());

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
            PartitionInfoUtil.FULL_PART_COL_COUNT);
    }

    public static void alterTableExtractPartitionCheck(String schemaName,
                                                       String logicalTableName,
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
        final SchemaManager schemaManager = executionContext.getSchemaManager();

        String tableInCurrentGroup = tableGroupConfig.getAllTables().get(0).getLogTbRec().tableName;
        TableMeta tableMeta = schemaManager.getTable(tableInCurrentGroup);
        PartitionInfo partitionInfo = tableMeta.getPartitionInfo();
        assert GeneralUtil.isNotEmpty(sqlAlterTableGroup.getPartRexInfoCtx());

        if (partitionInfo.isSingleTable() || partitionInfo.isBroadcastTable()) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                "can't split the partition group for single/broadcast tables");
        }

        if (partitionInfo.getPartitionBy().getStrategy() != PartitionStrategy.KEY) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                "it's not allow to split partition by hot value for non-key partition table");
        }

        SqlAlterTableGroupSplitPartitionByHotValue sqlAlterTableSplitPartitionByHotValue =
            (SqlAlterTableGroupSplitPartitionByHotValue) sqlAlterTableGroup.getAlters().get(0);
        SqlNode partitions = sqlAlterTableSplitPartitionByHotValue.getPartitions();
        int splitIntoParts = ((SqlNumericLiteral) (partitions)).intValue(true);
        if (splitIntoParts <= 0) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                "partitions must greater than 0");
        }
        TableGroupRecord tableGroupRecord = tableGroupConfig.getTableGroupRecord();
        int newPrefixPartColCnt = sqlAlterTableSplitPartitionByHotValue.getHotKeys().size();
        if (splitIntoParts > 1) {
            newPrefixPartColCnt += 1;
        }
        alterTableGroupCheckPartitionColumn(tableGroupRecord.getSchema(), tableGroupRecord.getTg_name(),
            newPrefixPartColCnt);

    }

    public static void alterTableSplitPartitionByHotValueCheck(String schemaName,
                                                               String logicalTableName,
                                                               SqlAlterTableSplitPartitionByHotValue sqlAlterTableSplitPartitionByHotValue,
                                                               Map<SqlNode, RexNode> rexExprInfo,
                                                               ExecutionContext executionContext) {
        final SchemaManager schemaManager = executionContext.getSchemaManager(schemaName);

        TableMeta tableMeta = schemaManager.getTable(logicalTableName);
        PartitionInfo partitionInfo = tableMeta.getPartitionInfo();
        assert GeneralUtil.isNotEmpty(rexExprInfo);

        if (partitionInfo.isSingleTable() || partitionInfo.isBroadcastTable()) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                "can't split the partition for single/broadcast tables");
        }

        if (partitionInfo.getPartitionBy().getStrategy() != PartitionStrategy.KEY) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                "it's not allow to split partition by hot value for non-key partition table");
        }

        SqlNode partitions = sqlAlterTableSplitPartitionByHotValue.getPartitions();
        int splitIntoParts = ((SqlNumericLiteral) (partitions)).intValue(true);
        if (splitIntoParts <= 0) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                "partitions must greater than 0");
        }

    }

    public static void alterTableGroupDropPartitionCheck(SqlAlterTableDropPartition sqlAlterTableDropPartition,
                                                         TableGroupConfig tableGroupConfig,
                                                         ExecutionContext executionContext) {
        final SchemaManager schemaManager = executionContext.getSchemaManager();

        String tableInCurrentGroup = tableGroupConfig.getAllTables().get(0).getLogTbRec().tableName;
        TableMeta tableMeta = schemaManager.getTable(tableInCurrentGroup);
        PartitionInfo partitionInfo = tableMeta.getPartitionInfo();
        Set<String> partitionGroupNames =
            tableGroupConfig.getPartitionGroupRecords().stream().map(o -> o.partition_name.toLowerCase())
                .collect(Collectors.toSet());
        for (SqlNode sqlNode : sqlAlterTableDropPartition.getPartitionNames()) {
            if (!partitionGroupNames.contains(((SqlIdentifier) sqlNode).getLastName().toLowerCase())) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                    "partition name:" + ((SqlIdentifier) sqlNode).getLastName() + " is not exists");
            }
        }

        if (partitionInfo.getPartitionBy().getStrategy() == PartitionStrategy.HASH
            || partitionInfo.getPartitionBy().getStrategy() == PartitionStrategy.KEY) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                "it's not allow to drop partition for hash/key partition strategy tables");
        }
        if (partitionInfo.getPartitionBy().getPartitions().size() <= 1) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                "it's not allow to drop partition for tables only has one partition");
        }
        for (TablePartRecordInfoContext record : tableGroupConfig.getAllTables()) {
            TableMeta tbMeta = schemaManager.getTable(record.getTableName());
            if (tbMeta.withGsi()) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                    String.format("it's not support to drop partition when table[%s] with GSI", record.getTableName()));
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
        if (partitionInfo.getPartitionBy().getStrategy() != PartitionStrategy.LIST
            && partitionInfo.getPartitionBy().getStrategy() != PartitionStrategy.LIST_COLUMNS) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                String.format("it's not allow to add/drop values for tableGroup[%s], which partition strategy is [%s]",
                    tableGroupConfig.getTableGroupRecord().getTg_name(),
                    partitionInfo.getPartitionBy().getStrategy().toString()));
        }
        for (PartitionSpec spec : partitionInfo.getPartitionBy().getOrderedPartitionSpec()) {
            if (spec.getIsDefaultPartition()) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                    String.format("It's not allowed to modify partition when table group contain default partition"));
            }
        }
        if (sqlModifyListPartitionValues.isDrop()) {
            String partName = ((SqlIdentifier) (sqlModifyListPartitionValues.getPartition().getName())).getLastName();
            PartitionSpec partitionSpec = partitionInfo.getPartitionBy().getPartitions().stream()
                .filter(o -> o.getName().equalsIgnoreCase(partName)).findFirst().orElse(null);
            if (partitionSpec == null) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                    String.format("partition[%s] is not exists", partName));
            }
            List<SearchDatumInfo> originalDatums = partitionSpec.getBoundSpec().getMultiDatums();

            if (originalDatums.size() == 1) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                    String.format("partition[%s] has only one value now, can't drop value any more", partName));
            }
            if (originalDatums.size() <= sqlModifyListPartitionValues.getPartition().getValues().getItems().size()) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                    String.format(
                        "the number of drop values should less than the number of values contain by partition[%s]",
                        partName));
            }
            for (TablePartRecordInfoContext record : tableGroupConfig.getAllTables()) {
                TableMeta tbMeta = schemaManager.getTable(record.getTableName());
                if (tbMeta.withGsi()) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                        String.format("it's not support to drop value when table[%s] with GSI", record.getTableName()));
                }
            }
        }
        //TODO @chengbi
    }

    public static void alterTableGroupRenamePartitionCheck(
        SqlAlterTableRenamePartition sqlAlterTableGroupRenamePartition,
        TableGroupConfig tableGroupConfig,
        ExecutionContext executionContext) {
        Set<String> oldPartitionNames = new HashSet<>();
        Set<String> newPartitionNames = new HashSet<>();
        Set<String> partitionGroupNames =
            tableGroupConfig.getPartitionGroupRecords().stream().map(o -> o.partition_name.toLowerCase())
                .collect(Collectors.toSet());
        List<Pair<String, String>> partitionNamesPair =
            sqlAlterTableGroupRenamePartition.getChangePartitionsPair();
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
            if (!partitionGroupNames.contains(pair.getKey().toLowerCase())) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                    "partition name:" + pair.getKey() + " is not exists");
            }
            PartitionNameUtil.validatePartName(pair.getValue(), KeyWordsUtil.isKeyWord(pair.getValue()));
        }
        for (Pair<String, String> pair : partitionNamesPair) {
            if (partitionGroupNames.contains(pair.getValue().toLowerCase()) && !oldPartitionNames.contains(
                pair.getValue())) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                    "partition name:" + pair.getValue() + " is exists");
            }
        }
    }

    private static void alterTableGroupAddPartitionCheck(TableGroupConfig tableGroupConfig,
                                                         String schemaName,
                                                         SqlAlterTableAddPartition sqlAlterTableAddPartition) {
        final SchemaManager schemaManager = OptimizerContext.getContext(schemaName).getLatestSchemaManager();
        TableGroupRecord tableGroupRecord = tableGroupConfig.getTableGroupRecord();
        String tgName = tableGroupConfig.getTableGroupRecord().getTg_name();
        String firstTableInCurrentGroup = tableGroupConfig.getAllTables().get(0).getLogTbRec().tableName;
        TableMeta tableMeta = schemaManager.getTable(firstTableInCurrentGroup);
        PartitionInfo partitionInfo = tableMeta.getPartitionInfo();
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
        int actualPartColCnt = PartitionInfoUtil.getActualPartColumnMetasInfoForTableGroup(schemaName, tgName).size();
        int newPrefixPartColCnt =
            PartitionInfoUtil.getNewPrefixPartColCntBySqlPartitionAst(Integer.MAX_VALUE/*ignore*/, actualPartColCnt,
                strategy, newPartitions);
        if (strategy == PartitionStrategy.RANGE_COLUMNS || strategy == PartitionStrategy.KEY) {
            /**
             * Check if is allowed to use newPrefixPartColCnt as new partColCnt for all tables in table group
             */
            alterTableGroupCheckPartitionColumn(schemaName, tgName, newPrefixPartColCnt);
        } else {
            alterTableGroupCheckPartitionColumn(schemaName, tgName, PartitionInfoUtil.FULL_PART_COL_COUNT);
        }

    }

    private static void alterTableGroupCheckPartitionColumn(String schemaName, String tableGroup,
                                                            int actualPartitionCols) {
        boolean identical =
            PartitionInfoUtil.allTablesWithIdenticalPartitionColumns(schemaName, tableGroup, actualPartitionCols);
        if (!identical) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                String.format("not all the tables in tablegroup[%s] has identical partition columns",
                    tableGroup));
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
            String errMsg = String.format(
                "The joinGroup of tableGroup:[%s] is not match with the joinGroup of tableGroup[%s]",
                tableGroup, targetTableGroup);
            if (GeneralUtil.isNotEmpty(tableGroupConfig.getAllTables())) {
                String sourceTable = tableGroupConfig.getAllTables().get(0).getTableName();
                PartitionInfo sourcePartitionInfo = partitionInfoManager.getPartitionInfo(sourceTable);
                if (!sourcePartitionInfo.equals(partitionInfo)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                        String.format(
                            "the partition definition of the tableGroup:[%s] mismatch the tableGroup[%s]",
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
                        String.format("the physical location of %s is not the same as %s",
                            tableGroup, targetTableGroup));
                }
                JoinGroupValidator.validateJoinGroupInfo(preparedData.getSchemaName(), tableGroup, joinGroupName,
                    errMsg,
                    ec, null);
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
                    String.format(
                        "the table:[%s] not exists",
                        tableName));
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
                        String.format(
                            "the partition definition of the table:[%s] does not match the tableGroup[%s]",
                            tableName, targetTableGroup));
                } else {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                        String.format(
                            "the partition definition of the table:[%s] is different from table:[%s]",
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
                            String.format("the physical location of %s is not match with %s",
                                tableName, partitionInfo.getTableName()));
                    } else {
                        throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                            String.format("the physical location of %s is not match with tables in %s",
                                tableName, targetTableGroup));
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
                    String errMsg = String.format(
                        "The joinGroup of table:[%s] is not match with the joinGroup of tableGroup[%s]",
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

    public static String fetchCreateTableDefinition(RelNode relNode,
                                                    ExecutionContext executionContext,
                                                    String groupKey,
                                                    String phyTableName,
                                                    String schemaName) {
        Cursor cursor = null;
        String primaryTableDefinition = null;
        try {
            cursor = ExecutorHelper.execute(
                new PhyShow(relNode.getCluster(),
                    relNode.getTraitSet(),
                    SqlShowCreateTable
                        .create(SqlParserPos.ZERO,
                            new SqlIdentifier(phyTableName, SqlParserPos.ZERO)),
                    relNode.getRowType(),
                    groupKey,
                    phyTableName,
                    schemaName), executionContext);
            Row row = null;
            if ((row = cursor.next()) != null) {
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
        LogicalAlterTableExtractPartition extractPartitionPlan,
        boolean isAlterTable, ExecutionContext executionContext) {
        final String SPLIT_SQL = isAlterTable ?
            "ALTER TABLE {0} SPLIT INTO {1} PARTITIONS 1 BY HOT VALUE({2});" :
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
        String splitSql =
            MessageFormat.format(SPLIT_SQL, objectName, newPartitionName,
                sqlAlterTableExtractPartition.getHotKeys().stream().map(o -> o.toString())
                    .collect(Collectors.joining(", ")));

        return splitSql;
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
        final String SPLIT_SQL = isAlterTable ?
            "ALTER TABLE {0} split PARTITION {1} into ({2});" :
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
            partBoundExprInfo = sqlAlterTableGroup.getPartRexInfoCtx();
            sqlAlterTableExtractPartition = (SqlAlterTableExtractPartition) sqlAlterTableGroup.getAlters().get(0);
        }

        PartitionStrategy strategy = partitionInfo.getPartitionBy().getStrategy();
        int partitionColumnCnt = partitionInfo.getPartitionColumns().size();
        boolean isMultiColumns = (partitionColumnCnt > 1);

        //convert hotKey sqlNode to SearchDatumInfo
        SearchDatumComparator comparator = partitionInfo.getPartitionBy().getPruningSpaceComparator();
        PartitionIntFunction partIntFunc = partitionInfo.getPartitionBy().getPartIntFunc();

        if (sqlAlterTableExtractPartition.getHotKeys() == null
            || sqlAlterTableExtractPartition.getHotKeys().size() != 1) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                "only allows to extract one key each time");
        }
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

        //use hot key to find split partition
        boolean hasDefaultPartition = false;
        PartitionSpec splitPartitionSpec = null, defaultPartitionSpec = null;
        for (PartitionSpec spec : partitionInfo.getPartitionBy().getOrderedPartitionSpec()) {
            if (spec.getIsDefaultPartition()) {
                hasDefaultPartition = true;
                defaultPartitionSpec = spec;
            }
            if (spec.getName().equals(newPartitionName)) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                    "new partition name [" + newPartitionName + "] duplicate with existing partition");
            }
            MultiValuePartitionBoundSpec listBoundSpec =
                (MultiValuePartitionBoundSpec) spec.getBoundSpec();
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
        MultiValuePartitionBoundSpec listBoundSpec = (MultiValuePartitionBoundSpec) splitPartitionSpec.getBoundSpec();
        for (SearchDatumInfo datumInfo : listBoundSpec.getMultiDatums()) {
            if (datumInfo.equals(hotKeyDatum)) {
                continue;
            }
            newDatumInfos.add(datumInfo);
        }
        MultiValuePartitionBoundSpec newListBoundSpec = new MultiValuePartitionBoundSpec();
        newListBoundSpec.setMultiDatums(newDatumInfos);

        String newVal = newListBoundSpec.toString();
        String newPartition = MessageFormat.format(PARTITION_DEF, newPartitionName, hotKeyDatum.toString());
        String oldPartition =
            newVal.isEmpty() ? null : MessageFormat.format(PARTITION_DEF, splitPartitionSpec.getName(), newVal);
        String partitions = oldPartition == null ? newPartition : newPartition + " , " + oldPartition;

        String splitSql =
            MessageFormat.format(SPLIT_SQL, objectName, splitPartitionSpec.getName(), partitions);

        return splitSql;
    }

    /**
     * need to check:
     * 1. new partitions should not have intersection with old partitions
     * 2. new partitions should not contain default, and new partitions should not intersection with each other.
     */
    public static String convertAddListRelToSplitListSql(LogicalAlterTableAddPartition addPartitionPlan,
                                                         boolean isAlterTable, ExecutionContext executionContext) {
        final String SPLIT_TABLEGROUP_SQL = isAlterTable ? "ALTER TABLE {0} split PARTITION {1} into ({2});"
            : "ALTER TABLEGROUP {0} split PARTITION {1} into ({2});";

        PartitionInfo partitionInfo;
        SqlAlterTableAddPartition sqlAlterTableAddPartition;
        String objectName;
        if (isAlterTable) {
            AlterTable alterTable = (AlterTable) addPartitionPlan.relDdl;
            objectName = Util.last(((SqlIdentifier) alterTable.getTableName()).names);
            partitionInfo = executionContext.getSchemaManager(addPartitionPlan.getSchemaName()).getTddlRuleManager()
                .getPartitionInfoManager().getPartitionInfo(objectName);
            SqlAlterTable sqlAlterTable = (SqlAlterTable) alterTable.getSqlNode();
            sqlAlterTableAddPartition = (SqlAlterTableAddPartition) sqlAlterTable.getAlters().get(0);
        } else {
            objectName = addPartitionPlan.getTableName();
            partitionInfo = getPartitionInfo(objectName, addPartitionPlan.getSchemaName());
            SqlAlterTableGroup sqlAlterTableGroup = (SqlAlterTableGroup) ((addPartitionPlan).relDdl.getSqlNode());
            sqlAlterTableAddPartition = (SqlAlterTableAddPartition) sqlAlterTableGroup.getAlters().get(0);
        }

        PartitionStrategy strategy = partitionInfo.getPartitionBy().getStrategy();

        PartitionSpec defaultSpec = null;
        //todo: validate if HashSet is ok
        Set<SearchDatumInfo> allPartsValSet = new HashSet<>();
        for (PartitionSpec spec : partitionInfo.getPartitionBy().getOrderedPartitionSpec()) {
            if (spec.getIsDefaultPartition()) {
                defaultSpec = spec;
            }
            MultiValuePartitionBoundSpec listBoundSpec =
                (MultiValuePartitionBoundSpec) spec.getBoundSpec();
            for (SearchDatumInfo datumInfo : listBoundSpec.getMultiDatums()) {
                if (datumInfo.containDefaultValue()) {
                    continue;
                }
                allPartsValSet.add(datumInfo);
            }
        }

        if (defaultSpec == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                "default partition not found");
        }
        List<SqlPartition> newPartitions = new ArrayList<>();
        sqlAlterTableAddPartition.getPartitions().stream().forEach(o -> newPartitions.add((SqlPartition) o));
        List<ColumnMeta> partColMetaList = partitionInfo.getPartitionBy().getPartitionFieldList();
        SearchDatumComparator comparator = partitionInfo.getPartitionBy().getPruningSpaceComparator();
        PartitionIntFunction partIntFunc = partitionInfo.getPartitionBy().getPartIntFunc();
        int actualPartColCnt = isAlterTable ? partitionInfo.getActualPartitionColumns().size() :
            PartitionInfoUtil.getActualPartColumnMetasInfoForTableGroup(partitionInfo.getTableSchema(), objectName)
                .size();
        int newPrefixPartColCnt =
            PartitionInfoUtil.getNewPrefixPartColCntBySqlPartitionAst(Integer.MAX_VALUE/*ignore*/, actualPartColCnt,
                strategy, newPartitions);

        HashSet<SearchDatumInfo> newPartsValSet = new HashSet<>();
        List<String> newPartitionsExpr = new ArrayList<>();
        for (SqlPartition sqlPartition : newPartitions) {
            PartitionSpec newSpec = PartitionInfoBuilder
                .buildPartitionSpecByPartSpecAst(
                    executionContext,
                    partColMetaList,
                    partIntFunc,
                    comparator,
                    ((SqlAlterTableGroup) sqlAlterTableAddPartition.getParent()).getPartRexInfoCtx(),
                    null,
                    sqlPartition,
                    strategy,
                    0,
                    newPrefixPartColCnt
                );
            if (newSpec.getIsDefaultPartition()) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                    "only allows one default partition.");
            }

            MultiValuePartitionBoundSpec newBoundSpec = (MultiValuePartitionBoundSpec) newSpec.getBoundSpec();
            for (SearchDatumInfo datumInfo : newBoundSpec.getMultiDatums()) {
                if (allPartsValSet.contains(datumInfo)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                        String.format("partition[%s] value [%s] duplicated with old partition", newSpec.getName(),
                            datumInfo));
                }
                if (newPartsValSet.contains(datumInfo)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                        String.format("partition[%s] value [%s] duplicated with new partition", newSpec.getName(),
                            datumInfo));
                }
                newPartsValSet.add(datumInfo);
            }

            newPartitionsExpr.add(newSpec.toString());
        }

        newPartitionsExpr.add(defaultSpec.toString());
        String finalNewPartsExpr = String.join(", ", newPartitionsExpr);
        String finalSql =
            MessageFormat.format(SPLIT_TABLEGROUP_SQL, objectName, defaultSpec.getName(), finalNewPartsExpr);
        return finalSql;
    }

}

