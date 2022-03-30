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
import com.alibaba.polardbx.gms.partition.TablePartRecordInfoContext;
import com.alibaba.polardbx.gms.partition.TablePartitionRecord;
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
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.partition.MultiValuePartitionBoundSpec;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoBuilder;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoUtil;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.partition.PartitionStrategy;
import com.alibaba.polardbx.optimizer.partition.RangeBoundSpec;
import com.alibaba.polardbx.optimizer.partition.datatype.PartitionField;
import com.alibaba.polardbx.optimizer.partition.datatype.PartitionFieldBuilder;
import com.alibaba.polardbx.optimizer.partition.datatype.function.PartitionIntFunction;
import com.alibaba.polardbx.optimizer.partition.pruning.SearchDatumComparator;
import com.alibaba.polardbx.optimizer.partition.pruning.SearchDatumInfo;
import com.alibaba.polardbx.optimizer.tablegroup.TableGroupInfoManager;
import com.alibaba.polardbx.optimizer.utils.KeyWordsUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlAlterSpecification;
import org.apache.calcite.sql.SqlAlterTableAddPartition;
import org.apache.calcite.sql.SqlAlterTableDropPartition;
import org.apache.calcite.sql.SqlAlterTableGroup;
import org.apache.calcite.sql.SqlAlterTableGroupExtractPartition;
import org.apache.calcite.sql.SqlAlterTableGroupMergePartition;
import org.apache.calcite.sql.SqlAlterTableGroupMovePartition;
import org.apache.calcite.sql.SqlAlterTableGroupRenamePartition;
import org.apache.calcite.sql.SqlAlterTableGroupSplitPartition;
import org.apache.calcite.sql.SqlAlterTableGroupSplitPartitionByHotValue;
import org.apache.calcite.sql.SqlAlterTableModifyPartitionValues;
import org.apache.calcite.sql.SqlAlterTableTruncatePartition;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlPartition;
import org.apache.calcite.sql.SqlShowCreateTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.Util;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by luoyanxin
 *
 * @author luoyanxin
 */
public class AlterTableGroupUtils {
    public static void alterTableGroupPreCheck(SqlAlterTableGroup sqlAlterTableGroup,
                                               ExecutionContext executionContext) {
        List<SqlAlterSpecification> alterSpecifications = sqlAlterTableGroup.getAlters();
        SqlIdentifier original = (SqlIdentifier) sqlAlterTableGroup.getTableGroupName();
        String tableGroupName = Util.last(original.names);
        if (GeneralUtil.isNotEmpty(alterSpecifications)) {
            assert alterSpecifications.size() == 1;

            final TableGroupInfoManager tableGroupInfoManager =
                OptimizerContext.getContext(executionContext.getSchemaName()).getTableGroupInfoManager();

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
                alterTableGroupSplitPartitionCheck(sqlAlterTableGroup, tableGroupConfig, executionContext);
            } else if (alterSpecifications.get(0) instanceof SqlAlterTableGroupMergePartition) {
                final SqlAlterTableGroupMergePartition sqlAlterTableGroupMergePartition =
                    (SqlAlterTableGroupMergePartition) alterSpecifications.get(0);
                sqlAlterTableGroupMergePartition.setParent(sqlAlterTableGroup);
                alterTableGroupMergePartitionCheck(sqlAlterTableGroup, tableGroupConfig, executionContext);
            } else if (alterSpecifications.get(0) instanceof SqlAlterTableGroupMovePartition) {
                final SqlAlterTableGroupMovePartition sqlAlterTableGroupMovePartition =
                    (SqlAlterTableGroupMovePartition) alterSpecifications.get(0);
                sqlAlterTableGroupMovePartition.setParent(sqlAlterTableGroup);
                alterTableGroupMovePartitionCheck(sqlAlterTableGroup, tableGroupConfig, executionContext);
            } else if (alterSpecifications.get(0) instanceof SqlAlterTableGroupExtractPartition) {
                final SqlAlterTableGroupExtractPartition sqlAlterTableGroupExtractPartition =
                    (SqlAlterTableGroupExtractPartition) alterSpecifications.get(0);
                sqlAlterTableGroupExtractPartition.setParent(sqlAlterTableGroup);
                alterTableGroupExtractPartitionCheck(sqlAlterTableGroup, tableGroupConfig, executionContext);

            } else if (alterSpecifications.get(0) instanceof SqlAlterTableAddPartition) {
                alterTableGroupAddPartitionCheck(tableGroupConfig, executionContext, (SqlAlterTableAddPartition) alterSpecifications.get(0));
            } else if (alterSpecifications.get(0) instanceof SqlAlterTableDropPartition) {
                alterTableGroupDropPartitionCheck(sqlAlterTableGroup, tableGroupConfig, executionContext);
            } else if (alterSpecifications.get(0) instanceof SqlAlterTableTruncatePartition) {
                // ignore
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
            } else {
                throw new UnsupportedOperationException(alterSpecifications.get(0).getClass().toString());
            }

        }
    }

    private static void alterTableGroupSplitPartitionCheck(SqlAlterTableGroup sqlAlterTableGroup,
                                                           TableGroupConfig tableGroupConfig,
                                                           ExecutionContext executionContext) {

        String schemaName = executionContext.getSchemaName();
        final SchemaManager schemaManager = OptimizerContext.getContext(schemaName).getLatestSchemaManager();
        List<SqlAlterSpecification> alterSpecifications = sqlAlterTableGroup.getAlters();
        String tableInCurrentGroup = tableGroupConfig.getAllTables().get(0).getLogTbRec().tableName;
        TableMeta tableMeta = schemaManager.getTable(tableInCurrentGroup);
        TableGroupRecord tableGroupRecord = tableGroupConfig.getTableGroupRecord();
        String tgName = tableGroupRecord.getTg_name();

        PartitionInfo partitionInfo = tableMeta.getPartitionInfo();

        if (partitionInfo.isSingleTable() || partitionInfo.isBroadcastTable()) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                "can't split the partition group for single/broadcast tables");
        }

        final SqlAlterTableGroupSplitPartition sqlAlterTableGroupSplitPartition =
            (SqlAlterTableGroupSplitPartition) (alterSpecifications.get(0));
        String splitPartitionName =
            Util.last(((SqlIdentifier) (sqlAlterTableGroupSplitPartition.getSplitPartitionName())).names);
        PartitionGroupRecord splitPartRecord = tableGroupConfig.getPartitionGroupRecords().stream()
            .filter(o -> o.partition_name.equalsIgnoreCase(splitPartitionName)).findFirst().orElse(null);
        if (splitPartRecord == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_NAME_NOT_EXISTS,
                "the partition:" + splitPartitionName + " is not exists this current table group");
        }

        PartitionStrategy strategy = partitionInfo.getPartitionBy().getStrategy();
        PartitionField atVal = calculateSplitAtVal(tableGroupConfig, partitionInfo, sqlAlterTableGroupSplitPartition);

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
            List<SqlPartition> newPartitions = sqlAlterTableGroupSplitPartition.getNewPartitions();
            List<PartitionSpec> newPartitionSpecs = new ArrayList<>();
            int actualPartColCnt = PartitionInfoUtil.getActualPartColumnMetasInfoForTableGroup(schemaName, tgName).size();
            int newPrefixPartColCnt = PartitionInfoUtil.getNewPrefixPartColCntBySqlPartitionAst(Integer.MAX_VALUE/*ignore*/, actualPartColCnt, strategy, newPartitions);
            if (strategy == PartitionStrategy.RANGE_COLUMNS || strategy == PartitionStrategy.KEY) {
                /**
                 * Check if is allowed to use newPrefixPartColCnt as neew partColCnt for all tables in table group
                 */
                if (!newPartitions.isEmpty()) {
                    alterTableGroupCheckPartitionColumn(schemaName, tgName, newPrefixPartColCnt);
                }
            }

            for (SqlPartition sqlPartition : sqlAlterTableGroupSplitPartition.getNewPartitions()) {
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
                        sqlAlterTableGroupSplitPartition.getParent().getPartRexInfoCtx(),
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
                                                      SqlAlterTableGroupSplitPartition sqlAlter) {
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
                if (partitionGroupRecord != null) {
                    throw new TddlRuntimeException(ErrorCode.ERR_DUPLICATED_PARTITION_NAME,
                        "partition name:" + partitionName + " is exists");
                }
            }
        }
        return atVal;
    }

    private static void alterTableGroupMergePartitionCheck(SqlAlterTableGroup sqlAlterTableGroup,
                                                           TableGroupConfig tableGroupConfig,
                                                           ExecutionContext executionContext) {
        final SchemaManager schemaManager = executionContext.getSchemaManager();

        List<SqlAlterSpecification> alterSpecifications = sqlAlterTableGroup.getAlters();

        String tableInCurrentGroup = tableGroupConfig.getAllTables().get(0).getLogTbRec().tableName;
        TableMeta tableMeta = schemaManager.getTable(tableInCurrentGroup);
        PartitionInfo partitionInfo = tableMeta.getPartitionInfo();

        final SqlAlterTableGroupMergePartition sqlAlterTableGroupMergePartition =
            (SqlAlterTableGroupMergePartition) (alterSpecifications.get(0));

        String targetPartitionName =
            Util.last(((SqlIdentifier) (sqlAlterTableGroupMergePartition.getTargetPartitionName())).names);
        PartitionGroupRecord partRecord = tableGroupConfig.getPartitionGroupRecords().stream()
            .filter(o -> o.partition_name.equalsIgnoreCase(targetPartitionName)).findFirst().orElse(null);

        Set<String> partitionsToBeMerged = sqlAlterTableGroupMergePartition.getOldPartitions().stream()
            .map(o -> Util.last(((SqlIdentifier) (o)).names).toLowerCase()).collect(
                Collectors.toSet());
        if (partitionsToBeMerged.size() < 2) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                "merge single partition is meaningless");
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

    private static void alterTableGroupMovePartitionCheck(SqlAlterTableGroup sqlAlterTableGroup,
                                                          TableGroupConfig tableGroupConfig,
                                                          ExecutionContext executionContext) {
        List<SqlAlterSpecification> alterSpecifications = sqlAlterTableGroup.getAlters();

        final SqlAlterTableGroupMovePartition sqlAlterTableGroupMovePartition =
            (SqlAlterTableGroupMovePartition) (alterSpecifications.get(0));

        Set<String> partitionsToBeMoved = sqlAlterTableGroupMovePartition.getOldPartitions().stream()
            .map(o -> Util.last(((SqlIdentifier) (o)).names).toLowerCase()).collect(
                Collectors.toSet());
        boolean oldPartitionsChange = false;
        List<SqlNode> oldPartitions = new ArrayList<>();
        String targetInstId = ((SqlIdentifier) sqlAlterTableGroupMovePartition.getTargetStorageId()).getSimple();
        if (!ScaleOutPlanUtil.storageInstIsReady(targetInstId)) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                sqlAlterTableGroupMovePartition.getTargetStorageId().toString()
                    + " is not a valid storage instance id");
        }

        final String metadbStorageInstId = ScaleOutPlanUtil.getMetaDbStorageInstId();
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
                .getStorageInstIdByGroupName(InstIdUtil.getInstId(), tableGroupConfig.getTableGroupRecord().getSchema(),
                    sourceGroupKey);
            if (!sourceInstId.equalsIgnoreCase(targetInstId)) {
                oldPartitions.add(new SqlIdentifier(partitionToBeMoved, SqlParserPos.ZERO));
            } else {
                oldPartitionsChange = true;
            }
        }
        if (oldPartitionsChange) {
            sqlAlterTableGroupMovePartition.getOldPartitions().clear();
            sqlAlterTableGroupMovePartition.getOldPartitions().addAll(oldPartitions);
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
            && partitionInfo.getPartitionBy().getStrategy() != PartitionStrategy.KEY) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                "it's not allow to extract to partition by hot value for non-hash partition table");
        }
        TableGroupRecord tableGroupRecord = tableGroupConfig.getTableGroupRecord();
        alterTableGroupCheckPartitionColumn(tableGroupRecord.getSchema(), tableGroupRecord.getTg_name(), PartitionInfoUtil.FULL_PART_COL_COUNT);
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
        alterTableGroupCheckPartitionColumn(tableGroupRecord.getSchema(), tableGroupRecord.getTg_name(),newPrefixPartColCnt);

    }

    private static void alterTableGroupDropPartitionCheck(SqlAlterTableGroup sqlAlterTableGroup,
                                                          TableGroupConfig tableGroupConfig,
                                                          ExecutionContext executionContext) {
        final SchemaManager schemaManager = executionContext.getSchemaManager();

        String tableInCurrentGroup = tableGroupConfig.getAllTables().get(0).getLogTbRec().tableName;
        TableMeta tableMeta = schemaManager.getTable(tableInCurrentGroup);
        PartitionInfo partitionInfo = tableMeta.getPartitionInfo();
        Set<String> partitionGroupNames =
            tableGroupConfig.getPartitionGroupRecords().stream().map(o -> o.partition_name.toLowerCase())
                .collect(Collectors.toSet());
        SqlAlterTableDropPartition sqlAlterTableDropPartition =
            (SqlAlterTableDropPartition) sqlAlterTableGroup.getAlters().get(0);
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
        SchemaManager schemaManager = OptimizerContext.getContext(tablePartitionRecord.getTableSchema()).getLatestSchemaManager();
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

    private static void alterTableGroupRenamePartitionCheck(
        SqlAlterTableGroupRenamePartition sqlAlterTableGroupRenamePartition,
        TableGroupConfig tableGroupConfig,
        ExecutionContext executionContext) {
        Set<String> oldPartitionNames = new HashSet<>();
        Set<String> newPartitionNames = new HashSet<>();
        Set<String> partitionGroupNames =
            tableGroupConfig.getPartitionGroupRecords().stream().map(o -> o.partition_name.toLowerCase())
                .collect(Collectors.toSet());
        List<Pair<String, String>> partitionNamesPair =
            sqlAlterTableGroupRenamePartition.getChangePartitionsPair();
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
            if (partitionGroupNames.contains(pair.getValue().toLowerCase())) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                    "partition name:" + pair.getValue() + " is exists");
            }
            PartitionNameUtil.validatePartName(pair.getValue(), KeyWordsUtil.isKeyWord(pair.getValue()));
        }

    }

    private static void alterTableGroupAddPartitionCheck(TableGroupConfig tableGroupConfig,
                                                         ExecutionContext executionContext,
                                                         SqlAlterTableAddPartition sqlAlterTableAddPartition) {
        String schemaName = executionContext.getSchemaName();
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
        int newPrefixPartColCnt = PartitionInfoUtil.getNewPrefixPartColCntBySqlPartitionAst(Integer.MAX_VALUE/*ignore*/, actualPartColCnt, strategy, newPartitions);
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
}

