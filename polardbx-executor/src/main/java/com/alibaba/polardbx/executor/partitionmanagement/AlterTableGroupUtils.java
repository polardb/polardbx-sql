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
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.executor.sync.TableMetaChangeSyncAction;
import com.alibaba.polardbx.gms.partition.TablePartRecordInfoContext;
import com.alibaba.polardbx.gms.partition.TablePartitionAccessor;
import com.alibaba.polardbx.gms.partition.TablePartitionRecord;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupAccessor;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupAccessor;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.tablegroup.TableGroupRecord;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoExRecord;
import com.alibaba.polardbx.gms.util.GroupInfoUtil;
import com.alibaba.polardbx.gms.util.InstIdUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.gms.util.TableGroupNameUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager;
import com.alibaba.polardbx.optimizer.config.table.ScaleOutPlanUtil;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.CharType;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.rel.dal.PhyShow;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.partition.ListBoundSpec;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoBuilder;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoUtil;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.partition.PartitionStrategy;
import com.alibaba.polardbx.optimizer.partition.PartitionTableType;
import com.alibaba.polardbx.optimizer.partition.RangeBoundSpec;
import com.alibaba.polardbx.optimizer.partition.datatype.PartitionField;
import com.alibaba.polardbx.optimizer.partition.datatype.PartitionFieldBuilder;
import com.alibaba.polardbx.optimizer.partition.datatype.function.PartitionIntFunction;
import com.alibaba.polardbx.optimizer.partition.pruning.SearchDatumComparator;
import com.alibaba.polardbx.optimizer.partition.pruning.SearchDatumInfo;
import com.alibaba.polardbx.optimizer.tablegroup.TableGroupInfoManager;
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
import org.apache.calcite.sql.SqlAlterTableModifyPartitionValues;
import org.apache.calcite.sql.SqlAlterTableTruncatePartition;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlPartition;
import org.apache.calcite.sql.SqlShowCreateTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.Util;

import java.sql.Connection;
import java.util.ArrayList;
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
                    "tablegroup:" + tableGroupName + " is not exists");
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
                // TODO @chengbi
            } else if (alterSpecifications.get(0) instanceof SqlAlterTableDropPartition) {
                alterTableGroupDropPartitionCheck(sqlAlterTableGroup, tableGroupConfig, executionContext);
            } else if (alterSpecifications.get(0) instanceof SqlAlterTableTruncatePartition) {
                // TODO @chengbi
                
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
            } else {
                throw new UnsupportedOperationException(alterSpecifications.get(0).getClass().toString());
            }
            
            
        }
    }

    private static void alterTableGroupSplitPartitionCheck(SqlAlterTableGroup sqlAlterTableGroup,
                                                           TableGroupConfig tableGroupConfig,
                                                           ExecutionContext executionContext) {
        final SchemaManager schemaManager = executionContext.getSchemaManager();

        List<SqlAlterSpecification> alterSpecifications = sqlAlterTableGroup.getAlters();

        String tableInCurrentGroup = tableGroupConfig.getAllTables().get(0).getLogTbRec().tableName;
        TableMeta tableMeta = schemaManager.getTable(tableInCurrentGroup);
        PartitionInfo partitionInfo = tableMeta.getPartitionInfo();

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
            List<PartitionSpec> newPartitionSpecs = new ArrayList<>();
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
                        pos++);
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
                Set<PartitionField> newPartsValSet = new TreeSet<>();
                Set<PartitionField> oldPartsValSet = new TreeSet<>();
                boolean oldPartHasNull = false;
                boolean newPartHasNull = false;

                for (PartitionSpec partitionSpec : newPartitionSpecs) {
                    Set<PartitionField> newPartValSet = new TreeSet<>();

                    for (SearchDatumInfo datum : partitionSpec.getBoundSpec().getMultiDatums()) {
                        PartitionField partitionField = datum.getSingletonValue().getValue();
                        if (partitionField.isNull()) {
                            if (!newPartHasNull) {
                                newPartHasNull = true;
                            } else {
                                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                                    "duplicate values:NULL");
                            }
                        } else {
                            if (newPartValSet.contains(partitionField)) {
                                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                                    "duplicate values:" + partitionField.toString());
                            }
                            newPartValSet.add(partitionField);
                            if (newPartsValSet.contains(partitionField)) {
                                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                                    "duplicate values:" + partitionField.toString());
                            } else {
                                newPartsValSet.add(partitionField);
                            }
                        }
                    }
                }
                ListBoundSpec newListBoundSpec = (ListBoundSpec) splitPartitionSpec.getBoundSpec();
                for (SearchDatumInfo oldDatum : newListBoundSpec.getMultiDatums()) {
                    PartitionField oldVal = oldDatum.getSingletonValue().getValue();
                    if (oldVal.isNull()) {
                        oldPartHasNull = true;
                    } else {
                        oldPartsValSet.add(oldVal);
                        if (!newPartsValSet.contains(oldVal)) {
                            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                                "missing " + oldVal.toString() + " in the new Partition spec");
                        }
                    }
                }
                for (PartitionField val : newPartsValSet) {
                    if (!oldPartsValSet.contains(val)) {
                        throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                            "orphan " + val.toString() + " in the new Partition spec");
                    }
                }
                if (oldPartHasNull != newPartHasNull) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                        oldPartHasNull ? "missing NULL value in the new Partition spec" :
                            "orphan NULL value in the new Partition spec");
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

        if (partRecord != null && !partitionsToBeMerged.contains(partRecord.partition_name)) {
            throw new TddlRuntimeException(ErrorCode.ERR_DUPLICATED_PARTITION_NAME,
                "the partition group:" + targetPartitionName + " is exists");
        }

        if (partitionInfo.getPartitionBy().getStrategy() != PartitionStrategy.LIST) {
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
        if (!ScaleOutPlanUtil.checkStorageIdExistence(targetInstId)) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                sqlAlterTableGroupMovePartition.getTargetStorageId().toString()
                    + " is not a valid storage instance id");
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

        if (partitionInfo.getPartitionBy().getStrategy() != PartitionStrategy.HASH) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                "it's not allow to extract to partition by hot value for non-hash partition table");
        }
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
    }

    private static void alterTableModifyListPartitionValuesCheck(SqlAlterTableGroup sqlAlterTableGroup,
                                                                 TableGroupConfig tableGroupConfig,
                                                                 SqlAlterTableModifyPartitionValues sqlModifyListPartitionValues,
                                                                 ExecutionContext executionContext) {

        TablePartitionRecord tablePartitionRecord = tableGroupConfig.getAllTables().get(0).getLogTbRec();
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
        List<org.apache.calcite.util.Pair<String, String>> partitionNamesPair =
            sqlAlterTableGroupRenamePartition.getChangePartitionsPair();
        for (org.apache.calcite.util.Pair<String, String> pair : partitionNamesPair) {
            if (oldPartitionNames.contains(pair.left)) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                    "reChange partition:" + pair.left + " is not allow");
            } else {
                oldPartitionNames.add(pair.left.toLowerCase());
            }
            if (newPartitionNames.contains(pair.right.toLowerCase())) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                    "duplicate partition name:" + pair.right);
            } else {
                oldPartitionNames.add(pair.right.toLowerCase());
            }
            if (!partitionGroupNames.contains(pair.left.toLowerCase())) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                    "partition name:" + pair.left + " is not exists");
            }
            if (partitionGroupNames.contains(pair.right.toLowerCase())) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                    "partition name:" + pair.right + " is exists");
            }
        }

    }

    /**
     * Allocate the locations ( a phy tbl should be located on which group key ) for all the physical tables,
     * and save the result into allTargetTableLocations
     */
    public static void generateSourceAndTargetTablesForAlterTableGroup(ExecutionContext executionContext,
                                                                       TableGroupConfig tableGroupConfig,
                                                                       List<GroupDetailInfoExRecord> groupDetailInfoExRecords,
                                                                       SqlNode sqlAlterTableGroupSpec,
                                                                       List<PartitionGroupRecord> outDatedPartRecords,
                                                                       int newPartitionCount,
                                                                       Map<String, Map<String, Set<String>>> allSourceTableLocations,
                                                                       Map<String, List<Pair<String, String>>> allTargetTableLocations,
                                                                       Map<String, Map<String, List<List<String>>>> allTargetTables) {
        final SchemaManager schemaManager = executionContext.getSchemaManager();
        boolean isMovePartition = (sqlAlterTableGroupSpec instanceof SqlAlterTableGroupMovePartition);
        boolean isModifyPartition = (sqlAlterTableGroupSpec instanceof SqlAlterTableModifyPartitionValues);
        boolean isDropPartition = (sqlAlterTableGroupSpec instanceof SqlAlterTableDropPartition);

        /**
         * Find phy db from the group detail info of the target partition group by partition_name
         *
         */
        GroupDetailInfoExRecord grpInfoOfNewPhyTblForModifyPartition = null;
        if (isModifyPartition) {

            String targetPartName;
            SqlAlterTableModifyPartitionValues modifyPartition =
                (SqlAlterTableModifyPartitionValues) sqlAlterTableGroupSpec;
            targetPartName = ((SqlIdentifier) modifyPartition.getPartition().getName()).getLastName();
            PartitionGroupRecord targetPartRecord = tableGroupConfig.getPartitionGroupByName(targetPartName);
            String phyDb = targetPartRecord.phy_db;
            grpInfoOfNewPhyTblForModifyPartition =
                groupDetailInfoExRecords.stream().filter(g -> g.getPhyDbName().equalsIgnoreCase(phyDb)).findFirst()
                    .orElse(null);

        }

        /**
         * For each logical table, build new phyTable names for added phy tbl
         */
        for (TablePartRecordInfoContext infoContext : tableGroupConfig.getAllTables()) {
            String tableName = infoContext.getLogTbRec().tableName;
            TableMeta meta = schemaManager.getTable(tableName);
            PartitionInfo partitionInfo = meta.getPartitionInfo();

            Map<String, List<List<String>>> targetTables = new HashMap<>();
            List<Pair<String, String>> locations = new ArrayList<>();

            int index = 0;
            List<String> newPhyTables;
            if (isMovePartition) {
                newPhyTables = new ArrayList<>();
                for (PartitionGroupRecord partitionGroupRecord : outDatedPartRecords) {
                    TablePartitionRecord tablePartitionRecord = infoContext.getPartitionRecList().stream()
                        .filter(o -> o.groupId.longValue() == partitionGroupRecord.id.longValue()).findFirst()
                        .orElse(null);
                    assert tablePartitionRecord != null;
                    newPhyTables.add(tablePartitionRecord.phyTable);
                }
            } else {
                newPhyTables = PartitionInfoUtil
                    .getNextNPhyTableNames(partitionInfo, newPartitionCount);
            }

            /**
             * Build a location for each new added phy tables by a pair [phyTbl, grpKey]
             */
            for (int i = 0; i < newPhyTables.size(); i++) {
                String groupName;
                if (isModifyPartition || isDropPartition) {
                    /**
                     * For DropPartition/ModifyPartition, keep the phy_db of new added partition the same as the target partition 
                     */
                    groupName = grpInfoOfNewPhyTblForModifyPartition.getGroupName();
                } else {
                    groupName = groupDetailInfoExRecords.get(index).getGroupName();
                }

                List<String> phyTables = new ArrayList<>();
                phyTables.add(newPhyTables.get(i));
                if (targetTables.containsKey(groupName)) {
                    targetTables.get(groupName).add(phyTables);
                } else {
                    List<List<String>> tables = new ArrayList<>();
                    tables.add(phyTables);
                    targetTables.put(groupName, tables);
                }
                index++;
                if (index >= groupDetailInfoExRecords.size()) {
                    index = 0;
                }
                locations.add(new Pair<>(newPhyTables.get(i), groupName));
            }

            allTargetTableLocations.put(tableName, locations);
            Map<String, Set<String>> sourceLocation = PartitionUtils
                .getSourcePhyTables(sqlAlterTableGroupSpec, executionContext.getSchemaName(), tableName,
                    executionContext, false);
            allSourceTableLocations.put(tableName, sourceLocation);
            allTargetTables.put(tableName, targetTables);
        }
    }

    public static void generateSourceAndTargetTablesForSetTableGroup(PartitionInfo sourcePartitionInfo,
                                                                     PartitionInfo targetPartitionInfo,
                                                                     Map<String, Set<String>> allSourceTableLocations,
                                                                     Map<String, List<List<String>>> allTargetTables) {
        for (PartitionSpec targetPartitionSpec : targetPartitionInfo.getPartitionBy().getPartitions()) {
            PartitionSpec sourcePartitionSpec = sourcePartitionInfo.getPartitionBy().getPartitions().stream()
                .filter(o -> o.getName().equalsIgnoreCase(targetPartitionSpec.getName())).findFirst().orElse(null);
            if (sourcePartitionSpec == null) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                    "can't find the partition:" + targetPartitionSpec.getName());
            }
            if (!sourcePartitionSpec.getLocation().getGroupKey()
                .equalsIgnoreCase(targetPartitionSpec.getLocation().getGroupKey())) {
                String targetGroupName = targetPartitionSpec.getLocation().getGroupKey();
                String sourceGroupName = sourcePartitionSpec.getLocation().getGroupKey();
                String phyTable = sourcePartitionSpec.getLocation().getPhyTableName();
                allSourceTableLocations.computeIfAbsent(sourceGroupName, k -> new HashSet<String>())
                    .add(sourcePartitionSpec.getLocation().getPhyTableName());

                List<String> phyTables = new ArrayList<>();
                phyTables.add(phyTable);
                allTargetTables.computeIfAbsent(targetGroupName, k -> new ArrayList<>()).add(phyTables);

            }
        }
    }

    public static void addNewPartitionGroupFromPartitionInfo(PartitionInfo partitionInfo,
                                                             List<PartitionGroupRecord> partitionGroupRecords,
                                                             Long tableGroupId,
                                                             boolean tableGroupExists,
                                                             boolean reCreatePartitionGroups) {
        try (Connection connection = MetaDbUtil.getConnection()) {
            PartitionGroupAccessor partitionGroupAccessor = new PartitionGroupAccessor();
            TablePartitionAccessor tablePartitionAccessor = new TablePartitionAccessor();
            TableGroupAccessor tableGroupAccessor = new TableGroupAccessor();
            partitionGroupAccessor.setConnection(connection);
            tablePartitionAccessor.setConnection(connection);
            tableGroupAccessor.setConnection(connection);
            connection.setAutoCommit(false);
            boolean firstPart = true;
            boolean isSuccess = false;
            try {
                if (reCreatePartitionGroups) {
                    if (!tableGroupExists) {
                        TableGroupRecord tableGroupRecord = new TableGroupRecord();
                        tableGroupRecord.schema = partitionInfo.getTableSchema();
                        tableGroupRecord.tg_name = String.valueOf(System.currentTimeMillis());
                        tableGroupRecord.meta_version = 0L;
                        if (partitionInfo.getTableType() == PartitionTableType.SINGLE_TABLE) {
                            if (partitionInfo.getTableGroupId() != TableGroupRecord.INVALID_TABLE_GROUP_ID) {
                                // Come here is alter table group id for a single table 
                                tableGroupRecord.tg_type = TableGroupRecord.TG_TYPE_NON_DEFAULT_SINGLE_TBL_TG;
                            } else {
                                tableGroupRecord.tg_type = TableGroupRecord.TG_TYPE_DEFAULT_SINGLE_TBL_TG;
                            }
                        } else if (partitionInfo.getTableType() == PartitionTableType.BROADCAST_TABLE) {
                            tableGroupRecord.tg_type = TableGroupRecord.TG_TYPE_BROADCAST_TBL_TG;
                        } else {
                            tableGroupRecord.tg_type = TableGroupRecord.TG_TYPE_PARTITION_TBL_TG;
                        }
                        tableGroupId = tableGroupAccessor.addNewTableGroup(tableGroupRecord);
                        int tgType = tableGroupRecord.tg_type;
                        String finalTgName = TableGroupNameUtil.autoBuildTableGroupName(tableGroupId, tgType);
                        tableGroupAccessor.updateTableGroupName(tableGroupId, finalTgName);
                    } else {
                        int tableGroupType = TableGroupRecord.TG_TYPE_PARTITION_TBL_TG;
                        if (partitionInfo.getTableType() == PartitionTableType.SINGLE_TABLE) {
                            if (partitionInfo.getTableGroupId() != TableGroupRecord.INVALID_TABLE_GROUP_ID) {
                                // Come here is alter table group id for a single table
                                tableGroupType = TableGroupRecord.TG_TYPE_NON_DEFAULT_SINGLE_TBL_TG;
                            } else {
                                tableGroupType = TableGroupRecord.TG_TYPE_DEFAULT_SINGLE_TBL_TG;
                            }
                        } else if (partitionInfo.getTableType() == PartitionTableType.BROADCAST_TABLE) {
                            tableGroupType = TableGroupRecord.TG_TYPE_BROADCAST_TBL_TG;
                        }
                        tableGroupAccessor.updateTableGroupType(tableGroupId, tableGroupType);
                        partitionGroupAccessor.deletePartitionGroupsByTableGroupId(tableGroupId, false);
                    }
                    for (PartitionSpec partitionSpec : partitionInfo.getPartitionBy().getPartitions()) {
                        PartitionGroupRecord partitionGroupRecord = new PartitionGroupRecord();
                        partitionGroupRecord.visible = 1;
                        partitionGroupRecord.partition_name = partitionSpec.getName();
                        partitionGroupRecord.tg_id = tableGroupId;
                        partitionGroupRecord.phy_db =
                            GroupInfoUtil.buildPhysicalDbNameFromGroupName(partitionSpec.getLocation().getGroupKey());
                        partitionGroupRecord.locality = "";
                        partitionGroupRecord.pax_group_id = 0L;
                        Long newPartitionGroupId =
                            partitionGroupAccessor.addNewPartitionGroup(partitionGroupRecord, false);
                        tablePartitionAccessor.updateGroupIdById(newPartitionGroupId, partitionSpec.getId());
                        if (firstPart) {
                            tablePartitionAccessor.updateGroupIdById(tableGroupId, partitionSpec.getParentId());
                        }
                        firstPart = false;
                    }
                } else {
                    assert partitionGroupRecords.size() == partitionInfo.getPartitionBy().getPartitions().size();
                    for (PartitionSpec partitionSpec : partitionInfo.getPartitionBy().getPartitions()) {
                        PartitionGroupRecord partitionGroupRecord = partitionGroupRecords.stream()
                            .filter(o -> o.partition_name.equalsIgnoreCase(partitionSpec.getName())).findFirst()
                            .orElse(null);
                        tablePartitionAccessor.updateGroupIdById(partitionGroupRecord.id, partitionSpec.getId());
                        if (firstPart) {
                            tablePartitionAccessor.updateGroupIdById(tableGroupId, partitionSpec.getParentId());
                        }
                        firstPart = false;
                    }

                }
                GsiMetaManager
                    .updateTableVersion(partitionInfo.getTableSchema(), partitionInfo.getTableName(), connection);
                connection.commit();
                isSuccess = true;
            } finally {
                if (!isSuccess) {
                    connection.rollback();
                }
            }
        } catch (Exception ex) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT, ex);
        }
        TableGroupInfoManager tableGroupInfoManager =
            OptimizerContext.getContext(partitionInfo.getTableSchema()).getTableGroupInfoManager();
        tableGroupInfoManager.reloadTableGroupByGroupId(tableGroupId);
        tableGroupInfoManager.reloadTableGroupByGroupId(partitionInfo.getTableGroupId());
        SyncManagerHelper.sync(new TableMetaChangeSyncAction(partitionInfo.getTableSchema(),
            partitionInfo.getTableName()), partitionInfo.getTableSchema());
    }

    public static SqlNode getSqlTemplate(String sqlTemplateStr, ExecutionContext executionContext) {
        return PartitionUtils.getSqlTemplate(sqlTemplateStr, executionContext);
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

