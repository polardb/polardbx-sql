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

package com.alibaba.polardbx.optimizer.tablegroup;

import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.util.GroupInfoUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.CharType;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.partition.HashBoundSpec;
import com.alibaba.polardbx.optimizer.partition.PartitionBoundVal;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoBuilder;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoUtil;
import com.alibaba.polardbx.optimizer.partition.PartitionLocation;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.partition.PartitionStrategy;
import com.alibaba.polardbx.optimizer.partition.SingleValuePartitionBoundSpec;
import com.alibaba.polardbx.optimizer.partition.datatype.PartitionField;
import com.alibaba.polardbx.optimizer.partition.datatype.PartitionFieldBuilder;
import com.alibaba.polardbx.optimizer.partition.datatype.function.PartitionIntFunction;
import com.alibaba.polardbx.optimizer.partition.pruning.PartFieldAccessType;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPrunerUtils;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionTupleRouteInfoBuilder;
import com.alibaba.polardbx.optimizer.partition.pruning.SearchDatumComparator;
import com.alibaba.polardbx.optimizer.partition.pruning.SearchDatumInfo;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAlterTableAddPartition;
import org.apache.calcite.sql.SqlAlterTableDropPartition;
import org.apache.calcite.sql.SqlAlterTableGroup;
import org.apache.calcite.sql.SqlAlterTableGroupExtractPartition;
import org.apache.calcite.sql.SqlAlterTableGroupMergePartition;
import org.apache.calcite.sql.SqlAlterTableGroupMovePartition;
import org.apache.calcite.sql.SqlAlterTableGroupSplitPartition;
import org.apache.calcite.sql.SqlAlterTableModifyPartitionValues;
import org.apache.calcite.sql.SqlAlterTableSetTableGroup;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlPartition;
import org.apache.calcite.sql.SqlPartitionValue;
import org.apache.calcite.sql.SqlPartitionValueItem;
import org.apache.calcite.sql.SqlRefreshTopology;
import org.apache.calcite.util.Util;
import org.apache.commons.lang3.RandomStringUtils;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class AlterTableGroupSnapShotUtils {
    //{"table_group_name":"xxx","type":"split", "at_value"="xx", "old_partition":"xx",
    // "ordered_locations":{"groupNum":"xxx","group1":"xx","group2":"xx",..,"groupN":xx},
    //"newPartition":{"num":"xx","p1":{"name":"xx"}, "p2":{"name":"xx"}},
    // "tables":{"table_name":{"phy_table1":{"name":"xx", "groupName":"xx"},"phy_table2":{"name":"xx", "groupName":"xx"}}}}
    private static int RANDOM_SUFFIX_LENGTH_OF_PHYSICAL_PARTITION_NAME = 4;

    private static void generateNewPartitionsForSplitRangeWithAtVal(
        List<PartitionGroupRecord> unVisiablePartitionGroups,
        SqlAlterTableGroupSplitPartition sqlAlterTableGroupSplitPartition,
        List<PartitionSpec> oldPartitions,
        List<PartitionSpec> newPartitions,
        List<Pair<String, String>> physicalTableAndGroupPairs,
        PartitionSpec splitSpec, PartitionField atVal) {
        int i = 0;
        int newPartitionNums = sqlAlterTableGroupSplitPartition.getNewPartitions().size();
        assert newPartitionNums == 2;
        for (SqlPartition sqlPartition : sqlAlterTableGroupSplitPartition.getNewPartitions()) {
            PartitionSpec newSpec = splitSpec.copy();
            if (i == 0) {
                newSpec.getBoundSpec().getSingleDatum().getSingletonValue().setValue(atVal);
            }

            newSpec.setName(sqlPartition.getName().toString());
            if (oldPartitions.stream().anyMatch(o -> o.getName().equalsIgnoreCase(newSpec.getName()))) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                    "the partition name:" + newSpec.getName() + " exists");
            }
            PartitionGroupRecord unVisiablePartitionGroup =
                unVisiablePartitionGroups.stream().filter(c -> c.partition_name.equalsIgnoreCase(newSpec.getName()))
                    .findFirst().orElse(null);
            assert unVisiablePartitionGroup != null;

            newSpec.getLocation().setPartitionGroupId(unVisiablePartitionGroup.id);
            newSpec.getLocation().setPhyTableName(physicalTableAndGroupPairs.get(i).getKey());
            newSpec.getLocation().setGroupKey(physicalTableAndGroupPairs.get(i).getValue());
            newSpec.getLocation().setVisiable(false);
            newPartitions.add(newSpec);
            i++;
        }
    }

    private static void generateNewPartitionsForSplitHashType(
        int partColCnt,
        List<PartitionGroupRecord> invisiblePartitionGroups,
        SqlAlterTableGroupSplitPartition sqlAlter,
        List<PartitionSpec> oldPartitions,
        List<PartitionSpec> newPartitions,
        List<Pair<String, String>> physicalTableAndGroupPairs,
        PartitionSpec splitSpec) {

        boolean firstPartition = newPartitions.isEmpty();
        assert splitSpec.getStrategy() == PartitionStrategy.HASH || splitSpec.getStrategy() == PartitionStrategy.KEY;
        for (int i = 0; i < sqlAlter.getNewPartitions().size(); i++) {
            SqlPartition sqlPartition = sqlAlter.getNewPartitions().get(i);
            PartitionSpec newSpec = splitSpec.copy();
            SingleValuePartitionBoundSpec newBound = (SingleValuePartitionBoundSpec) newSpec.getBoundSpec();

            String newName = sqlPartition.getName().toString();
            newSpec.setName(newName);

            if (GeneralUtil.isNotEmpty(invisiblePartitionGroups)) {
                PartitionGroupRecord pg =
                    invisiblePartitionGroups.stream()
                        .filter(c -> c.partition_name.equalsIgnoreCase(newName))
                        .findFirst().get();

                newSpec.getLocation().setPartitionGroupId(pg.id);
            }
            newSpec.getLocation().setPhyTableName(physicalTableAndGroupPairs.get(i).getKey());
            newSpec.getLocation().setGroupKey(physicalTableAndGroupPairs.get(i).getValue());
            newSpec.getLocation().setVisiable(false);
            newPartitions.add(newSpec);

            /*
             * Generate bound value:
             * If not specified from SQL, calculate hash value
             * Else extract it from SQL node
             */
            if (sqlPartition.getValues() != null) {
                buildNewBoundForSplitHashOrKey(partColCnt, sqlPartition, newBound);
            } else {
                if (sqlAlter.getNewPartitions().size() != 2) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                        "split-at could only split into two partitions, but got " + sqlAlter.getNewPartitions().size());
                }

                // only the left part need to be changed
                if (i == 0) {
                    calcSplitAtForHashOrKeyAndBuildNewBoundValue(partColCnt, sqlAlter, oldPartitions, newPartitions,
                        splitSpec, firstPartition, newBound);
                }
            }
        }
    }

    private static void buildNewBoundForSplitHash(SqlPartition sqlPartition, SingleValuePartitionBoundSpec newBound) {
        SqlNode value = sqlPartition.getValues().getLessThanValue();
        if (value instanceof SqlLiteral) {
            Long longValue = ((SqlLiteral) value).getValueAs(Long.class);
            PartitionField partFld = PartitionFieldBuilder.createField(DataTypes.LongType);
            partFld.store(longValue, DataTypes.LongType);
            newBound.setRAWValue(partFld);
        } else {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                "Unsupported partition bound type: " + value);
        }
    }

    private static PartitionField buildNewBoundPartFieldForHashValLiteral(SqlPartitionValueItem valueItem) {

        if (valueItem.isNull() || valueItem.isMaxValue() || valueItem.isMinValue()) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                "Unsupported partition bound value: " + valueItem.getValue().toString());
        }

        SqlNode value = valueItem.getValue();
        if (value instanceof SqlLiteral) {
            Long longValue = ((SqlLiteral) value).getValueAs(Long.class);
            PartitionField partFld = PartitionPrunerUtils
                .buildPartField(longValue, DataTypes.LongType, DataTypes.LongType, null, null,
                    PartFieldAccessType.DDL_EXECUTION);
            return partFld;
        } else {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                "Unsupported partition bound type: " + value);
        }
    }

    private static void buildNewBoundForSplitHashOrKey(int partColCnt,
                                                       SqlPartition sqlPartition,
                                                       SingleValuePartitionBoundSpec newBound) {
        SqlPartitionValue partVals = sqlPartition.getValues();
        SearchDatumInfo newBndValDatum;
        if (partColCnt <= 1) {
            /**
             * Build new bound spec for Hash or Key with single part col
             */
            SqlPartitionValueItem oneValItem = partVals.getItems().get(0);
            PartitionField bndValPartFld = buildNewBoundPartFieldForHashValLiteral(oneValItem);
            newBndValDatum = SearchDatumInfo.createFromField(bndValPartFld);
            newBound.setSingleDatum(newBndValDatum);
        } else {
            /**
             * Build new bound spec for Key with multi part cols
             */
            List<PartitionField> bndFields = new ArrayList<>();
            for (int i = 0; i < partVals.getItems().size(); i++) {
                SqlPartitionValueItem oneValItem = partVals.getItems().get(i);
                PartitionField bndValPartFld = buildNewBoundPartFieldForHashValLiteral(oneValItem);
                bndFields.add(bndValPartFld);
            }
            newBndValDatum = SearchDatumInfo.createFromFields(bndFields);
            newBound.setSingleDatum(newBndValDatum);
        }
    }

    private static Long calculateSplitAtHash(
        SqlAlterTableGroupSplitPartition sqlAlter,
        List<PartitionSpec> oldPartitions, List<PartitionSpec> newPartitions,
        PartitionSpec splitSpec, boolean firstPartition) {
        Long atVal;

        if (sqlAlter.getAtValue() != null && sqlAlter.getAtValue() instanceof SqlNumericLiteral) {
            atVal = ((SqlLiteral) sqlAlter.getAtValue()).longValue(true);
        } else if (oldPartitions.size() == 1 || firstPartition) {
            //todo overflow issue
            // TODO avoid convert to string then parse
            BigInteger minVal = new BigInteger(String.valueOf(Long.MIN_VALUE));
            BigInteger splitVal = new BigInteger(
                splitSpec.getBoundSpec().getSingleDatum().getSingletonValue().getValue().stringValue().toStringUtf8());
            atVal = calcMiddlePoint(minVal, splitVal);
        } else {
            PartitionSpec lastPart = newPartitions.get(newPartitions.size() - 2);
            BigInteger minVal = new BigInteger(
                lastPart.getBoundSpec().getSingleDatum().getSingletonValue().getValue().stringValue().toStringUtf8());
            BigInteger splitVal = new BigInteger(
                splitSpec.getBoundSpec().getSingleDatum().getSingletonValue().getValue().stringValue().toStringUtf8());
            atVal = calcMiddlePoint(minVal, splitVal);
            if (atVal.longValue() == minVal.longValue()
                ||
                atVal.longValue() == splitVal.longValue()
            ) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                    "the partition:" + splitSpec.getName() + " can't split anymore");
            }
        }
        return atVal;
    }

    private static long calcMiddlePoint(BigInteger minVal, BigInteger maxVal) {
        BigInteger divideVal = new BigInteger("2");
        long atVal = minVal.add(maxVal).divide(divideVal).longValue();
        return atVal;
    }

    private static void calcSplitAtForHashOrKeyAndBuildNewBoundValue(int partColCnt,
                                                                     SqlAlterTableGroupSplitPartition sqlAlter,
                                                                     List<PartitionSpec> oldPartitions,
                                                                     List<PartitionSpec> newPartitions,
                                                                     PartitionSpec splitSpec,
                                                                     boolean firstPartition,
                                                                     SingleValuePartitionBoundSpec newBound /*Output params*/) {

        long splitAtVal;
        if (partColCnt <= 1) {
            splitAtVal = calculateSplitAtHash(sqlAlter, oldPartitions, newPartitions, splitSpec, firstPartition);
            SearchDatumInfo newBndValDatum = SearchDatumInfo.createFromHashCode(splitAtVal);
            newBound.setSingleDatum(newBndValDatum);
        } else {

            List<PartitionField> middlePointFlds = new ArrayList<>();
            if (sqlAlter.getAtValue() != null) {
                //atVal = ((SqlLiteral) sqlAlter.getAtValue()).longValue(true);
                throw new TddlRuntimeException(ErrorCode.ERR_NOT_SUPPORT);
            } else if (oldPartitions.size() == 1 || firstPartition) {
                for (int i = 0; i < partColCnt; i++) {
                    PartitionBoundVal maxBnd = splitSpec.getBoundSpec().getSingleDatum().getDatumInfo()[i];
                    BigInteger minVal = BigInteger.valueOf(Long.MIN_VALUE);
                    if (i > 0) {
                        /**
                         * For non-first-part-column, only use MAX_VALUE as default low bound
                         */
                        minVal = BigInteger.valueOf(Long.MAX_VALUE);
                    }
                    BigInteger maxVal = new BigInteger(maxBnd.getValue().stringValue().toStringUtf8());
                    Long middlePointVal = calcMiddlePoint(minVal, maxVal);
                    PartitionField newBndFld = PartitionPrunerUtils
                        .buildPartField(middlePointVal, DataTypes.LongType, DataTypes.LongType, null, null,
                            PartFieldAccessType.DDL_EXECUTION);
                    middlePointFlds.add(newBndFld);
                }
            } else {
                PartitionSpec lastPartSpec = newPartitions.get(newPartitions.size() - 2);
                PartitionSpec toBeSplitSpec = splitSpec;

                for (int i = 0; i < partColCnt; i++) {
                    PartitionBoundVal minBnd = lastPartSpec.getBoundSpec().getSingleDatum().getDatumInfo()[i];
                    PartitionBoundVal maxBnd = toBeSplitSpec.getBoundSpec().getSingleDatum().getDatumInfo()[i];

                    BigInteger minVal = new BigInteger(minBnd.getValue().stringValue().toStringUtf8());
                    BigInteger maxVal = new BigInteger(maxBnd.getValue().stringValue().toStringUtf8());
                    Long middlePointVal = calcMiddlePoint(minVal, maxVal);
                    PartitionField newBndFld = PartitionPrunerUtils
                        .buildPartField(middlePointVal, DataTypes.LongType, DataTypes.LongType, null, null,
                            PartFieldAccessType.DDL_EXECUTION);
                    middlePointFlds.add(newBndFld);
                }
            }

            SearchDatumInfo newDatum = SearchDatumInfo.createFromFields(middlePointFlds);
            newBound.setSingleDatum(newDatum);
        }
    }

    private static void generateNewPartitionsForSplitWithPartitionSpec(
        ExecutionContext executionContext,
        PartitionInfo curPartitionInfo,
        List<PartitionGroupRecord> unVisiablePartitionGroups,
        SqlAlterTableGroupSplitPartition sqlAlterTableGroupSplitPartition,
        List<PartitionSpec> oldPartitions,
        List<PartitionSpec> newPartitions,
        List<Pair<String, String>> physicalTableAndGroupPairs,
        PartitionSpec splitSpec) {
        int i = 0;
        List<ColumnMeta> partColMetaList = curPartitionInfo.getPartitionBy().getPartitionFieldList();
        SearchDatumComparator comparator = curPartitionInfo.getPartitionBy().getPruningSpaceComparator();
        PartitionIntFunction partIntFunc = curPartitionInfo.getPartitionBy().getPartIntFunc();
        for (SqlPartition sqlPartition : sqlAlterTableGroupSplitPartition.getNewPartitions()) {
            PartitionSpec newSpec = PartitionInfoBuilder
                .buildPartitionSpecByPartSpecAst(
                    executionContext,
                    partColMetaList,
                    partIntFunc,
                    comparator,
                    sqlAlterTableGroupSplitPartition.getParent().getPartRexInfoCtx(),
                    null,
                    sqlPartition,
                    curPartitionInfo.getPartitionBy().getStrategy(),
                    newPartitions.size());

            PartitionGroupRecord unVisiablePartitionGroup =
                unVisiablePartitionGroups.stream().filter(c -> c.partition_name.equalsIgnoreCase(newSpec.getName()))
                    .findFirst().orElse(null);
            assert unVisiablePartitionGroup != null;
            newSpec.setLocation(new PartitionLocation());
            newSpec.getLocation().setPartitionGroupId(unVisiablePartitionGroup.id);
            newSpec.getLocation().setPhyTableName(physicalTableAndGroupPairs.get(i).getKey());
            newSpec.getLocation().setGroupKey(physicalTableAndGroupPairs.get(i).getValue());
            newSpec.getLocation().setVisiable(false);
            newPartitions.add(newSpec);
            i++;
        }
    }

    public static PartitionInfo getNewPartitionInfoForSplitType(
        ExecutionContext executionContext,
        PartitionInfo curPartitionInfo,
        List<PartitionGroupRecord> unVisiablePartitionGroups,
        SqlNode sqlNode,
        String tableGroupName, String splitPartitionName,
        List<Pair<String, String>> physicalTableAndGroupPairs) {

        final TableGroupInfoManager tableGroupInfoManager =
            OptimizerContext.getContext(curPartitionInfo.getTableSchema()).getTableGroupInfoManager();

        TableGroupConfig tableGroupConfig = tableGroupInfoManager.getTableGroupConfigByName(tableGroupName);
        PartitionGroupRecord splitPartRecord = tableGroupConfig.getPartitionGroupRecords().stream()
            .filter(o -> o.partition_name.equalsIgnoreCase(splitPartitionName)).findFirst().orElse(null);

        if (splitPartRecord == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_NAME_NOT_EXISTS,
                "the partition:" + splitPartitionName + " is not exists this current table group");
        }

        /**
         * Make sure that the datatype of atVal is valid
         */
        //Long atVal = null;

        PartitionField atVal = null;
        SqlAlterTableGroupSplitPartition sqlAlterTableGroupSplitPartition =
            (SqlAlterTableGroupSplitPartition) sqlNode;
        if (sqlAlterTableGroupSplitPartition.getAtValue() != null) {
            DataType atValDataType =
                DataTypeUtil.calciteToDrdsType(curPartitionInfo.getPartitionBy().getPartitionExprTypeList().get(0));
            atVal = PartitionFieldBuilder.createField(atValDataType);
            SqlLiteral constLiteral = ((SqlLiteral) sqlAlterTableGroupSplitPartition.getAtValue());
            String constStr = constLiteral.getValueAs(String.class);
            atVal.store(constStr, new CharType());
            //atVal = ((SqlLiteral) sqlAlterTableGroupSplitPartition.getAtValue()).getValueAs(Long.class);
        }

        assert GeneralUtil.isNotEmpty(physicalTableAndGroupPairs) && physicalTableAndGroupPairs.size() > 1;
        assert GeneralUtil.isNotEmpty(unVisiablePartitionGroups);
        List<PartitionSpec> oldPartitions = curPartitionInfo.getPartitionBy().getPartitions();
        List<PartitionSpec> newPartitions = new ArrayList<>();
        PartitionStrategy strategy = curPartitionInfo.getPartitionBy().getStrategy();
        int partColCnt = curPartitionInfo.getPartitionBy().getPartitionColumnNameList().size();

        for (PartitionSpec spec : oldPartitions) {
            if (spec.getLocation().getPartitionGroupId().longValue() == splitPartRecord.id) {
                if (strategy == PartitionStrategy.HASH || strategy == PartitionStrategy.KEY) {
                    generateNewPartitionsForSplitHashType(partColCnt, unVisiablePartitionGroups,
                        sqlAlterTableGroupSplitPartition,
                        oldPartitions, newPartitions, physicalTableAndGroupPairs, spec);
                } else if (atVal != null) {
                    assert strategy == PartitionStrategy.RANGE;
                    generateNewPartitionsForSplitRangeWithAtVal(unVisiablePartitionGroups,
                        sqlAlterTableGroupSplitPartition,
                        oldPartitions, newPartitions, physicalTableAndGroupPairs, spec, atVal);
                } else {
                    generateNewPartitionsForSplitWithPartitionSpec(executionContext, curPartitionInfo,
                        unVisiablePartitionGroups,
                        sqlAlterTableGroupSplitPartition,
                        oldPartitions, newPartitions, physicalTableAndGroupPairs, spec);
                }
            } else {
                newPartitions.add(spec);
            }
        }

        PartitionInfo newPartitionInfo = curPartitionInfo.copy();
        newPartitionInfo.getPartitionBy().setPartitions(newPartitions);
        return newPartitionInfo;
    }

    public static PartitionInfo getNewPartitionInfoForExtractType(
        PartitionInfo curPartitionInfo,
        List<PartitionGroupRecord> unVisiablePartitionGroups,
        SqlNode sqlNode,
        String tableGroupName, String extractPartitionName,
        List<Pair<String, String>> physicalTableAndGroupPairs,
        List<String> newPartitionNames,
        ExecutionContext executionContext) {

        final TableGroupInfoManager tableGroupInfoManager =
            OptimizerContext.getContext(curPartitionInfo.getTableSchema()).getTableGroupInfoManager();

        TableGroupConfig tableGroupConfig = tableGroupInfoManager.getTableGroupConfigByName(tableGroupName);
        PartitionGroupRecord extractPartRecord = tableGroupConfig.getPartitionGroupRecords().stream()
            .filter(o -> o.partition_name.equalsIgnoreCase(extractPartitionName)).findFirst().orElse(null);

        if (extractPartRecord == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_NAME_NOT_EXISTS,
                "the partition:" + extractPartitionName + " is not exists this current table group");
        }
        if (curPartitionInfo.getPartitionBy().getStrategy() != PartitionStrategy.HASH) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                "it's not support the extract to partition by hot value for " + curPartitionInfo.getPartitionBy()
                    .getStrategy().toString() + " table");
        }
        SqlAlterTableGroupExtractPartition sqlAlterTableGroupExtractPartition =
            (SqlAlterTableGroupExtractPartition) sqlNode;

        RexNode rexNode = sqlAlterTableGroupExtractPartition.getParent().getPartRexInfoCtx()
            .get(sqlAlterTableGroupExtractPartition.getHotKey());
        Long atVal = PartitionTupleRouteInfoBuilder
            .computeExprValuesHashCode(curPartitionInfo, ImmutableList.of(rexNode), executionContext);

        assert GeneralUtil.isNotEmpty(newPartitionNames) && (newPartitionNames.size() == 3
            || newPartitionNames.size() == 2);

        assert
            GeneralUtil.isNotEmpty(physicalTableAndGroupPairs) && physicalTableAndGroupPairs.size() == newPartitionNames
                .size();

        assert GeneralUtil.isNotEmpty(unVisiablePartitionGroups);
        List<PartitionSpec> oldPartitions = curPartitionInfo.getPartitionBy().getPartitions();
        List<PartitionSpec> newPartitions = new ArrayList<>();
        PartitionSpec extractPartitionSpec = curPartitionInfo.getPartitionBy().getPartitions().stream()
            .filter(o -> o.getLocation().getPartitionGroupId().longValue() == extractPartRecord.id).findFirst()
            .orElse(null);
        PartitionSpec prevPartitionSpec = curPartitionInfo.getPartitionBy().getPartitions().stream()
            .filter(o -> o.getPosition().longValue() == extractPartitionSpec.getPosition().longValue() - 1).findFirst()
            .orElse(null);

        /**
         * i.e. p=[1,9), if atVal=1, flag = -1, [1,2),[2,9)
         * if atval=3, flag = 0, [1,3),[3,4),[4,9)
         * if atVal=8, flag = 1, [1,8),[8,9)
         * */
        int flag = PartitionPrunerUtils.getExtractPosition(extractPartitionSpec, prevPartitionSpec, atVal);

        for (PartitionSpec spec : oldPartitions) {
            if (spec.getLocation().getPartitionGroupId().longValue() == extractPartRecord.id) {
                PartitionSpec newSpec1 = spec.copy();
                PartitionField partFld =
                    PartitionPrunerUtils
                        .buildPartField(atVal, DataTypes.LongType, DataTypes.LongType, null, executionContext,
                            PartFieldAccessType.DDL_EXECUTION);
                ((HashBoundSpec) newSpec1.getBoundSpec()).setRAWValue(partFld);
                int i = 0;
                final String partitionName1 = newPartitionNames.get(i);
                newSpec1.setName(partitionName1);
                while (oldPartitions.stream().anyMatch(o -> o.getName().equalsIgnoreCase(newSpec1.getName()))) {
                    newSpec1.setName(
                        "p_" + RandomStringUtils.randomAlphanumeric(RANDOM_SUFFIX_LENGTH_OF_PHYSICAL_PARTITION_NAME));
                }
                PartitionGroupRecord unVisiablePartitionGroup1 =
                    unVisiablePartitionGroups.stream()
                        .filter(c -> c.partition_name.equalsIgnoreCase(partitionName1))
                        .findFirst().orElse(null);
                assert unVisiablePartitionGroup1 != null;

                newSpec1.getLocation().setPartitionGroupId(unVisiablePartitionGroup1.id);
                newSpec1.getLocation().setPhyTableName(physicalTableAndGroupPairs.get(i).getKey());
                newSpec1.getLocation().setGroupKey(physicalTableAndGroupPairs.get(i).getValue());
                newSpec1.getLocation().setVisiable(false);
                i++;
                newPartitions.add(newSpec1);
                Long newAtVal = null;
                if (flag == -1) {
                    newAtVal = atVal + 1;
                } else {
                    newAtVal = atVal;
                }

                PartitionField newPartFld = PartitionPrunerUtils
                    .buildPartField(newAtVal, DataTypes.LongType, DataTypes.LongType, null, executionContext,
                        PartFieldAccessType.DDL_EXECUTION);
                ((HashBoundSpec) newSpec1.getBoundSpec()).setRAWValue(newPartFld);

                final String partitionName2 = newPartitionNames.get(i);
                PartitionSpec newSpec2 = spec.copy();
                newSpec2.setName(partitionName2);
                while (oldPartitions.stream().anyMatch(o -> o.getName().equalsIgnoreCase(newSpec2.getName()))) {
                    newSpec2.setName(
                        "p_" + RandomStringUtils.randomAlphanumeric(RANDOM_SUFFIX_LENGTH_OF_PHYSICAL_PARTITION_NAME));
                }
                PartitionGroupRecord unVisiablePartitionGroup2 =
                    unVisiablePartitionGroups.stream().filter(c -> c.partition_name.equalsIgnoreCase(partitionName2))
                        .findFirst().orElse(null);
                assert unVisiablePartitionGroup2 != null;

                newSpec2.getLocation().setPartitionGroupId(unVisiablePartitionGroup2.id);
                newSpec2.getLocation().setPhyTableName(physicalTableAndGroupPairs.get(i).getKey());
                newSpec2.getLocation().setGroupKey(physicalTableAndGroupPairs.get(i).getValue());
                newSpec2.getLocation().setVisiable(false);
                if (flag == 0) {
                    partFld = PartitionPrunerUtils
                        .buildPartField(atVal + 1, DataTypes.LongType, DataTypes.LongType, null, executionContext,
                            PartFieldAccessType.DDL_EXECUTION);
                    ((HashBoundSpec) newSpec2.getBoundSpec()).setRAWValue(partFld);
                }

                i++;
                newPartitions.add(newSpec2);

                if (flag == 0) {
                    PartitionSpec newSpec3 = spec.copy();
                    final String partitionName3 = newPartitionNames.get(i);
                    newSpec3.setName(partitionName3);
                    while (oldPartitions.stream().anyMatch(o -> o.getName().equalsIgnoreCase(newSpec3.getName()))) {
                        newSpec3.setName(
                            "p_" + RandomStringUtils
                                .randomAlphanumeric(RANDOM_SUFFIX_LENGTH_OF_PHYSICAL_PARTITION_NAME));
                    }
                    PartitionGroupRecord unVisiablePartitionGroup3 =
                        unVisiablePartitionGroups.stream()
                            .filter(c -> c.partition_name.equalsIgnoreCase(partitionName3))
                            .findFirst().orElse(null);
                    assert unVisiablePartitionGroup3 != null;

                    newSpec3.getLocation().setPartitionGroupId(unVisiablePartitionGroup3.id);
                    newSpec3.getLocation().setPhyTableName(physicalTableAndGroupPairs.get(i).getKey());
                    newSpec3.getLocation().setGroupKey(physicalTableAndGroupPairs.get(i).getValue());
                    newSpec3.getLocation().setVisiable(false);
                    i++;
                    newPartitions.add(newSpec3);
                }
            } else {
                newPartitions.add(spec);
            }
        }

        PartitionInfo newPartitionInfo = curPartitionInfo.copy();
        newPartitionInfo.getPartitionBy().setPartitions(newPartitions);
        return newPartitionInfo;
    }

    public static PartitionInfo getNewPartitionInfoForMergeType(PartitionInfo curPartitionInfo,
                                                                List<PartitionGroupRecord> unVisiablePartitionGroups,
                                                                SqlNode sqlNode,
                                                                String tableGroupName,
                                                                List<Pair<String, String>> physicalTableAndGroupPairs,
                                                                List<String> newPartitionNames) {
        final TableGroupInfoManager tableGroupInfoManager =
            OptimizerContext.getContext(curPartitionInfo.getTableSchema()).getTableGroupInfoManager();
        SqlAlterTableGroupMergePartition sqlAlterTableGroupMergePartition =
            (SqlAlterTableGroupMergePartition) sqlNode;
        Set<String> mergePartitionsName = sqlAlterTableGroupMergePartition.getOldPartitions().stream()
            .map(o -> Util.last(((SqlIdentifier) (o)).names).toLowerCase()).collect(
                Collectors.toSet());
        TableGroupConfig tableGroupConfig = tableGroupInfoManager.getTableGroupConfigByName(tableGroupName);
        List<PartitionGroupRecord> mergePartRecords = tableGroupConfig.getPartitionGroupRecords().stream()
            .filter(o -> mergePartitionsName.contains(o.partition_name.toLowerCase())).collect(Collectors.toList());
        Set<Long> toBeMergedPartGroupIds = mergePartRecords.stream().map(o -> o.id).collect(Collectors.toSet());
        List<PartitionSpec> mergePartSpecs = curPartitionInfo.getPartitionBy().getPartitions().stream()
            .filter(o -> toBeMergedPartGroupIds.contains(o.getLocation().getPartitionGroupId())).collect(
                Collectors.toList());
        mergePartSpecs.sort(Comparator.comparing(PartitionSpec::getPosition));
        PartitionSpec reservedSpec = mergePartSpecs.get(mergePartSpecs.size() - 1);

        List<PartitionSpec> oldPartitions = curPartitionInfo.getPartitionBy().getPartitions();
        List<PartitionSpec> newPartitions = new ArrayList<>();

        String newPartName = GeneralUtil.isEmpty(newPartitionNames) ?
            "p_" + RandomStringUtils.randomAlphanumeric(RANDOM_SUFFIX_LENGTH_OF_PHYSICAL_PARTITION_NAME) :
            newPartitionNames.get(0);
        List<SearchDatumInfo> listDatums = new ArrayList<>();

        for (PartitionSpec spec : oldPartitions) {
            if (toBeMergedPartGroupIds.contains(spec.getLocation().getPartitionGroupId())) {
                if (spec.getLocation().getPartitionGroupId().longValue() == reservedSpec.getLocation()
                    .getPartitionGroupId().longValue()) {
                    PartitionSpec newSpec = reservedSpec.copy();
                    PartitionGroupRecord unVisiablePartitionGroup1 =
                        unVisiablePartitionGroups.stream().filter(c -> c.partition_name.equalsIgnoreCase(newPartName))
                            .findFirst().orElse(null);
                    assert unVisiablePartitionGroup1 != null;
                    newSpec.getLocation().setPartitionGroupId(unVisiablePartitionGroup1.id);
                    newSpec.getLocation().setPhyTableName(physicalTableAndGroupPairs.get(0).getKey());
                    newSpec.getLocation().setGroupKey(physicalTableAndGroupPairs.get(0).getValue());
                    newSpec.getLocation().setVisiable(false);
                    newSpec.setName(newPartName);
                    if (spec.getStrategy() == PartitionStrategy.LIST) {
                        newSpec.getBoundSpec().getMultiDatums().addAll(listDatums);
                    }
                    newPartitions.add(newSpec);
                } else if (spec.getStrategy() == PartitionStrategy.LIST) {
                    listDatums.addAll(spec.getBoundSpec().getMultiDatums());
                }
            } else {
                newPartitions.add(spec);
            }
        }

        PartitionInfo newPartitionInfo = curPartitionInfo.copy();
        newPartitionInfo.getPartitionBy().setPartitions(newPartitions);
        return newPartitionInfo;
    }

    public static PartitionInfo getNewPartitionInfoForMoveType(PartitionInfo curPartitionInfo,
                                                               SqlNode sqlNode,
                                                               String tableGroupName,
                                                               List<Pair<String, String>> physicalTableAndGroupPairs) {
        final TableGroupInfoManager tableGroupInfoManager =
            OptimizerContext.getContext(curPartitionInfo.getTableSchema()).getTableGroupInfoManager();

        SqlAlterTableGroupMovePartition sqlAlterTableGroupMovePartition = (SqlAlterTableGroupMovePartition) sqlNode;
        Set<String> movePartitionsName = sqlAlterTableGroupMovePartition.getOldPartitions().stream()
            .map(o -> Util.last(((SqlIdentifier) (o)).names).toLowerCase()).collect(
                Collectors.toSet());
        TableGroupConfig tableGroupConfig = tableGroupInfoManager.getTableGroupConfigByName(tableGroupName);
        List<PartitionGroupRecord> movePartRecords = tableGroupConfig.getPartitionGroupRecords().stream()
            .filter(o -> movePartitionsName.contains(o.partition_name.toLowerCase())).collect(Collectors.toList());
        Set<Long> movePartGroupIds = movePartRecords.stream().map(o -> o.id).collect(Collectors.toSet());

        List<PartitionSpec> oldPartitions = curPartitionInfo.getPartitionBy().getPartitions();
        List<PartitionSpec> newPartitions = new ArrayList<>();

        Map<String, String> physicalTableMap = new HashMap<>();
        for (Pair<String, String> pair : physicalTableAndGroupPairs) {
            physicalTableMap.put(pair.getKey(), pair.getValue());
        }

        for (PartitionSpec spec : oldPartitions) {
            if (movePartGroupIds.contains(spec.getLocation().getPartitionGroupId())) {
                PartitionSpec newSpec = spec.copy();
                newSpec.getLocation()
                    .setGroupKey(physicalTableMap.get(spec.getLocation().getPhyTableName().toLowerCase()));
                newSpec.getLocation().setVisiable(false);
                newPartitions.add(newSpec);
            } else {
                newPartitions.add(spec);
            }
        }

        PartitionInfo newPartitionInfo = curPartitionInfo.copy();
        newPartitionInfo.getPartitionBy().setPartitions(newPartitions);
        return newPartitionInfo;
    }

    public static PartitionInfo getNewPartitionInfoForSetTableGroup(PartitionInfo curPartitionInfo,
                                                                    SqlNode sqlNode) {
        final TableGroupInfoManager tableGroupInfoManager =
            OptimizerContext.getContext(curPartitionInfo.getTableSchema()).getTableGroupInfoManager();

        SqlAlterTableSetTableGroup sqlAlterTableSetTableGroup = (SqlAlterTableSetTableGroup) sqlNode;
        String targetTableGroup = sqlAlterTableSetTableGroup.getTargetTableGroup();

        TableGroupConfig tableGroupConfig = tableGroupInfoManager.getTableGroupConfigByName(targetTableGroup);
        List<PartitionGroupRecord> partitionGroupRecords = tableGroupConfig.getPartitionGroupRecords();
        List<PartitionSpec> newPartitions = new ArrayList<>();
        for (PartitionSpec partSpec : curPartitionInfo.getPartitionBy().getPartitions()) {
            PartitionGroupRecord partitionGroupRecord =
                partitionGroupRecords.stream().filter(o -> o.partition_name.equalsIgnoreCase(partSpec.getName()))
                    .findFirst().orElse(null);
            assert partitionGroupRecord != null;
            PartitionSpec newPartSpec = partSpec.copy();
            newPartSpec.getLocation()
                .setGroupKey(GroupInfoUtil.buildPhysicalDbNameFromGroupName(partitionGroupRecord.phy_db));
            newPartSpec.getLocation().setPartitionGroupId(partitionGroupRecord.id);
            newPartitions.add(newPartSpec);
        }
        PartitionInfo newPartitionInfo = curPartitionInfo.copy();
        newPartitionInfo.getPartitionBy().setPartitions(newPartitions);
        //change the tablegroup id to the target tablegroup
        newPartitionInfo.setTableGroupId(tableGroupConfig.getTableGroupRecord().getId());
        return newPartitionInfo;
    }

    public static PartitionInfo getNewPartitionInfoForAddPartition(PartitionInfo curPartitionInfo,
                                                                   List<PartitionGroupRecord> unVisiablePartitionGroupRecords,
                                                                   SqlNode sqlNode,
                                                                   List<Pair<String, String>> physicalTableAndGroupPairs,
                                                                   ExecutionContext executionContext) {
        SqlAlterTableAddPartition addPartition = (SqlAlterTableAddPartition) sqlNode;
        Map<SqlNode, RexNode> partBoundExprInfo = ((SqlAlterTableGroup) addPartition.getParent()).getPartRexInfoCtx();
        PartitionInfo newPartitionInfo = PartitionInfoBuilder
            .buildNewPartitionInfoByAddingPartition(executionContext, curPartitionInfo, addPartition, partBoundExprInfo,
                unVisiablePartitionGroupRecords, physicalTableAndGroupPairs);

        return newPartitionInfo;

    }

    public static PartitionInfo getNewPartitionInfoForDropPartition(PartitionInfo curPartitionInfo,
                                                                    List<PartitionGroupRecord> unVisiablePartitionGroupRecords,
                                                                    SqlNode sqlNode,
                                                                    List<Pair<String, String>> physicalTableAndGroupPairs) {
        SqlAlterTableDropPartition dropPartition = (SqlAlterTableDropPartition) sqlNode;
        return PartitionInfoBuilder
            .buildNewPartitionInfoByDroppingPartition(curPartitionInfo, dropPartition,
                unVisiablePartitionGroupRecords, physicalTableAndGroupPairs);

    }

    public static PartitionInfo getNewPartitionInfoForModifyPartition(PartitionInfo curPartitionInfo,
                                                                      SqlNode sqlNode,
                                                                      String targetPartitionName,
                                                                      List<Pair<String, String>> physicalTableAndGroupPairs,
                                                                      ExecutionContext executionContext) {

        SqlAlterTableModifyPartitionValues modifyPartition = (SqlAlterTableModifyPartitionValues) sqlNode;
        Map<SqlNode, RexNode> partBoundExprInfo =
            ((SqlAlterTableGroup) modifyPartition.getParent()).getPartRexInfoCtx();
        PartitionSpec[] outputNewPartSpec = new PartitionSpec[1];
        PartitionInfo newPartitionInfo = PartitionInfoBuilder
            .buildNewPartitionInfoByModifyingPartitionValues(curPartitionInfo, modifyPartition, partBoundExprInfo,
                executionContext, outputNewPartSpec);
        String oldPartition = ((SqlIdentifier) modifyPartition.getPartition().getName()).getLastName();
        PartitionSpec targetPartSepc = curPartitionInfo.getPartitionBy().getPartitions().stream()
            .filter(o -> o.getName().equalsIgnoreCase(oldPartition)).findFirst().orElse(null);
        Long pgId = targetPartSepc.getLocation().getPartitionGroupId();
        outputNewPartSpec[0].getLocation().setPartitionGroupId(pgId);
        outputNewPartSpec[0].getLocation().setPhyTableName(physicalTableAndGroupPairs.get(0).getKey());
        outputNewPartSpec[0].getLocation().setGroupKey(physicalTableAndGroupPairs.get(0).getValue());
        outputNewPartSpec[0].getLocation().setVisiable(false);
        return newPartitionInfo;

    }

    public static PartitionInfo getNewPartitionInfoForCopyPartition(PartitionInfo curPartitionInfo,
                                                                    List<PartitionGroupRecord> unVisiablePartitionGroupRecords) {
        PartitionInfo newPartitionInfo = curPartitionInfo.copy();
        PartitionSpec partitionSpec = curPartitionInfo.getPartitionBy().getPartitions().get(0).copy();
        for (PartitionGroupRecord record : unVisiablePartitionGroupRecords) {
            partitionSpec.setName(record.partition_name);
            partitionSpec.getLocation().setVisiable(false);
            partitionSpec.getLocation().setGroupKey(GroupInfoUtil.buildGroupNameFromPhysicalDb(record.phy_db));
            newPartitionInfo.getPartitionBy().getPartitions().add(partitionSpec);
            partitionSpec = partitionSpec.copy();
        }
        return newPartitionInfo;

    }

    public static PartitionInfo getNewPartitionInfo(
        PartitionInfo curPartitionInfo,
        List<PartitionGroupRecord> unVisiablePartitionGroupRecords,
        SqlNode sqlNode,
        String tableGroupName, String targetPartitionNameToBeAltered,
        List<Pair<String, String>> physicalTableAndGroupPairs,
        List<String> newPartitionNames,
        ExecutionContext executionContext) {

        PartitionInfo newPartInfo = null;
        if (sqlNode instanceof SqlAlterTableGroupSplitPartition) {
            newPartInfo =
                getNewPartitionInfoForSplitType(executionContext, curPartitionInfo, unVisiablePartitionGroupRecords,
                    sqlNode,
                    tableGroupName, targetPartitionNameToBeAltered,
                    physicalTableAndGroupPairs);
        } else if (sqlNode instanceof SqlAlterTableGroupExtractPartition) {
            newPartInfo = getNewPartitionInfoForExtractType(curPartitionInfo, unVisiablePartitionGroupRecords, sqlNode,
                tableGroupName, targetPartitionNameToBeAltered,
                physicalTableAndGroupPairs, newPartitionNames, executionContext);
        } else if (sqlNode instanceof SqlAlterTableGroupMergePartition) {
            newPartInfo = getNewPartitionInfoForMergeType(curPartitionInfo, unVisiablePartitionGroupRecords, sqlNode,
                tableGroupName,
                physicalTableAndGroupPairs, newPartitionNames);
        } else if (sqlNode instanceof SqlAlterTableGroupMovePartition) {
            newPartInfo = getNewPartitionInfoForMoveType(curPartitionInfo, sqlNode, tableGroupName,
                physicalTableAndGroupPairs);
        } else if (sqlNode instanceof SqlAlterTableSetTableGroup) {
            newPartInfo = getNewPartitionInfoForSetTableGroup(curPartitionInfo, sqlNode);
        } else if (sqlNode instanceof SqlAlterTableAddPartition) {
            newPartInfo = getNewPartitionInfoForAddPartition(curPartitionInfo, unVisiablePartitionGroupRecords, sqlNode,
                physicalTableAndGroupPairs, executionContext);
        } else if (sqlNode instanceof SqlAlterTableDropPartition) {
            newPartInfo =
                getNewPartitionInfoForDropPartition(curPartitionInfo, unVisiablePartitionGroupRecords, sqlNode,
                    physicalTableAndGroupPairs);
        } else if (sqlNode instanceof SqlAlterTableModifyPartitionValues) {
            newPartInfo =
                getNewPartitionInfoForModifyPartition(curPartitionInfo, sqlNode,
                    targetPartitionNameToBeAltered, physicalTableAndGroupPairs,
                    executionContext);
        } else if (sqlNode instanceof SqlRefreshTopology) {
            newPartInfo = getNewPartitionInfoForCopyPartition(curPartitionInfo, unVisiablePartitionGroupRecords);
        } else {
            throw new NotSupportException(sqlNode.getKind().toString() + " is not support yet");
        }

        // Reset the partition position for each new partition
        // FIXME @chegnbi , for range/range columns, the position of partitons must be same as the order of bound value!!
        List<PartitionSpec> newSpecs = newPartInfo.getPartitionBy().getPartitions();
        for (int i = 0; i < newSpecs.size(); i++) {
            PartitionSpec spec = newSpecs.get(i);
            long newPosi = i + 1;
            spec.setPosition(newPosi);
        }

        PartitionInfoUtil.validatePartitionInfoForDdl(newPartInfo, executionContext);
        return newPartInfo;

    }
}
