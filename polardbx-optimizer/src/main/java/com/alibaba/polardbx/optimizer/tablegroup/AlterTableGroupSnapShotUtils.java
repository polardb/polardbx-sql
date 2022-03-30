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
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupExtractPartitionPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupSplitPartitionByHotValuePreparedData;
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
import com.alibaba.polardbx.optimizer.partition.pruning.SearchDatumComparator;
import com.alibaba.polardbx.optimizer.partition.pruning.SearchDatumInfo;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAlterTableAddPartition;
import org.apache.calcite.sql.SqlAlterTableDropPartition;
import org.apache.calcite.sql.SqlAlterTableGroup;
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
import java.util.TreeMap;
import java.util.TreeSet;
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
        int actualPartColCnt,
        List<PartitionGroupRecord> invisiblePartitionGroups,
        SqlAlterTableGroupSplitPartition sqlAlter,
        List<PartitionSpec> oldPartitions,
        List<PartitionSpec> newPartitions,
        List<Pair<String, String>> physicalTableAndGroupPairs,
        PartitionSpec splitSpec) {

        boolean firstPartition = newPartitions.isEmpty();
        PartitionStrategy strategy = splitSpec.getStrategy();
        assert strategy == PartitionStrategy.HASH || splitSpec.getStrategy() == PartitionStrategy.KEY;
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
                buildNewBoundForSplitHashOrKey(partColCnt, actualPartColCnt, strategy, sqlPartition, newBound);
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

    public static PartitionField buildBoundPartFieldForHashLongVal(Long hashLongVal) {
        PartitionField partFld = PartitionPrunerUtils
            .buildPartField(hashLongVal, DataTypes.LongType, DataTypes.LongType, null, null,
                PartFieldAccessType.DDL_EXECUTION);
        return partFld;
    }


    private static void buildNewBoundForSplitHashOrKey(int partColCnt,
                                                       int actualPartColCnt,
                                                       PartitionStrategy strategy,
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
             * Build new bound spec for Key with multi-part cols
             */
            List<PartitionField> bndFields = new ArrayList<>();
            List<SqlPartition> newPartAstList = new ArrayList<>();
            newPartAstList.add(sqlPartition);
            int newPrefixPartColCnt = PartitionInfoUtil.getNewPrefixPartColCntBySqlPartitionAst(partColCnt, actualPartColCnt, strategy, newPartAstList);

            for (int i = 0; i < newPrefixPartColCnt; i++) {
                SqlPartitionValueItem oneValItem = partVals.getItems().get(i);
                PartitionField bndValPartFld = buildNewBoundPartFieldForHashValLiteral(oneValItem);
                bndFields.add(bndValPartFld);
            }

            if (newPrefixPartColCnt < partColCnt && strategy == PartitionStrategy.KEY) {
                for (int i = newPrefixPartColCnt; i < partColCnt; i++) {
                    PartitionField maxBndValPartFld = buildBoundPartFieldForHashLongVal(PartitionInfoUtil.getHashSpaceMaxValue());
                    bndFields.add(maxBndValPartFld);
                }
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
        if (partColCnt <= 1 || splitSpec.getStrategy() == PartitionStrategy.HASH) {
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

        List<SqlPartition> newPartitionsAst = sqlAlterTableGroupSplitPartition.getNewPartitions();
        int fullPartColCnt = partColMetaList.size();
        int actualPartColCnt = PartitionInfoUtil.getActualPartitionColumns(curPartitionInfo).size();
        PartitionStrategy strategy = curPartitionInfo.getPartitionBy().getStrategy();
        int newPrefixColCnt = PartitionInfoUtil.getNewPrefixPartColCntBySqlPartitionAst(fullPartColCnt,actualPartColCnt,strategy, newPartitionsAst);
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
                    newPartitions.size(),
                    newPrefixColCnt);

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
        List<String> actualPartColList = PartitionInfoUtil.getActualPartitionColumns(curPartitionInfo);
        int actualPartColCnt = actualPartColList.size();
        for (PartitionSpec spec : oldPartitions) {
            if (spec.getLocation().getPartitionGroupId().longValue() == splitPartRecord.id) {
                if (strategy == PartitionStrategy.HASH || strategy == PartitionStrategy.KEY) {
                    generateNewPartitionsForSplitHashType(partColCnt, actualPartColCnt, unVisiablePartitionGroups,
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

    public static PartitionInfo getNewPartitionInfoForExtractType(PartitionInfo curPartitionInfo,
                                                                  AlterTableGroupExtractPartitionPreparedData parentPrepareData,
                                                                  List<Pair<String, String>> physicalTableAndGroupPairs,
                                                                  ExecutionContext executionContext) {

        final TableGroupInfoManager tableGroupInfoManager =
            OptimizerContext.getContext(curPartitionInfo.getTableSchema()).getTableGroupInfoManager();

        TableGroupConfig tableGroupConfig =
            tableGroupInfoManager.getTableGroupConfigById(curPartitionInfo.getTableGroupId());
        String extractPartitionName = parentPrepareData.getOldPartitionNames().get(0);
        PartitionGroupRecord extractPartRecord = tableGroupConfig.getPartitionGroupRecords().stream()
            .filter(o -> o.partition_name.equalsIgnoreCase(extractPartitionName)).findFirst().orElse(null);

        if (extractPartRecord == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_NAME_NOT_EXISTS,
                "the partition:" + extractPartitionName + " is not exists this current table group");
        }
        if (curPartitionInfo.getPartitionBy().getStrategy() != PartitionStrategy.HASH
            && curPartitionInfo.getPartitionBy().getStrategy() != PartitionStrategy.KEY) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                "it's not support the extract to partition by hot value for " + curPartitionInfo.getPartitionBy()
                    .getStrategy().toString() + " table");
        }

        PartitionInfo newPartitionInfo = curPartitionInfo.copy();
        List<PartitionGroupRecord> newPartitionGroups = parentPrepareData.getInvisiblePartitionGroups();
        assert newPartitionGroups.size() == physicalTableAndGroupPairs.size();
        Set<String> oldPartitions = new TreeSet<>(String::compareToIgnoreCase);
        parentPrepareData.getOldPartitionNames().stream().forEach(o -> oldPartitions.add(o));
        boolean foundSplitPoint = false;
        List<PartitionSpec> newPartitions = new ArrayList<>();
        PartitionSpec lastOldPartitionSpec = null;
        int i = 0;
        for (PartitionSpec partitionSpec : curPartitionInfo.getPartitionBy().getPartitions()) {
            if (oldPartitions.contains(partitionSpec.getName())) {
                lastOldPartitionSpec = partitionSpec;
                if (foundSplitPoint) {
                    continue;
                }
                foundSplitPoint = true;
                for (Long[] splitPoint : parentPrepareData.getSplitPoints()) {
                    PartitionSpec newPartitionSpec = partitionSpec.copy();
                    newPartitionSpec.setName(newPartitionGroups.get(i).partition_name);
                    PartitionLocation location = newPartitionSpec.getLocation();
                    location.setGroupKey(physicalTableAndGroupPairs.get(i).getValue());
                    location.setPhyTableName(physicalTableAndGroupPairs.get(i).getKey());
                    location.setVisiable(false);
                    SearchDatumInfo searchDatumInfo = SearchDatumInfo.createFromHashCodes(splitPoint);
                    newPartitionSpec.getBoundSpec().setSingleDatum(searchDatumInfo);
                    newPartitions.add(newPartitionSpec);
                    i++;
                }
            } else {
                PartitionSpec newPartitionSpec;
                if (foundSplitPoint && lastOldPartitionSpec != null) {
                    newPartitionSpec = lastOldPartitionSpec.copy();
                    newPartitionSpec.setName(newPartitionGroups.get(i).partition_name);
                    PartitionLocation location = newPartitionSpec.getLocation();
                    location.setGroupKey(physicalTableAndGroupPairs.get(i).getValue());
                    location.setPhyTableName(physicalTableAndGroupPairs.get(i).getKey());
                    location.setVisiable(false);
                    newPartitions.add(newPartitionSpec);
                    lastOldPartitionSpec = null;
                }
                newPartitionSpec = partitionSpec.copy();
                newPartitions.add(newPartitionSpec);
            }
        }
        //oldPartition is the last one
        if (foundSplitPoint && lastOldPartitionSpec != null) {
            PartitionSpec newPartitionSpec = lastOldPartitionSpec.copy();
            newPartitionSpec.setName(newPartitionGroups.get(i).partition_name);
            PartitionLocation location = newPartitionSpec.getLocation();
            location.setGroupKey(physicalTableAndGroupPairs.get(i).getValue());
            location.setPhyTableName(physicalTableAndGroupPairs.get(i).getKey());
            location.setVisiable(false);
            newPartitions.add(newPartitionSpec);
            lastOldPartitionSpec = null;
        }
        newPartitionInfo.getPartitionBy().setPartitions(newPartitions);
        PartitionInfoUtil.adjustPartitionPositionsForNewPartInfo(newPartitionInfo);
        PartitionInfoUtil.validatePartitionInfoForDdl(newPartitionInfo, executionContext);
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
                    if (spec.getStrategy() == PartitionStrategy.LIST
                        || spec.getStrategy() == PartitionStrategy.LIST_COLUMNS) {
                        newSpec.getBoundSpec().getMultiDatums().addAll(listDatums);
                    }
                    newPartitions.add(newSpec);
                } else if (spec.getStrategy() == PartitionStrategy.LIST
                    || spec.getStrategy() == PartitionStrategy.LIST_COLUMNS) {
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

        Map<String, String> physicalTableMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (Pair<String, String> pair : physicalTableAndGroupPairs) {
            physicalTableMap.put(pair.getKey(), pair.getValue());
        }

        for (PartitionSpec spec : oldPartitions) {
            if (movePartGroupIds.contains(spec.getLocation().getPartitionGroupId())) {
                PartitionSpec newSpec = spec.copy();
                String phyTableName = spec.getLocation().getPhyTableName().toLowerCase();
                newSpec.getLocation().setGroupKey(physicalTableMap.get(phyTableName));
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
                                                                   SqlNode addPartitionAst,
                                                                   List<Pair<String, String>> physicalTableAndGroupPairs,
                                                                   ExecutionContext executionContext) {
        SqlAlterTableAddPartition addPartition = (SqlAlterTableAddPartition) addPartitionAst;
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
                                                                    List<PartitionGroupRecord> unVisiablePartitionGroupRecords,
                                                                    ExecutionContext executionContext) {
        PartitionInfo newPartitionInfo = curPartitionInfo.copy();
        PartitionSpec partitionSpec = curPartitionInfo.getPartitionBy().getPartitions().get(0).copy();
        for (PartitionGroupRecord record : unVisiablePartitionGroupRecords) {
            partitionSpec.setName(record.partition_name);
            partitionSpec.getLocation().setVisiable(false);
            partitionSpec.getLocation().setGroupKey(GroupInfoUtil.buildGroupNameFromPhysicalDb(record.phy_db));
            newPartitionInfo.getPartitionBy().getPartitions().add(partitionSpec);
            partitionSpec = partitionSpec.copy();
        }
        PartitionInfoUtil.adjustPartitionPositionsForNewPartInfo(newPartitionInfo);
        PartitionInfoUtil.validatePartitionInfoForDdl(newPartitionInfo, executionContext);
        return newPartitionInfo;

    }

    public static PartitionInfo getNewPartitionInfoForSplitPartitionByHotValue(PartitionInfo curPartitionInfo,
                                                                               AlterTableGroupSplitPartitionByHotValuePreparedData parentPrepareData,
                                                                               List<Pair<String, String>> physicalTableAndGroupPairs,
                                                                               ExecutionContext executionContext) {
        assert curPartitionInfo.getPartitionBy().getStrategy() == PartitionStrategy.KEY;
        assert curPartitionInfo.getPartitionColumns().size() > 1;
        PartitionInfo newPartitionInfo = curPartitionInfo.copy();
        List<PartitionGroupRecord> newPartitionGroups = parentPrepareData.getInvisiblePartitionGroups();
        assert newPartitionGroups.size() == physicalTableAndGroupPairs.size();
        Set<String> oldPartitions = new TreeSet<>(String::compareToIgnoreCase);
        parentPrepareData.getOldPartitionNames().stream().forEach(o -> oldPartitions.add(o));
        boolean foundSplitPoint = false;
        List<PartitionSpec> newPartitions = new ArrayList<>();
        PartitionSpec lastOldPartitionSpec = null;

        String tblName = curPartitionInfo.getTableName();
        int i = 0;
        int fullPartColCnt = curPartitionInfo.getPartitionBy().getPartitionColumnNameList().size();
        int actualPartColCnt = PartitionInfoUtil.getActualPartitionColumns(curPartitionInfo).size();
        for (PartitionSpec partitionSpec : curPartitionInfo.getPartitionBy().getPartitions()) {
            if (oldPartitions.contains(partitionSpec.getName())) {
                lastOldPartitionSpec = partitionSpec;
                if (foundSplitPoint) {
                    continue;
                }
                foundSplitPoint = true;
                List<Long[]> splitPointList = parentPrepareData.getSplitPointInfos().get(tblName);
                for (Long[] splitPoint : splitPointList) {

                    Long[] finalSplitPoint = null;
                    int newPrefixPartColCnt = splitPoint.length;
                    if (newPrefixPartColCnt < fullPartColCnt) {
                        finalSplitPoint = new Long[fullPartColCnt];
                        for (int j = 0; j < newPrefixPartColCnt; j++) {
                            finalSplitPoint[j] = splitPoint[j];
                        }
                        for (int j = newPrefixPartColCnt; j < fullPartColCnt; j++) {
                            finalSplitPoint[j] = PartitionInfoUtil.getHashSpaceMaxValue();
                        }
                    } else {
                        finalSplitPoint = splitPoint;
                    }

                    PartitionSpec newPartitionSpec = partitionSpec.copy();
                    newPartitionSpec.setName(newPartitionGroups.get(i).partition_name);
                    PartitionLocation location = newPartitionSpec.getLocation();
                    location.setGroupKey(physicalTableAndGroupPairs.get(i).getValue());
                    location.setPhyTableName(physicalTableAndGroupPairs.get(i).getKey());
                    location.setVisiable(false);
                    SearchDatumInfo searchDatumInfo = SearchDatumInfo.createFromHashCodes(finalSplitPoint);
                    newPartitionSpec.getBoundSpec().setSingleDatum(searchDatumInfo);
                    newPartitions.add(newPartitionSpec);
                    i++;
                }
            } else {
                PartitionSpec newPartitionSpec;
                if (foundSplitPoint && lastOldPartitionSpec != null) {
                    newPartitionSpec = lastOldPartitionSpec.copy();
                    newPartitionSpec.setName(newPartitionGroups.get(i).partition_name);
                    PartitionLocation location = newPartitionSpec.getLocation();
                    location.setGroupKey(physicalTableAndGroupPairs.get(i).getValue());
                    location.setPhyTableName(physicalTableAndGroupPairs.get(i).getKey());
                    location.setVisiable(false);
                    newPartitions.add(newPartitionSpec);
                    lastOldPartitionSpec = null;
                }
                newPartitionSpec = partitionSpec.copy();
                newPartitions.add(newPartitionSpec);
            }
        }
        //oldPartition is the last one
        if (foundSplitPoint && lastOldPartitionSpec != null) {
            PartitionSpec newPartitionSpec = lastOldPartitionSpec.copy();
            newPartitionSpec.setName(newPartitionGroups.get(i).partition_name);
            PartitionLocation location = newPartitionSpec.getLocation();
            location.setGroupKey(physicalTableAndGroupPairs.get(i).getValue());
            location.setPhyTableName(physicalTableAndGroupPairs.get(i).getKey());
            location.setVisiable(false);
            newPartitions.add(newPartitionSpec);
            lastOldPartitionSpec = null;
        }
        newPartitionInfo.getPartitionBy().setPartitions(newPartitions);
        PartitionInfoUtil.adjustPartitionPositionsForNewPartInfo(newPartitionInfo);
        PartitionInfoUtil.validatePartitionInfoForDdl(newPartitionInfo, executionContext);
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
            newPartInfo = getNewPartitionInfoForCopyPartition(curPartitionInfo, unVisiablePartitionGroupRecords,
                executionContext);
        } else {
            throw new NotSupportException(sqlNode.getKind().toString() + " is not support yet");
        }
        PartitionInfoUtil.adjustPartitionPositionsForNewPartInfo(newPartInfo);
        PartitionInfoUtil.validatePartitionInfoForDdl(newPartInfo, executionContext);
        return newPartInfo;

    }
}
