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
import com.alibaba.polardbx.common.utils.CaseInsensitive;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.gms.partition.TablePartitionRecord;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.util.GroupInfoUtil;
import com.alibaba.polardbx.gms.util.PartitionNameUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.CharType;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupAddPartitionPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupBasePreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupExtractPartitionPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupMergePartitionPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupModifyPartitionPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupReorgPartitionPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupSplitPartitionByHotValuePreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupSplitPartitionPreparedData;
import com.alibaba.polardbx.optimizer.partition.PartitionByDefinition;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoBuilder;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoUtil;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.partition.boundspec.HashPartBoundValBuilder;
import com.alibaba.polardbx.optimizer.partition.boundspec.KeyPartBoundValBuilder;
import com.alibaba.polardbx.optimizer.partition.boundspec.MultiValuePartitionBoundSpec;
import com.alibaba.polardbx.optimizer.partition.boundspec.PartBoundValBuilder;
import com.alibaba.polardbx.optimizer.partition.boundspec.PartitionBoundVal;
import com.alibaba.polardbx.optimizer.partition.boundspec.PartitionBoundValueKind;
import com.alibaba.polardbx.optimizer.partition.boundspec.SingleValuePartitionBoundSpec;
import com.alibaba.polardbx.optimizer.partition.common.BuildPartSpecFromAstParams;
import com.alibaba.polardbx.optimizer.partition.common.PartKeyLevel;
import com.alibaba.polardbx.optimizer.partition.common.PartitionLocation;
import com.alibaba.polardbx.optimizer.partition.common.PartitionStrategy;
import com.alibaba.polardbx.optimizer.partition.datatype.PartitionField;
import com.alibaba.polardbx.optimizer.partition.datatype.PartitionFieldBuilder;
import com.alibaba.polardbx.optimizer.partition.datatype.function.PartitionIntFunction;
import com.alibaba.polardbx.optimizer.partition.pruning.PartFieldAccessType;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPrunerUtils;
import com.alibaba.polardbx.optimizer.partition.pruning.SearchDatumComparator;
import com.alibaba.polardbx.optimizer.partition.pruning.SearchDatumInfo;
import com.alibaba.polardbx.optimizer.utils.KeyWordsUtil;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAlterTableAddPartition;
import org.apache.calcite.sql.SqlAlterTableDropPartition;
import org.apache.calcite.sql.SqlAlterTableExtractPartition;
import org.apache.calcite.sql.SqlAlterTableGroup;
import org.apache.calcite.sql.SqlAlterTableGroupMergePartition;
import org.apache.calcite.sql.SqlAlterTableGroupMovePartition;
import org.apache.calcite.sql.SqlAlterTableGroupSplitPartition;
import org.apache.calcite.sql.SqlAlterTableMergePartition;
import org.apache.calcite.sql.SqlAlterTableModifyPartitionValues;
import org.apache.calcite.sql.SqlAlterTableMovePartition;
import org.apache.calcite.sql.SqlAlterTableRenamePartition;
import org.apache.calcite.sql.SqlAlterTableReorgPartition;
import org.apache.calcite.sql.SqlAlterTableSetTableGroup;
import org.apache.calcite.sql.SqlAlterTableSplitPartition;
import org.apache.calcite.sql.SqlAlterTableSplitPartitionByHotValue;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlPartition;
import org.apache.calcite.sql.SqlPartitionValue;
import org.apache.calcite.sql.SqlPartitionValueItem;
import org.apache.calcite.sql.SqlRefreshTopology;
import org.apache.calcite.sql.SqlSubPartition;
import org.apache.calcite.util.Util;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.gms.partition.TablePartitionRecord.PARTITION_LEVEL_PARTITION;

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
        PartitionInfo curPartitionInfo,
        List<PartitionGroupRecord> unVisiablePartitionGroups,
        SqlAlterTableSplitPartition sqlAlterTableGroupSplitPartition,
        PartitionSpec parentPartition,
        List<PartitionSpec> oldPartitions,
        List<PartitionSpec> newPartitions,
        Map<String, Pair<String, String>> physicalTableAndGroupPairs,
        Map<SqlNode, RexNode> partRexInfoCtx,
        PartitionSpec splitSpec, PartitionField atVal,
        ExecutionContext ec) {
        int i = 0;
        int newPartitionNums = sqlAlterTableGroupSplitPartition.getNewPartitions().size();
        assert newPartitionNums == 2;
        AtomicLong physicalPartIndex = new AtomicLong(0);

        PartitionByDefinition subPartitionBy = curPartitionInfo.getPartitionBy().getSubPartitionBy();
        boolean splitTemplateSubPart =
            sqlAlterTableGroupSplitPartition.isSubPartitionsSplit() && (subPartitionBy != null
                && subPartitionBy.isUseSubPartTemplate());

        for (SqlPartition sqlPartition : sqlAlterTableGroupSplitPartition.getNewPartitions()) {
            String templatePartitionName = ((SqlIdentifier) sqlPartition.getName()).getLastName();
            String fullPartitionName = templatePartitionName;
            if (splitTemplateSubPart) {
                fullPartitionName =
                    PartitionNameUtil.autoBuildSubPartitionName(parentPartition.getName(), templatePartitionName);
            }
            PartitionSpec newSpec =
                generateNewPartitionSpec(curPartitionInfo,
                    sqlAlterTableGroupSplitPartition.isSubPartitionsSplit(),
                    splitSpec.copy(),
                    fullPartitionName,
                    sqlPartition,
                    unVisiablePartitionGroups,
                    physicalTableAndGroupPairs,
                    partRexInfoCtx,
                    physicalPartIndex,
                    ec);
            if (splitTemplateSubPart) {
                newSpec.setName(fullPartitionName);
                newSpec.setTemplateName(templatePartitionName);
            }
            if (i == 0) {
                newSpec.getBoundSpec().getSingleDatum().getSingletonValue().setValue(atVal);
                newSpec.getBoundSpec().getSingleDatum().getSingletonValue()
                    .setValueKind(PartitionBoundValueKind.DATUM_NORMAL_VALUE);
            }

            if (oldPartitions.stream().anyMatch(o -> o.getName().equalsIgnoreCase(newSpec.getName()))) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                    "the partition name:" + newSpec.getName() + " exists");
            }

            newPartitions.add(newSpec);
            i++;
        }
    }

    private static void generateNewPartitionsForSplitHashType(
        PartitionInfo curPartitionInfo,
        int partColCnt,
        int actualPartColCnt,
        List<PartitionGroupRecord> invisiblePartitionGroups,
        SqlAlterTableSplitPartition sqlAlter,
        PartitionSpec parentPartition,
        List<PartitionSpec> oldPartitions,
        List<PartitionSpec> newPartitions,
        Map<String, Pair<String, String>> physicalTableAndGroupPairs,
        Map<SqlNode, RexNode> partRexInfoCtx,
        PartitionSpec splitSpec,
        ExecutionContext ec) {

        boolean firstPartition = newPartitions.isEmpty();
        PartitionStrategy strategy = splitSpec.getStrategy();
        assert strategy == PartitionStrategy.HASH || splitSpec.getStrategy() == PartitionStrategy.KEY;
        PartitionByDefinition subPartitionBy = curPartitionInfo.getPartitionBy().getSubPartitionBy();
        boolean splitTemplateSubPart =
            sqlAlter.isSubPartitionsSplit() && (subPartitionBy != null && subPartitionBy.isUseSubPartTemplate());

        AtomicLong physicalPartIndex = new AtomicLong(0);
        for (int i = 0; i < sqlAlter.getNewPartitions().size(); i++) {
            SqlPartition sqlPartition = sqlAlter.getNewPartitions().get(i);
            String templatePartitionName = ((SqlIdentifier) sqlPartition.getName()).getLastName();
            String fullPartitionName = templatePartitionName;
            if (splitTemplateSubPart) {
                fullPartitionName =
                    PartitionNameUtil.autoBuildSubPartitionName(parentPartition.getName(), templatePartitionName);
            }
            PartitionSpec newSpec =
                generateNewPartitionSpec(curPartitionInfo,
                    sqlAlter.isSubPartitionsSplit(),
                    splitSpec.copy(),
                    fullPartitionName,
                    sqlPartition,
                    invisiblePartitionGroups,
                    physicalTableAndGroupPairs,
                    partRexInfoCtx,
                    physicalPartIndex,
                    ec);

            if (splitTemplateSubPart) {
                newSpec.setName(fullPartitionName);
                newSpec.setTemplateName(templatePartitionName);
            }

            SingleValuePartitionBoundSpec newBound = (SingleValuePartitionBoundSpec) newSpec.getBoundSpec();

            newPartitions.add(newSpec);

            /*
             * Generate bound value:
             * If not specified from SQL, calculate hash value
             * Else extract it from SQL node
             */
            if (sqlPartition.getValues() != null) {
                buildNewBoundForSplitHashOrKey(partColCnt, actualPartColCnt, strategy, sqlPartition, newBound);
            } else {
                // only the last part not need to changed
                if (i + 1 != sqlAlter.getNewPartitions().size()) {
                    calcSplitAtForHashOrKeyAndBuildNewBoundValue(partColCnt, sqlAlter, oldPartitions, newPartitions,
                        splitSpec, firstPartition, i + 1, newBound);
                }
            }
        }
    }

    private static PartitionSpec generateNewPartitionSpec(PartitionInfo curPartitionInfo,
                                                          boolean isSubPartitionChanged,
                                                          PartitionSpec newSpec,
                                                          String physicalPartitionName,
                                                          SqlPartition sqlPartition,
                                                          List<PartitionGroupRecord> invisiblePartitionGroups,
                                                          Map<String, Pair<String, String>> physicalTableAndGroupPairs,
                                                          Map<SqlNode, RexNode> partRexInfoCtx,
                                                          AtomicLong physicalPartIndex,
                                                          ExecutionContext ec) {
        newSpec.setName(((SqlIdentifier) sqlPartition.getName()).getLastName());

        if (GeneralUtil.isNotEmpty(invisiblePartitionGroups)) {
            if (GeneralUtil.isEmpty(sqlPartition.getSubPartitions())) {
                String partitionGroupName = physicalPartitionName;
                PartitionGroupRecord pg =
                    invisiblePartitionGroups.stream()
                        .filter(c -> c.partition_name.equalsIgnoreCase(partitionGroupName))
                        .findFirst().get();

                int index = (int) physicalPartIndex.getAndIncrement();
                newSpec.setLocation(new PartitionLocation());
                newSpec.getLocation().setPartitionGroupId(pg.id);
                newSpec.getLocation().setPhyTableName(physicalTableAndGroupPairs.get(physicalPartitionName).getKey());
                newSpec.getLocation().setGroupKey(physicalTableAndGroupPairs.get(physicalPartitionName).getValue());
                newSpec.getLocation().setVisiable(false);
            } else {
                long pos = 1l;
                newSpec.getSubPartitions().clear();
                int partIndex = 0;
                PartitionByDefinition partitionBy = curPartitionInfo.getPartitionBy();
                PartitionByDefinition subPartitionBy = partitionBy.getSubPartitionBy();
                AtomicInteger phyPartCounter = new AtomicInteger(0);

                PartBoundValBuilder boundValBuilder = null;

                boolean isAutoSubPart = false;
                if (subPartitionBy.getStrategy() == PartitionStrategy.KEY
                    || subPartitionBy.getStrategy() == PartitionStrategy.HASH) {
                    if (GeneralUtil.isNotEmpty(sqlPartition.getSubPartitions())) {
                        SqlSubPartition sqlSubPartition = (SqlSubPartition) sqlPartition.getSubPartitions().get(0);
                        if (sqlSubPartition.getValues() == null) {
                            isAutoSubPart = true;
                            boolean isMultiCol = subPartitionBy.getPartitionFieldList().size() > 1;
                            if (subPartitionBy.getStrategy() == PartitionStrategy.HASH || (
                                subPartitionBy.getStrategy() == PartitionStrategy.KEY && !isMultiCol)) {
                                boundValBuilder = new HashPartBoundValBuilder(sqlPartition.getSubPartitions().size());
                            } else {
                                boundValBuilder =
                                    new KeyPartBoundValBuilder(sqlPartition.getSubPartitions().size(),
                                        subPartitionBy.getPartitionFieldList().size());
                            }
                        }
                    }
                }

                for (SqlNode sqlNode : sqlPartition.getSubPartitions()) {
                    int index = (int) physicalPartIndex.getAndIncrement();
                    SqlSubPartition sqlSubPartition = (SqlSubPartition) sqlNode;
                    String subPartName = ((SqlIdentifier) sqlSubPartition.getName()).getSimple();

                    PartitionGroupRecord pg =
                        invisiblePartitionGroups.stream()
                            .filter(c -> c.partition_name.equalsIgnoreCase(subPartName))
                            .findFirst().get();

                    BuildPartSpecFromAstParams partSpecAstParams = new BuildPartSpecFromAstParams();
                    partSpecAstParams.setContext(ec);
                    partSpecAstParams.setPartKeyLevel(PartKeyLevel.SUBPARTITION_KEY);
                    partSpecAstParams.setPartIntFunc(subPartitionBy.getPartIntFunc());
                    partSpecAstParams.setPruningComparator(subPartitionBy.getPruningSpaceComparator());
                    partSpecAstParams.setPartBoundExprInfo(partRexInfoCtx);
                    partSpecAstParams.setPartBoundValBuilder(null);
                    partSpecAstParams.setStrategy(subPartitionBy.getStrategy());
                    partSpecAstParams.setPartPosition(partIndex + 1);
                    partSpecAstParams.setAllLevelPrefixPartColCnts(partitionBy.getAllLevelFullPartColumnCounts());
                    partSpecAstParams.setPartNameAst(sqlSubPartition.getName());
                    partSpecAstParams.setPartComment(sqlSubPartition.getComment());
                    partSpecAstParams.setPartLocality(sqlSubPartition.getLocality());
                    partSpecAstParams.setPartBndValuesAst(sqlSubPartition.getValues());

                    partSpecAstParams.setLogical(false);

                    partSpecAstParams.setPartColMetaList(subPartitionBy.getPartitionFieldList());

                    partSpecAstParams.setSpecTemplate(false);
                    partSpecAstParams.setSubPartSpec(true);
                    partSpecAstParams.setParentPartSpec(null);
                    partSpecAstParams.setPhySpecCounter(phyPartCounter);
                    partSpecAstParams.setAutoBuildPart(isAutoSubPart);
                    partSpecAstParams.setPartBoundValBuilder(boundValBuilder);

                    PartitionSpec subPartSpec = PartitionInfoBuilder.buildPartSpecByAstParams(partSpecAstParams);
                    subPartSpec.setLocation(new PartitionLocation());
                    subPartSpec.getLocation().setPartitionGroupId(pg.id);
                    if (!isSubPartitionChanged) {
                        physicalPartitionName = subPartName;
                        String templateName = PartitionNameUtil.getTemplateName(newSpec.getName().toLowerCase(),
                            subPartName.toLowerCase());
                        subPartSpec.setTemplateName(templateName);
                    }
                    subPartSpec.getLocation()
                        .setPhyTableName(physicalTableAndGroupPairs.get(physicalPartitionName).getKey());
                    subPartSpec.getLocation()
                        .setGroupKey(physicalTableAndGroupPairs.get(physicalPartitionName).getValue());
                    subPartSpec.getLocation().setVisiable(false);
                    newSpec.getSubPartitions().add(subPartSpec);
                    partIndex++;
                }
            }
        }
        return newSpec;
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
            int newPrefixPartColCnt =
                PartitionInfoUtil.getNewPrefixPartColCntBySqlPartitionAst(partColCnt, actualPartColCnt, strategy,
                    newPartAstList);
            if (newPrefixPartColCnt == PartitionInfoUtil.FULL_PART_COL_COUNT) {
                newPrefixPartColCnt = partColCnt;
            }
            if (strategy == PartitionStrategy.HASH) {
                SqlPartitionValueItem oneValItem = partVals.getItems().get(0);
                PartitionField bndValPartFld = buildNewBoundPartFieldForHashValLiteral(oneValItem);
                bndFields.add(bndValPartFld);
            } else {
                for (int i = 0; i < newPrefixPartColCnt; i++) {
                    SqlPartitionValueItem oneValItem = partVals.getItems().get(i);
                    PartitionField bndValPartFld = buildNewBoundPartFieldForHashValLiteral(oneValItem);
                    bndFields.add(bndValPartFld);
                }
            }

            if (newPrefixPartColCnt < partColCnt && strategy == PartitionStrategy.KEY) {
                for (int i = newPrefixPartColCnt; i < partColCnt; i++) {
                    PartitionField maxBndValPartFld =
                        buildBoundPartFieldForHashLongVal(PartitionInfoUtil.getHashSpaceMaxValue());
                    bndFields.add(maxBndValPartFld);
                }
            }
            newBndValDatum = SearchDatumInfo.createFromFields(bndFields);
            newBound.setSingleDatum(newBndValDatum);
        }
    }

    private static Long calculateSplitAtHash(
        SqlAlterTableSplitPartition sqlAlter,
        List<PartitionSpec> oldPartitions, List<PartitionSpec> newPartitions,
        PartitionSpec splitSpec, boolean firstPartition,
        int newRangeNum,
        int curRange) {
        Long atVal;

        if (sqlAlter.getAtValue() != null && sqlAlter.getAtValue() instanceof SqlNumericLiteral) {
            atVal = ((SqlLiteral) sqlAlter.getAtValue()).longValue(true);
        } else if (oldPartitions.size() == 1 || firstPartition) {
            //todo overflow issue
            // TODO avoid convert to string then parse
            BigInteger minVal = new BigInteger(String.valueOf(Long.MIN_VALUE));
            BigInteger splitVal = new BigInteger(
                splitSpec.getBoundSpec().getSingleDatum().getSingletonValue().getValue().stringValue().toStringUtf8());
            atVal = calcMiddlePoint(minVal, splitVal, newRangeNum, curRange);
        } else {
            PartitionSpec lastPart = newPartitions.get(newPartitions.size() - 2 - (curRange - 1));
            BigInteger minVal = new BigInteger(
                lastPart.getBoundSpec().getSingleDatum().getSingletonValue().getValue().stringValue().toStringUtf8());
            BigInteger splitVal = new BigInteger(
                splitSpec.getBoundSpec().getSingleDatum().getSingletonValue().getValue().stringValue().toStringUtf8());
            atVal = calcMiddlePoint(minVal, splitVal, newRangeNum, curRange);
            if (atVal.longValue() == minVal.longValue() ||
                atVal.longValue() == splitVal.longValue()
            ) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                    "the partition:" + splitSpec.getName() + " can't split anymore");
            }
        }
        return atVal;
    }

    private static long calcMiddlePoint(BigInteger minVal, BigInteger maxVal, int newRangeNum, int curRange) {
        BigInteger divideVal = new BigInteger(String.valueOf(newRangeNum));
        BigInteger curRangeVal = new BigInteger(String.valueOf(curRange));
        if (newRangeNum == 2 && curRange == 1) {
            //keep the same behavior with "OLD-style" split 2 partition
            return minVal.add(maxVal).divide(divideVal).longValue();
        } else {
            return minVal.add(maxVal.subtract(minVal).divide(divideVal).multiply(curRangeVal)).longValue();
        }
    }

    private static void calcSplitAtForHashOrKeyAndBuildNewBoundValue(int partColCnt,
                                                                     SqlAlterTableSplitPartition sqlAlter,
                                                                     List<PartitionSpec> oldPartitions,
                                                                     List<PartitionSpec> newPartitions,
                                                                     PartitionSpec splitSpec,
                                                                     boolean firstPartition,
                                                                     int curRange,
                                                                     SingleValuePartitionBoundSpec newBound /*Output params*/) {

        long splitAtVal;
        if (partColCnt <= 1 || splitSpec.getStrategy() == PartitionStrategy.HASH) {
            splitAtVal = calculateSplitAtHash(sqlAlter, oldPartitions, newPartitions, splitSpec, firstPartition,
                sqlAlter.getNewPartitions().size(), curRange);
            SearchDatumInfo newBndValDatum = SearchDatumInfo.createFromHashCode(splitAtVal);
            newBound.setSingleDatum(newBndValDatum);
        } else {

            List<PartitionField> splitPointFlds = new ArrayList<>();
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
                    Long middlePointVal = calcMiddlePoint(minVal, maxVal, sqlAlter.getNewPartitions().size(), curRange);
                    PartitionField newBndFld = PartitionPrunerUtils
                        .buildPartField(middlePointVal, DataTypes.LongType, DataTypes.LongType, null, null,
                            PartFieldAccessType.DDL_EXECUTION);
                    splitPointFlds.add(newBndFld);
                }
            } else {
                PartitionSpec lastPartSpec = newPartitions.get(newPartitions.size() - 2);
                PartitionSpec toBeSplitSpec = splitSpec;

                for (int i = 0; i < partColCnt; i++) {
                    PartitionBoundVal minBnd = lastPartSpec.getBoundSpec().getSingleDatum().getDatumInfo()[i];
                    PartitionBoundVal maxBnd = toBeSplitSpec.getBoundSpec().getSingleDatum().getDatumInfo()[i];

                    BigInteger minVal = new BigInteger(minBnd.getValue().stringValue().toStringUtf8());
                    BigInteger maxVal = new BigInteger(maxBnd.getValue().stringValue().toStringUtf8());
                    Long middlePointVal = calcMiddlePoint(minVal, maxVal, sqlAlter.getNewPartitions().size(), curRange);
                    PartitionField newBndFld = PartitionPrunerUtils
                        .buildPartField(middlePointVal, DataTypes.LongType, DataTypes.LongType, null, null,
                            PartFieldAccessType.DDL_EXECUTION);
                    splitPointFlds.add(newBndFld);
                }
            }

            SearchDatumInfo newDatum = SearchDatumInfo.createFromFields(splitPointFlds);
            newBound.setSingleDatum(newDatum);
        }
    }

    private static void generateNewPartitionsForSplitWithPartitionSpec(
        ExecutionContext executionContext,
        PartitionInfo curPartitionInfo,
        List<PartitionGroupRecord> unVisiablePartitionGroups,
        SqlAlterTableSplitPartition sqlAlterTableSplitPartition,
        List<PartitionSpec> oldPartitions,
        List<PartitionSpec> newPartitions,
        Map<String, Pair<String, String>> physicalTableAndGroupPairs,
        PartitionSpec splitSpec,
        Map<SqlNode, RexNode> partRexInfoCtx,
        AtomicLong physicalPartIndex) {
        int i = 0;
        List<ColumnMeta> partColMetaList = sqlAlterTableSplitPartition.isSubPartitionsSplit() ?
            curPartitionInfo.getPartitionBy().getSubPartitionBy().getPartitionFieldList() :
            curPartitionInfo.getPartitionBy().getPartitionFieldList();
        SearchDatumComparator comparator = sqlAlterTableSplitPartition.isSubPartitionsSplit() ?
            curPartitionInfo.getPartitionBy().getSubPartitionBy().getPruningSpaceComparator() :
            curPartitionInfo.getPartitionBy().getPruningSpaceComparator();
        PartitionIntFunction partIntFunc = sqlAlterTableSplitPartition.isSubPartitionsSplit() ?
            curPartitionInfo.getPartitionBy().getSubPartitionBy().getPartIntFunc() :
            curPartitionInfo.getPartitionBy().getPartIntFunc();
        PartitionStrategy strategy = sqlAlterTableSplitPartition.isSubPartitionsSplit() ?
            curPartitionInfo.getPartitionBy().getSubPartitionBy().getStrategy() :
            curPartitionInfo.getPartitionBy().getStrategy();
        List<SqlPartition> newPartitionsAst = sqlAlterTableSplitPartition.getNewPartitions();

//        int fullPartColCnt = partColMetaList.size();
//        List<List<String>> allLevelActualPartCols = curPartitionInfo.getAllLevelActualPartCols();
//        int actualPartColCnt = allLevelActualPartCols.get(0).size();
//        int newPrefixColCnt =
//            PartitionInfoUtil.getNewPrefixPartColCntBySqlPartitionAst(fullPartColCnt, actualPartColCnt, strategy,
//                newPartitionsAst);

//        GetNewActPartColCntFromAstParams params = new GetNewActPartColCntFromAstParams();
//        List<Integer> allLevelFullPartColCnts = new ArrayList<>();
//        List<Integer> allLevelActPartColCnts = new ArrayList<>();
//        List<PartitionStrategy> allLevelPartStrategies = new ArrayList<>();
//
//        int index = sqlAlterTableGroupSplitPartition.isSubPartitionsSplit() ? 1 : 0;
//        allLevelPartStrategies.add(curPartitionInfo.getAllLevelPartitionStrategies().get(index));
//        allLevelFullPartColCnts.add(curPartitionInfo.getAllLevelFullPartColCounts().get(index));
//        allLevelActPartColCnts.add(curPartitionInfo.getAllLevelActualPartColCounts().get(index));
//
//        params.setAllLevelFullPartColCnts(allLevelFullPartColCnts);
//        params.setAllLevelActualPartColCnts(allLevelActPartColCnts);
//        params.setAllLevelStrategies(allLevelPartStrategies);
//        params.setNewPartitions(newPartitionsAst);
//
//        List<Integer> newActualPartColCnts = PartitionInfoUtil.getNewAllLevelPrefixPartColCntBySqlPartitionAst(params);

        boolean isAlterTableGroup = sqlAlterTableSplitPartition instanceof SqlAlterTableGroupSplitPartition;

        List<Integer> newActualPartColCnts =
            PartitionInfoUtil.getNewAllLevelPrefixPartColCntByPartInfoAndNewParts(isAlterTableGroup, curPartitionInfo,
                newPartitionsAst, sqlAlterTableSplitPartition.isSubPartitionsSplit());

        AtomicInteger phyPartCounter = new AtomicInteger(0);

        PartitionByDefinition partByDef = curPartitionInfo.getPartitionBy();
        PartitionByDefinition subPartByDef = partByDef.getSubPartitionBy();

        for (SqlPartition sqlPartition : sqlAlterTableSplitPartition.getNewPartitions()) {

            BuildPartSpecFromAstParams partSpecAstParams = new BuildPartSpecFromAstParams();
            partSpecAstParams.setContext(executionContext);
            partSpecAstParams.setPartKeyLevel(
                sqlAlterTableSplitPartition.isSubPartitionsSplit() ? PartKeyLevel.SUBPARTITION_KEY :
                    PartKeyLevel.PARTITION_KEY);
            partSpecAstParams.setPartIntFunc(partIntFunc);
            partSpecAstParams.setPruningComparator(comparator);
            partSpecAstParams.setPartBoundExprInfo(partRexInfoCtx);
            partSpecAstParams.setPartBoundValBuilder(null);
            partSpecAstParams.setStrategy(strategy);
            partSpecAstParams.setPartPosition(newPartitions.size());
            partSpecAstParams.setAllLevelPrefixPartColCnts(newActualPartColCnts);
            partSpecAstParams.setPartNameAst(sqlPartition.getName());
            partSpecAstParams.setPartComment(sqlPartition.getComment());
            partSpecAstParams.setPartLocality(sqlPartition.getLocality());
            partSpecAstParams.setPartBndValuesAst(sqlPartition.getValues());

            partSpecAstParams.setLogical(false);

            boolean changeTemplateSubPart = false;
            String parentName = "";
            if (sqlAlterTableSplitPartition.isSubPartitionsSplit()) {
                partSpecAstParams.setPartColMetaList(subPartByDef.getPartitionFieldList());
                if (curPartitionInfo.getPartitionBy().getSubPartitionBy().isUseSubPartTemplate()) {
                    changeTemplateSubPart = true;
                    Optional<PartitionSpec> parentPartSpec = curPartitionInfo.getPartitionBy().getPartitions().stream()
                        .filter(o -> o.getId().intValue() == splitSpec.getParentId().longValue()).findFirst();
                    parentName = parentPartSpec.get().getName();
                }
            } else {
                partSpecAstParams.setPartColMetaList(partByDef.getPartitionFieldList());
                if (splitSpec.isLogical()) {
                    partSpecAstParams.setLogical(true);
                }
            }

            partSpecAstParams.setSpecTemplate(false);
            partSpecAstParams.setSubPartSpec(false);
            partSpecAstParams.setParentPartSpec(null);
            partSpecAstParams.setPhySpecCounter(phyPartCounter);
            PartitionSpec newSpec = PartitionInfoBuilder.buildPartSpecByAstParams(partSpecAstParams);

            String partName = sqlPartition.getName().toString();
            if (changeTemplateSubPart) {
                partName = PartitionNameUtil.autoBuildSubPartitionName(parentName, partName);
            }

            newSpec =
                generateNewPartitionSpec(curPartitionInfo,
                    sqlAlterTableSplitPartition.isSubPartitionsSplit(),
                    newSpec,
                    partName,
                    sqlPartition,
                    unVisiablePartitionGroups,
                    physicalTableAndGroupPairs,
                    partRexInfoCtx,
                    physicalPartIndex,
                    executionContext);
//            PartitionSpec newSpec = PartitionInfoBuilder
//                .buildPartitionSpecByPartSpecAst(
//                    executionContext,
//                    partColMetaList,
//                    partIntFunc,
//                    comparator,
//                    sqlAlterTableGroupSplitPartition.getParent().getPartRexInfoCtx(),
//                    null,
//                    sqlPartition,
//                    curPartitionInfo.getPartitionBy().getStrategy(),
//                    newPartitions.size(),
//                    newPrefixColCnt);

            if (changeTemplateSubPart) {
                newSpec.setTemplateName(newSpec.getName());
                newSpec.setName(PartitionNameUtil.autoBuildSubPartitionName(parentName, newSpec.getName()));
            }
            newPartitions.add(newSpec);
            i++;
        }
    }

    private static PartitionInfo getNewPartitionInfoForSplitType(
        ExecutionContext executionContext,
        PartitionInfo curPartitionInfo,
        List<PartitionGroupRecord> invisiblePartitionGroups,
        SqlAlterTableSplitPartition sqlAlterTableSplitPartition,
        String tableGroupName,
        String mayTempPartitionName,
        Map<String, Pair<String, String>> physicalTableAndGroupPairs,
        Map<SqlNode, RexNode> partRexInfoCtx) {

        final TableGroupInfoManager tableGroupInfoManager =
            OptimizerContext.getContext(curPartitionInfo.getTableSchema()).getTableGroupInfoManager();

        TableGroupConfig tableGroupConfig = tableGroupInfoManager.getTableGroupConfigByName(tableGroupName);

        PartitionGroupRecord splitPartRecord = tableGroupConfig.getPartitionGroupRecords().stream()
            .filter(o -> o.partition_name.equalsIgnoreCase(mayTempPartitionName)).findFirst().orElse(null);

        if (curPartitionInfo.getSpTemplateFlag() == TablePartitionRecord.SUBPARTITION_TEMPLATE_USING
            && sqlAlterTableSplitPartition.isSubPartitionsSplit()) {
            for (PartitionSpec partitionSpec : curPartitionInfo.getPartitionBy().getPhysicalPartitions()) {
                if (partitionSpec.getTemplateName().equalsIgnoreCase(mayTempPartitionName)) {
                    splitPartRecord = tableGroupConfig.getPartitionGroupRecords().stream()
                        .filter(o -> o.partition_name.equalsIgnoreCase(partitionSpec.getName())).findFirst()
                        .orElse(null);
                    //select one partition to check is enough
                    break;
                }
            }
        }
        String splitPartitionName = splitPartRecord == null ? mayTempPartitionName : splitPartRecord.partition_name;

        PartitionSpec splitPartitionSpec = null;
        boolean isSplitLogicaPart = false;
        if (splitPartRecord == null) {
            for (PartitionSpec partitionSpec : curPartitionInfo.getPartitionBy().getPartitions()) {
                if (!partitionSpec.isLogical()) {
                    break;
                } else if (partitionSpec.getName().equalsIgnoreCase(splitPartitionName)) {
                    isSplitLogicaPart = true;
                    splitPartitionSpec = partitionSpec;
                    break;
                }
            }
            if (!isSplitLogicaPart) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_NAME_NOT_EXISTS,
                    "the partition:" + splitPartitionName + " is not exists");
            }
        } else {
            List<PartitionSpec> partitionSpecs = curPartitionInfo.getPartitionBy().getOrderedPartitionSpecs();

            for (PartitionSpec spec : partitionSpecs) {
                if (spec.isLogical()) {
                    for (PartitionSpec subSpec : spec.getSubPartitions()) {
                        if (subSpec.getLocation().getPartitionGroupId().longValue() == splitPartRecord.id.longValue()) {
                            splitPartitionSpec = subSpec;
                            break;
                        }
                    }
                    if (splitPartitionSpec != null) {
                        break;
                    }
                } else {
                    if (spec.getLocation().getPartitionGroupId().longValue() == splitPartRecord.id.longValue()) {
                        splitPartitionSpec = spec;
                        break;
                    }
                }
            }
        }

        /**
         * Make sure that the datatype of atVal is valid
         */
        //Long atVal = null;

        PartitionField atVal = null;

        if (sqlAlterTableSplitPartition.getAtValue() != null) {
            RelDataType type;
            if (sqlAlterTableSplitPartition.isSubPartitionsSplit()) {
                type = curPartitionInfo.getPartitionBy().getSubPartitionBy().getPartitionExprTypeList().get(0);
            } else {
                type = curPartitionInfo.getPartitionBy().getPartitionExprTypeList().get(0);
            }
            DataType atValDataType = DataTypeUtil.calciteToDrdsType(type);
            atVal = PartitionFieldBuilder.createField(atValDataType);
            SqlLiteral constLiteral = ((SqlLiteral) sqlAlterTableSplitPartition.getAtValue());
            String constStr = constLiteral.getValueAs(String.class);
            atVal.store(constStr, new CharType());
            //atVal = ((SqlLiteral) sqlAlterTableGroupSplitPartition.getAtValue()).getValueAs(Long.class);
        }

        assert GeneralUtil.isNotEmpty(physicalTableAndGroupPairs) && physicalTableAndGroupPairs.size() > 1;
        assert GeneralUtil.isNotEmpty(invisiblePartitionGroups);
        List<PartitionSpec> oldPartitions = curPartitionInfo.getPartitionBy().getPartitions();
        List<PartitionSpec> newPartitions = new ArrayList<>();
        AtomicLong physicalPartIndex = new AtomicLong(0);
        if (!sqlAlterTableSplitPartition.isSubPartitionsSplit()) {
            newPartitions =
                generateNewPartitionSpecForSplit(executionContext,
                    curPartitionInfo,
                    invisiblePartitionGroups,
                    physicalTableAndGroupPairs,
                    partRexInfoCtx,
                    null,
                    oldPartitions,
                    splitPartitionSpec,
                    sqlAlterTableSplitPartition,
                    atVal,
                    physicalPartIndex);
        } else {
            for (PartitionSpec spec : oldPartitions) {
                PartitionSpec curPartSpec = spec.copy();
                List<PartitionSpec> subNewParts =
                    generateNewPartitionSpecForSplit(executionContext,
                        curPartitionInfo,
                        invisiblePartitionGroups,
                        physicalTableAndGroupPairs,
                        partRexInfoCtx,
                        curPartSpec,
                        curPartSpec.getSubPartitions(),
                        splitPartitionSpec,
                        sqlAlterTableSplitPartition,
                        atVal,
                        physicalPartIndex);
                curPartSpec.setSubPartitions(subNewParts);
                newPartitions.add(curPartSpec);
            }
        }

        PartitionInfo newPartitionInfo = curPartitionInfo.copy();
        newPartitionInfo.getPartitionBy().setPartitions(newPartitions);
        updateSubPartitionTemplate(newPartitionInfo);
        updatePartitionSpecRelationship(newPartitionInfo);
        newPartitionInfo.getPartitionBy().getPhysicalPartitions().clear();
        return newPartitionInfo;
    }

    private static List<PartitionSpec> generateNewPartitionSpecForSplit(ExecutionContext executionContext,
                                                                        PartitionInfo curPartitionInfo,
                                                                        List<PartitionGroupRecord> unVisiablePartitionGroups,
                                                                        Map<String, Pair<String, String>> physicalTableAndGroupPairs,
                                                                        Map<SqlNode, RexNode> partRexInfoCtx,
                                                                        PartitionSpec parentPartition,
                                                                        List<PartitionSpec> oldPartitionSpecs,
                                                                        PartitionSpec splitPartitionSpec,
                                                                        SqlAlterTableSplitPartition sqlAlterTableSplitPartition,
                                                                        PartitionField atVal,
                                                                        AtomicLong physicalPartIndex) {
        List<PartitionSpec> newPartitions = new ArrayList<>();
        boolean isHandleLogicalPart = oldPartitionSpecs.get(0).isLogical();
        boolean isSplitSubPartition = sqlAlterTableSplitPartition.isSubPartitionsSplit();

        PartitionStrategy strategy = oldPartitionSpecs.get(0).getStrategy();
        List<List<String>> allLevelActualPartCols = curPartitionInfo.getPartitionBy().getAllLevelActualPartCols();
        int index = 1;
        if (isHandleLogicalPart || !isSplitSubPartition) {
            index = 0;
        }
        //int index = isHandleLogicalPart  ? 0 : Math.min(allLevelActualPartCols.size() - 1, 1);

        int actualPartColCnt = allLevelActualPartCols.get(index).size();
        int partColCnt = curPartitionInfo.getPartitionBy().getAllLevelFullPartColumnCounts().get(index);
        PartitionByDefinition subPartitionBy = curPartitionInfo.getPartitionBy().getSubPartitionBy();
        boolean isUseSubPartitionTemplate = subPartitionBy != null && subPartitionBy.isUseSubPartTemplate();

        for (PartitionSpec partSpec : oldPartitionSpecs) {
            if (partSpec.getId().equals(splitPartitionSpec.getId()) || (!partSpec.isLogical()
                && isUseSubPartitionTemplate
                && partSpec.getTemplateName()
                .equalsIgnoreCase(splitPartitionSpec.getTemplateName()))) {
                if (strategy == PartitionStrategy.HASH || strategy == PartitionStrategy.KEY) {
                    generateNewPartitionsForSplitHashType(curPartitionInfo,
                        partColCnt,
                        actualPartColCnt,
                        unVisiablePartitionGroups,
                        sqlAlterTableSplitPartition,
                        parentPartition,
                        oldPartitionSpecs,
                        newPartitions,
                        physicalTableAndGroupPairs,
                        partRexInfoCtx,
                        partSpec,
                        executionContext);
                } else if (atVal != null) {
                    assert strategy == PartitionStrategy.RANGE;
                    generateNewPartitionsForSplitRangeWithAtVal(curPartitionInfo,
                        unVisiablePartitionGroups,
                        sqlAlterTableSplitPartition,
                        parentPartition,
                        oldPartitionSpecs,
                        newPartitions,
                        physicalTableAndGroupPairs,
                        partRexInfoCtx,
                        partSpec,
                        atVal,
                        executionContext);
                } else {
                    generateNewPartitionsForSplitWithPartitionSpec(executionContext,
                        curPartitionInfo,
                        unVisiablePartitionGroups,
                        sqlAlterTableSplitPartition,
                        oldPartitionSpecs,
                        newPartitions,
                        physicalTableAndGroupPairs,
                        partSpec,
                        partRexInfoCtx,
                        physicalPartIndex);
                }
            } else {
                newPartitions.add(partSpec.copy());
            }
        }
        return newPartitions;
    }

    private static PartitionInfo getNewPartitionInfoForExtractType(PartitionInfo curPartitionInfo,
                                                                   AlterTableGroupExtractPartitionPreparedData parentPrepareData,
                                                                   Map<String, Pair<String, String>> physicalTableAndGroupPairs,
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
        newPartitionInfo.getPartitionBy().getPhysicalPartitions().clear();
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
                    location.setGroupKey(physicalTableAndGroupPairs.get(newPartitionSpec.getName()).getValue());
                    location.setPhyTableName(physicalTableAndGroupPairs.get(newPartitionSpec.getName()).getKey());
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
                    location.setGroupKey(physicalTableAndGroupPairs.get(newPartitionSpec.getName()).getValue());
                    location.setPhyTableName(physicalTableAndGroupPairs.get(newPartitionSpec.getName()).getKey());
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
            location.setGroupKey(physicalTableAndGroupPairs.get(newPartitionSpec.getName()).getValue());
            location.setPhyTableName(physicalTableAndGroupPairs.get(newPartitionSpec.getName()).getKey());
            location.setVisiable(false);
            newPartitions.add(newPartitionSpec);
            lastOldPartitionSpec = null;
        }
        newPartitionInfo.getPartitionBy().setPartitions(newPartitions);
        updatePartitionSpecRelationship(newPartitionInfo);
        return newPartitionInfo;
    }

    private static PartitionInfo getNewPartitionInfoForMergeType(
        AlterTableGroupMergePartitionPreparedData preparedData,
        PartitionInfo curPartitionInfo,
        List<PartitionGroupRecord> unVisiablePartitionGroups,
        String tableGroupName,
        Map<String, Pair<String, String>> physicalTableAndGroupPairs,
        Set<String> mergePartitionsName,
        List<String> newPartitionNames,
        ExecutionContext context) {
        final TableGroupInfoManager tableGroupInfoManager =
            OptimizerContext.getContext(curPartitionInfo.getTableSchema()).getTableGroupInfoManager();

        TableGroupConfig tableGroupConfig = tableGroupInfoManager.getTableGroupConfigByName(tableGroupName);

        List<PartitionSpec> oldPartitions = curPartitionInfo.getPartitionBy().getPartitions();
        List<PartitionSpec> newPartitions = new ArrayList<>();
        List<SearchDatumInfo> listDatums = new ArrayList<>();
        List<SearchDatumInfo> subPartListDatums = new ArrayList<>();

        PartitionInfo newPartitionInfo;
        if (preparedData.isMergeSubPartition() && preparedData.isUseTemplatePart()) {
            //merge template subpartitions
            List<Pair<PartitionSpec, List<PartitionSpec>>> mergePartitions = new ArrayList<>();
            boolean boundValIsConfirmed = false;

            for (PartitionSpec partitionSpec : oldPartitions) {
                List<PartitionSpec> mergeSubPartitions = new ArrayList<>();
                for (PartitionSpec subPartitionSpec : partitionSpec.getSubPartitions()) {
                    if (mergePartitionsName.contains(subPartitionSpec.getTemplateName())) {
                        mergeSubPartitions.add(subPartitionSpec);
                        if (!boundValIsConfirmed) {
                            if (subPartitionSpec.getStrategy().isList()) {
                                if (subPartitionSpec.isDefaultPartition()) {
                                    subPartListDatums.clear();
                                    subPartListDatums.addAll(subPartitionSpec.getBoundSpec().getMultiDatums());
                                    break;
                                } else {
                                    subPartListDatums.addAll(subPartitionSpec.getBoundSpec().getMultiDatums());
                                }
                            }
                        }
                    }
                }
                boundValIsConfirmed = true;
                mergePartitions.add(Pair.of(partitionSpec, mergeSubPartitions));
            }
            for (Pair<PartitionSpec, List<PartitionSpec>> pair : mergePartitions) {
                PartitionSpec parentPartition = pair.getKey().copy();
                PartitionSpec reservedSubPartition = pair.getValue().get(pair.getValue().size() - 1);
                List<PartitionSpec> subPartitions = new ArrayList<>();
                for (PartitionSpec subPartition : parentPartition.getSubPartitions()) {
                    if (mergePartitionsName.contains(subPartition.getTemplateName())) {
                        if (subPartition.getLocation().getPartitionGroupId().longValue()
                            == reservedSubPartition.getLocation().getPartitionGroupId().longValue()) {
                            subPartitions.add(subPartition);
                            if (subPartition.getStrategy().isList()) {
                                List<SearchDatumInfo> distinctRecords =
                                    PartitionByDefinition.getDistinctSearchDatumInfos(subPartition,
                                        subPartition.getBoundSpaceComparator(),
                                        subPartListDatums);
                                subPartition.getBoundSpec().setMultiDatums(distinctRecords);
                            }
                            subPartition.setTemplateName(newPartitionNames.get(0));
                            String newPartName = parentPartition.getName() + newPartitionNames.get(0);
                            subPartition.setName(newPartName);
                            PartitionGroupRecord unVisiablePartitionGroup =
                                unVisiablePartitionGroups.stream()
                                    .filter(c -> c.partition_name.equalsIgnoreCase(newPartName))
                                    .findFirst().orElse(null);
                            assert unVisiablePartitionGroup != null;
                            subPartition.getLocation().setPartitionGroupId(unVisiablePartitionGroup.id);
                            subPartition.getLocation()
                                .setPhyTableName(physicalTableAndGroupPairs.get(newPartName).getKey());
                            subPartition.getLocation()
                                .setGroupKey(physicalTableAndGroupPairs.get(newPartName).getValue());
                            subPartition.getLocation().setVisiable(false);
                        }
                    } else {
                        subPartitions.add(subPartition);
                    }
                }
                parentPartition.setSubPartitions(subPartitions);
                newPartitions.add(parentPartition);
            }
        } else if (preparedData.isMergeSubPartition() || !preparedData.isHasSubPartition()) {
            //merge physical partitions
            List<PartitionGroupRecord> mergePartRecords = tableGroupConfig.getPartitionGroupRecords().stream()
                .filter(o -> mergePartitionsName.contains(o.partition_name.toLowerCase())).collect(Collectors.toList());
            Set<Long> toBeMergedPartGroupIds = mergePartRecords.stream().map(o -> o.id).collect(Collectors.toSet());
            List<PartitionSpec> mergePartSpecs = curPartitionInfo.getPartitionBy().getPhysicalPartitions().stream()
                .filter(o -> toBeMergedPartGroupIds.contains(o.getLocation().getPartitionGroupId())).collect(
                    Collectors.toList());
            mergePartSpecs.sort(Comparator.comparing(PartitionSpec::getPosition));
            PartitionSpec reservedSpec = mergePartSpecs.get(mergePartSpecs.size() - 1);

            //todo why empty
            String newPartName = GeneralUtil.isEmpty(newPartitionNames) ?
                "p_" + RandomStringUtils.randomAlphanumeric(RANDOM_SUFFIX_LENGTH_OF_PHYSICAL_PARTITION_NAME) :
                newPartitionNames.get(0);

            for (PartitionSpec spec : oldPartitions) {
                if (preparedData.isHasSubPartition()) {
                    List<PartitionSpec> newSubPartitions = new ArrayList<>();
                    for (PartitionSpec subPartitionSpec : spec.getSubPartitions()) {
                        constructNewPartitionSpec(unVisiablePartitionGroups, physicalTableAndGroupPairs,
                            subPartListDatums,
                            toBeMergedPartGroupIds, newSubPartitions, newPartName, subPartitionSpec, reservedSpec);
                    }
                    PartitionSpec logicalPartitionSpec = spec.copy();
                    logicalPartitionSpec.setSubPartitions(newSubPartitions);
                    newPartitions.add(logicalPartitionSpec);
                } else {
                    constructNewPartitionSpec(unVisiablePartitionGroups, physicalTableAndGroupPairs, listDatums,
                        toBeMergedPartGroupIds, newPartitions, newPartName, spec, reservedSpec);
                }
            }

        } else {
            //merge logical partitions
            List<PartitionSpec> mergePartSpecs = curPartitionInfo.getPartitionBy().getPartitions().stream()
                .filter(o -> mergePartitionsName.contains(o.getName())).collect(
                    Collectors.toList());
            mergePartSpecs.sort(Comparator.comparing(PartitionSpec::getPosition));
            PartitionSpec reservedSpec = mergePartSpecs.get(mergePartSpecs.size() - 1);
            PartitionStrategy strategy = curPartitionInfo.getPartitionBy().getStrategy();
            PartitionStrategy subPartStrategy = curPartitionInfo.getPartitionBy().getSubPartitionBy().getStrategy();

            for (PartitionSpec partitionSpec : oldPartitions) {
                if (mergePartitionsName.contains(partitionSpec.getName())) {
                    boolean isReservedSpec = partitionSpec.getName().equalsIgnoreCase(reservedSpec.getName());
                    if (subPartStrategy.isList() && !preparedData.isUseTemplatePart()) {
                        for (PartitionSpec subPartSpec : partitionSpec.getSubPartitions()) {
                            subPartListDatums.addAll(subPartSpec.getBoundSpec().getMultiDatums());
                        }
                    } else if (!subPartStrategy.isList() && !preparedData.isUseTemplatePart()) {
                        for (PartitionSpec subPartSpec : partitionSpec.getSubPartitions()) {

                            SearchDatumInfo searchDatumInfo = subPartSpec.getBoundSpec().getSingleDatum();
                            if (GeneralUtil.isEmpty(subPartListDatums)) {
                                subPartListDatums.add(searchDatumInfo);
                            } else {
                                if (subPartSpec.getBoundSpaceComparator()
                                    .compare(searchDatumInfo, subPartListDatums.get(0)) == 1) {
                                    subPartListDatums.clear();
                                    subPartListDatums.add(searchDatumInfo);
                                }
                            }
                        }
                    }
                    if (isReservedSpec) {
                        PartitionSpec newPartitionSpec = partitionSpec.copy();
                        newPartitionSpec.setName(newPartitionNames.get(0));
                        if (strategy.isList()) {
                            //default + normal = default, so no need to add listDatums for default partition
                            boolean containDefaultValue =
                                listDatums.stream().anyMatch(SearchDatumInfo::containDefaultValue);
                            if (containDefaultValue || newPartitionSpec.isDefaultPartition()) {
                                newPartitionSpec.setDefaultPartition(true);
                                newPartitionSpec.getBoundSpec().getMultiDatums().clear();
                                newPartitionSpec.getBoundSpec().getMultiDatums()
                                    .add(new SearchDatumInfo(PartitionBoundVal.createDefaultValue()));
                            } else {
                                newPartitionSpec.getBoundSpec().getMultiDatums().addAll(listDatums);
                            }
                        }
                        if (preparedData.isUseTemplatePart()) {
                            for (PartitionSpec subPartitionSpec : newPartitionSpec.getSubPartitions()) {
                                PartitionLocation location = subPartitionSpec.getLocation();
                                subPartitionSpec.setName(newPartitionNames.get(0) + subPartitionSpec.getTemplateName());
                                PartitionGroupRecord unVisiablePartitionGroup =
                                    unVisiablePartitionGroups.stream()
                                        .filter(c -> c.partition_name.equalsIgnoreCase(subPartitionSpec.getName()))
                                        .findFirst().orElse(null);
                                assert unVisiablePartitionGroup != null;
                                location.setPartitionGroupId(unVisiablePartitionGroup.id);
                                location.setPhyTableName(
                                    physicalTableAndGroupPairs.get(subPartitionSpec.getName()).getKey());
                                location.setGroupKey(
                                    physicalTableAndGroupPairs.get(subPartitionSpec.getName()).getValue());
                                location.setVisiable(false);
                            }
                        } else {
                            PartitionSpec subPartitionSpec =
                                newPartitionSpec.getSubPartitions().get(newPartitionSpec.getSubPartitions().size() - 1);
                            if (subPartStrategy.isList()) {

                                if (subPartListDatums.stream().anyMatch(o -> o.containDefaultValue())) {
                                    ((MultiValuePartitionBoundSpec) subPartitionSpec.getBoundSpec()).setDefault(true);
                                } else {
                                    List<SearchDatumInfo> distinctRecords =
                                        PartitionByDefinition.getDistinctSearchDatumInfos(subPartitionSpec,
                                            subPartitionSpec.getBoundSpaceComparator(),
                                            subPartListDatums);
                                    subPartitionSpec.getBoundSpec().setMultiDatums(distinctRecords);
                                }

                            } else if (GeneralUtil.isNotEmpty(subPartListDatums)) {
                                subPartitionSpec.getBoundSpec().setSingleDatum(subPartListDatums.get(0));
                            }
                            subPartitionSpec.setName(preparedData.getNewPhysicalPartName());
                            PartitionLocation location = subPartitionSpec.getLocation();
                            PartitionGroupRecord unVisiablePartitionGroup =
                                unVisiablePartitionGroups.stream()
                                    .filter(c -> c.partition_name.equalsIgnoreCase(subPartitionSpec.getName()))
                                    .findFirst().orElse(null);
                            assert unVisiablePartitionGroup != null;
                            location.setPartitionGroupId(unVisiablePartitionGroup.id);
                            location.setPhyTableName(
                                physicalTableAndGroupPairs.get(subPartitionSpec.getName()).getKey());
                            location.setGroupKey(
                                physicalTableAndGroupPairs.get(subPartitionSpec.getName()).getValue());
                            location.setVisiable(false);
                            List<PartitionSpec> subPartSpecs = new ArrayList<>();
                            subPartSpecs.add(subPartitionSpec);
                            newPartitionSpec.setSubPartitions(subPartSpecs);
                        }
                        newPartitions.add(newPartitionSpec);
                    } else {
                        if (strategy.isList()) {
                            listDatums.addAll(partitionSpec.getBoundSpec().getMultiDatums());
                        }
                    }
                } else {
                    newPartitions.add(partitionSpec.copy());
                }
            }
        }
        newPartitionInfo = curPartitionInfo.copy();
        newPartitionInfo.getPartitionBy().getPhysicalPartitions().clear();
        newPartitionInfo.getPartitionBy().setPartitions(newPartitions);
        updatePartitionSpecRelationship(newPartitionInfo);
        return newPartitionInfo;
    }

    private static PartitionInfo getNewPartitionInfoForReorgType(
        PartitionInfo curPartitionInfo,
        Map<SqlNode, RexNode> partRexInfoCtx,
        List<PartitionGroupRecord> invisiblePartitionGroups,
        Map<String, Pair<String, String>> physicalTableAndGroupPairs,
        List<String> oldPartNames,
        List<SqlPartition> newPartDefs,
        boolean isReorgSubPartition,
        boolean isAlterTableGroup,
        ExecutionContext executionContext) {

        PartitionByDefinition partByDef = curPartitionInfo.getPartitionBy();
        PartitionByDefinition subPartByDef = partByDef.getSubPartitionBy();

        List<PartitionSpec> curPartSpecs = partByDef.getPartitions();
        List<PartitionSpec> newPartSpecs = new ArrayList<>();

        AtomicLong physicalPartIndex = new AtomicLong(0);

        assert GeneralUtil.isNotEmpty(physicalTableAndGroupPairs) && physicalTableAndGroupPairs.size() > 1;
        assert GeneralUtil.isNotEmpty(invisiblePartitionGroups);

        if (isReorgSubPartition) {
            for (PartitionSpec curPartSpec : curPartSpecs) {
                PartitionSpec newPartSpec = curPartSpec.copy();
                List<PartitionSpec> newSubPartSpecs = new ArrayList<>();

                for (PartitionSpec subPartSpec : newPartSpec.getSubPartitions()) {
                    boolean hasSameSubPartName = oldPartNames.contains(subPartSpec.getName().toLowerCase());
                    boolean hasSameSubPartTemplateName = subPartByDef != null && subPartByDef.isUseSubPartTemplate() &&
                        oldPartNames.contains(subPartSpec.getTemplateName().toLowerCase());
                    if (hasSameSubPartName || hasSameSubPartTemplateName) {
                        genNewPartSpecsForReorg(curPartitionInfo,
                            partRexInfoCtx,
                            invisiblePartitionGroups,
                            physicalTableAndGroupPairs,
                            newPartDefs,
                            subPartSpec,
                            newSubPartSpecs,
                            physicalPartIndex,
                            isReorgSubPartition,
                            isAlterTableGroup,
                            executionContext
                        );
                    } else {
                        newSubPartSpecs.add(subPartSpec);
                    }
                }

                newPartSpec.setSubPartitions(newSubPartSpecs);
                newPartSpecs.add(newPartSpec);
            }
        } else {
            for (PartitionSpec curPartSpec : curPartSpecs) {
                PartitionSpec newPartSpec = curPartSpec.copy();
                if (oldPartNames.contains(newPartSpec.getName().toLowerCase())) {
                    genNewPartSpecsForReorg(curPartitionInfo,
                        partRexInfoCtx,
                        invisiblePartitionGroups,
                        physicalTableAndGroupPairs,
                        newPartDefs,
                        newPartSpec,
                        newPartSpecs,
                        physicalPartIndex,
                        isReorgSubPartition,
                        isAlterTableGroup,
                        executionContext
                    );
                } else {
                    newPartSpecs.add(newPartSpec);
                }
            }
        }

        PartitionInfo newPartitionInfo = curPartitionInfo.copy();

        newPartitionInfo.getPartitionBy().getPhysicalPartitions().clear();
        newPartitionInfo.getPartitionBy().setPartitions(newPartSpecs);

        updateSubPartitionTemplate(newPartitionInfo);
        updatePartitionSpecRelationship(newPartitionInfo);

        return newPartitionInfo;
    }

    private static void genNewPartSpecsForReorg(PartitionInfo curPartitionInfo,
                                                Map<SqlNode, RexNode> partRexInfoCtx,
                                                List<PartitionGroupRecord> invisiblePartitionGroups,
                                                Map<String, Pair<String, String>> physicalTableAndGroupPairs,
                                                List<SqlPartition> newPartDefs,
                                                PartitionSpec curPartSpec,
                                                List<PartitionSpec> newPartSpecs,
                                                AtomicLong physicalPartIndex,
                                                boolean isReorgSubPartition,
                                                boolean isAlterTableGroup,
                                                ExecutionContext executionContext) {
        PartitionByDefinition partByDef = curPartitionInfo.getPartitionBy();
        PartitionByDefinition subPartByDef = partByDef.getSubPartitionBy();

        PartitionStrategy strategy;
        List<ColumnMeta> partColMetaList;
        SearchDatumComparator comparator;
        PartitionIntFunction partIntFunc;

        if (isReorgSubPartition) {
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

//        int levelIndex = isReorgSubPartition ? 1 : 0;
//
//        List<Integer> allLevelFullPartColCnts = new ArrayList<>();
//        List<Integer> allLevelActPartColCnts = new ArrayList<>();
//        List<PartitionStrategy> allLevelPartStrategies = new ArrayList<>();
//
//        allLevelFullPartColCnts.add(curPartitionInfo.getAllLevelFullPartColCounts().get(levelIndex));
//        allLevelActPartColCnts.add(curPartitionInfo.getAllLevelActualPartColCounts().get(levelIndex));
//        allLevelPartStrategies.add(curPartitionInfo.getAllLevelPartitionStrategies().get(levelIndex));
//
//        GetNewActPartColCntFromAstParams params = new GetNewActPartColCntFromAstParams();
//        params.setAllLevelFullPartColCnts(allLevelFullPartColCnts);
//        params.setAllLevelActualPartColCnts(allLevelActPartColCnts);
//        params.setAllLevelStrategies(allLevelPartStrategies);
//        params.setNewPartitions(newPartDefs);
//
//        List<Integer> newActualPartColCnts = PartitionInfoUtil.getNewAllLevelPrefixPartColCntBySqlPartitionAst(params);

        List<Integer> newActualPartColCnts =
            PartitionInfoUtil.getNewAllLevelPrefixPartColCntByPartInfoAndNewParts(isAlterTableGroup, curPartitionInfo,
                newPartDefs, isReorgSubPartition);

        AtomicInteger phyPartCounter = new AtomicInteger(0);

        for (SqlPartition sqlPartition : newPartDefs) {
            BuildPartSpecFromAstParams partSpecAstParams = new BuildPartSpecFromAstParams();
            partSpecAstParams.setContext(executionContext);
            partSpecAstParams.setPartColMetaList(partColMetaList);
            partSpecAstParams.setPartKeyLevel(
                isReorgSubPartition ? PartKeyLevel.SUBPARTITION_KEY : PartKeyLevel.PARTITION_KEY);
            partSpecAstParams.setPartIntFunc(partIntFunc);
            partSpecAstParams.setPruningComparator(comparator);
            partSpecAstParams.setPartBoundExprInfo(partRexInfoCtx);
            partSpecAstParams.setPartBoundValBuilder(null);
            partSpecAstParams.setStrategy(strategy);
            partSpecAstParams.setPartPosition(newPartSpecs.size());
            partSpecAstParams.setAllLevelPrefixPartColCnts(newActualPartColCnts);
            partSpecAstParams.setPartNameAst(sqlPartition.getName());
            partSpecAstParams.setPartComment(sqlPartition.getComment());
            partSpecAstParams.setPartLocality(sqlPartition.getLocality());
            partSpecAstParams.setPartBndValuesAst(sqlPartition.getValues());

            partSpecAstParams.setLogical(false);

            boolean changeSubPartTemplate = false;
            String parentPartName = "";
            if (isReorgSubPartition) {
                if (subPartByDef.isUseSubPartTemplate()) {
                    changeSubPartTemplate = true;
                    Optional<PartitionSpec> parentPartSpec = partByDef.getPartitions().stream()
                        .filter(o -> o.getId().intValue() == curPartSpec.getParentId().longValue()).findFirst();
                    parentPartName = parentPartSpec.get().getName();
                }
            } else {
                if (curPartSpec.isLogical()) {
                    partSpecAstParams.setLogical(true);
                }
            }

            partSpecAstParams.setSpecTemplate(false);
            partSpecAstParams.setSubPartSpec(false);
            partSpecAstParams.setParentPartSpec(null);
            partSpecAstParams.setPhySpecCounter(phyPartCounter);

            final String partName = sqlPartition.getName().toString();

            Optional<PartitionSpec> alreadyAddedName =
                newPartSpecs.stream().filter(np -> np.getName().equalsIgnoreCase(partName)).findFirst();
            Optional<PartitionSpec> alreadyAddedTemplate =
                newPartSpecs.stream().filter(np -> np.getTemplateName().equalsIgnoreCase(partName)).findFirst();

            if (alreadyAddedName.isPresent() || alreadyAddedTemplate.isPresent()) {
                // The partition is already added previously.
                continue;
            }

            PartitionSpec newPartSpec = PartitionInfoBuilder.buildPartSpecByAstParams(partSpecAstParams);

            String actualPartName = partName;

            if (changeSubPartTemplate) {
                actualPartName = PartitionNameUtil.autoBuildSubPartitionName(parentPartName, actualPartName);
            }

            newPartSpec = generateNewPartitionSpec(
                curPartitionInfo,
                isReorgSubPartition,
                newPartSpec,
                actualPartName,
                sqlPartition,
                invisiblePartitionGroups,
                physicalTableAndGroupPairs,
                partRexInfoCtx,
                physicalPartIndex,
                executionContext
            );

            if (changeSubPartTemplate) {
                newPartSpec.setTemplateName(newPartSpec.getName());
                newPartSpec.setName(PartitionNameUtil.autoBuildSubPartitionName(parentPartName, newPartSpec.getName()));
            }

            newPartSpecs.add(newPartSpec);
        }
    }

    private static void constructNewPartitionSpec(List<PartitionGroupRecord> unVisiablePartitionGroups,
                                                  Map<String, Pair<String, String>> physicalTableAndGroupPairs,
                                                  List<SearchDatumInfo> listDatums,
                                                  Set<Long> toBeMergedPartGroupIds,
                                                  List<PartitionSpec> newPartitions,
                                                  String newPartName,
                                                  PartitionSpec spec,
                                                  PartitionSpec reservedSpec) {
        if (toBeMergedPartGroupIds.contains(spec.getLocation().getPartitionGroupId())) {
            if (spec.getStrategy().isList()) {
                if (spec.getBoundSpec().isDefaultPartSpec()) {
                    listDatums.clear();
                    listDatums.addAll(spec.getBoundSpec().getMultiDatums());
                } else {
                    boolean containDefaultValue = listDatums.stream().anyMatch(SearchDatumInfo::containDefaultValue);
                    if (!containDefaultValue) {
                        listDatums.addAll(spec.getBoundSpec().getMultiDatums());
                    }
                }
            } else {
                SearchDatumInfo searchDatumInfo = spec.getBoundSpec().getSingleDatum();
                if (GeneralUtil.isEmpty(listDatums)) {
                    listDatums.add(searchDatumInfo);
                } else if (spec.getBoundSpaceComparator()
                    .compare(searchDatumInfo, listDatums.stream().iterator().next()) == 1) {
                    listDatums.clear();
                    listDatums.add(searchDatumInfo);
                }
            }
            if (spec.getLocation().getPartitionGroupId().longValue() == reservedSpec.getLocation()
                .getPartitionGroupId().longValue()) {
                PartitionSpec newSpec = reservedSpec.copy();
                PartitionGroupRecord unVisiablePartitionGroup1 =
                    unVisiablePartitionGroups.stream().filter(c -> c.partition_name.equalsIgnoreCase(newPartName))
                        .findFirst().orElse(null);
                assert unVisiablePartitionGroup1 != null;
                newSpec.getLocation().setPartitionGroupId(unVisiablePartitionGroup1.id);
                newSpec.getLocation().setPhyTableName(physicalTableAndGroupPairs.get(newPartName).getKey());
                newSpec.getLocation().setGroupKey(physicalTableAndGroupPairs.get(newPartName).getValue());
                newSpec.getLocation().setVisiable(false);
                newSpec.setName(newPartName);
                if (spec.getStrategy().isList()) {
                    List<SearchDatumInfo> distinctResults =
                        PartitionByDefinition.getDistinctSearchDatumInfos(newSpec, newSpec.getBoundSpaceComparator(),
                            listDatums);
                    newSpec.getBoundSpec().setMultiDatums(distinctResults);
                } else {
                    newSpec.getBoundSpec().setSingleDatum(listDatums.get(0));
                }
                newPartitions.add(newSpec);
            }
        } else {
            newPartitions.add(spec.copy());
        }
    }

    private static PartitionInfo getNewPartitionInfoForMoveType(PartitionInfo curPartitionInfo,
                                                                Map<String, Set<String>> targetPartitions,
                                                                String tableGroupName,
                                                                Map<String, Pair<String, String>> physicalTableAndGroupPairs,
                                                                ExecutionContext context) {
        final TableGroupInfoManager tableGroupInfoManager =
            OptimizerContext.getContext(curPartitionInfo.getTableSchema()).getTableGroupInfoManager();

        Set<String> movePartitionsName = new TreeSet<>(String::compareToIgnoreCase);
        targetPartitions.entrySet().stream().forEach(o -> movePartitionsName.addAll(o.getValue()));

        TableGroupConfig tableGroupConfig = tableGroupInfoManager.getTableGroupConfigByName(tableGroupName);
        List<PartitionGroupRecord> movePartRecords = tableGroupConfig.getPartitionGroupRecords().stream()
            .filter(o -> movePartitionsName.contains(o.partition_name)).collect(Collectors.toList());
        Set<Long> movePartGroupIds = movePartRecords.stream().map(o -> o.id).collect(Collectors.toSet());

        List<PartitionSpec> oldPartitions = curPartitionInfo.getPartitionBy().getPartitions();
        List<PartitionSpec> newPartitions = new ArrayList<>();

        Map<String, String> physicalTableMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

        for (PartitionSpec spec : oldPartitions) {
            if (curPartitionInfo.getPartitionBy().getSubPartitionBy() != null &&
                GeneralUtil.isNotEmpty(spec.getSubPartitions())) {
                List<PartitionSpec> newSubPartitions = new ArrayList<>();
                for (PartitionSpec subSpec : spec.getSubPartitions()) {
                    if (movePartGroupIds.contains(subSpec.getLocation().getPartitionGroupId())) {
                        PartitionSpec newSubSpec = subSpec.copy();
                        Pair<String, String> phyTableAndGroup = physicalTableAndGroupPairs.get(newSubSpec.getName());
                        newSubSpec.getLocation().setPhyTableName(phyTableAndGroup.getKey());
                        newSubSpec.getLocation().setGroupKey(phyTableAndGroup.getValue());
                        newSubSpec.getLocation().setVisiable(false);
                        newSubPartitions.add(newSubSpec);
                    } else {
                        newSubPartitions.add(subSpec.copy());
                    }
                }
                PartitionSpec newSpec = spec.copy();
                newSpec.setSubPartitions(newSubPartitions);
                newPartitions.add(newSpec);
            } else {
                if (movePartGroupIds.contains(spec.getLocation().getPartitionGroupId())) {
                    PartitionSpec newSpec = spec.copy();
                    Pair<String, String> phyTableAndGroup = physicalTableAndGroupPairs.get(newSpec.getName());
                    newSpec.getLocation().setPhyTableName(phyTableAndGroup.getKey());
                    newSpec.getLocation().setGroupKey(phyTableAndGroup.getValue());
                    newSpec.getLocation().setVisiable(false);
                    newPartitions.add(newSpec.copy());
                } else {
                    newPartitions.add(spec.copy());
                }
            }
        }

        PartitionInfo newPartitionInfo = curPartitionInfo.copy();
        newPartitionInfo.getPartitionBy().getPhysicalPartitions().clear();
        newPartitionInfo.getPartitionBy().setPartitions(newPartitions);
        updatePartitionSpecRelationship(newPartitionInfo);
        return newPartitionInfo;
    }

    private static PartitionInfo getNewPartitionInfoForSetTableGroup(PartitionInfo curPartitionInfo,
                                                                     SqlAlterTableSetTableGroup sqlAlterTableSetTableGroup,
                                                                     ExecutionContext executionContext) {
        final TableGroupInfoManager tableGroupInfoManager =
            OptimizerContext.getContext(curPartitionInfo.getTableSchema()).getTableGroupInfoManager();

        String targetTableGroup = sqlAlterTableSetTableGroup.getTargetTableGroup();

        TableGroupConfig tableGroupConfig = tableGroupInfoManager.getTableGroupConfigByName(targetTableGroup);
        List<PartitionGroupRecord> partitionGroupRecords = tableGroupConfig.getPartitionGroupRecords();

        List<PartitionSpec> newPartitions = new ArrayList<>();

        if (curPartitionInfo.getPartitionBy().getSubPartitionBy() != null) {
            for (PartitionSpec partSpec : curPartitionInfo.getPartitionBy().getPartitions()) {
                PartitionSpec newPartSpec = partSpec.copy();
                List<PartitionSpec> newSubParts = new ArrayList<>();

                for (PartitionSpec subPartSpec : partSpec.getSubPartitions()) {
                    PartitionGroupRecord partitionGroupRecord = partitionGroupRecords.stream()
                        .filter(pg -> pg.partition_name.equalsIgnoreCase(subPartSpec.getName())).findFirst()
                        .orElse(null);
                    assert partitionGroupRecord != null;
                    PartitionSpec newSubPartSpec = buildNewPartitionSpec(subPartSpec, partitionGroupRecord);
                    newSubParts.add(newSubPartSpec);
                }

                newPartSpec.setSubPartitions(newSubParts);
                newPartitions.add(newPartSpec);
            }
        } else {
            for (PartitionSpec partSpec : curPartitionInfo.getPartitionBy().getPartitions()) {
                PartitionGroupRecord partitionGroupRecord =
                    partitionGroupRecords.stream().filter(o -> o.partition_name.equalsIgnoreCase(partSpec.getName()))
                        .findFirst().orElse(null);
                assert partitionGroupRecord != null;
                PartitionSpec newPartSpec = buildNewPartitionSpec(partSpec, partitionGroupRecord);
                newPartitions.add(newPartSpec);
            }
        }

        PartitionInfo newPartitionInfo = curPartitionInfo.copy();
        newPartitionInfo.getPartitionBy().getPhysicalPartitions().clear();
        newPartitionInfo.getPartitionBy().setPartitions(newPartitions);

        //change the tablegroup id to the target tablegroup
        newPartitionInfo.setTableGroupId(tableGroupConfig.getTableGroupRecord().getId());

        updatePartitionSpecRelationship(newPartitionInfo);

        return newPartitionInfo;
    }

    private static PartitionSpec buildNewPartitionSpec(PartitionSpec partitionSpec,
                                                       PartitionGroupRecord partitionGroupRecord) {
        PartitionSpec newPartSpec = partitionSpec.copy();
        newPartSpec.getLocation()
            .setGroupKey(GroupInfoUtil.buildGroupNameFromPhysicalDb(partitionGroupRecord.phy_db));
        newPartSpec.getLocation().setPartitionGroupId(partitionGroupRecord.id);
        return newPartSpec;
    }

    private static PartitionInfo getNewPartitionInfoForAddPartition(PartitionInfo curPartitionInfo,
                                                                    List<PartitionGroupRecord> unVisiablePartitionGroupRecords,
                                                                    boolean isAlterTableGroup,
                                                                    SqlAlterTableAddPartition addPartition,
                                                                    Map<String, Pair<String, String>> physicalTableAndGroupPairs,
                                                                    Map<Integer, Map<SqlNode, RexNode>> partBoundExprInfoByLevel,
                                                                    ExecutionContext executionContext) {
        PartitionInfo newPartitionInfo = PartitionInfoBuilder.buildNewPartitionInfoByAddingPartition(
            executionContext,
            curPartitionInfo,
            isAlterTableGroup,
            addPartition,
            partBoundExprInfoByLevel,
            unVisiablePartitionGroupRecords,
            physicalTableAndGroupPairs
        );

        updatePartitionSpecRelationship(newPartitionInfo);

        return newPartitionInfo;
    }

    private static PartitionInfo getNewPartitionInfoForDropPartition(PartitionInfo curPartitionInfo,
                                                                     SqlAlterTableDropPartition dropPartition,
                                                                     List<String> oldPartitionNames,
                                                                     List<String> newPartitionNames,
                                                                     List<PartitionGroupRecord> invisiblePartitionGroupRecords,
                                                                     Map<String, Pair<String, String>> physicalTableAndGroupPairs,
                                                                     ExecutionContext context) {
        PartitionInfo newPartitionInfo = PartitionInfoBuilder.buildNewPartitionInfoByDroppingPartition(
            curPartitionInfo,
            dropPartition,
            oldPartitionNames,
            newPartitionNames,
            invisiblePartitionGroupRecords,
            physicalTableAndGroupPairs,
            context
        );

        updatePartitionSpecRelationship(newPartitionInfo);

        return newPartitionInfo;
    }

    private static PartitionInfo getNewPartitionInfoForModifyPartition(PartitionInfo curPartitionInfo,
                                                                       SqlNode sqlNode,
                                                                       Map<SqlNode, RexNode> partBoundExprInfo,
                                                                       List<PartitionGroupRecord> unVisiablePartitionGroups,
                                                                       Map<String, Pair<String, String>> physicalTableAndGroupPairs,
                                                                       ExecutionContext executionContext) {

        SqlNode modifyPartitionValuesAst = null;

        SqlAlterTableModifyPartitionValues modifyPartitionValues = (SqlAlterTableModifyPartitionValues) sqlNode;
        modifyPartitionValuesAst = modifyPartitionValues;

        SqlPartition targetModifyPart = modifyPartitionValues.getPartition();
        boolean isModifySubPart = modifyPartitionValues.isSubPartition();
        boolean useSubPartTemp = false;
        boolean useSubPart = curPartitionInfo.getPartitionBy().getSubPartitionBy() != null;
        if (useSubPart) {
            useSubPartTemp = curPartitionInfo.getPartitionBy().getSubPartitionBy().isUseSubPartTemplate();
        }

        /**
         * Fetch the target phy part spec names from the oldPartitionName from ast of ddl
         */
        String oldPartitionName = null;
        if (!isModifySubPart) {
            oldPartitionName = ((SqlIdentifier) targetModifyPart.getName()).getLastName();
        } else {
            oldPartitionName = ((SqlIdentifier) ((SqlSubPartition) targetModifyPart.getSubPartitions()
                .get(0)).getName()).getLastName();
        }
        oldPartitionName = SQLUtils.normalizeNoTrim(oldPartitionName);
        final Set<String> targetPhySpecNames = new TreeSet<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
        if (isModifySubPart) {
            if (useSubPartTemp) {
                List<PartitionSpec> phySpecsOfSameSubPartTemp =
                    curPartitionInfo.getPartitionBy().getPhysicalPartitionsBySubPartTempName(oldPartitionName);
                for (int i = 0; i < phySpecsOfSameSubPartTemp.size(); i++) {
                    PartitionSpec phySpec = phySpecsOfSameSubPartTemp.get(i);
                    targetPhySpecNames.add(phySpec.getName());
                }
            } else {
                targetPhySpecNames.add(oldPartitionName);
            }
        } else {
            if (useSubPart) {
                PartitionSpec targetPart =
                    curPartitionInfo.getPartSpecSearcher().getPartSpecByPartName(oldPartitionName);
                List<PartitionSpec> phyPartsOfTargetPart = targetPart.getSubPartitions();
                for (int i = 0; i < phyPartsOfTargetPart.size(); i++) {
                    targetPhySpecNames.add(phyPartsOfTargetPart.get(i).getName());
                }
            } else {
                targetPhySpecNames.add(oldPartitionName);
            }
        }

        /**
         * According to the modified values to add/drop, build a new partInfo,
         * and output the new physical PartSpecs from new PartInfo
         * which will be used to build new partition group
         */
        PartitionSpec[] outputNewPartSpec = new PartitionSpec[targetPhySpecNames.size()];
        PartitionInfo newPartitionInfo = PartitionInfoBuilder
            .buildNewPartitionInfoByModifyingPartitionValues(curPartitionInfo, modifyPartitionValuesAst,
                partBoundExprInfo,
                executionContext,
                outputNewPartSpec);

        /**
         * Build a pgMap phyPartName->PgRec for unVisiablePartitionGroups
         */
        Map<String, PartitionGroupRecord> phyPartNameToPartGrpMap =
            new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
        for (int i = 0; i < unVisiablePartitionGroups.size(); i++) {
            PartitionGroupRecord partGrp = unVisiablePartitionGroups.get(i);
            phyPartNameToPartGrpMap.put(partGrp.getPartition_name(), partGrp);
        }

        /**
         * Assign actual location(phyTb, phyDb) for each phy partSpec by using its phyPartSpec Name
         * to finding its pgRec from pgMap
         */
        for (int i = 0; i < outputNewPartSpec.length; i++) {
            PartitionSpec outputPhyPart = outputNewPartSpec[i];
            String phyPartName = outputPhyPart.getName();
            PartitionGroupRecord newPartGrp = phyPartNameToPartGrpMap.get(phyPartName);
            String newPhyTb = physicalTableAndGroupPairs.get(phyPartName).getKey();
            String newGrpKey = physicalTableAndGroupPairs.get(phyPartName).getValue();
            Long pgId = newPartGrp.getId();

            PartitionLocation phyPartLocation = outputPhyPart.getLocation();
            phyPartLocation.setPartitionGroupId(pgId);
            phyPartLocation.setPhyTableName(newPhyTb);
            phyPartLocation.setGroupKey(newGrpKey);
            phyPartLocation.setVisiable(false);
        }

        updatePartitionSpecRelationship(newPartitionInfo);
        return newPartitionInfo;

    }

    private static PartitionInfo getNewPartitionInfoForCopyPartition(PartitionInfo curPartitionInfo,
                                                                     List<PartitionGroupRecord> unVisiablePartitionGroupRecords,
                                                                     ExecutionContext executionContext) {
        PartitionInfo newPartitionInfo = curPartitionInfo.copy();
        newPartitionInfo.getPartitionBy().getPhysicalPartitions().clear();
        PartitionSpec partitionSpec = curPartitionInfo.getPartitionBy().getPartitions().get(0).copy();
        for (PartitionGroupRecord record : unVisiablePartitionGroupRecords) {
            partitionSpec.setName(record.partition_name);
            partitionSpec.getLocation().setVisiable(false);
            partitionSpec.getLocation().setGroupKey(GroupInfoUtil.buildGroupNameFromPhysicalDb(record.phy_db));
            newPartitionInfo.getPartitionBy().getPartitions().add(partitionSpec);
            partitionSpec = partitionSpec.copy();
        }

        updatePartitionSpecRelationship(newPartitionInfo);
        return newPartitionInfo;

    }

    private static PartitionInfo getNewPartitionInfoForSplitPartitionByHotValue(PartitionInfo curPartitionInfo,
                                                                                AlterTableGroupSplitPartitionByHotValuePreparedData parentPrepareData,
                                                                                Map<String, Pair<String, String>> physicalTableAndGroupPairs,
                                                                                ExecutionContext executionContext) {
        PartitionInfo newPartitionInfo = curPartitionInfo.copy();
        newPartitionInfo.getPartitionBy().getPhysicalPartitions().clear();
        List<PartitionSpec> newPartitions = null;

        AtomicInteger newPartitionIndex = new AtomicInteger(0);
        if (parentPrepareData.isSplitSubPartition()) {
            newPartitions = new ArrayList<>();
            if (parentPrepareData.isUseTemplatePart()) {
                for (PartitionSpec parentPartitionSpec : curPartitionInfo.getPartitionBy().getPartitions()) {
                    PartitionSpec newParentPartitionSpec = parentPartitionSpec.copy();
                    newPartitions.add(newParentPartitionSpec);

                    List<PartitionSpec> subPartitions =
                        generateNewPartitionSpecForSplitPartitionByHotValue(curPartitionInfo.getTableName(),
                            parentPartitionSpec,
                            parentPartitionSpec.getSubPartitions(),
                            curPartitionInfo.getPartitionBy().getSubPartitionBy().getPartitionColumnNameList().size(),
                            parentPrepareData,
                            physicalTableAndGroupPairs, newPartitionIndex);

                    newParentPartitionSpec.setSubPartitions(subPartitions);
                }
            } else {
                assert StringUtils.isNotEmpty(parentPrepareData.getParentPartitionName());
                for (PartitionSpec parentPartitionSpec : curPartitionInfo.getPartitionBy().getPartitions()) {
                    if (parentPartitionSpec.getName().equalsIgnoreCase(parentPrepareData.getParentPartitionName())) {
                        PartitionSpec modifyParentPartitionSpec = curPartitionInfo.getPartitionBy()
                            .getPartitionByPartName(parentPrepareData.getParentPartitionName());
                        List<PartitionSpec> subPartitions =
                            generateNewPartitionSpecForSplitPartitionByHotValue(curPartitionInfo.getTableName(),
                                null,
                                modifyParentPartitionSpec.getSubPartitions(),
                                curPartitionInfo.getPartitionBy().getSubPartitionBy().getPartitionColumnNameList()
                                    .size(),
                                parentPrepareData,
                                physicalTableAndGroupPairs, newPartitionIndex);
                        PartitionSpec newParentPartitionSpec = modifyParentPartitionSpec.copy();
                        newParentPartitionSpec.setSubPartitions(subPartitions);
                        newPartitions.add(newParentPartitionSpec);
                    } else {
                        newPartitions.add(parentPartitionSpec.copy());
                    }
                }
            }
        } else {
            newPartitions = generateNewPartitionSpecForSplitPartitionByHotValue(curPartitionInfo.getTableName(),
                null,
                curPartitionInfo.getPartitionBy().getPartitions(),
                curPartitionInfo.getPartitionBy().getPartitionColumnNameList().size(), parentPrepareData,
                physicalTableAndGroupPairs, newPartitionIndex);
        }
        newPartitionInfo.getPartitionBy().setPartitions(newPartitions);
        updatePartitionSpecRelationship(newPartitionInfo);
        return newPartitionInfo;
    }

    private static List<PartitionSpec> generateNewPartitionSpecForSplitPartitionByHotValue(
        String tableName,
        PartitionSpec parentPartitionSpec,
        List<PartitionSpec> oldPartitionSpecs,
        int fullPartColCnt,
        AlterTableGroupSplitPartitionByHotValuePreparedData parentPrepareData,
        Map<String, Pair<String, String>> physicalTableAndGroupPairs,
        AtomicInteger newPartitionIndex) {
        List<PartitionGroupRecord> newPartitionGroups = parentPrepareData.getInvisiblePartitionGroups();
        assert newPartitionGroups.size() == physicalTableAndGroupPairs.size();
        Set<String> oldPartitions = new TreeSet<>(String::compareToIgnoreCase);
        boolean foundSplitPoint = false;
        List<PartitionSpec> newPartitions = new ArrayList<>();
        PartitionSpec lastOldPartitionSpec = null;

        if (parentPrepareData.isUseTemplatePart() && parentPrepareData.isSplitSubPartition()) {
            parentPrepareData.getOldPartitionNames().stream()
                .forEach(o -> oldPartitions.add(parentPartitionSpec.getName() + o));
        } else {
            parentPrepareData.getOldPartitionNames().stream().forEach(o -> oldPartitions.add(o));
        }
        int subPartitionIdex = 0;
        for (PartitionSpec partitionSpec : oldPartitionSpecs) {
            if (oldPartitions.contains(partitionSpec.getName())) {
                lastOldPartitionSpec = partitionSpec;
                if (foundSplitPoint) {
                    continue;
                }
                foundSplitPoint = true;
                List<Long[]> splitPointList = parentPrepareData.getSplitPointInfos().get(tableName);
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
                    if (GeneralUtil.isNotEmpty(newPartitionSpec.getSubPartitions())) {
                        newPartitionSpec.setName(parentPrepareData.getLogicalParts().get(newPartitionIndex.get()));
                        for (PartitionSpec subPartition : newPartitionSpec.getSubPartitions()) {
                            subPartition.setName(newPartitionGroups.get(subPartitionIdex).partition_name);
                            PartitionLocation location = subPartition.getLocation();
                            location.setGroupKey(physicalTableAndGroupPairs.get(subPartition.getName()).getValue());
                            location.setPhyTableName(physicalTableAndGroupPairs.get(subPartition.getName()).getKey());
                            location.setVisiable(false);
                            subPartitionIdex++;
                        }
                    } else {
                        newPartitionSpec.setName(newPartitionGroups.get(newPartitionIndex.get()).partition_name);
                        if (StringUtils.isNotEmpty(newPartitionSpec.getTemplateName())
                            && parentPrepareData.isUseTemplatePart() && parentPrepareData.isSplitSubPartition()) {
                            if (newPartitionSpec.getName().indexOf(parentPartitionSpec.getName()) != 0) {
                                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                                    String.format(
                                        "the new subPartition name[%s] is not compatible with parent partition[%s]",
                                        newPartitionSpec.getName(), parentPartitionSpec.getName()));
                            }
                            newPartitionSpec.setTemplateName(
                                PartitionNameUtil.getTemplateName(parentPartitionSpec.getName(),
                                    newPartitionSpec.getName()));
                        }
                        PartitionLocation location = newPartitionSpec.getLocation();
                        location.setGroupKey(physicalTableAndGroupPairs.get(newPartitionSpec.getName()).getValue());
                        location.setPhyTableName(physicalTableAndGroupPairs.get(newPartitionSpec.getName()).getKey());
                        location.setVisiable(false);
                    }
                    SearchDatumInfo searchDatumInfo = SearchDatumInfo.createFromHashCodes(finalSplitPoint);
                    newPartitionSpec.getBoundSpec().setSingleDatum(searchDatumInfo);
                    newPartitions.add(newPartitionSpec);
                    newPartitionIndex.getAndIncrement();
                }
            } else {
                PartitionSpec newPartitionSpec;
                if (foundSplitPoint && lastOldPartitionSpec != null) {
                    newPartitionSpec = lastOldPartitionSpec.copy();
                    if (GeneralUtil.isNotEmpty(newPartitionSpec.getSubPartitions())) {
                        newPartitionSpec.setName(parentPrepareData.getLogicalParts().get(newPartitionIndex.get()));
                        for (PartitionSpec subPartition : newPartitionSpec.getSubPartitions()) {
                            subPartition.setName(newPartitionGroups.get(subPartitionIdex).partition_name);
                            PartitionLocation location = subPartition.getLocation();
                            location.setGroupKey(physicalTableAndGroupPairs.get(subPartition.getName()).getValue());
                            location.setPhyTableName(physicalTableAndGroupPairs.get(subPartition.getName()).getKey());
                            location.setVisiable(false);
                            subPartitionIdex++;
                        }
                    } else {
                        newPartitionSpec.setName(newPartitionGroups.get(newPartitionIndex.get()).partition_name);
                        if (StringUtils.isNotEmpty(newPartitionSpec.getTemplateName())
                            && parentPrepareData.isUseTemplatePart() && parentPrepareData.isSplitSubPartition()) {
                            if (newPartitionSpec.getName().indexOf(parentPartitionSpec.getName()) != 0) {
                                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                                    String.format(
                                        "the new subPartition name[%s] is not compatible with parent partition[%s]",
                                        newPartitionSpec.getName(), parentPartitionSpec.getName()));
                            }
                            newPartitionSpec.setTemplateName(
                                PartitionNameUtil.getTemplateName(parentPartitionSpec.getName(),
                                    newPartitionSpec.getName()));
                        }
                        PartitionLocation location = newPartitionSpec.getLocation();
                        location.setGroupKey(physicalTableAndGroupPairs.get(newPartitionSpec.getName()).getValue());
                        location.setPhyTableName(physicalTableAndGroupPairs.get(newPartitionSpec.getName()).getKey());
                        location.setVisiable(false);
                    }

                    newPartitions.add(newPartitionSpec);
                    lastOldPartitionSpec = null;
                    newPartitionIndex.getAndIncrement();
                }
                newPartitionSpec = partitionSpec.copy();
                newPartitions.add(newPartitionSpec);
            }
        }
        //oldPartition is the last one
        if (foundSplitPoint && lastOldPartitionSpec != null) {
            PartitionSpec newPartitionSpec = lastOldPartitionSpec.copy();
            if (GeneralUtil.isNotEmpty(newPartitionSpec.getSubPartitions())) {
                newPartitionSpec.setName(parentPrepareData.getLogicalParts().get(newPartitionIndex.get()));
                for (PartitionSpec subPartition : newPartitionSpec.getSubPartitions()) {
                    subPartition.setName(newPartitionGroups.get(subPartitionIdex).partition_name);
                    PartitionLocation location = subPartition.getLocation();
                    location.setGroupKey(physicalTableAndGroupPairs.get(subPartition.getName()).getValue());
                    location.setPhyTableName(physicalTableAndGroupPairs.get(subPartition.getName()).getKey());
                    location.setVisiable(false);
                    subPartitionIdex++;
                }
            } else {
                newPartitionSpec.setName(newPartitionGroups.get(newPartitionIndex.get()).partition_name);
                if (StringUtils.isNotEmpty(newPartitionSpec.getTemplateName())
                    && parentPrepareData.isUseTemplatePart() && parentPrepareData.isSplitSubPartition()) {
                    if (newPartitionSpec.getName().indexOf(parentPartitionSpec.getName()) != 0) {
                        throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                            String.format(
                                "the new subPartition name[%s] is not compatible with parent partition[%s]",
                                newPartitionSpec.getName(), parentPartitionSpec.getName()));
                    }
                    newPartitionSpec.setTemplateName(
                        PartitionNameUtil.getTemplateName(parentPartitionSpec.getName(),
                            newPartitionSpec.getName()));
                }
                PartitionLocation location = newPartitionSpec.getLocation();
                location.setGroupKey(physicalTableAndGroupPairs.get(newPartitionSpec.getName()).getValue());
                location.setPhyTableName(physicalTableAndGroupPairs.get(newPartitionSpec.getName()).getKey());
                location.setVisiable(false);
            }
            newPartitionIndex.getAndIncrement();
            newPartitions.add(newPartitionSpec);
            lastOldPartitionSpec = null;
        }
        return newPartitions;
    }

    private static PartitionInfo getNewPartitionInfoForRenamePartition(PartitionInfo curPartitionInfo,
                                                                       Map<String, String> changePartitionsMap,
                                                                       boolean renameSubPartition) {
        if (GeneralUtil.isEmpty(changePartitionsMap)) {
            return curPartitionInfo;
        }

        boolean isUseTemplatePart = curPartitionInfo.getPartitionBy().getSubPartitionBy() != null ?
            curPartitionInfo.getPartitionBy().getSubPartitionBy().isUseSubPartTemplate() : false;

        PartitionInfo newPartitionInfo = curPartitionInfo.copy();
        for (PartitionSpec partitionSpec : newPartitionInfo.getPartitionBy().getPartitions()) {
            if (!renameSubPartition && changePartitionsMap.containsKey(partitionSpec.getName())) {
                String newName = changePartitionsMap.get(partitionSpec.getName());
                PartKeyLevel level = partitionSpec.getPartLevel();
                PartitionNameUtil.validatePartName(newName, KeyWordsUtil.isKeyWord(newName),
                    level == PartKeyLevel.SUBPARTITION_KEY);
                partitionSpec.setName(newName);
                if (isUseTemplatePart) {
                    for (PartitionSpec subPartSpec : partitionSpec.getSubPartitions()) {
                        subPartSpec.setName(newName + subPartSpec.getTemplateName());
                        level = subPartSpec.getPartLevel();
                        PartitionNameUtil.validatePartName(newName + subPartSpec.getTemplateName(),
                            KeyWordsUtil.isKeyWord(newName + subPartSpec.getTemplateName()),
                            level == PartKeyLevel.SUBPARTITION_KEY);
                    }
                }
            } else {
                for (PartitionSpec subPartSpec : partitionSpec.getSubPartitions()) {
                    if (isUseTemplatePart) {
                        if (changePartitionsMap.containsKey(subPartSpec.getTemplateName())) {
                            String newName = changePartitionsMap.get(subPartSpec.getTemplateName());
                            PartKeyLevel level = subPartSpec.getPartLevel();
                            PartitionNameUtil.validatePartName(newName, KeyWordsUtil.isKeyWord(newName),
                                level == PartKeyLevel.SUBPARTITION_KEY);
                            PartitionNameUtil.validatePartName(partitionSpec.getName() + newName,
                                KeyWordsUtil.isKeyWord(partitionSpec.getName() + newName),
                                level == PartKeyLevel.SUBPARTITION_KEY);
                            subPartSpec.setTemplateName(newName);
                            subPartSpec.setName(partitionSpec.getName() + newName);
                        }
                    } else if (changePartitionsMap.containsKey(subPartSpec.getName())) {
                        String newName = changePartitionsMap.get(subPartSpec.getName());
                        PartKeyLevel level = subPartSpec.getPartLevel();
                        PartitionNameUtil.validatePartName(newName, KeyWordsUtil.isKeyWord(newName),
                            level == PartKeyLevel.SUBPARTITION_KEY);
                        subPartSpec.setName(newName);
                    }
                }
            }
        }
        newPartitionInfo.getPartitionBy().getPhysicalPartitions().clear();
        updatePartitionSpecRelationship(newPartitionInfo);
        return newPartitionInfo;
    }

    public static PartitionInfo getNewPartitionInfo(
        AlterTableGroupBasePreparedData parentPreparedData,
        PartitionInfo curPartitionInfo,
        boolean isAlterTableGroup,
        SqlNode sqlNode,
        List<String> oldPartitionNames,
        List<String> newPartitionNames,
        String tableGroupName,
        String targetPartitionNameToBeAltered,
        List<PartitionGroupRecord> invisiblePartitionGroupRecords,
        Map<String, Pair<String, String>> physicalTableAndGroupPairs,
        ExecutionContext executionContext) {

        checkIfAlterPartitionActionAllowed(curPartitionInfo, isAlterTableGroup, sqlNode, executionContext);

        PartitionInfo newPartInfo;

        if (sqlNode instanceof SqlAlterTableSplitPartition) {
            SqlAlterTableSplitPartition splitPartition = (SqlAlterTableSplitPartition) sqlNode;
            AlterTableGroupSplitPartitionPreparedData alterTableGroupSplitPartitionPreparedData =
                (AlterTableGroupSplitPartitionPreparedData) parentPreparedData;
            newPartInfo = getNewPartitionInfoForSplitType(
                executionContext,
                curPartitionInfo,
                invisiblePartitionGroupRecords,
                splitPartition,
                tableGroupName,
                targetPartitionNameToBeAltered,
                physicalTableAndGroupPairs,
                alterTableGroupSplitPartitionPreparedData.getPartBoundExprInfo()
            );
        } else if (sqlNode instanceof SqlAlterTableMergePartition) {
            SqlAlterTableMergePartition sqlAlterTableMergePartition =
                (SqlAlterTableMergePartition) sqlNode;
            Set<String> mergePartitionsName = new TreeSet<>(String::compareToIgnoreCase);
            mergePartitionsName.addAll(sqlAlterTableMergePartition.getOldPartitions().stream()
                .map(o -> Util.last(((SqlIdentifier) (o)).names)).collect(
                    Collectors.toSet()));
            String targetPartitionName =
                Util.last(((SqlIdentifier) (sqlAlterTableMergePartition.getTargetPartitionName())).names);
            newPartInfo =
                getNewPartitionInfoForMergeType((AlterTableGroupMergePartitionPreparedData) parentPreparedData,
                    curPartitionInfo, invisiblePartitionGroupRecords, tableGroupName,
                    physicalTableAndGroupPairs, mergePartitionsName, ImmutableList.of(targetPartitionName),
                    executionContext);
        } else if (sqlNode instanceof SqlAlterTableReorgPartition) {
            SqlAlterTableReorgPartition sqlAlterTableReorgPartition = (SqlAlterTableReorgPartition) sqlNode;
            AlterTableGroupReorgPartitionPreparedData preparedData =
                (AlterTableGroupReorgPartitionPreparedData) parentPreparedData;
            newPartInfo = getNewPartitionInfoForReorgType(
                curPartitionInfo,
                preparedData.getPartRexInfoCtx(),
                invisiblePartitionGroupRecords,
                physicalTableAndGroupPairs,
                preparedData.getOldPartitionNames(),
                preparedData.getNewPartitions(),
                sqlAlterTableReorgPartition.isSubPartition(),
                isAlterTableGroup,
                executionContext
            );
        } else if (sqlNode instanceof SqlAlterTableMovePartition) {
            SqlAlterTableMovePartition movePartition = (SqlAlterTableMovePartition) sqlNode;
            newPartInfo = getNewPartitionInfoForMoveType(
                curPartitionInfo,
                movePartition.getTargetPartitions(),
                tableGroupName,
                physicalTableAndGroupPairs,
                executionContext
            );
        } else if (sqlNode instanceof SqlAlterTableSetTableGroup) {
            SqlAlterTableSetTableGroup setTableGroup = (SqlAlterTableSetTableGroup) sqlNode;
            newPartInfo = getNewPartitionInfoForSetTableGroup(
                curPartitionInfo,
                setTableGroup,
                executionContext
            );
        } else if (sqlNode instanceof SqlAlterTableAddPartition) {
            SqlAlterTableAddPartition addPartition = (SqlAlterTableAddPartition) sqlNode;
            AlterTableGroupAddPartitionPreparedData addPartitionPreparedData =
                (AlterTableGroupAddPartitionPreparedData) parentPreparedData;
            newPartInfo = getNewPartitionInfoForAddPartition(
                curPartitionInfo,
                invisiblePartitionGroupRecords,
                isAlterTableGroup,
                addPartition,
                physicalTableAndGroupPairs,
                addPartitionPreparedData.getPartBoundExprInfoByLevel(),
                executionContext
            );
        } else if (sqlNode instanceof SqlAlterTableDropPartition) {
            SqlAlterTableDropPartition dropPartition = (SqlAlterTableDropPartition) sqlNode;
            newPartInfo = getNewPartitionInfoForDropPartition(
                curPartitionInfo,
                dropPartition,
                oldPartitionNames,
                newPartitionNames,
                invisiblePartitionGroupRecords,
                physicalTableAndGroupPairs,
                executionContext
            );
        } else if (sqlNode instanceof SqlAlterTableModifyPartitionValues) {
            AlterTableGroupModifyPartitionPreparedData modifyPartitionPreparedData =
                (AlterTableGroupModifyPartitionPreparedData) parentPreparedData;
            newPartInfo = getNewPartitionInfoForModifyPartition(
                curPartitionInfo,
                sqlNode,
                modifyPartitionPreparedData.getPartBoundExprInfo(),
                invisiblePartitionGroupRecords,
                physicalTableAndGroupPairs,
                executionContext
            );
        } else if (sqlNode instanceof SqlRefreshTopology) {
            newPartInfo = getNewPartitionInfoForCopyPartition(
                curPartitionInfo,
                invisiblePartitionGroupRecords,
                executionContext
            );
        } else if (sqlNode instanceof SqlAlterTableExtractPartition) {
            newPartInfo = getNewPartitionInfoForExtractType(curPartitionInfo,
                (AlterTableGroupExtractPartitionPreparedData) parentPreparedData,
                physicalTableAndGroupPairs, executionContext);
        } else if (sqlNode instanceof SqlAlterTableSplitPartitionByHotValue) {
            newPartInfo = getNewPartitionInfoForSplitPartitionByHotValue(curPartitionInfo,
                (AlterTableGroupSplitPartitionByHotValuePreparedData) parentPreparedData,
                physicalTableAndGroupPairs, executionContext);
        } else if (sqlNode instanceof SqlAlterTableRenamePartition) {
            Map<String, String> changePartitions = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
            SqlAlterTableRenamePartition sqlAlterTableRenamePartition = (SqlAlterTableRenamePartition) sqlNode;
            for (Pair<String, String> pair : sqlAlterTableRenamePartition.getChangePartitionsPair()) {
                changePartitions.put(pair.getKey(), pair.getValue());
            }
            newPartInfo = getNewPartitionInfoForRenamePartition(curPartitionInfo, changePartitions,
                sqlAlterTableRenamePartition.isSubPartitionsRename());
        } else {
            throw new NotSupportException(sqlNode.getKind().toString() + " is not support yet");
        }

        updateTemplateSubPartitions(newPartInfo);
        PartitionInfoUtil.adjustPartitionPositionsForNewPartInfo(newPartInfo);
        PartitionInfoUtil.validatePartitionInfoForDdl(newPartInfo, executionContext);

        return newPartInfo;
    }

    public static void updateTemplateSubPartitions(PartitionInfo newPartitionInfo) {
        PartitionByDefinition subPartitionByDefinition = newPartitionInfo.getPartitionBy().getSubPartitionBy();
        boolean hasSubPartition = subPartitionByDefinition != null;
        boolean useSubPartTemplate = hasSubPartition && subPartitionByDefinition.isUseSubPartTemplate();
        if (useSubPartTemplate) {
            PartitionSpec partitionSpec = newPartitionInfo.getPartitionBy().getNthPartition(1);
            List<PartitionSpec> subPartitionSpecs =
                PartitionInfoUtil.updateTemplatePartitionSpec(partitionSpec.getSubPartitions(),
                    subPartitionByDefinition.getStrategy());
            subPartitionByDefinition.setPartitions(subPartitionSpecs);
            subPartitionByDefinition.getPhysicalPartitions().clear();
        }
    }

    public static void updatePartitionSpecRelationship(PartitionInfo newPartitionInfo) {
        Long id = 0L;
        for (PartitionSpec partitionSpec : newPartitionInfo.getPartitionBy().getPartitions()) {
            partitionSpec.setId(id);
            if (partitionSpec.isLogical()) {
                for (PartitionSpec subPartitionSpec : partitionSpec.getSubPartitions()) {
                    subPartitionSpec.setParentId(id);
                }
            }
            id++;
        }
    }

    public static void updateSubPartitionTemplate(PartitionInfo newPartitionInfo) {
        PartitionByDefinition subPartBy = newPartitionInfo.getPartitionBy().getSubPartitionBy();
        if (subPartBy != null && subPartBy.isUseSubPartTemplate()) {
            List<PartitionSpec> firstSubPartitions =
                newPartitionInfo.getPartitionBy().getPartitions().get(0).getSubPartitions();
            subPartBy.getPhysicalPartitions().clear();
            subPartBy.setPartitions(new ArrayList<>());
            for (PartitionSpec subPartitionSpec : firstSubPartitions) {
                PartitionSpec newSubPartitionSpec = subPartitionSpec.copy();
                newSubPartitionSpec.setLogical(true);
                newSubPartitionSpec.setLocation(null);
                newSubPartitionSpec.setSpecTemplate(true);
                newSubPartitionSpec.setName(subPartitionSpec.getTemplateName());
                newSubPartitionSpec.setPhyPartPosition(0L);
                subPartBy.getPartitions().add(newSubPartitionSpec);
            }
        }
    }

    protected static void checkIfAlterPartitionActionAllowed(
        PartitionInfo curPartitionInfo,
        boolean isAlterTableGroup,
        SqlNode sqlNode,
        ExecutionContext executionContext
    ) {

        boolean modifyPartBy = false;
        if (
            (sqlNode instanceof SqlAlterTableSplitPartition)
                || (sqlNode instanceof SqlAlterTableMergePartition)
                || (sqlNode instanceof SqlAlterTableReorgPartition)
                || (sqlNode instanceof SqlAlterTableExtractPartition)
                || (sqlNode instanceof SqlAlterTableSplitPartitionByHotValue)
                || (sqlNode instanceof SqlAlterTableAddPartition)
                || (sqlNode instanceof SqlAlterTableDropPartition)
                || (sqlNode instanceof SqlAlterTableModifyPartitionValues)
        ) {
            modifyPartBy = true;
        }

        boolean allowAlterPart = true;
        if (modifyPartBy) {
            PartitionByDefinition partBy = curPartitionInfo.getPartitionBy();
            PartitionByDefinition subPartBy = partBy.getSubPartitionBy();

            if (partBy != null) {
                if (partBy.getStrategy() == PartitionStrategy.UDF_HASH) {
                    allowAlterPart = false;
                }
            }
            if (subPartBy != null && allowAlterPart) {
                if (subPartBy.getStrategy() == PartitionStrategy.UDF_HASH) {
                    allowAlterPart = false;
                }
            }
            if (!allowAlterPart) {
                throw new NotSupportException(sqlNode.getKind().toString() + " with using partition strategy "
                    + PartitionStrategy.UDF_HASH.getStrategyExplainName());
            }
        }
    }
}
