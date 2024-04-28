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

package com.alibaba.polardbx.optimizer.partition;

import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.CaseInsensitive;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.time.calculator.MySQLIntervalType;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.partition.boundspec.PartitionBoundSpec;
import com.alibaba.polardbx.optimizer.partition.boundspec.PartitionBoundVal;
import com.alibaba.polardbx.optimizer.partition.boundspec.PartitionBoundValueKind;
import com.alibaba.polardbx.optimizer.partition.boundspec.SingleValuePartitionBoundSpec;
import com.alibaba.polardbx.optimizer.partition.common.PartKeyLevel;
import com.alibaba.polardbx.optimizer.partition.common.PartSpecNormalizationParams;
import com.alibaba.polardbx.optimizer.partition.common.PartitionByNormalizationParams;
import com.alibaba.polardbx.optimizer.partition.common.PartitionStrategy;
import com.alibaba.polardbx.optimizer.partition.common.PartitionTableType;
import com.alibaba.polardbx.optimizer.partition.common.PartitionsNormalizationParams;
import com.alibaba.polardbx.optimizer.partition.datatype.PartitionField;
import com.alibaba.polardbx.optimizer.partition.datatype.function.PartitionIntFunction;
import com.alibaba.polardbx.optimizer.partition.pruning.SearchDatumComparator;
import com.alibaba.polardbx.optimizer.partition.pruning.SearchDatumInfo;
import com.google.common.collect.Lists;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * @author chenghui.lch
 */
public class PartitionByDefinition extends PartitionByDefinitionBase {

    /**
     * Label the partition level of current partition definition
     */
    protected PartKeyLevel partLevel;

    /**
     * The partition definition of next partition level
     * <pre>
     *     when current partition level is subpartition level or has no any next partition definition,
     *     subPartitionBy will be null
     * </pre>
     */
    protected PartitionByDefinition subPartitionBy;

    /**
     * Label if it is using subpartition spec template
     */
    protected boolean useSubPartTemplate;

    /**
     * the definition of partitions of current partition level
     * <pre>
     *     if partLevel is subpartition, and subpartition definition use non-templated definition,
     *     then the partitions of subPartitionBy will empty,
     *     because all the real subpartitions are defined in the partitions of partitionBy of partInfo
     * </pre>
     */
    protected List<PartitionSpec> partitions;

    /**
     * the cache of the definition of all physical partitions of curr partition level & its all sub-level partitions
     * (read only)
     * <pre>
     *     the order of all return physical partitions is order by PartitionSpec.phyPartPosition
     * </pre>
     */
    protected volatile List<PartitionSpec> physicalPartitionsCache;

    public PartitionByDefinition() {
        partitions = new ArrayList<>();
        partitionExprList = new ArrayList<>();
        partitionExprTypeList = new ArrayList<>();
        partitionFieldList = new ArrayList<>();
        partitionColumnNameList = new ArrayList<>();
    }

    protected static List<String> getActualPartitionColumnsByPartByDefAndPartitions(PartitionByDefinition partByDef,
                                                                                    List<PartitionSpec> partitions) {
        List<String> fullPartColList = partByDef.getPartitionColumnNameList();
        int fullPartColCnt = fullPartColList.size();
        int actualPartColCnt = 0;
        List<String> targetPartColList = new ArrayList<>();
        PartitionStrategy strategy = partByDef.getStrategy();
        if (strategy == PartitionStrategy.HASH || strategy == PartitionStrategy.DIRECT_HASH
            || strategy == PartitionStrategy.CO_HASH) {
            actualPartColCnt = fullPartColCnt;
        } else if (strategy == PartitionStrategy.LIST || strategy == PartitionStrategy.LIST_COLUMNS) {
            actualPartColCnt = fullPartColCnt;
        } else {
            boolean findActualPartColCnt = false;
            for (int i = fullPartColCnt - 1; i > -1; i--) {
                int partCnt = partitions.size();
                for (int j = 0; j < partCnt; j++) {
                    PartitionSpec spec = partitions.get(j);
                    SingleValuePartitionBoundSpec bndSpec = (SingleValuePartitionBoundSpec) spec.getBoundSpec();
                    PartitionBoundVal[] bndVal = bndSpec.getSingleDatum().getDatumInfo();
                    PartitionBoundVal bndValOfOneCol = bndVal[i];
                    PartitionBoundValueKind bndValKind = bndVal[i].getValueKind();

                    if (strategy == PartitionStrategy.KEY) {
                        if (bndValKind != PartitionBoundValueKind.DATUM_NORMAL_VALUE) {
                            actualPartColCnt = i + 1;
                            findActualPartColCnt = true;
                            break;
                        }
                        PartitionField valFld = bndValOfOneCol.getValue();
                        if (valFld.longValue() != Long.MAX_VALUE) {
                            actualPartColCnt = i + 1;
                            findActualPartColCnt = true;
                            break;
                        }
                    } else if (strategy == PartitionStrategy.RANGE || strategy == PartitionStrategy.RANGE_COLUMNS) {
                        if (bndValKind != PartitionBoundValueKind.DATUM_MAX_VALUE) {
                            actualPartColCnt = i + 1;
                            findActualPartColCnt = true;
                            break;
                        }
                    } else if (strategy == PartitionStrategy.UDF_HASH) {
                        if (bndValKind != PartitionBoundValueKind.DATUM_NORMAL_VALUE) {
                            actualPartColCnt = i + 1;
                            findActualPartColCnt = true;
                            break;
                        }
                        PartitionField valFld = bndValOfOneCol.getValue();
                        if (valFld.longValue() != Long.MAX_VALUE) {
                            actualPartColCnt = i + 1;
                            findActualPartColCnt = true;
                            break;
                        }
                    } else {
                        throw new NotSupportException("Not support for list/list columns partitions");
                    }
                    continue;
                }
                if (findActualPartColCnt) {
                    break;
                }
            }
        }
        /**
         * actualPartColCnt must be >= 1
         */
        if (actualPartColCnt == 0 && fullPartColCnt > 0) {
            actualPartColCnt = 1;
        }
        for (int i = 0; i < actualPartColCnt; i++) {
            targetPartColList.add(fullPartColList.get(i));
        }
        return targetPartColList;
    }

    public static void normalizePartitions(PartitionsNormalizationParams params) {
        StringBuilder sb = params.getBuilder();
        boolean usePartGroupNameAsPartName = params.isUsePartGroupNameAsPartName();
        Map<Long, String> partGrpNameInfo = params.getPartGrpNameInfo();
        boolean needSortPartitions = params.isNeedSortPartitions();
        boolean showHashByRange = params.isShowHashByRange();
        //int prefixPartColCnt = params.getPrefixPartColCnt();

        List<Integer> allLevelPrefixPartColCnt = params.getAllLevelPrefixPartColCnt();

        boolean useSubPartBy = params.isUseSubPartBy();
        boolean useSubPartByTemp = params.isUseSubPartByTemp();
        boolean nextPartLevelUseTemp = params.isNextPartLevelUseTemp();
        boolean buildSubPartByTemp = params.isBuildSubPartByTemp();
        PartitionStrategy parentPartStrategy = params.getParentPartStrategy();
        SearchDatumComparator boundSpaceComparator = params.getBoundSpaceComparator();

        List<PartitionSpec> partSpecList = params.getPartSpecList();
        List<PartitionSpec> orderedPartSpecList = params.getOrderedPartSpecList();
        String textIndentBase = params.getTextIndentBase();
        String textIndent = params.getTextIndent();
        if (needSortPartitions) {
            partSpecList = orderedPartSpecList;
        }
        PartitionStrategy partStrategy = partSpecList.get(0).getStrategy();
        PartKeyLevel partKeyLevel = partSpecList.get(0).getPartLevel();
        boolean isSpecTemplate = partSpecList.get(0).isSpecTemplate();
        int i;
        switch (partStrategy) {
        case KEY:
        case HASH:
        case DIRECT_HASH:
        case CO_HASH:
            if (partKeyLevel == PartKeyLevel.PARTITION_KEY) {
                if (!useSubPartBy) {
                    if (!showHashByRange) {
                        break;
                    }
                } else {
                    if (nextPartLevelUseTemp) {
                        if (!showHashByRange) {
                            break;
                        }
                    }
                }
            } else if (partKeyLevel == PartKeyLevel.SUBPARTITION_KEY) {
                if (buildSubPartByTemp) {
                    if (!showHashByRange) {
                        break;
                    }
                }
            }
        case UDF_HASH:
        case LIST:
        case LIST_COLUMNS:
        case RANGE:
        case RANGE_COLUMNS:

            sb.append(textIndentBase);
            sb.append(textIndent);
            sb.append("(");
            i = 0;
            for (PartitionSpec pSpec : partSpecList) {
                if (i > 0) {
                    sb.append(",\n");
                    sb.append(textIndentBase);
                    sb.append(" ");
                    sb.append(textIndent);
                }
                String partGrpName = null;
                if (usePartGroupNameAsPartName && partGrpNameInfo != null) {
                    if (!pSpec.isLogical()) {
                        partGrpName = partGrpNameInfo.get(pSpec.getLocation().getPartitionGroupId());
                    } else {
                        partGrpName = pSpec.getName();
                    }
                }
                PartSpecNormalizationParams normalSpecParams = new PartSpecNormalizationParams();
                normalSpecParams.setUsePartGroupNameAsPartName(usePartGroupNameAsPartName);
                normalSpecParams.setPartGrpName(partGrpName);
                normalSpecParams.setAllLevelPrefixPartColCnts(allLevelPrefixPartColCnt);
                normalSpecParams.setPartSpec(pSpec);
                normalSpecParams.setNeedSortBoundValues(needSortPartitions);
                normalSpecParams.setPartGrpNameInfo(partGrpNameInfo);
                normalSpecParams.setShowHashByRange(showHashByRange);
                normalSpecParams.setBoundSpaceComparator(boundSpaceComparator);
                normalSpecParams.setNeedSortPartitions(needSortPartitions);
                normalSpecParams.setUseSubPartByTemp(useSubPartByTemp);
                normalSpecParams.setTextIntentBase(textIndentBase);
                sb.append(PartitionSpec.normalizePartSpec(normalSpecParams));
                i++;
            }
            sb.append(")");

            if (buildSubPartByTemp) {
                boolean hasMoreParentPartSpecsToShow = needShowMoreParentPartSpecs(showHashByRange, parentPartStrategy);
                if (hasMoreParentPartSpecsToShow) {
                    sb.append('\n');
                }
            }
            break;
        }

        //sb.append('\n');
    }

    private static boolean needShowMoreParentPartSpecs(boolean showHashByRange,
                                                       PartitionStrategy parentPartStrategy) {
        boolean hasMoreParentPartSpecsToShow = true;
        if (parentPartStrategy != null && (parentPartStrategy == PartitionStrategy.KEY
            || parentPartStrategy == PartitionStrategy.HASH || parentPartStrategy == PartitionStrategy.DIRECT_HASH
            || parentPartStrategy == PartitionStrategy.CO_HASH)
            && !showHashByRange) {
            hasMoreParentPartSpecsToShow = false;
        }
        return hasMoreParentPartSpecsToShow;
    }

    public static List<SearchDatumInfo> getDistinctSearchDatumInfos(PartitionSpec partitionSpec,
                                                                    SearchDatumComparator bndSpaceComparator,
                                                                    List<SearchDatumInfo> searchRecords) {
        if (!partitionSpec.getStrategy().isList()) {
            return searchRecords;
        }
        List<SearchDatumInfo> distinctResults = new ArrayList<>();
        PartSpecComparator partSpecComp = new PartSpecComparator(bndSpaceComparator);
        TreeMap<PartSpecBoundValCompKey, PartitionSpec> listValPartMap = new TreeMap(partSpecComp);
        PartitionBoundSpec newBndSpec = partitionSpec.getBoundSpec().copy();
        newBndSpec.setMultiDatums(searchRecords);
        PartitionBoundSpec newSortedValsBndSpec =
            sortListPartitionsAllValues(bndSpaceComparator, newBndSpec);
        for (int j = 0; j < newSortedValsBndSpec.getMultiDatums().size(); j++) {
            SearchDatumInfo oneListVal = newSortedValsBndSpec.getMultiDatums().get(j);
            PartSpecBoundValCompKey cmpKey = new PartSpecBoundValCompKey(partitionSpec, oneListVal);
            if (!listValPartMap.containsKey(cmpKey)) {
                listValPartMap.put(cmpKey, partitionSpec);
                distinctResults.add(oneListVal);
            }
        }
        return distinctResults;
    }

    public static List<PartitionSpec> getOrderedPhyPartSpecsByPartStrategy(PartitionStrategy strategy,
                                                                           SearchDatumComparator bndSpaceComparator,
                                                                           List<PartitionSpec> originalPhyPartSpecs) {
        if (strategy != PartitionStrategy.LIST && strategy != PartitionStrategy.LIST_COLUMNS) {
            List<PartitionSpec> newPartSpecs = new ArrayList<>();
            for (int i = 0; i < originalPhyPartSpecs.size(); i++) {
                newPartSpecs.add(originalPhyPartSpecs.get(i).copy());
            }
            return newPartSpecs;
        }
        List<PartitionSpec> newPartSpecList = new ArrayList<>();
        SearchDatumComparator cmp = bndSpaceComparator;

        PartSpecComparator partSpecComp = new PartSpecComparator(cmp);
        TreeMap<PartSpecBoundValCompKey, PartitionSpec> partFirstValMap = new TreeMap(partSpecComp);
        TreeMap<PartSpecBoundValCompKey, PartitionSpec> listValPartMap = new TreeMap(partSpecComp);

        PartitionSpec mayHaveDefaultSpec = null;
        for (int i = 0; i < originalPhyPartSpecs.size(); i++) {
            PartitionSpec pSpec = originalPhyPartSpecs.get(i);
            PartitionSpec newSpec = pSpec.copy();
            if (newSpec.isDefaultPartition()) {
                mayHaveDefaultSpec = newSpec;
                continue;
            }

            PartitionBoundSpec newSortedValsBndSpec = sortListPartitionsAllValues(cmp, newSpec.getBoundSpec());

            for (int j = 0; j < newSortedValsBndSpec.getMultiDatums().size(); j++) {
                SearchDatumInfo oneListVal = newSortedValsBndSpec.getMultiDatums().get(j);
                PartSpecBoundValCompKey cmpKey = new PartSpecBoundValCompKey(newSpec, oneListVal);
                if (listValPartMap.containsKey(cmpKey)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_INVALID_DDL_PARAMS,
                        "Multiple definition of same constant in list partitioning");
                }
                listValPartMap.put(cmpKey, newSpec);
            }

            newSpec.setBoundSpec(newSortedValsBndSpec);
            SearchDatumInfo firstValOfOnePart = newSortedValsBndSpec.getMultiDatums().get(0);

            PartSpecBoundValCompKey cmpKeyOfFirstValOfOnePart = new PartSpecBoundValCompKey(newSpec, firstValOfOnePart);
            partFirstValMap.put(cmpKeyOfFirstValOfOnePart, newSpec);
        }

        for (PartSpecBoundValCompKey datumCmpKeyInfo : partFirstValMap.keySet()) {
            newPartSpecList.add(partFirstValMap.get(datumCmpKeyInfo));
        }
        if (mayHaveDefaultSpec != null) {
            newPartSpecList.add(mayHaveDefaultSpec);
        }
        return newPartSpecList;
    }

    public static void adjustPartitionPositionsByBoundVal(PartitionTableType tblType,
                                                          PartitionByDefinition partByDef) {

        /**
         * Adjust order positions for first-level partitions
         */
        List<PartitionSpec> firstLevelParts = partByDef.getPartitions();
        List<PartitionSpec> newFirstLevelPartsOutput = new ArrayList<>();
        adjustPartitionPositionsByBoundValInner(tblType, partByDef, firstLevelParts, newFirstLevelPartsOutput);
        partByDef.setPartitions(newFirstLevelPartsOutput);

        boolean useSubPart = partByDef.getSubPartitionBy() != null;
        if (useSubPart) {
            /**
             * Adjust order positions for second-level partitions
             */
            PartitionByDefinition subPartBy = partByDef.getSubPartitionBy();
            boolean useSubPartTemp = subPartBy.isUseSubPartTemplate();
            if (useSubPartTemp) {
                /**
                 * Adjust order positions for second-level partitions template
                 */
                List<PartitionSpec> secondLevelPartsTemp = subPartBy.getPartitions();
                List<PartitionSpec> newSecondLevelPartsTempOutput = new ArrayList<>();
                adjustPartitionPositionsByBoundValInner(tblType, subPartBy, secondLevelPartsTemp,
                    newSecondLevelPartsTempOutput);
                subPartBy.setPartitions(newSecondLevelPartsTempOutput);
            }

            /**
             * Adjust order positions for second-level partitions of each first-level partition
             */
            for (int i = 0; i < firstLevelParts.size(); i++) {
                PartitionSpec firstLevelPart = firstLevelParts.get(i);
                List<PartitionSpec> secondLevelPartsOfOnePart = firstLevelPart.getSubPartitions();
                List<PartitionSpec> newSecondLevelPartsOutput = new ArrayList<>();
                adjustPartitionPositionsByBoundValInner(tblType, subPartBy, secondLevelPartsOfOnePart,
                    newSecondLevelPartsOutput);
                firstLevelPart.setSubPartitions(newSecondLevelPartsOutput);
            }
        }
        return;
    }

    private static void adjustPartitionPositionsByBoundValInner(
        PartitionTableType tblType,
        PartitionByDefinition targetPartBy,
        List<PartitionSpec> originalPartSpecs,
        List<PartitionSpec> newOrderPartSpecsOutput) {
        SearchDatumComparator boundSpaceComparator = targetPartBy.getBoundSpaceComparator();
        PartitionStrategy partStrategy = targetPartBy.getStrategy();
        if (tblType.isA(PartitionTableType.PARTITIONED_TABLE)) {
            if (partStrategy == PartitionStrategy.LIST
                || partStrategy == PartitionStrategy.LIST_COLUMNS) {

                /**
                 * Because the sort method of PartitionByDefinition.getOrderedPhyPartSpecsByPartStrategy
                 * need to compare the parent position of two partition if they are physical partitions
                 * (The compare class: PartitionByDefinition.PartSpecComparator),
                 * but here the partitions of originalPartSpecs must be from the same parent,
                 * so here just to reset the parent part posi of copy spec to just compare their bound values.
                 */
                List<PartitionSpec> originalPartSpecsCopy = new ArrayList<>();
                for (int i = 0; i < originalPartSpecs.size(); i++) {
                    PartitionSpec partSpecCopy = originalPartSpecs.get(i).copy();
                    partSpecCopy.setParentPartPosi(0L);
                    originalPartSpecsCopy.add(partSpecCopy);
                }

                List<PartitionSpec> orderedPartSpecs =
                    PartitionByDefinition.getOrderedPhyPartSpecsByPartStrategy(
                        partStrategy,
                        boundSpaceComparator,
                        originalPartSpecsCopy);
                TreeMap<String, PartitionSpec> partNameToPartSpecMap =
                    new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
                for (int i = 0; i < originalPartSpecs.size(); i++) {
                    partNameToPartSpecMap.put(originalPartSpecs.get(i).getName(), originalPartSpecs.get(i));
                }
                for (int i = 0; i < orderedPartSpecs.size(); i++) {
                    PartitionSpec part = orderedPartSpecs.get(i);
                    String partName = part.getName();
                    PartitionSpec originalPart = partNameToPartSpecMap.get(partName);
                    long orderPosi = i + 1;
                    originalPart.setPosition(orderPosi);
                    originalPart.setBoundSpec(part.getBoundSpec());
                    newOrderPartSpecsOutput.add(originalPart);
                }
            } else {
                for (int i = 0; i < originalPartSpecs.size(); i++) {
                    long newOrderPosi = i + 1;
                    originalPartSpecs.get(i).setPosition(newOrderPosi);
                    newOrderPartSpecsOutput.add(originalPartSpecs.get(i));
                }
            }
        } else {
            for (int i = 0; i < originalPartSpecs.size(); i++) {
                long newOrderPosi = i + 1;
                originalPartSpecs.get(i).setPosition(newOrderPosi);
                newOrderPartSpecsOutput.add(originalPartSpecs.get(i));
            }
        }
    }

    public static void adjustPartPosiAndParentPartPosiForNewOrderPartSpecs(
        List<PartitionSpec> newOrderedPartSpecs,
        PartitionSpec parentSpec,
        boolean isSubPartSpec,
        boolean isSubPartSpecTemp) {

        /**
         * Reset the phyPartPosition and parentPartPosition for both partitions and subpartitions
         */
        if (!isSubPartSpec) {
            List<PartitionSpec> newSpecs = newOrderedPartSpecs;
            for (int i = 0; i < newSpecs.size(); i++) {
                PartitionSpec spec = newSpecs.get(i);
                long newPosi = i + 1;
                spec.setPosition(newPosi);
                spec.setParentPartPosi(0L);
                if (spec.isLogical()) {
                    spec.setPhyPartPosition(0L);
                } else {
                    spec.setPhyPartPosition(newPosi);
                }
            }
        } else {
            if (isSubPartSpecTemp) {
                List<PartitionSpec> newSpecs = newOrderedPartSpecs;
                for (int i = 0; i < newSpecs.size(); i++) {
                    PartitionSpec spec = newSpecs.get(i);
                    long newPosi = i + 1;
                    spec.setPosition(newPosi);
                    spec.setParentPartPosi(0L);
                    spec.setPhyPartPosition(0L);
                }
            } else {
                long parentPosi = 0L;
                if (parentSpec != null) {
                    parentPosi = parentSpec.getPosition();
                }
                List<PartitionSpec> newSpecs = newOrderedPartSpecs;
                for (int i = 0; i < newSpecs.size(); i++) {
                    PartitionSpec spec = newSpecs.get(i);
                    long newPosi = i + 1;
                    spec.setPosition(newPosi);
                    spec.setParentPartPosi(parentPosi);
                    // here set phyPartPosition by newPosi because cannot compute its really phyPartPosition
                    // by just one parentPart
                    spec.setPhyPartPosition(newPosi);
                }
            }
        }

        /**
         * Reset the phyPartPosition and parentPartPosition for both partitions and subpartitions
         */
        List<PartitionSpec> newSpecs = newOrderedPartSpecs;
        long allPhyPartPosiCounter = 0L;
        for (int i = 0; i < newSpecs.size(); i++) {
            PartitionSpec spec = newSpecs.get(i);
            spec.setParentPartPosi(0L);
            if (spec.isLogical()) {
                if (spec.isSpecTemplate()) {
                    spec.setParentPartPosi(0L);
                } else {
                    for (int j = 0; j < spec.getSubPartitions().size(); j++) {
                        PartitionSpec spSpec = spec.getSubPartitions().get(j);
                        spSpec.setParentPartPosi(spec.getPosition());
                        ++allPhyPartPosiCounter;
                        spSpec.setPhyPartPosition(allPhyPartPosiCounter);
                    }
                }
                spec.setPhyPartPosition(0L);
            } else {
                ++allPhyPartPosiCounter;
                spec.setPhyPartPosition(allPhyPartPosiCounter);
            }
        }
    }

    public static boolean equalsInner(
        PartitionByDefinition localPartByDef,
        List<PartitionSpec> localPartitions,
        PartitionByDefinition otherPartByDef,
        List<PartitionSpec> otherPartitions,
        PartKeyLevel partLevel,
        int prefixPartColCnt) {
        if (localPartByDef == otherPartByDef) {
            return true;
        }

        boolean allPartSpecsEqual = false;
        if (otherPartByDef != null && otherPartByDef.getClass() == localPartByDef.getClass()) {

            PartitionByDefinition local = localPartByDef;
            PartitionByDefinition other = otherPartByDef;

            if (local.getStrategy() != other.getStrategy()) {
                return false;
            }

            if (!local.getPartLevel().equals(other.getPartLevel())) {
                return false;
            }

            int partColCnt = local.getPartitionFieldList().size();
            int partColCntOther = other.getPartitionFieldList().size();
            PartitionIntFunction localPartIntFunc = local.getPartIntFunc();
            PartitionIntFunction otherPartIntFunc = other.getPartIntFunc();
            PartitionIntFunction[] localPartIntFuncArr = local.getPartFuncArr();
            PartitionIntFunction[] otherPartIntFuncArr = other.getPartFuncArr();
            PartitionStrategy localStrategy = local.getStrategy();
            List<String> localActualPartCol = getActualPartitionColumnsByPartByDefAndPartitions(local, localPartitions);
            List<String> otherActualPartCol = getActualPartitionColumnsByPartByDefAndPartitions(other, otherPartitions);
            int actualPartColCnt = localActualPartCol.size();
            int actualPartColCntOther = otherActualPartCol.size();

            int comparePartColCnt = partColCnt;
            if (prefixPartColCnt == PartitionInfoUtil.FULL_PART_COL_COUNT) {
                /***
                 * compare the full part cols
                 */
                if (partColCntOther != partColCnt) {
                    return false;
                }
            } else {
                int minPartColCnt = partColCnt > partColCntOther ? partColCntOther : partColCnt;
                int maxPartColCnt = partColCnt < partColCntOther ? partColCntOther : partColCnt;
                int maxActPartColCnt =
                    actualPartColCnt > actualPartColCntOther ? actualPartColCnt : actualPartColCntOther;
                int minActPartColCnt =
                    actualPartColCnt < actualPartColCntOther ? actualPartColCnt : actualPartColCntOther;

                if (minActPartColCnt != maxActPartColCnt) {
                    /**
                     * When the actual partColCounts of local and other are different,
                     * the specified prefixPartCol will be ignored because
                     * if the actual partColCounts are different, their partition pruning result
                     * must be different.
                     */
                    return false;
                } else {
                    /**
                     * Come here, must be minActPartColCnt=maxActPartColCnt，
                     * that means local partBy and other partBy has the same count of actual part cols.
                     */
                    if (prefixPartColCnt < maxActPartColCnt) {
                        //comparePartColCnt = maxPartColCnt;
                        comparePartColCnt = maxActPartColCnt;
                    } else {
                        if (minPartColCnt != maxPartColCnt) {
                            if (prefixPartColCnt > minPartColCnt) {
                                return false;
                            } else {
                                comparePartColCnt = prefixPartColCnt;
                            }
                        } else {
                            /**
                             * Come here, must be minPartColCnt=maxPartColCnt，
                             * that means local partBy and other partBy has the same count of full part cols.
                             */
                            if (prefixPartColCnt > minPartColCnt) {
                                comparePartColCnt = minPartColCnt;
                            } else {
                                comparePartColCnt = prefixPartColCnt;
                            }
                        }
                    }
                }
            }

            for (int i = 0; i < comparePartColCnt; i++) {
                if (!PartitionInfoUtil.partitionDataTypeEquals(local.getPartitionFieldList().get(i),
                    other.getPartitionFieldList().get(i))) {
                    return false;
                }

                if (localStrategy != PartitionStrategy.CO_HASH) {
                    if ((localPartIntFunc == null && otherPartIntFunc != null) ||
                        (localPartIntFunc != null && otherPartIntFunc == null)) {
                        return false;
                    } else if (localPartIntFunc != null) {
                        if (localPartIntFunc.getFunctionNames().length != otherPartIntFunc.getFunctionNames().length) {
                            return false;
                        } else {
                            for (int j = 0; j < localPartIntFunc.getFunctionNames().length; j++) {
                                if (localPartIntFunc.getFunctionNames()[j] == null
                                    && otherPartIntFunc.getFunctionNames()[j] != null ||
                                    localPartIntFunc.getFunctionNames()[j] != null
                                        && otherPartIntFunc.getFunctionNames()[j] == null) {
                                    return false;
                                }
                                if (!localPartIntFunc.getFunctionNames()[j]
                                    .equalsIgnoreCase(otherPartIntFunc.getFunctionNames()[j])) {
                                    return false;
                                }

                                if (!localPartIntFunc.equals(other.getPartIntFunc())) {
                                    return false;
                                }
                            }
                        }
                    }
                } else {
                    PartitionIntFunction tmpLocalPartFunc = localPartIntFuncArr[i];
                    PartitionIntFunction tmpOtherPartFunc = otherPartIntFuncArr[i];
                    if ((tmpLocalPartFunc == null && tmpOtherPartFunc != null)
                        || (tmpLocalPartFunc != null && tmpOtherPartFunc == null)) {
                        return false;
                    } else if (tmpLocalPartFunc != null && tmpOtherPartFunc != null) {
                        if (!tmpLocalPartFunc.equals(tmpOtherPartFunc)) {
                            return false;
                        }
                    }
                }
            }

            if (local.isUseSubPartTemplate() != other.isUseSubPartTemplate()) {
                return false;
            }

            List<PartitionSpec> partitionSpecs1 =
                PartitionByDefinition.getOrderedPhyPartSpecsByPartStrategy(local.getStrategy(),
                    local.getBoundSpaceComparator(), localPartitions);
            List<PartitionSpec> partitionSpecs2 =
                PartitionByDefinition.getOrderedPhyPartSpecsByPartStrategy(other.getStrategy(),
                    other.getBoundSpaceComparator(), otherPartitions);
            PartitionByDefinition.adjustPartPosiAndParentPartPosiForNewOrderPartSpecs(partitionSpecs1,
                null, false, false);
            PartitionByDefinition.adjustPartPosiAndParentPartPosiForNewOrderPartSpecs(partitionSpecs2,
                null, false, false);

            if (GeneralUtil.isNotEmpty(partitionSpecs1) && GeneralUtil.isNotEmpty(partitionSpecs2)
                && partitionSpecs1.size() == partitionSpecs2.size()) {
                if (prefixPartColCnt == PartitionInfoUtil.FULL_PART_COL_COUNT) {
                    for (int i = 0; i < partitionSpecs1.size(); i++) {
                        if (partitionSpecs1.get(i) == null || !partitionSpecs1.get(i)
                            .equalsInner(partitionSpecs2.get(i), partLevel, PartitionInfoUtil.FULL_PART_COL_COUNT)) {
                            return false;
                        }
                    }
                    allPartSpecsEqual = true;
                } else {
                    for (int i = 0; i < partitionSpecs1.size(); i++) {
                        if (partitionSpecs1.get(i) == null || !partitionSpecs1.get(i)
                            .equalsInner(partitionSpecs2.get(i), partLevel, comparePartColCnt)) {
                            return false;
                        }
                    }
                    allPartSpecsEqual = true;
                }
            } else {
                if (GeneralUtil.isEmpty(partitionSpecs1) && GeneralUtil.isEmpty(partitionSpecs2)) {
                    allPartSpecsEqual = true;
                }
            }
            return allPartSpecsEqual;
        }

        return false;
    }

    private static boolean containsPrefixPartCols(Collection<String> actualPartCols, Collection<String> allPartCols) {
        List<String> ca = actualPartCols.stream().map(String::toLowerCase).collect(Collectors.toList());
        List<String> cb = allPartCols.stream().map(String::toLowerCase).collect(Collectors.toList());
        String firstPartCol = cb.get(0);
        return ca.contains(firstPartCol);
    }

    private static boolean containsAllIgnoreCase(Collection<String> actualPartCols, Collection<String> allPartCols) {
        List<String> ca = actualPartCols.stream().map(String::toLowerCase).collect(Collectors.toList());
        List<String> cb = allPartCols.stream().map(String::toLowerCase).collect(Collectors.toList());
        return ca.containsAll(cb);
    }

    public List<PartitionSpec> getPartitions() {
        return partitions;
    }

    public void setPartitions(List<PartitionSpec> partitions) {
        this.partitions = partitions;
    }

    public List<PartitionSpec> getPhysicalPartitions() {
        if (physicalPartitionsCache == null || physicalPartitionsCache.isEmpty()) {
            synchronized (this) {
                if (physicalPartitionsCache == null || physicalPartitionsCache.isEmpty()) {
                    refreshPhysicalPartitionsCache();
                }
            }
        }
        return physicalPartitionsCache;
    }

    public List<PartitionSpec> getPhysicalPartitionsBySubPartTempName(String subpartTempName) {

        if (this.subPartitionBy == null) {
            return new ArrayList<>();
        }

        if (!this.subPartitionBy.isUseSubPartTemplate()) {
            return new ArrayList<>();
        }

        List<PartitionSpec> subPartTemps = this.subPartitionBy.getPartitions();
        boolean foundTargetSubPartTemp = false;
        for (int i = 0; i < subPartTemps.size(); i++) {
            PartitionSpec subPartTemp = subPartTemps.get(i);
            if (subPartTemp.getName().equalsIgnoreCase(subpartTempName)) {
                foundTargetSubPartTemp = true;
                break;
            }
        }
        if (!foundTargetSubPartTemp) {
            return new ArrayList<>();
        }

        List<PartitionSpec> targetPhySpecs = new ArrayList<>();
        List<PartitionSpec> phySpecs = getPhysicalPartitions();
        for (int i = 0; i < phySpecs.size(); i++) {
            PartitionSpec phySpec = phySpecs.get(i);
            if (!phySpec.isLogical() && phySpec.getTemplateName().equalsIgnoreCase(subpartTempName)) {
                targetPhySpecs.add(phySpec);
            }
        }
        return targetPhySpecs;
    }

    public Map<String, PartitionSpec> getPartNameToPhyPartMap() {
        Map<String, PartitionSpec> nameToSpecMap = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
        List<PartitionSpec> phySpecList = getPhysicalPartitions();
        for (int i = 0; i < phySpecList.size(); i++) {
            String phyPartName = phySpecList.get(i).getName();
            if (nameToSpecMap.containsKey(phyPartName)) {
                throw new TddlRuntimeException(ErrorCode.ERR_DUPLICATED_PARTITION_NAME,
                    String.format("partition name `%s` is duplicated", phyPartName));
            }
            nameToSpecMap.put(phyPartName, phySpecList.get(i));
        }
        return nameToSpecMap;
    }

    /**
     * nth starts from 1
     */
    public PartitionSpec getNthPartition(int nth) {
        return this.partitions.get(nth - 1);
    }

//    public List<String> getActualPartitionColumns() {
//        List<String> fullPartColList = this.partitionColumnNameList;
//        int fullPartColCnt = fullPartColList.size();
//        int actualPartColCnt = 0;
//        List<String> targetPartColList = new ArrayList<>();
//        if (strategy == PartitionStrategy.HASH) {
//            actualPartColCnt = fullPartColCnt;
//        } else if (strategy == PartitionStrategy.LIST || strategy == PartitionStrategy.LIST_COLUMNS) {
//            actualPartColCnt = fullPartColCnt;
//        } else {
//            boolean findActualPartColCnt = false;
//            for (int i = fullPartColCnt - 1; i > -1; i--) {
//                int partCnt = this.partitions.size();
//                for (int j = 0; j < partCnt; j++) {
//                    PartitionSpec spec = this.partitions.get(j);
//                    SingleValuePartitionBoundSpec bndSpec = (SingleValuePartitionBoundSpec) spec.getBoundSpec();
//                    PartitionBoundVal[] bndVal = bndSpec.getSingleDatum().getDatumInfo();
//                    PartitionBoundVal bndValOfOneCol = bndVal[i];
//                    PartitionBoundValueKind bndValKind = bndVal[i].getValueKind();
//
//                    if (strategy == PartitionStrategy.KEY || strategy == PartitionStrategy.HASH) {
//                        if (bndValKind != PartitionBoundValueKind.DATUM_NORMAL_VALUE) {
//                            actualPartColCnt = i + 1;
//                            findActualPartColCnt = true;
//                            break;
//                        }
//                        PartitionField valFld = bndValOfOneCol.getValue();
//                        if (valFld.longValue() != Long.MAX_VALUE) {
//                            actualPartColCnt = i + 1;
//                            findActualPartColCnt = true;
//                            break;
//                        }
//                    } else if (strategy == PartitionStrategy.RANGE || strategy == PartitionStrategy.RANGE_COLUMNS) {
//                        if (bndValKind != PartitionBoundValueKind.DATUM_MAX_VALUE) {
//                            actualPartColCnt = i + 1;
//                            findActualPartColCnt = true;
//                            break;
//                        }
//                    } else {
//                        throw new NotSupportException("Not support for list/list columns partitions");
//                    }
//                    continue;
//                }
//                if (findActualPartColCnt) {
//                    break;
//                }
//            }
//        }
//        /**
//         * actualPartColCnt must be >= 1
//         */
//        if (actualPartColCnt == 0 && fullPartColCnt > 0) {
//            actualPartColCnt = 1;
//        }
//        for (int i = 0; i < actualPartColCnt; i++) {
//            targetPartColList.add(fullPartColList.get(i));
//        }
//        return targetPartColList;
//    }

    public PartitionSpec getPartitionByPartName(String partName) {
        for (int i = 0; i < partitions.size(); i++) {
            String name = partitions.get(i).getName();
            if (partName.equalsIgnoreCase(name)) {
                return partitions.get(i);
            }
        }
        return null;
    }

//    public String normalizePartitionByInfo2(TableGroupConfig tableGroupConfig,
//                                           boolean showHashByRange,
//                                           List<Integer> allLevelPrefixPartColCnts) {
//
//        Map<Long, String> partGrpNameInfo = new HashMap<>();
//        tableGroupConfig.getPartitionGroupRecords().stream().forEach(o -> partGrpNameInfo.put(o.id, o.partition_name));
//        return normalizePartitionByInfoInner(true, partGrpNameInfo, true, showHashByRange, allLevelPrefixPartColCnts);
//    }
//    private String normalizePartitionByInfoInner(boolean usePartGroupNameAsPartName,
//                                                 Map<Long, String> partGrpNameInfo,
//                                                 boolean needSortPartitions,
//                                                 boolean showHashByRange,
//                                                 List<Integer> allLevelPrefixPartColCnts) {
//        StringBuilder sb = new StringBuilder();
//        boolean isSubPartSpec = !(this.partLevel == null || this.partLevel == PartKeyLevel.PARTITION_KEY);
//        boolean hasNextPartLevel = this.subPartitionBy != null;
//        boolean useSubPartByTemp = this.useSubPartTemplate;
//        Integer curLevelPrefixPartColCnt;
//        if (isSubPartSpec) {
//            curLevelPrefixPartColCnt = allLevelPrefixPartColCnts.get(1);
//            sb.append("SUBPARTITION BY ");
//        } else {
//            curLevelPrefixPartColCnt = allLevelPrefixPartColCnts.get(0);
//            sb.append("PARTITION BY ");
//        }
//
//        sb.append(strategy.getStrategyExplainName().toUpperCase());
//        sb.append("(");
//        int i = 0;
//        assert partitionFieldList.size() == partitionExprList.size();
//        int targetPartColCnt = partitionFieldList.size();
//        if (curLevelPrefixPartColCnt != PartitionInfoUtil.FULL_PART_COL_COUNT) {
//            targetPartColCnt = curLevelPrefixPartColCnt;
//        }
//        for (int j = 0; j < targetPartColCnt; j++) {
//            SqlNode sqlNode = partitionExprList.get(j);
//            if (i > 0) {
//                sb.append(",");
//            }
//            ColumnMeta columnMeta = partitionFieldList.get(i++);
//            if (sqlNode instanceof SqlCall) {
//                sb.append(((SqlCall) sqlNode).getOperator().getName());
//                sb.append("(");
//                sb.append(columnMeta.getField().getRelType().getSqlTypeName());
//                sb.append(")");
//            } else {
//                sb.append(columnMeta.getField().getRelType().getSqlTypeName());
//            }
//        }
//        sb.append(")\n");
//        if (hasNextPartLevel) {
//            String subPartByDef =
//                this.subPartitionBy.normalizePartitionByInfoInner(usePartGroupNameAsPartName, partGrpNameInfo,
//                    needSortPartitions, showHashByRange,
//                    allLevelPrefixPartColCnts);
//            sb.append(subPartByDef);
//        }
//
//        PartitionsNormalizationParams params = new PartitionsNormalizationParams();
//        params.setBuilder(sb);
//        params.setUsePartGroupNameAsPartName(usePartGroupNameAsPartName);
//        params.setPartGrpNameInfo(partGrpNameInfo);
//        params.setNeedSortPartitions(needSortPartitions);
//        params.setShowHashByRange(showHashByRange);
//        //params.setPrefixPartColCnt(curLevelPrefixPartColCnt);
//        params.setAllLevelPrefixPartColCnt(allLevelPrefixPartColCnts);
//        params.setPartSpecList(this.partitions);
//        params.setOrderedPartSpecList(needSortPartitions ? this.getOrderedPartitionSpecs() : this.partitions);
//        params.setUseSubPartBy(hasNextPartLevel);
//        if (this.partLevel == PartKeyLevel.PARTITION_KEY) {
//            params.setBuildSubPartByTemp(false);
//            normalizePartitions(params);
//        } else if (this.partLevel == PartKeyLevel.SUBPARTITION_KEY) {
//            if (useSubPartByTemp) {
//                params.setBuildSubPartByTemp(true);
//                normalizePartitions(params);
//                sb.append("\n");
//            }
//        }
//        return sb.toString();
//    }

    public PartitionSpec getPhysicalPartitionByPartName(String partName) {
        List<PartitionSpec> phyPartSpecList = getPhysicalPartitions();
        for (int i = 0; i < phyPartSpecList.size(); i++) {
            String name = phyPartSpecList.get(i).getName();
            if (partName.equalsIgnoreCase(name)) {
                return phyPartSpecList.get(i);
            }
        }
        return null;
    }

    public PartitionSpec getPartitionByPhyGrpAndPhyTbl(String phyGrp, String phyTbl) {
        PartitionSpec targetPartSpec = null;
        for (int i = 0; i < partitions.size(); i++) {
            PartitionSpec ps = partitions.get(i);
            String grp = ps.getLocation().getGroupKey();
            String tbl = ps.getLocation().getPhyTableName();
            if (phyGrp.equalsIgnoreCase(grp) && phyTbl.equalsIgnoreCase(tbl)) {
                targetPartSpec = ps;
                break;
            }
        }
        return targetPartSpec;
    }

    /**
     * Return both partBy col metas and subPartBy col metas
     * <pre>
     *     !!! Notice :
     *          if the cols of partBy and the cols of subPartBy have some common part columns,
     *          these common columns will be return only once.
     *
     *     e.g
     *      cols-of-a-tuple: a, b,  c,  d,  e
     *      1st-level-part-cols: c, a
     *      2nd-level-part-cols: d, c,  b
     *      the return result is
     *          c,a,d,b
     *          (the common col c will return once instead of becoming a duplicated col)
     * </pre>
     */
    public List<ColumnMeta> getFullPartitionColumnMetas() {
        List<ColumnMeta> fullPartColMetas = new ArrayList<>();
        fullPartColMetas.addAll(this.getPartitionFieldList());
        if (this.subPartitionBy != null) {
            Set<String> partColNameSet = new TreeSet<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
            partColNameSet.addAll(this.getPartitionColumnNameList());
            List<ColumnMeta> subPartColMetas = this.subPartitionBy.getFullPartitionColumnMetas();
            for (int i = 0; i < subPartColMetas.size(); i++) {
                ColumnMeta subPartCol = subPartColMetas.get(i);
                if (partColNameSet.contains(subPartCol.getName())) {
                    continue;
                }
                fullPartColMetas.add(subPartCol);
                partColNameSet.add(subPartCol.getName());
            }
        }
        return fullPartColMetas;
    }

    public List<Integer> getAllLevelFullPartColumnCounts() {
        List<Integer> results = new ArrayList<>();
        results.add(this.partitionFieldList.size());
        if (this.subPartitionBy != null) {
            results.add(this.subPartitionBy.getPartitionFieldList().size());
        } else {
            if (getPartLevel() != PartKeyLevel.PARTITION_KEY) {
                return results;
            }
            results.add(0);
        }
        return results;
    }

    public Integer getAllPartLevelCount() {
        if (this.subPartitionBy != null) {
            return 2;
        } else {
            return 1;
        }
    }

    /**
     * Return the actual partition columns of all partition level.
     * <p>
     * if a partition level has no partition definition, such broadcast/single/ not-exists subpartition,
     * its actual partition columns is empty list.
     * <p>
     * for 1st-level-partition, at least one partition bound values of its part col are not maxvalue of range or LONG.MAX of hash;
     * for 2nd-level-partition(including template and non-templated),
     * at least one subpartition bound values of its subpart col are not maxvalue of range or LONG.MAX of hash.
     *
     * @return List[0]: the actual partition columns of partition (1st-partition-level)
     * List[1]: the actual partition columns of subpartition (2nd-partition-level)
     */
    public List<List<String>> getAllLevelActualPartCols() {
        List<List<String>> result = new ArrayList<>();
        List<String> actualPartCols = getActualPartitionColumnsByPartByDefAndPartitions(this, this.getPartitions());
        result.add(actualPartCols);
        List<String> actualSubPartCols = new ArrayList<>();
        if (this.getSubPartitionBy() != null) {
            actualSubPartCols = getActualPartitionColumnsByPartByDefAndPartitions(this.getSubPartitionBy(),
                this.getPhysicalPartitions());
        }
        result.add(actualSubPartCols);
        return result;
    }

    /**
     * Return the full partition columns of all partition level.
     * <p>
     * if a partition level has no partition definition, such broadcast/single/ not-exists subpartition,
     * its actual partition columns is empty list.
     *
     * @return List[0]: the full partition columns of partition (1st-partition-level)
     * List[1]: the full partition columns of subpartition (2nd-partition-level)
     */
    public List<List<String>> getAllLevelFullPartCols() {
        List<List<String>> result = new ArrayList<>();
        List<String> fullPartCols = getPartitionColumnNameList();
        result.add(fullPartCols);
        if (this.subPartitionBy != null) {
            result.add(this.subPartitionBy.getPartitionColumnNameList());
        } else {
            if (getPartLevel() != PartKeyLevel.PARTITION_KEY) {
                return result;
            }
            result.add(Lists.newArrayList());
        }
        return result;
    }

    protected void refreshPhysicalPartitionsCache() {
        List<PartitionSpec> allPhySpecsList = new ArrayList<>();
        if (subPartitionBy == null) {
            allPhySpecsList.addAll(partitions);
        } else {
            for (int i = 0; i < partitions.size(); i++) {
                PartitionSpec p = partitions.get(i);
                allPhySpecsList.addAll(p.getSubPartitions());
            }
            this.subPartitionBy.refreshPhysicalPartitionsCache();
        }
        physicalPartitionsCache = allPhySpecsList;
        return;
    }

//    protected static PartitionRouter getPartRouter(PartitionByDefinition partitionBy) {
//
//        PartitionRouter router = null;
//        Comparator comp = partitionBy.getPruningSpaceComparator();
//        Comparator bndComp = partitionBy.getBoundSpaceComparator();
//        PartitionByDefinition partDef = partitionBy;
//        PartitionStrategy strategy = partDef.getStrategy();
//        SearchDatumHasher hasher = partitionBy.getHasher();
//
//        if (strategy.isHashed()) {
//            // Extract hash value for hash-partition
//
//            if (!strategy.isKey() || (strategy.isKey() && partitionBy.getPartitionFieldList().size() == 1)) {
//                Object[] datumArr = partDef.getPartitions().stream()
//                    .map(part -> extractHashCodeFromPartitionBound(part.getBoundSpec())).toArray();
//                router = PartitionRouter.createByHasher(strategy, datumArr, hasher, null/*use Long Comp*/);
//            } else {
//                Object[] datumArr = partDef.getPartitions().stream()
//                    .map(part -> part.getBoundSpec().getSingleDatum()).toArray();
//                router = PartitionRouter.createByHasher(strategy, datumArr, hasher, bndComp);
//            }
//
//        } else if (strategy.isRange()) {
//            Object[] datumArr = partDef.getPartitions().stream()
//                .map(part -> part.getBoundSpec().getSingleDatum()).toArray();
//            router = PartitionRouter.createByComparator(strategy, datumArr, comp);
//        } else if (strategy.isList()) {
//            TreeMap<Object, Integer> boundValPartPosiInfo = new TreeMap<>(comp);
//            boolean containDefaultPartition = false;
//            int defaultPartitionPosition = 0;
//            for (PartitionSpec ps : partDef.getPartitions()) {
//                if (ps.isDefaultPartition()) {
//                    containDefaultPartition = true;
//                    defaultPartitionPosition = ps.getPosition().intValue();
//                    continue;
//                }
//                for (SearchDatumInfo datum : ps.getBoundSpec().getMultiDatums()) {
//                    boundValPartPosiInfo.put(datum, ps.getPosition().intValue());
//                }
//            }
//            router = PartitionRouter.createByList(strategy, boundValPartPosiInfo, comp);
//            if (containDefaultPartition && router instanceof ListPartRouter) {
//                ((ListPartRouter) router).setHasDefaultPartition(true);
//                ((ListPartRouter) router).setDefaultPartitionPosition(defaultPartitionPosition);
//            }
//        } else {
//            throw new UnsupportedOperationException("partition strategy " + strategy);
//        }
//
//        return router;
//    }

    public String normalizePartitionKeyIgnoreColName(int prefixPartCol) {

        StringBuilder sb = new StringBuilder();
        sb.append(strategy.toString());
        sb.append("(");
        int i = 0;

        int fullPartCount = getPartitionColumnNameList().size();
        int partCount = fullPartCount;
        if (prefixPartCol != PartitionInfoUtil.FULL_PART_COL_COUNT) {
            if (prefixPartCol > fullPartCount) {
                partCount = fullPartCount;
            } else {
                partCount = prefixPartCol;
            }
        }
        for (int j = 0; j < partCount; j++) {
            SqlNode sqlNode = partitionExprList.get(j);
            if (i > 0) {
                sb.append(",");
            }
            ColumnMeta columnMeta = partitionFieldList.get(i++);
            if (sqlNode instanceof SqlCall) {
                sb.append(((SqlCall) sqlNode).getOperator().getName());
                sb.append("(");
                sb.append(columnMeta.getField().getRelType().getSqlTypeName());
                sb.append(")");
            } else {
                sb.append(columnMeta.getField().getRelType().getSqlTypeName());
            }
        }
        sb.append(")");
        return sb.toString();
    }

    public String normalizePartitionByDefForShowTableGroup(TableGroupConfig tableGroupConfig,
                                                           boolean showHashByRange,
                                                           List<Integer> allLevelPrefixPartColCnts) {

        PartitionByNormalizationParams partByNormParams = new PartitionByNormalizationParams();
        partByNormParams.setTextIntentBase("");
        partByNormParams.setShowHashByRange(showHashByRange);

        /**
         * partGrpId -> phyPartName
         */
        Map<Long, String> partGrpNameInfo = new HashMap<>();
        tableGroupConfig.getPartitionGroupRecords().stream().forEach(o -> partGrpNameInfo.put(o.id, o.partition_name));
        partByNormParams.setNormalizeForTableGroup(true);
        partByNormParams.setTgAllLevelPrefixPartColCnts(allLevelPrefixPartColCnts);
        partByNormParams.setPartGrpNameInfo(partGrpNameInfo);
        partByNormParams.setUsePartGroupNameAsPartName(true);
        partByNormParams.setNeedSortPartitions(true);

        return normalizePartByDefForShowCreateTable(partByNormParams);
    }

    public String normalizePartByDefForShowCreateTable(PartitionByNormalizationParams normalizationParams) {

        StringBuilder sb = new StringBuilder();
        boolean isSubPartSpec = !(this.partLevel == null || this.partLevel == PartKeyLevel.PARTITION_KEY);
        boolean hasNextPartLevel = this.subPartitionBy != null;
        boolean nextPartLevelUseTemp = hasNextPartLevel ? this.subPartitionBy.isUseSubPartTemplate() : false;
        boolean useSubPartByTemp = this.useSubPartTemplate;

        PartitionByDefinition parentPartByDef = normalizationParams.getParentPartBy();
        boolean showHashByRange = normalizationParams.isShowHashByRange();
        String textIntentBase = normalizationParams.getTextIntentBase();

        boolean normalizeForTableGroup = normalizationParams.isNormalizeForTableGroup();
        TableGroupConfig tableGroupConfig = null;
        boolean usePartGroupNameAsPartName = false;
        boolean needSortPartitions = false;
        Map<Long, String> partGrpNameInfo = null;
        List<Integer> allLevelPrefixPartColCnts = PartitionInfoUtil.ALL_LEVEL_FULL_PART_COL_COUNT_LIST;
        if (normalizeForTableGroup) {
            tableGroupConfig = normalizationParams.getTableGroupConfig();
            usePartGroupNameAsPartName = normalizationParams.isUsePartGroupNameAsPartName();
            needSortPartitions = normalizationParams.isNeedSortPartitions();
            partGrpNameInfo = normalizationParams.getPartGrpNameInfo();
            allLevelPrefixPartColCnts = normalizationParams.getTgAllLevelPrefixPartColCnts();
        }

        sb.append(textIntentBase);
        if (isSubPartSpec) {
            sb.append("SUBPARTITION BY ");
        } else {
            sb.append("PARTITION BY ");
        }

        String strategyName = strategy.getStrategyExplainName().toUpperCase();
        if (PartitionStrategy.DIRECT_HASH == strategy) {
            strategyName = PartitionStrategy.HASH.getStrategyExplainName().toUpperCase();
        }
        sb.append(strategyName);
        sb.append("(");
        int i = 0;
        assert partitionFieldList.size() == partitionExprList.size();

        Integer curLevelPrefixPartColCnt;
        if (isSubPartSpec) {
            curLevelPrefixPartColCnt = allLevelPrefixPartColCnts.get(1);
        } else {
            curLevelPrefixPartColCnt = allLevelPrefixPartColCnts.get(0);
        }
        int targetPartColCnt = partitionFieldList.size();
        if (curLevelPrefixPartColCnt != PartitionInfoUtil.FULL_PART_COL_COUNT) {
            targetPartColCnt = curLevelPrefixPartColCnt;
        }
        for (int c = 0; c < targetPartColCnt; c++) {
            SqlNode sqlNode = partitionExprList.get(c);
            if (i > 0) {
                sb.append(",");
            }
            ColumnMeta columnMeta = partitionFieldList.get(i++);
            if (normalizeForTableGroup) {
//                if (sqlNode instanceof SqlCall) {
//                    sb.append(((SqlCall) sqlNode).getOperator().getName());
//                    sb.append("(");
//                    sb.append(columnMeta.getField().getRelType().getSqlTypeName());
//                    sb.append(")");
//                } else {
//                    sb.append(columnMeta.getField().getRelType().getSqlTypeName());
//                }
                if (sqlNode instanceof SqlCall) {
                    sb.append(((SqlCall) sqlNode).getOperator().getName());
                    sb.append("(");
                    sb.append(columnMeta.getField().getRelType().getSqlTypeName());
                    if (((SqlCall) sqlNode).getOperandList().size() > 1) {
                        for (int j = 1; j < ((SqlCall) sqlNode).getOperandList().size(); j++) {
                            sb.append(",");
                            SqlNode op = ((SqlCall) sqlNode).getOperandList().get(j);
                            sb.append(op.toString());
                        }
                    }
                    sb.append(")");
                } else {
                    sb.append(columnMeta.getField().getRelType().getSqlTypeName());
                }
            } else {
                if (sqlNode instanceof SqlCall) {
                    sb.append(((SqlCall) sqlNode).getOperator().getName());
                    sb.append("(");
                    sb.append(SqlIdentifier.surroundWithBacktick(columnMeta.getName()));
                    if (((SqlCall) sqlNode).getOperandList().size() > 1) {
                        for (int j = 1; j < ((SqlCall) sqlNode).getOperandList().size(); j++) {
                            sb.append(",");
                            SqlNode op = ((SqlCall) sqlNode).getOperandList().get(j);
                            sb.append(op.toString());
                        }
                    }
                    sb.append(")");
                } else {
                    sb.append(SqlIdentifier.surroundWithBacktick((columnMeta.getName())));
                }
            }

        }
        sb.append(")\n");
        if (this.strategy == PartitionStrategy.KEY || this.strategy == PartitionStrategy.HASH
            || this.strategy == PartitionStrategy.DIRECT_HASH || this.strategy == PartitionStrategy.CO_HASH) {
            PartKeyLevel partKeyLevel = this.partLevel;
            List<PartitionSpec> partSpecList = this.partitions;
            if (partKeyLevel == PartKeyLevel.PARTITION_KEY) {
                if (!hasNextPartLevel || (hasNextPartLevel && nextPartLevelUseTemp)) {
                    /**
                     * <pre>
                     *  first-level hash partition only
                     *   or first-level hash partition with using templated-subpartitions
                     *  just show its own bound count and bound values
                     * </pre>
                     */
                    sb.append(textIntentBase);
                    sb.append("PARTITIONS ");
                    sb.append(partSpecList.size());

                    boolean moreParentPartSpecsToShow = true;
                    if (!isSubPartSpec && !hasNextPartLevel && !showHashByRange) {
                        /**
                         * For partition only (without any subpartitions)
                         */
                        moreParentPartSpecsToShow = false;
                    }
                    if (moreParentPartSpecsToShow) {
                        sb.append("\n");
                    }
                }
            } else if (partKeyLevel == PartKeyLevel.SUBPARTITION_KEY) {
                boolean buildSubPartByTemp = this.useSubPartTemplate;
                if (buildSubPartByTemp) {

                    /**
                     * <pre>
                     *  second-level templated hash subpartition
                     *  show its own bound count and bound values
                     * </pre>
                     */

                    sb.append(textIntentBase);
                    sb.append("SUBPARTITIONS ");
                    sb.append(partSpecList.size());

                    /**
                     * Check if need show the bound values of first-level partSpecs for
                     * range/list/hash(showHashByRange=true)
                     */
                    boolean moreParentPartSpecsToShow = true;
                    if (parentPartByDef != null && !needShowMoreParentPartSpecs(showHashByRange,
                        parentPartByDef.getStrategy())) {
                        moreParentPartSpecsToShow = false;
                    }
                    if (moreParentPartSpecsToShow) {
                        sb.append("\n");
                    }
                }
            }
        } else if (this.strategy == PartitionStrategy.UDF_HASH) {
            PartKeyLevel partKeyLevel = this.partLevel;
            List<PartitionSpec> partSpecList = this.partitions;
            if (partKeyLevel == PartKeyLevel.PARTITION_KEY) {
                /**
                 * <pre>
                 *  first-level udf_hash partition
                 *  must show its own bound count and bound values
                 * </pre>
                 */
                sb.append(textIntentBase);
                sb.append("PARTITIONS ");
                sb.append(partSpecList.size());
                sb.append("\n");
            } else if (partKeyLevel == PartKeyLevel.SUBPARTITION_KEY) {
                boolean buildSubPartByTemp = this.useSubPartTemplate;
                if (buildSubPartByTemp) {
                    sb.append(textIntentBase);
                    sb.append("SUBPARTITIONS ");
                    sb.append(partSpecList.size());

                    boolean moreParentPartSpecsToShow = true;
                    if (parentPartByDef != null && !needShowMoreParentPartSpecs(showHashByRange,
                        parentPartByDef.getStrategy())) {
                        moreParentPartSpecsToShow = false;
                    }
                    if (moreParentPartSpecsToShow) {
                        sb.append("\n");
                    }
                }
            }
        } else if (this.strategy == PartitionStrategy.CO_HASH) {
            PartKeyLevel partKeyLevel = this.partLevel;
            List<PartitionSpec> partSpecList = this.partitions;
            if (partKeyLevel == PartKeyLevel.PARTITION_KEY) {
                /**
                 * <pre>
                 *  first-level udf_hash partition
                 *  must show its own bound count and bound values
                 * </pre>
                 */
                sb.append(textIntentBase);
                sb.append("PARTITIONS ");
                sb.append(partSpecList.size());
                sb.append("\n");
            } else if (partKeyLevel == PartKeyLevel.SUBPARTITION_KEY) {
                boolean buildSubPartByTemp = this.useSubPartTemplate;
                if (buildSubPartByTemp) {
                    sb.append(textIntentBase);
                    sb.append("SUBPARTITIONS ");
                    sb.append(partSpecList.size());

                    boolean moreParentPartSpecsToShow = true;
                    if (parentPartByDef != null && !needShowMoreParentPartSpecs(showHashByRange,
                        parentPartByDef.getStrategy())) {
                        moreParentPartSpecsToShow = false;
                    }
                    if (moreParentPartSpecsToShow) {
                        sb.append("\n");
                    }
                }
            }
        }

        if (hasNextPartLevel) {
            PartitionByNormalizationParams subPartByNormParams = new PartitionByNormalizationParams();
            subPartByNormParams.setTextIntentBase(textIntentBase);
            subPartByNormParams.setShowHashByRange(showHashByRange);
            subPartByNormParams.setParentPartBy(this);

            subPartByNormParams.setNormalizeForTableGroup(normalizeForTableGroup);
            subPartByNormParams.setUsePartGroupNameAsPartName(usePartGroupNameAsPartName);
            subPartByNormParams.setPartGrpNameInfo(partGrpNameInfo);
            subPartByNormParams.setNeedSortPartitions(needSortPartitions);
            subPartByNormParams.setTgAllLevelPrefixPartColCnts(allLevelPrefixPartColCnts);

            String subPartByDef = this.subPartitionBy.normalizePartByDefForShowCreateTable(subPartByNormParams);
            sb.append(subPartByDef);
        }

        PartitionsNormalizationParams params = new PartitionsNormalizationParams();
        params.setBuilder(sb);
        params.setUsePartGroupNameAsPartName(usePartGroupNameAsPartName);
        params.setPartGrpNameInfo(partGrpNameInfo);
        params.setNeedSortPartitions(needSortPartitions);
        params.setShowHashByRange(showHashByRange);
        params.setAllLevelPrefixPartColCnt(allLevelPrefixPartColCnts);
        params.setPartSpecList(this.partitions);
        params.setBoundSpaceComparator(this.boundSpaceComparator);
        params.setOrderedPartSpecList(needSortPartitions ? this.getOrderedPartitionSpecs() : this.partitions);
        params.setUseSubPartBy(hasNextPartLevel);
        params.setTextIndentBase(textIntentBase);

        if (parentPartByDef != null) {
            params.setParentPartStrategy(parentPartByDef.getStrategy());
        }
        if (this.partLevel == PartKeyLevel.PARTITION_KEY) {
            params.setBuildSubPartByTemp(false);
            params.setNextPartLevelUseTemp(nextPartLevelUseTemp);
            normalizePartitions(params);
        } else if (this.partLevel == PartKeyLevel.SUBPARTITION_KEY) {
            if (useSubPartByTemp) {
                params.setBuildSubPartByTemp(true);
                params.setNextPartLevelUseTemp(nextPartLevelUseTemp);
                normalizePartitions(params);
            }
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        PartitionByNormalizationParams partByNormParams = new PartitionByNormalizationParams();
        partByNormParams.setTextIntentBase("");
        partByNormParams.setShowHashByRange(false);
        return normalizePartByDefForShowCreateTable(partByNormParams);
    }

    public PartitionByDefinition copy() {

        PartitionByDefinition newPartDef = new PartitionByDefinition();

        newPartDef.setStrategy(this.strategy);

        if (this.subPartitionBy != null) {
            newPartDef.setSubPartitionBy(this.subPartitionBy.copy());
        } else {
            newPartDef.setSubPartitionBy(null);
        }
        List<PartitionSpec> newPartitions = new ArrayList<>();
        for (int i = 0; i < this.getPartitions().size(); i++) {
            newPartitions.add(this.getPartitions().get(i).copy());
        }
        newPartDef.setPartitions(newPartitions);

        List<SqlNode> newPartitionExprList = new ArrayList<>();
        newPartitionExprList.addAll(this.getPartitionExprList());
        newPartDef.setPartitionExprList(newPartitionExprList);

        List<RelDataType> newPartitionExprTypeList = new ArrayList<>();
        newPartitionExprTypeList.addAll(this.getPartitionExprTypeList());
        newPartDef.setPartitionExprTypeList(newPartitionExprTypeList);

        List<ColumnMeta> newPartitionFieldList = new ArrayList<>();
        newPartitionFieldList.addAll(this.getPartitionFieldList());
        newPartDef.setPartitionFieldList(newPartitionFieldList);

        List<String> newPartitionColumnNameList = new ArrayList<>();
        newPartitionColumnNameList.addAll(this.getPartitionColumnNameList());
        newPartDef.setPartitionColumnNameList(newPartitionColumnNameList);

        newPartDef.setNeedEnumRange(this.isNeedEnumRange());

        newPartDef.setPruningSpaceComparator(this.getPruningSpaceComparator());
        newPartDef.setHasher(this.getHasher());
        newPartDef.setBoundSpaceComparator(this.getBoundSpaceComparator());
        newPartDef.setPartIntFuncOperator(this.getPartIntFuncOperator());
        newPartDef.setPartIntFunc(this.getPartIntFunc());
        newPartDef.setPartFuncArr(this.getPartFuncArr());
        newPartDef.setPartIntFuncMonotonicity(this.getPartIntFuncMonotonicity());
        newPartDef.setQuerySpaceComparator(this.getQuerySpaceComparator());
        newPartDef.setRouter(this.getRouter());
        newPartDef.setPartLevel(this.getPartLevel());
        newPartDef.setUseSubPartTemplate(this.isUseSubPartTemplate());

        return newPartDef;
    }

    public List<PartitionSpec> getOrderedPartitionSpecs() {
        return PartitionByDefinition.getOrderedPhyPartSpecsByPartStrategy(this.strategy, this.boundSpaceComparator,
            this.partitions);
    }

    @Override
    public int hashCode() {

        int hashCodeVal = Objects.hashCode(strategy);
        hashCodeVal ^= Objects.hashCode(partLevel);
        hashCodeVal ^= Objects.hashCode(useSubPartTemplate);
        int partColCnt = partitionExprList.size();
        for (int i = 0; i < partColCnt; i++) {
            String partColName = partitionColumnNameList.get(i);
            hashCodeVal ^= partColName.toLowerCase().hashCode();

            ColumnMeta partColMeta = partitionFieldList.get(i);
            hashCodeVal ^= partColMeta.hashCode();

            SqlNode partColExprAst = partitionExprList.get(i);
            hashCodeVal ^= partColExprAst.toString().hashCode();
        }

        if (partIntFunc != null) {
            hashCodeVal ^= partIntFunc.hashCode();
        }

        int partCnt = partitions.size();
        for (int i = 0; i < partCnt; i++) {
            PartitionSpec pSpec = partitions.get(i);
            hashCodeVal ^= pSpec.hashCode();
        }

        if (this.subPartitionBy != null) {
            hashCodeVal ^= this.subPartitionBy.hashCode();
        }

        return hashCodeVal;
    }

//    public boolean equals(Object obj,
//                          int prefixPartColCnt) {
//        if (this == obj) {
//            return true;
//        }
//
//        boolean isEqual = false;
//        if (obj != null && obj.getClass() == this.getClass()) {
//
//            PartitionByDefinition other = (PartitionByDefinition) obj;
//
//            if (strategy != other.getStrategy()) {
//                return false;
//            }
//
//            if (!partLevel.equals(other.getPartLevel())) {
//                return false;
//            }
//
//            int partColCnt = partitionExprList.size();
//            int partColCntOther = other.getPartitionColumnNameList().size();
//
//            int actualPartColCnt = partColCnt;
//            int actualPartColCntOther = partColCntOther;
//            if (prefixPartColCnt > 0) {
//                actualPartColCnt = getActualPartitionColumns().size();
//                actualPartColCntOther = other.getActualPartitionColumns().size();
//            }
//
//            int comparePartColCnt = partColCnt;
//            if (prefixPartColCnt == PartitionInfoUtil.FULL_PART_COL_COUNT) {
//                if (other.getPartitionExprList().size() != partColCnt) {
//                    return false;
//                }
//            } else {
//
//                if (actualPartColCnt != actualPartColCntOther) {
//                    return false;
//                }
//                int minPartColCnt = partColCnt > partColCntOther ? partColCntOther : partColCnt;
//                if (minPartColCnt <= prefixPartColCnt) {
//                    comparePartColCnt = minPartColCnt;
//                } else {
//                    if (prefixPartColCnt < actualPartColCnt) {
//                        comparePartColCnt = actualPartColCnt;
//                    } else {
//                        comparePartColCnt = prefixPartColCnt;
//                    }
//                }
//            }
//
//            for (int i = 0; i < comparePartColCnt; i++) {
//                if (!PartitionInfoUtil.partitionDataTypeEquals(partitionFieldList.get(i),
//                    other.getPartitionFieldList().get(i))) {
//                    return false;
//                }
//
//                if (partIntFunc == null && other.getPartIntFunc() != null ||
//                    partIntFunc != null && other.getPartIntFunc() == null) {
//                    return false;
//                } else if (partIntFunc != null) {
//                    if (partIntFunc.getFunctionNames().length != other.getPartIntFunc().getFunctionNames().length) {
//                        return false;
//                    } else {
//                        for (int j = 0; j < partIntFunc.getFunctionNames().length; j++) {
//                            if (partIntFunc.getFunctionNames()[j] == null
//                                && other.getPartIntFunc().getFunctionNames()[j] != null ||
//                                partIntFunc.getFunctionNames()[j] != null
//                                    && other.getPartIntFunc().getFunctionNames()[j] == null) {
//                                return false;
//                            }
//                            if (!partIntFunc.getFunctionNames()[j]
//                                .equalsIgnoreCase(other.getPartIntFunc().getFunctionNames()[j])) {
//                                return false;
//                            }
//                        }
//                    }
//                }
//            }
//
//            if (this.useSubPartTemplate != other.isUseSubPartTemplate()) {
//                return false;
//            }
//
//            List<PartitionSpec> partitionSpecs1 = PartitionByDefinition.getOrderedPhyPartSpecsByPartStrategy(this.strategy, this.boundSpaceComparator, this.partitions);
//            List<PartitionSpec> partitionSpecs2 = PartitionByDefinition.getOrderedPhyPartSpecsByPartStrategy(other.getStrategy(), other.getBoundSpaceComparator(), other.getPartitions());
//
//            if (GeneralUtil.isNotEmpty(partitionSpecs1) && GeneralUtil.isNotEmpty(partitionSpecs2)
//                && partitionSpecs1.size() == partitionSpecs2.size()) {
//                if (prefixPartColCnt == PartitionInfoUtil.FULL_PART_COL_COUNT) {
//                    for (int i = 0; i < partitionSpecs1.size(); i++) {
//                        if (partitionSpecs1.get(i) == null || !partitionSpecs1.get(i).equals(partitionSpecs2.get(i))) {
//                            return false;
//                        }
//                    }
//                    isEqual = true;
//                } else {
//                    for (int i = 0; i < partitionSpecs1.size(); i++) {
//                        if (partitionSpecs1.get(i) == null || !partitionSpecs1.get(i)
//                            .equalsInner(partitionSpecs2.get(i), PartKeyLevel.BOTH_PART_SUBPART_KEY, comparePartColCnt)) {
//                            return false;
//                        }
//                    }
//                    isEqual = true;
//                }
//            } else if (!useSubPartTemplate && GeneralUtil.isEmpty(partitionSpecs1) && GeneralUtil.isEmpty(partitionSpecs2)) {
//                isEqual = true;
//            }
//
//
//            if (isEqual) {
//
//                PartitionByDefinition partByDef = (PartitionByDefinition) obj;
//                PartitionByDefinition localSubPartBy = this.getSubPartitionBy();
//                PartitionByDefinition tarSubPartBy = partByDef.getSubPartitionBy();
//
//                if (localSubPartBy == null && tarSubPartBy == null) {
//                    return true;
//                }
//
//                if (localSubPartBy != null && tarSubPartBy == null) {
//                    return false;
//                }
//
//                if (localSubPartBy == null && tarSubPartBy != null) {
//                    return false;
//                }
//
//                if (!localSubPartBy.equals(tarSubPartBy, prefixPartColCnt)) {
//                    return false;
//                }
//
//                return true;
//            }
//        }
//
//        return false;
//    }

    @Override
    public boolean equals(Object obj) {
        return equals(obj, PartitionInfoUtil.ALL_LEVEL_FULL_PART_COL_COUNT_LIST);
        //return equals(obj, -1);
    }

    /**
     *
     */
    public boolean equals(Object obj,
                          List<Integer> allLevelPrefixPartCnts) {
        if (this == obj) {
            return true;
        }

        boolean isEqual = false;
        if (obj != null && obj.getClass() == this.getClass()) {
            PartitionByDefinition local = this;
            PartitionByDefinition other = (PartitionByDefinition) obj;
            int prefixPartColCnt = allLevelPrefixPartCnts.get(0);
            isEqual = equalsInner(local, getPartitions(), other, other.getPartitions(), PartKeyLevel.PARTITION_KEY,
                prefixPartColCnt);
            if (isEqual) {
                PartitionByDefinition localSubPartBy = local.getSubPartitionBy();
                PartitionByDefinition otherSubPartBy = other.getSubPartitionBy();
                if ((localSubPartBy == null && otherSubPartBy != null) ||
                    (localSubPartBy != null && otherSubPartBy == null)) {
                    isEqual = false;
                } else if (localSubPartBy == null && otherSubPartBy == null) {
                    isEqual = true;
                } else {
                    if (localSubPartBy.isUseSubPartTemplate() != otherSubPartBy.isUseSubPartTemplate()) {
                        isEqual = false;
                    } else {
                        int prefixSubPartColCnt = allLevelPrefixPartCnts.get(1);
                        if (localSubPartBy.isUseSubPartTemplate()) {
                            isEqual = equalsInner(localSubPartBy,
                                PartitionInfoUtil.updateTemplatePartitionSpec(localSubPartBy.getPartitions(),
                                    localSubPartBy.getStrategy()), otherSubPartBy,
                                PartitionInfoUtil.updateTemplatePartitionSpec(otherSubPartBy.getPartitions(),
                                    otherSubPartBy.getStrategy()), PartKeyLevel.SUBPARTITION_KEY, prefixSubPartColCnt);
                        } else {
                            List<PartitionSpec> localPartitionSpecs =
                                PartitionByDefinition.getOrderedPhyPartSpecsByPartStrategy(local.getStrategy(),
                                    local.getBoundSpaceComparator(), local.getPartitions());
                            List<PartitionSpec> otherPartitionSpecs =
                                PartitionByDefinition.getOrderedPhyPartSpecsByPartStrategy(other.getStrategy(),
                                    other.getBoundSpaceComparator(), other.getPartitions());
                            for (int i = 0; i < localPartitionSpecs.size(); i++) {
                                isEqual = equalsInner(localSubPartBy, localPartitionSpecs.get(i).getSubPartitions(),
                                    otherSubPartBy,
                                    otherPartitionSpecs.get(i).getSubPartitions(), PartKeyLevel.SUBPARTITION_KEY,
                                    prefixSubPartColCnt);
                                if (!isEqual) {
                                    break;
                                }
                            }

                        }
                    }
                }
            }
            return isEqual;
        }
        return false;
    }

    /**
     * Types of partition columns
     */
    public List<DataType> getPartitionColumnTypeList() {
        return this.partitionFieldList.stream().map(ColumnMeta::getDataType).collect(Collectors.toList());
    }

    public MySQLIntervalType getIntervalType() {
        if (partIntFunc != null) {
            return partIntFunc.getIntervalType();
        }
        return null;
    }

    public boolean canPerformPruning(List<String> actualUsedPartCols) {
        List<String> allPartCols = this.partitionColumnNameList;
        if (this.strategy == PartitionStrategy.KEY || this.strategy == PartitionStrategy.RANGE_COLUMNS) {
            return containsPrefixPartCols(actualUsedPartCols, allPartCols);
        } else {
            return containsAllIgnoreCase(actualUsedPartCols, allPartCols);
        }
    }

    public PartKeyLevel getPhysicalPartLevel() {
        if (this.subPartitionBy != null) {
            return PartKeyLevel.SUBPARTITION_KEY;
        } else {
            return PartKeyLevel.PARTITION_KEY;
        }
    }

    public PartKeyLevel getPartLevel() {
        return partLevel;
    }

    public void setPartLevel(PartKeyLevel partLevel) {
        this.partLevel = partLevel;
    }

    public PartitionByDefinition getSubPartitionBy() {
        return subPartitionBy;
    }

    public void setSubPartitionBy(PartitionByDefinition subPartitionBy) {
        this.subPartitionBy = subPartitionBy;
    }

    public boolean isUseSubPartTemplate() {
        return useSubPartTemplate;
    }

    public void setUseSubPartTemplate(boolean useSubPartTemplate) {
        this.useSubPartTemplate = useSubPartTemplate;
    }

    /**
     * The Compare Key of a bound value
     */
    protected static class PartSpecBoundValCompKey {
        protected final Long parentSpecPosi; // position of parent parent
        protected final SearchDatumInfo bndValDatum;

        protected PartSpecBoundValCompKey(PartitionSpec currSpec,
                                          SearchDatumInfo currSpecBndVal) {
            this.parentSpecPosi = currSpec.getParentPartPosi();
            this.bndValDatum = currSpecBndVal;
        }

        public SearchDatumInfo getBndValDatum() {
            return bndValDatum;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) {
                return true;
            }
            if (!(obj instanceof PartSpecBoundValCompKey)) {
                return false;
            }
            PartSpecBoundValCompKey other = (PartSpecBoundValCompKey) obj;
            if (!this.parentSpecPosi.equals(other.getParentSpecPosi())) {
                return false;
            }
            return this.bndValDatum.equals(obj);
        }

        public Long getParentSpecPosi() {
            return parentSpecPosi;
        }
    }

    protected static class PartSpecComparator implements Comparator<PartSpecBoundValCompKey> {
        protected SearchDatumComparator bndSpaceComp = null;

        protected PartSpecComparator(SearchDatumComparator bndSpaceComparator) {
            bndSpaceComp = bndSpaceComparator;
        }

        @Override
        public int compare(PartSpecBoundValCompKey o1, PartSpecBoundValCompKey o2) {

            if (o1 == o2) {
                return 0;
            }

            Long o1ParentPosi = o1.getParentSpecPosi();
            Long o2ParentPosi = o2.getParentSpecPosi();
            /**
             * Check if their parent partition are the same
             */
            if (o1ParentPosi > o2ParentPosi) {
                return 1;
            } else if (o1ParentPosi < o2ParentPosi) {
                return -1;
            }

            SearchDatumInfo o1Datum = o1.getBndValDatum();
            SearchDatumInfo o2Datum = o2.getBndValDatum();
            int ret = bndSpaceComp.compare(o1Datum, o2Datum);
            return ret;
        }
    }
}
