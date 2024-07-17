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

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.gms.locality.LocalityDesc;
import com.alibaba.polardbx.gms.partition.ExtraFieldJSON;
import com.alibaba.polardbx.gms.partition.TablePartitionRecord;
import com.alibaba.polardbx.optimizer.locality.LocalityInfoUtils;
import com.alibaba.polardbx.optimizer.partition.boundspec.PartitionBoundSpec;
import com.alibaba.polardbx.optimizer.partition.common.PartKeyLevel;
import com.alibaba.polardbx.optimizer.partition.common.PartSpecNormalizationParams;
import com.alibaba.polardbx.optimizer.partition.common.PartitionStrategy;
import com.alibaba.polardbx.optimizer.partition.common.PartitionsNormalizationParams;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionRouter;
import com.alibaba.polardbx.gms.util.PartitionNameUtil;
import com.alibaba.polardbx.optimizer.partition.pruning.SearchDatumComparator;
import org.apache.calcite.sql.SqlIdentifier;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The definition of one (sub)partition
 *
 * @author chenghui.lch
 */
public class PartitionSpec extends PartSpecBase {

    /**
     * the definition of next level partition of current level partition
     */
    protected List<PartitionSpec> subPartitions;

    /**
     * the router of next levle partition of current level partition
     */
    protected PartitionRouter subRouter;

    public PartitionSpec() {
        this.subPartitions = new ArrayList<>();
        this.isDefaultPartition = false;
    }

    public PartitionSpec copy() {

        PartitionSpec spec = new PartitionSpec();
        spec.setStrategy(this.strategy);
        spec.setId(this.id);
        spec.setParentId(this.parentId);
        spec.setPosition(this.position);
        spec.setPhyPartPosition(this.phyPartPosition);
        spec.setParentPartPosi(this.parentPartPosi);
        spec.setName(this.name);
        spec.setComment(this.comment);
        spec.setEngine(this.engine);
        spec.setStatus(this.status);
        spec.setExtras(this.extras);
        spec.setFlags(this.flags);
        spec.setVersion(this.version);
        spec.setMaxValueRange(this.isMaxValueRange);
        spec.setBoundSpaceComparator(this.boundSpaceComparator);
        spec.setDefaultPartition(this.isDefaultPartition);
        spec.setPartLevel(this.partLevel);
        spec.setLogical(this.isLogical);
        spec.setSpecTemplate(this.isSpecTemplate);
        spec.setTemplateName(this.templateName);
        spec.setIntraGroupConnKey(this.intraGroupConnKey);

        if (this.location != null) {
            spec.setLocation(this.location.copy());
        } else {
            spec.setLocation(null);
        }
        spec.setLocality(this.locality);

        spec.setBoundSpec(this.boundSpec.copy());
        List<PartitionSpec> newSubPartitions = new ArrayList<>();
        for (int i = 0; i < this.subPartitions.size(); i++) {
            newSubPartitions.add(this.getSubPartitions().get(i).copy());
        }
        spec.setSubPartitions(newSubPartitions);

        return spec;

    }

    public List<PartitionSpec> getSubPartitions() {
        return subPartitions;
    }

    public void setSubPartitions(List<PartitionSpec> subPartitions) {
        this.subPartitions = subPartitions;
    }

    public static String normalizePartSpec(PartSpecNormalizationParams params) {

        boolean usePartGroupNameAsPartName = params.isUsePartGroupNameAsPartName();
        String partGrpName = params.getPartGrpName();
        boolean needSortBoundValues = params.isNeedSortBoundValues();
        List<Integer> allLevelPrefixPartColCnts = params.getAllLevelPrefixPartColCnts();
        Map<Long, String> partGrpNameInfo = params.getPartGrpNameInfo();
        boolean needSortPartitions = params.isNeedSortPartitions();
        boolean showHashByRange = params.isShowHashByRange();
        SearchDatumComparator boundSpaceComparator = params.getBoundSpaceComparator();
        PartitionSpec partSpec = params.getPartSpec();
        PartitionStrategy partStrategy = partSpec.getStrategy();
        PartKeyLevel partKeyLevel = partSpec.getPartLevel();
        boolean useSubPart = allLevelPrefixPartColCnts.size() == 2;
        boolean isLogical = partSpec.isLogical();
        boolean isSpecTemplate = partSpec.isSpecTemplate();
        boolean useSubPartByTemp = partSpec.isUseSpecTemplate();
        String textIntendBase = params.getTextIntentBase();
        int prefixPartColCnt;

        StringBuilder sb = new StringBuilder();
        boolean isSubPartSpec = !(partKeyLevel == null || partKeyLevel == PartKeyLevel.PARTITION_KEY);
        if (isSubPartSpec) {
            prefixPartColCnt = allLevelPrefixPartColCnts.get(1);
            sb.append("SUBPARTITION ");
        } else {
            prefixPartColCnt = allLevelPrefixPartColCnts.get(0);
            sb.append("PARTITION ");
        }

        if (!usePartGroupNameAsPartName) {
            String partName = SqlIdentifier.surroundWithBacktick(partSpec.getName());
            sb.append(partName);
        } else {
            if (partGrpName != null) {
                sb.append(partGrpName);
            }
        }
        int bndValPrefixColCnt = prefixPartColCnt;
        boolean needExpandBoundVal = true;
        switch (partStrategy) {
        case HASH:
        case DIRECT_HASH:
        case KEY:
            if (partStrategy == PartitionStrategy.HASH || partStrategy == PartitionStrategy.DIRECT_HASH) {
                /**
                 * Because the length of all the bound values of hash is 1
                 */
                bndValPrefixColCnt = 1;
            }
            if (!showHashByRange) {
                needExpandBoundVal = false;
                break;
            }
            sb.append(" VALUES LESS THAN (");
            break;
        case UDF_HASH:
            /**
             * Because the length of all the bound values of udf_hash is always 1
             */
            bndValPrefixColCnt = 1;
            /**
             * show the full bound values of udf_hash
             */
            needExpandBoundVal = true;
            sb.append(" VALUES LESS THAN (");
            break;
        case CO_HASH:
            /**
             * Because the length of all the bound values of co_hash is always 1
             */
            bndValPrefixColCnt = 1;
            /**
             * show the full bound values of co_hash
             */
            if (!showHashByRange) {
                needExpandBoundVal = false;
                break;
            }
            sb.append(" VALUES LESS THAN (");
            break;
        case RANGE:
        case RANGE_COLUMNS:
            sb.append(" VALUES LESS THAN (");
            break;
        case LIST:
        case LIST_COLUMNS:
            sb.append(" VALUES IN (");
            break;
        }
        if (needExpandBoundVal) {
            PartitionBoundSpec tmpBoundSpec = partSpec.getBoundSpec();
            if (needSortBoundValues) {
                tmpBoundSpec = PartitionSpec.sortPartitionsAllValues(partSpec.copy());
            }
            sb.append(tmpBoundSpec.getPartitionBoundDescription(bndValPrefixColCnt));
            sb.append(")");
        }

        if (!isLogical) {
            /**
             * <pre>
             * Current partSpec to be show is a physical partSpec, maybe:
             *  current partSpec is a partition without using subpartitions;
             *  current partSpec is a subpartition;
             *  </pre>
             */
            if (TStringUtil.isNotEmpty(partSpec.getEngine())) {
                sb.append(" ENGINE = ");
                sb.append(partSpec.getEngine());
            }
            if (TStringUtil.isNotEmpty(partSpec.getLocality())) {
                sb.append(" LOCALITY=").append(TStringUtil.quoteString(partSpec.getLocality()));
            }
        } else {
            if (isSpecTemplate) {
                /**
                 * <pre>
                 * Current partSpec to be show  is a logical partSpec of subpartition template.
                 * </pre>
                 */
                if (TStringUtil.isNotEmpty(partSpec.getEngine())) {
                    sb.append(" ENGINE = ");
                    sb.append(partSpec.getEngine());
                }
                if (TStringUtil.isNotEmpty(partSpec.getLocality())) {
                    sb.append(" LOCALITY=").append(TStringUtil.quoteString(partSpec.getLocality()));
                }
            } else {
                if (!useSubPartByTemp) {
                    /**
                     * <pre>
                     * Current partSpec to be show is a logical partSpec
                     *  of first-level partition with some non-templated subpartitions,
                     * so the bound values of first-level partition to be show need split to two steps:
                     *  1. show the bound values of a first-level partition;
                     *  2. show all bound values of all second-level subpartition of current first-level partition.
                     * </pre>
                     */
                    if (TStringUtil.isNotEmpty(partSpec.getLocality())) {
                        sb.append(" LOCALITY=").append(TStringUtil.quoteString(partSpec.getLocality()));
                    }
                    List<PartitionSpec> subPartSpecList = partSpec.getSubPartitions();
                    List<PartitionSpec> orderedSubPartSpecList = null;
                    PartitionStrategy subPartStrategy = partStrategy;
                    if (useSubPart) {
                        subPartStrategy = subPartSpecList.get(0).getStrategy();
                    }
                    if (needSortBoundValues) {
                        orderedSubPartSpecList =
                            PartitionByDefinition.getOrderedPhyPartSpecsByPartStrategy(subPartStrategy,
                                boundSpaceComparator, subPartSpecList);
                    }

                    sb.append("\n");
                    PartitionsNormalizationParams normalSubPartSpecsParams = new PartitionsNormalizationParams();
                    normalSubPartSpecsParams.setBuilder(sb);
                    normalSubPartSpecsParams.setUsePartGroupNameAsPartName(usePartGroupNameAsPartName);
                    normalSubPartSpecsParams.setPartGrpNameInfo(partGrpNameInfo);
                    normalSubPartSpecsParams.setNeedSortPartitions(needSortPartitions);
                    normalSubPartSpecsParams.setShowHashByRange(showHashByRange);
                    //normalSubPartSpecsParams.setPrefixPartColCnt(prefixPartColCnt);
                    normalSubPartSpecsParams.setAllLevelPrefixPartColCnt(allLevelPrefixPartColCnts);
                    normalSubPartSpecsParams.setPartSpecList(subPartSpecList);
                    normalSubPartSpecsParams.setOrderedPartSpecList(orderedSubPartSpecList);
                    normalSubPartSpecsParams.setTextIndent(" ");
                    normalSubPartSpecsParams.setTextIndentBase(textIntendBase);
                    PartitionByDefinition.normalizePartitions(normalSubPartSpecsParams);
                } else {
                    /**
                     * <pre>
                     * Current partSpec to be show is a logical
                     *  partSpec of first-level partition with using templated subpartitions,
                     *  so just show its bound of first-level partition
                     *  (becase all the bound values of  templated subpartitions have finish showing
                     *      before showing first-level partition bounds).
                     * </pre>
                     */
                    if (TStringUtil.isNotEmpty(partSpec.getEngine())) {
                        sb.append(" ENGINE = ");
                        sb.append(partSpec.getEngine());
                    }
                    if (TStringUtil.isNotEmpty(partSpec.getLocality())) {
                        sb.append(" LOCALITY=").append(TStringUtil.quoteString(partSpec.getLocality()));
                    }
                }
            }
        }

        return sb.toString();
    }

    protected static PartitionBoundSpec sortPartitionsAllValues(PartitionSpec newSpecCopy) {
        PartitionSpec newSpec = newSpecCopy;
        if (newSpec.isDefaultPartition()) {
            return newSpec.getBoundSpec();
        } else {
            if (newSpec.getStrategy() != PartitionStrategy.LIST
                && newSpec.getStrategy() != PartitionStrategy.LIST_COLUMNS) {
                return newSpec.getBoundSpec();
            }
            PartitionBoundSpec newSortedValsBndSpec =
                PartitionByDefinition.sortListPartitionsAllValues(newSpec.getBoundSpaceComparator(),
                    newSpec.getBoundSpec());
            return newSortedValsBndSpec;
        }
    }

    public PartitionSpec getSubPartitionBySubPartName(String subPartName) {
        for (int i = 0; i < subPartitions.size(); i++) {
            String name = subPartitions.get(i).getName();
            if (subPartName.equalsIgnoreCase(name)) {
                return subPartitions.get(i);
            }
        }
        return null;
    }

    public PartitionSpec getNthSubPartition(int nth) {
        return this.subPartitions.get(nth - 1);
    }

    @Override
    public int hashCode() {
        int hashCodeVal = id.intValue();
        hashCodeVal ^= parentId.intValue();
        hashCodeVal ^= position.intValue();
        hashCodeVal ^= name.toLowerCase().hashCode();
        hashCodeVal ^= engine.toLowerCase().hashCode();
        hashCodeVal ^= status.intValue();
        hashCodeVal ^= version.intValue();
        hashCodeVal ^= strategy.hashCode();
        hashCodeVal ^= boundSpec.hashCode();
        if (templateName != null) {
            hashCodeVal ^= Objects.hashCode(templateName.toUpperCase());
        }
        hashCodeVal ^= Objects.hashCode(parentPartPosi);
        hashCodeVal ^= Objects.hashCode(partLevel);
        hashCodeVal ^= Objects.hashCode(isSpecTemplate);
        hashCodeVal ^= Objects.hashCode(isLogical);
        hashCodeVal ^= Objects.hashCode(isDefaultPartition);
        if (location != null) {
            hashCodeVal ^= location.hashCode();
        }
        if (locality != null) {
            hashCodeVal ^= Objects.hashCode(locality);
        }
        if (!subPartitions.isEmpty()) {
            for (int i = 0; i < subPartitions.size(); i++) {
                hashCodeVal ^= subPartitions.get(i).hashCode();
            }
        }
        return hashCodeVal;

    }

    @Override
    public boolean equals(Object obj) {
        return equalsInner(obj, PartKeyLevel.BOTH_PART_SUBPART_KEY, PartitionInfoUtil.FULL_PART_COL_COUNT);
    }

    public boolean equalsInner(Object obj, PartKeyLevel partLevel, int prefixPartColCnt) {
        if (this == obj) {
            return true;
        }

        boolean isEqual = false;
        if (obj != null && boundSpec != null && obj.getClass() == this.getClass()) {

            PartitionSpec other = (PartitionSpec) obj;
            if (this.strategy != other.getStrategy()) {
                return false;
            }

            if (!this.name.equals(other.getName())) {
                return false;
            }

            if (this.isLogical != other.isLogical()) {
                return false;
            }

            if (this.isSpecTemplate != other.isSpecTemplate()) {
                return false;
            }

            if (!this.position.equals(other.getPosition())) {
                return false;
            }

            if (!this.parentPartPosi.equals(other.getParentPartPosi())) {
                return false;
            }

//            Long otherPhyPosi = other.getPhyPartPosition();
//            if (this.phyPartPosition != null && other.getPhyPartPosition() != null) {
//                if (!this.phyPartPosition.equals(other.getPhyPartPosition())) {
//                    return false;
//                }
//            } else if (this.phyPartPosition != null && otherPhyPosi == null) {
//                return false;
//            } else if (this.phyPartPosition == null && otherPhyPosi != null) {
//                return false;
//            }

            if (!LocalityInfoUtils.equals(this.locality, other.getLocality())) {
                return false;
            }

            if (prefixPartColCnt == PartitionInfoUtil.FULL_PART_COL_COUNT) {
                isEqual = boundSpec.equals(((PartitionSpec) obj).getBoundSpec());
            } else {
                if (strategy == PartitionStrategy.HASH || strategy == PartitionStrategy.DIRECT_HASH
                    || strategy == PartitionStrategy.CO_HASH) {
                    /**
                     * Both single-part-columns hash or mulit-part-columns hash, their bound value col has only one,
                     * and not support prefix partition column comparing, so must use full-part-col equal method
                     */

                    isEqual = boundSpec.equals(((PartitionSpec) obj).getBoundSpec());
                } else {
                    isEqual = boundSpec.equals(((PartitionSpec) obj).getBoundSpec(), prefixPartColCnt);
                }
            }
        }

        if (isEqual) {
            if (partLevel != PartKeyLevel.BOTH_PART_SUBPART_KEY) {
                return true;
            }
            PartitionSpec tarPartSpec = (PartitionSpec) obj;
            List<PartitionSpec> localSubPartSpecList = this.getSubPartitions();
            List<PartitionSpec> tarSubPartSpecList = tarPartSpec.getSubPartitions();

            if (localSubPartSpecList == null && tarSubPartSpecList == null) {
                return true;
            }

            if (localSubPartSpecList != null && tarSubPartSpecList == null) {
                return false;
            }

            if (localSubPartSpecList == null && tarSubPartSpecList != null) {
                return false;
            }

            if (!this.getStrategy().equals(tarPartSpec.getStrategy())) {
                return false;
            }

            if (localSubPartSpecList.size() != tarSubPartSpecList.size()) {
                return false;
            }
            if (GeneralUtil.isNotEmpty(localSubPartSpecList) && GeneralUtil.isNotEmpty(tarSubPartSpecList)
                && localSubPartSpecList.get(0).getStrategy() != tarSubPartSpecList.get(0).getStrategy()) {
                return false;
            }

            PartitionStrategy localPartitionStrategy =
                GeneralUtil.isNotEmpty(localSubPartSpecList) ? localSubPartSpecList.get(0).getStrategy() :
                    this.getStrategy();
            PartitionStrategy tarPartitionStrategy =
                GeneralUtil.isNotEmpty(tarSubPartSpecList) ? tarSubPartSpecList.get(0).getStrategy() :
                    tarPartSpec.getStrategy();
            List<PartitionSpec> orderedLocalSubPartSpecList =
                PartitionByDefinition.getOrderedPhyPartSpecsByPartStrategy(localPartitionStrategy,
                    this.getBoundSpaceComparator(), localSubPartSpecList);
            List<PartitionSpec> orderedTargetSubPartSpecList =
                PartitionByDefinition.getOrderedPhyPartSpecsByPartStrategy(tarPartitionStrategy,
                    tarPartSpec.getBoundSpaceComparator(), tarSubPartSpecList);

            PartitionByDefinition.adjustPartPosiAndParentPartPosiForNewOrderPartSpecs(orderedLocalSubPartSpecList,
                null, true, false);
            PartitionByDefinition.adjustPartPosiAndParentPartPosiForNewOrderPartSpecs(orderedTargetSubPartSpecList,
                null, true, false);

            for (int i = 0; i < orderedLocalSubPartSpecList.size(); i++) {
                PartitionSpec localSubPartSpec = orderedLocalSubPartSpecList.get(i);
                PartitionSpec tarSubPartSpec = orderedTargetSubPartSpecList.get(i);
                if (!localSubPartSpec.equalsInner(tarSubPartSpec, partLevel, prefixPartColCnt)) {
                    return false;
                }
            }
            return true;
        }
        return false;

    }

    @Override
    public String toString() {
        PartSpecNormalizationParams normalSpecParams = new PartSpecNormalizationParams();
        normalSpecParams.setUsePartGroupNameAsPartName(false);
        normalSpecParams.setPartGrpName(null);
        //normalSpecParams.setPrefixPartColCnt(PartitionInfoUtil.FULL_PART_COL_COUNT);
        normalSpecParams.setAllLevelPrefixPartColCnts(PartitionInfoUtil.ALL_LEVEL_FULL_PART_COL_COUNT_LIST);
        normalSpecParams.setPartSpec(this);
        normalSpecParams.setNeedSortBoundValues(false);
        normalSpecParams.setPartGrpNameInfo(null);
        normalSpecParams.setShowHashByRange(false);
        normalSpecParams.setBoundSpaceComparator(boundSpaceComparator);
        normalSpecParams.setNeedSortPartitions(false);
        normalSpecParams.setUseSubPartByTemp(false);
        normalSpecParams.setTextIntentBase("");
        return normalizePartSpec(normalSpecParams);
    }

    public String getDigest() {
        StringBuilder sb = new StringBuilder();

        PartSpecNormalizationParams normalSpecParams = new PartSpecNormalizationParams();
        normalSpecParams.setUsePartGroupNameAsPartName(false);
        normalSpecParams.setPartGrpName(null);
        //normalSpecParams.setPrefixPartColCnt(PartitionInfoUtil.FULL_PART_COL_COUNT);
        normalSpecParams.setAllLevelPrefixPartColCnts(PartitionInfoUtil.ALL_LEVEL_FULL_PART_COL_COUNT_LIST);
        normalSpecParams.setPartSpec(this);
        normalSpecParams.setNeedSortBoundValues(false);
        normalSpecParams.setPartGrpNameInfo(null);
        normalSpecParams.setShowHashByRange(false);
        normalSpecParams.setBoundSpaceComparator(boundSpaceComparator);
        normalSpecParams.setNeedSortPartitions(false);
        normalSpecParams.setUseSubPartByTemp(false);
        sb.append(" partitionSpec:[");
        sb.append(normalizePartSpec(normalSpecParams));
        sb.append(",");
        if (!isLogical) {
            sb.append(this.getLocation().getDigest());
        }
        sb.append("]");
        return sb.toString();
    }

    public PartitionRouter getSubRouter() {
        return subRouter;
    }

    public void setSubRouter(PartitionRouter subRouter) {
        this.subRouter = subRouter;
    }

}
