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

package com.alibaba.polardbx.optimizer.partition.pruning;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.optimizer.partition.PartitionByDefinition;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoUtil;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.partition.common.BitSetLevel;
import com.alibaba.polardbx.optimizer.partition.common.PartKeyLevel;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

/**
 * @author chenghui.lch
 */
public class PartPrunedResult {
    protected PartitionInfo partInfo;
    protected BitSet partBitSet;
    /**
     * Label the part level that the bitset belongs to
     */
    protected BitSetLevel bitSetLevel;
    protected Integer parentSpecPosi;
    protected volatile List<PhysicalPartitionInfo> cache;
    protected boolean useSubPart = false;

    private PartPrunedResult(PartitionInfo partInfo,
                             BitSet partBitSet,
                             PartKeyLevel partLevel,
                             Integer parentSpecPosi,
                             boolean useFullSubPartBitSet) {
        this.partInfo = partInfo;
        this.partBitSet = partBitSet;
        this.bitSetLevel = BitSetLevel.getBitSetLevelByPartLevel(partLevel, useFullSubPartBitSet);
        this.useSubPart = partInfo.getPartitionBy().getSubPartitionBy() != null;
        this.parentSpecPosi = parentSpecPosi;
    }

    private PartPrunedResult() {
    }

    public static PartPrunedResult buildPartPrunedResult(PartitionInfo partInfo,
                                                         BitSet partBitSet,
                                                         PartKeyLevel partLevel,
                                                         Integer parentSpecPosi,
                                                         boolean useFullSubPartBitSet) {
        return new PartPrunedResult(partInfo, partBitSet, partLevel, parentSpecPosi, useFullSubPartBitSet);
    }

    public PartPrunedResult copy() {
        PartPrunedResult newRs = new PartPrunedResult();
        newRs.setPartInfo(this.partInfo);
        BitSet newBs = new BitSet(this.partBitSet.length());
        newBs.or(this.partBitSet);
        newRs.setPartBitSet(newBs);
        newRs.setBitSetLevel(this.bitSetLevel);
        newRs.setParentSpecPosi(this.parentSpecPosi);
        return newRs;
    }

    @Override
    public String toString() {
        List<String> partNameSet = new ArrayList<>();
        List<PartitionSpec> partitions = null;
        switch (bitSetLevel) {
        case BIT_SET_ALL_SUBPART:
            partitions = partInfo.getPartitionBy().getPhysicalPartitions();
            break;
        case BIT_SET_ONE_SUBPART:
            partitions = partInfo.getPartitionBy().getPartitions().get(parentSpecPosi - 1).getSubPartitions();
            break;
        default:
            partitions = partInfo.getPartitionBy().getPartitions();
            break;
        }

        int partCnt = partitions.size();
        for (int i = partBitSet.nextSetBit(0); i >= 0; i = partBitSet.nextSetBit(i + 1)) {
            if (i >= partCnt) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                    "Find pruned partition error");
            }
            PartitionSpec ps = partitions.get(i);
            partNameSet.add(ps.getName());
        }
        StringBuilder resultSb = new StringBuilder("");
        resultSb.append(bitSetLevel.getBitSetLevelName());
        resultSb.append("{");
        resultSb.append(String.join(",", partNameSet));
        resultSb.append("}");
        return resultSb.toString();
    }

    public String toAllPhyPartBitString() {
        List<String> phyPartNameSet = new ArrayList<>();
        BitSet allPhyBitSet = getPhysicalPartBitSet();
        BitSetLevel phyBitSetLevel =
            BitSetLevel.getBitSetLevelByPartLevel(partInfo.getPartitionBy().getPhysicalPartLevel(), true);
        List<PartitionSpec> phyPartSpecs = partInfo.getPartitionBy().getPhysicalPartitions();
        int partCnt = phyPartSpecs.size();
        for (int i = allPhyBitSet.nextSetBit(0); i >= 0; i = allPhyBitSet.nextSetBit(i + 1)) {
            if (i >= partCnt) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                    "Find pruned partition error");
            }
            PartitionSpec phySpec = phyPartSpecs.get(i);
            phyPartNameSet.add(phySpec.getName());

        }
        StringBuilder resultSb = new StringBuilder("");
        resultSb.append(phyBitSetLevel.getBitSetLevelName());
        resultSb.append("{");
        resultSb.append(String.join(",", phyPartNameSet));
        resultSb.append("}");
        return resultSb.toString();
    }

    public String getLogicalTableName() {
        return partInfo.getTableName();
    }

    public BitSet getPartBitSet() {
        return partBitSet;
    }

    public BitSet getPhysicalPartBitSet() {
        if (bitSetLevel == BitSetLevel.BIT_SET_ALL_SUBPART) {
            return partBitSet;
        } else if (bitSetLevel == BitSetLevel.BIT_SET_PART) {
            if (!useSubPart) {
                return partBitSet;
            }
            BitSet resultBitSet = PartitionPrunerUtils.buildEmptyPhysicalPartitionsBitSet(partInfo);
            List<PartitionSpec> partSpecList = partInfo.getPartitionBy().getPartitions();
            for (int i = partBitSet.nextSetBit(0); i >= 0; i = partBitSet.nextSetBit(i + 1)) {
                List<PartitionSpec> subPartSpecList = partSpecList.get(i).getSubPartitions();
                for (int j = 0; j < subPartSpecList.size(); j++) {
                    resultBitSet.set(Long.valueOf(subPartSpecList.get(j).getPhyPartPosition() - 1).intValue(), true);
                }
            }
            return resultBitSet;
        } else if (bitSetLevel == BitSetLevel.BIT_SET_ONE_SUBPART) {
            BitSet resultBitSet = PartitionPrunerUtils.buildEmptyPhysicalPartitionsBitSet(partInfo);
            List<PartitionSpec> partSpecList = partInfo.getPartitionBy().getPartitions();
            List<PartitionSpec> subPartSpecList = partSpecList.get(parentSpecPosi - 1).getSubPartitions();
            for (int i = partBitSet.nextSetBit(0); i >= 0; i = partBitSet.nextSetBit(i + 1)) {
                resultBitSet.set(Long.valueOf(subPartSpecList.get(i).getPhyPartPosition() - 1).intValue(), true);
            }
            return resultBitSet;
        } else {
            return null;
        }

    }

    public List<PhysicalPartitionInfo> getPrunedParttions() {
        return getPrunedPartInfosFromBitSet();
    }

    public boolean isEmpty() {
        return partBitSet.isEmpty();
    }

    private List<PhysicalPartitionInfo> getPrunedPartInfosFromBitSet() {
        List<PhysicalPartitionInfo> prunedPartInfos = new ArrayList<>();

        PartKeyLevel phyPartKeyLevel = partInfo.getPartitionBy().getPhysicalPartLevel();
        List<PartitionSpec> phyPartitions = partInfo.getPartitionBy().getPhysicalPartitions();
        List<PartitionSpec> firstLevelParts = partInfo.getPartitionBy().getPartitions();
        boolean useSubPartBy = phyPartKeyLevel == PartKeyLevel.SUBPARTITION_KEY;

        int partCnt = phyPartitions.size();
        BitSet phyPartBitSet = getPhysicalPartBitSet();
        for (int i = phyPartBitSet.nextSetBit(0); i >= 0; i = phyPartBitSet.nextSetBit(i + 1)) {
            if (i >= partCnt) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                    "Find pruned partition error");
            }
            // operate on index i here
            PartitionSpec ps = phyPartitions.get(i);
            PartitionSpec parentPs = null;
            String parentPartName = null;
            if (useSubPartBy) {
                Integer parentPartPosi = ps.getParentPartPosi().intValue() - 1;
                parentPs = firstLevelParts.get(parentPartPosi);
                parentPartName = parentPs.getName();
            }
            PhysicalPartitionInfo prunedPartInfo = new PhysicalPartitionInfo();
            prunedPartInfo.setPartLevel(phyPartKeyLevel);
            prunedPartInfo.setPartName(ps.getName());
            if (phyPartKeyLevel == PartKeyLevel.SUBPARTITION_KEY) {
                Long parentPartPosi = ps.getParentPartPosi();
                PartitionSpec parentPartSpec = partInfo.getPartitionBy().getNthPartition(parentPartPosi.intValue());
                prunedPartInfo.setParentPartName(parentPartSpec.getName());
            } else {
                prunedPartInfo.setParentPartName(null);
            }
            prunedPartInfo.setPartId(ps.getId());
            prunedPartInfo.setPartBitSetIdx(i);
            prunedPartInfo.setGroupKey(ps.getLocation().getGroupKey());
            prunedPartInfo.setPhyTable(ps.getLocation().getPhyTableName());
            prunedPartInfo.setParentPartName(parentPartName);
            prunedPartInfos.add(prunedPartInfo);
        }

        return prunedPartInfos;
    }

    public PartitionInfo getPartInfo() {
        return partInfo;
    }

    public void setPartInfo(PartitionInfo partInfo) {
        this.partInfo = partInfo;
    }

    public void setPartBitSet(BitSet partBitSet) {
        this.partBitSet = partBitSet;
    }

    public List<PhysicalPartitionInfo> getCache() {
        return cache;
    }

    public void setCache(List<PhysicalPartitionInfo> cache) {
        this.cache = cache;
    }

    public BitSetLevel getBitSetLevel() {
        return bitSetLevel;
    }

    public void setBitSetLevel(BitSetLevel bitSetLevel) {
        this.bitSetLevel = bitSetLevel;
    }

    public Integer getParentSpecPosi() {
        return parentSpecPosi;
    }

    public void setParentSpecPosi(Integer parentSpecPosi) {
        this.parentSpecPosi = parentSpecPosi;
    }

}
