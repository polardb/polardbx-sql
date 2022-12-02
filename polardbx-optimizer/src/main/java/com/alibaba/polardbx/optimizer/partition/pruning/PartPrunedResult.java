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
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

/**
 * @author chenghui.lch
 */
public class PartPrunedResult {
    protected PartitionInfo partInfo;
    protected BitSet partBitSet;
    protected volatile List<PhysicalPartitionInfo> cache;

    public PartPrunedResult() {
    }

    @Override
    public String toString() {
        List<String> partNameSet = new ArrayList<>();
        List<PartitionSpec> partitions = partInfo.getPartitionBy().getPartitions();
        int partCnt = partitions.size();
        for (int i = partBitSet.nextSetBit(0); i >= 0; i = partBitSet.nextSetBit(i + 1)) {
            if (i >= partCnt) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                    "Find pruned partition error");
            }
            PartitionSpec ps = partitions.get(i);
            partNameSet.add(ps.getName());
        }
        return String.join(",", partNameSet);
    }

    public String getLogicalTableName() {
        return partInfo.getTableName();
    }

    public BitSet getPartBitSet() {
        return partBitSet;
    }

    public List<PhysicalPartitionInfo> getPrunedPartitions() {
        return getPrunedPartInfosFromBitSet();
    }

    public boolean isEmpty() {
        return partBitSet.isEmpty();
    }

    private List<PhysicalPartitionInfo> getPrunedPartInfosFromBitSet() {
        List<PhysicalPartitionInfo> prunedPartInfos = new ArrayList<>();
        boolean hasSubPart = partInfo.containSubPartitions();
        List<PartitionSpec> partitions = partInfo.getPartitionBy().getPartitions();
        int partCnt = partitions.size();

        if (hasSubPart) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                "Not supported partitioning with subpartitions");
        }
        for (int i = partBitSet.nextSetBit(0); i >= 0; i = partBitSet.nextSetBit(i + 1)) {
            if (i >= partCnt) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                    "Find pruned partition error");
            }
            // operate on index i here
            PartitionSpec ps = partitions.get(i);
            PhysicalPartitionInfo prunedPartInfo = new PhysicalPartitionInfo();
            prunedPartInfo.setPartLevel(PartKeyLevel.PARTITION_KEY);
            prunedPartInfo.setPartName(ps.getName());
            prunedPartInfo.setPartId(ps.getId());
            prunedPartInfo.setPartBitSetIdx(i);
            prunedPartInfo.setGroupKey(ps.getLocation().getGroupKey());
            prunedPartInfo.setPhyTable(ps.getLocation().getPhyTableName());
            prunedPartInfos.add(prunedPartInfo);
        }

//        if (!hasSubPart) {
//            for (int i = 0; i < partCnt; i++) {
//                PartitionSpec ps = partitions.get(i);
//                if (partBitSet.get(i)) {
//                    PhysicalPartitionInfo prunedPartInfo = new PhysicalPartitionInfo();
//                    prunedPartInfo.setPartLevel(PartKeyLevel.PARTITION_KEY);
//                    prunedPartInfo.setPartName(ps.getName());
//                    prunedPartInfo.setPartId(ps.getId());
//                    prunedPartInfo.setPartBitSetIdx(i);
//                    prunedPartInfo.setGroupKey(ps.getLocation().getGroupKey());
//                    prunedPartInfo.setPhyTable(ps.getLocation().getPhyTableName());
//                    prunedPartInfos.add(prunedPartInfo);
//                }
//            }
//
//        } else {
//            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
//                "Not supported partitioning with subpartitions");
//        }
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

}
