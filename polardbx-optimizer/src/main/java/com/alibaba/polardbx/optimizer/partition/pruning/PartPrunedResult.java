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
    protected List<PhysicalPartitionInfo> cache;

    public PartPrunedResult() {
    }

    public String getLogicalTableName() {
        return partInfo.getTableName();
    }

    public BitSet getPartBitSet() {
        return partBitSet;
    }

    public List<PhysicalPartitionInfo> getPrunedParttions() {
        if (cache == null) {
            synchronized (this) {
                cache = getPrunedPartInfosFromBitSet();
            }
        }
        return cache;
    }

    public boolean isEmpty() {
        return partBitSet.isEmpty();
    }

    private List<PhysicalPartitionInfo> getPrunedPartInfosFromBitSet() {
        List<PhysicalPartitionInfo> prunedPartInfos = new ArrayList<>();
        boolean hasSubPart = partInfo.containSubPartitions();
        List<PartitionSpec> partitions = partInfo.getPartitionBy().getPartitions();
        int partCnt = partitions.size();
        if (!hasSubPart) {
            for (int i = 0; i < partCnt; i++) {
                PartitionSpec ps = partitions.get(i);
                if (partBitSet.get(i)) {
                    PhysicalPartitionInfo prunedPartInfo = new PhysicalPartitionInfo();
                    prunedPartInfo.setPartLevel(PartKeyLevel.PARTITION_KEY);
                    prunedPartInfo.setPartName(ps.getName());
                    prunedPartInfo.setPartId(ps.getId());
                    prunedPartInfo.setPartBitSetIdx(i);
                    prunedPartInfo.setGroupKey(ps.getLocation().getGroupKey());
                    prunedPartInfo.setPhyTable(ps.getLocation().getPhyTableName());
                    prunedPartInfos.add(prunedPartInfo);
                }
            }

        } else {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                "Not supported partitioning with subpartitions");

//            PartitionSpec part0 = partitions.get(0);
//            List<SubPartitionSpec> subpartitions = part0.getSubPartitions();
//            int subPartCnt = subpartitions.size();
//            for (int i = 0; i < partCnt; i++) {
//                PartitionSpec ps = partitions.get(i);
//                for (int j = 0; j < subPartCnt; j++) {
//                    int bsIndex = i * subPartCnt + j;
//                    if (partBitSet.get(bsIndex)) {
//                        SubPartitionSpec spec = ps.getSubPartitions().get(j);
//                        PhysicalPartitionInfo prunedPartInfo = new PhysicalPartitionInfo();
//                        prunedPartInfo.setPartLevel( PartKeyLevel.SUBPARTITION_KEY);
//                        prunedPartInfo.setPartName(spec.getName());
//                        prunedPartInfo.setPartId( spec.getId());
//                        prunedPartInfo.setPartBitSetIdx(bsIndex);
//                        prunedPartInfo.setGroupKey(spec.getLocation().getGroupKey());
//                        prunedPartInfo.setPhyTable(spec.getLocation().getPhyTableName());
//                        prunedPartInfos.add(prunedPartInfo);
//                    }
//                }
//            }
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

}
