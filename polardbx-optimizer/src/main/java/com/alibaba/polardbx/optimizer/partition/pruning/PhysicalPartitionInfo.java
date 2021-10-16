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

/**
 * @author chenghui.lch
 */
public class PhysicalPartitionInfo {
    protected PartKeyLevel partLevel;
    protected Long partId;
    protected Integer partBitSetIdx;
    protected String partName;
    protected String groupKey;
    protected String phyTable;
    public PhysicalPartitionInfo() {
    }

    public PartKeyLevel getPartLevel() {
        return partLevel;
    }

    public void setPartLevel(PartKeyLevel partLevel) {
        this.partLevel = partLevel;
    }

    public Long getPartId() {
        return partId;
    }

    public void setPartId(Long partId) {
        this.partId = partId;
    }

    public Integer getPartBitSetIdx() {
        return partBitSetIdx;
    }

    public void setPartBitSetIdx(Integer partBitSetIdx) {
        this.partBitSetIdx = partBitSetIdx;
    }

    public String getPartName() {
        return partName;
    }

    public void setPartName(String partName) {
        this.partName = partName;
    }

    public String getGroupKey() {
        return groupKey;
    }

    public void setGroupKey(String groupKey) {
        this.groupKey = groupKey;
    }

    public String getPhyTable() {
        return phyTable;
    }

    public void setPhyTable(String phyTable) {
        this.phyTable = phyTable;
    }
}
