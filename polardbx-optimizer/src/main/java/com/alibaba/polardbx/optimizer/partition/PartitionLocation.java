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

/**
 * @author chenghui.lch
 */
public class PartitionLocation {

    protected String groupKey;
    protected String phyTableName;
    protected Long partitionGroupId;
    /**
     * A label if current partition need do multi write
     */
    protected boolean visiable = true;
    public static final Long INVALID_PARTITION_GROUP_ID = -1L;

    public PartitionLocation() {
    }

    public PartitionLocation(String groupKey, String phyTableName, Long partitionGroupId) {
        this.groupKey = groupKey;
        this.phyTableName = phyTableName;
        this.partitionGroupId = partitionGroupId;
    }

    @Override
    public int hashCode() {
        int hashCodeVal = groupKey.toUpperCase().hashCode();
        hashCodeVal ^= phyTableName.toLowerCase().hashCode();
        hashCodeVal ^= partitionGroupId;
        if (visiable) {
            hashCodeVal |= 1;
        }
        return hashCodeVal;
    }

    @Override
    public boolean equals(Object obj) {

        if (this == obj) {
            return true;
        }

        if (!(obj instanceof PartitionLocation)) {
            return false;
        }

        PartitionLocation tarLocation = (PartitionLocation) obj;

        if (!this.groupKey.equalsIgnoreCase(tarLocation.groupKey)) {
            return false;
        }

        if (!this.phyTableName.equalsIgnoreCase(tarLocation.phyTableName)) {
            return false;
        }

        if (this.partitionGroupId.equals(tarLocation.partitionGroupId)) {
            return false;
        }

        if (this.visiable != tarLocation.visiable) {
            return false;
        }
        return true;
    }

    public PartitionLocation copy() {
        PartitionLocation newLocation = new PartitionLocation(this.groupKey, this.phyTableName, this.partitionGroupId);
        newLocation.visiable = this.visiable;
        return newLocation;
    }

    public String getGroupKey() {
        return groupKey;
    }

    public void setGroupKey(String groupKey) {
        this.groupKey = groupKey;
    }

    public String getPhyTableName() {
        return phyTableName;
    }

    public void setPhyTableName(String phyTableName) {
        this.phyTableName = phyTableName;
    }

    public Long getPartitionGroupId() {
        return partitionGroupId;
    }

    public void setPartitionGroupId(Long partitionGroupId) {
        this.partitionGroupId = partitionGroupId;
    }

    public boolean isValidLocation() {
        return !INVALID_PARTITION_GROUP_ID.equals(partitionGroupId);
    }

    public boolean isVisiable() {
        return visiable;
    }

    public void setVisiable(boolean visiable) {
        this.visiable = visiable;
    }

    public String getDigest() {
        StringBuilder sb = new StringBuilder();
        sb.append("location:[");
        sb.append(this.partitionGroupId);
        sb.append(",");
        sb.append(this.groupKey);
        sb.append(",");
        sb.append(this.phyTableName);
        sb.append(",");
        sb.append(this.visiable);
        sb.append("]");
        return sb.toString();
    }
}
