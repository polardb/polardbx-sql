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

package com.alibaba.polardbx.executor.balancer.stats;

import com.alibaba.polardbx.gms.partition.TablePartitionRecord;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.tablegroup.TableGroupRecord;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.partition.PartitionByDefinition;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.partition.boundspec.PartitionBoundVal;
import com.alibaba.polardbx.optimizer.partition.common.PartitionLocation;
import com.alibaba.polardbx.optimizer.partition.common.PartitionStrategy;
import com.alibaba.polardbx.optimizer.partition.pruning.SearchDatumInfo;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Statistics of a partition-group
 *
 * @author moyi
 * @since 2021/03
 */
public class PartitionStat {

    private final TableGroupConfig tableGroupConfig;
    private final TablePartitionRecord partitionRecord;
    private final PartitionGroupRecord partitionGroupRecord;
    private final PartitionInfo partitionInfo;
    /**
     * Approximate size of data table from information_schema
     */
    protected long dataLength;
    /**
     * Approximate size of index table from information_schema
     */
    protected long indexLength;
    /**
     * Approximate rows of table
     */
    protected long dataRows;

    public PartitionStat(TableGroupConfig tableGroupConfig,
                         TablePartitionRecord partitionRecord,
                         PartitionInfo partitionInfo) {
        this.tableGroupConfig = tableGroupConfig;
        this.partitionRecord = partitionRecord;
        this.partitionGroupRecord = tableGroupConfig.getPartitionGroup(partitionRecord.groupId);
        this.partitionInfo = partitionInfo;
    }

    public PartitionStrategy getPartitionStrategy() {
        return this.getPartitionBy().getStrategy();
    }

    public PartitionLocation getLocation() {
        return this.getCurrentPartition().getLocation();
    }

    public long getPartitionGroupId() {
        assert this.getPartitionRecord().partLevel >= 1;
        return this.getPartitionRecord().groupId;
    }

    public List<PartitionGroupRecord> getPartitionGroups() {
        return this.tableGroupConfig.getPartitionGroupRecords();
    }

    public int getPartitionCount() {
        return partitionInfo.getPartitionBy().getPartitions().size();
    }

    public boolean isFirst() {
        return getPosition() == 1;
    }

    public List<ColumnMeta> getPartitionFields() {
        return this.getPartitionBy().getPartitionFieldList();
    }

    public String getSchema() {
        return this.tableGroupConfig.getTableGroupRecord().schema;
    }

    public String getPhysicalDatabase() {
        return partitionGroupRecord.phy_db;
    }

    public String getPhysicalTableName() {
        return getCurrentPartition().getLocation().getPhyTableName();
    }

    public int getPosition() {
        return getPartitionRecord().partPosition.intValue();
    }

    public Boolean isSubPartition() {
        return getPartitionRecord().getPartLevel() == 2;
    }

    public PartitionSpec getCurrentPartition() {
        if (isSubPartition()) {
            Long parentId = getPartitionRecord().getParentId();
            String subPartName = getPartitionRecord().partName;
            return this.partitionInfo.getPartitionBy().getPartitions().stream().filter(o -> Objects.equals(o.getId(),
                    parentId))
                .collect(Collectors.toList()).get(0).getSubPartitionBySubPartName(subPartName);
        } else {
            return this.partitionInfo.getPartitionBy().getNthPartition(getPosition());
        }
    }

    public SearchDatumInfo getCurrentBound() {
        return getCurrentPartition().getBoundSpec().getSingleDatum();
    }

    public SearchDatumInfo leftBound() {
        if (isFirst()) {
            PartitionBoundVal value = PartitionBoundVal.createMinValue();
            return new SearchDatumInfo(value);
        } else {
            return getPrevBound();
        }
    }

    public SearchDatumInfo rightBound() {
        return getCurrentBound();
    }

    public PartitionSpec getPrevPartition() {
        int pos = getPosition();
        if (pos == 1) {
            return null;
        }
        return this.partitionInfo.getPartitionBy().getNthPartition(pos - 1);
    }

    public PartitionSpec getNextPartition() {
        int pos = getPosition();
        if (this.partitionInfo.getPartitionBy().getPartitions().size() == pos) {
            return null;
        }
        return this.partitionInfo.getPartitionBy().getNthPartition(pos + 1);
    }

    public SearchDatumInfo getPrevBound() {
        return getPrevPartition().getBoundSpec().getSingleDatum();
    }

    public boolean enableAutoSplit() {
        return partitionInfo.enableAutoSplit();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof PartitionStat)) {
            return false;
        }
        PartitionStat that = (PartitionStat) o;
        return getPartitionGroupId() == that.getPartitionGroupId() &&
            Objects.equals(getSchema(), that.getSchema()) &&
            Objects.equals(getTableGroupName(), that.getTableGroupName()) &&
            Objects.equals(getPartitionName(), that.getPartitionName());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getSchema(), getTableGroupName(), getPartitionName(), getPartitionGroupId());
    }

    @Override
    public String toString() {
        return "PartitionStat{" +
            this.getSchema() + "." +
            this.getTableGroupName() + "." +
            this.getPartitionName();
    }

    public String getPartitionName() {
        return this.getPartitionRecord().partName;
    }

    public PartitionByDefinition getPartitionBy() {
        return this.partitionInfo.getPartitionBy();
    }

    public String getTableGroupName() {
        return this.tableGroupConfig.getTableGroupRecord().tg_name;
    }

    public PartitionInfo getPartitionInfo() {
        return this.partitionInfo;
    }

    public PartitionGroupRecord getPartitionGroupRecord() {
        return this.partitionGroupRecord;
    }

    public TableGroupConfig getTableGroupConfig() {
        return this.tableGroupConfig;
    }

    public TableGroupRecord getTableGroupRecord() {
        return this.tableGroupConfig.getTableGroupRecord();
    }

    public TablePartitionRecord getPartitionRecord() {
        return this.partitionRecord;
    }

    public long getPartitionDiskSize() {
        return dataLength + indexLength;
    }

    public long getPartitionRows() {
        return dataRows;
    }

    public void setDataLength(long dataLength) {
        this.dataLength = dataLength;
    }

    public void setIndexLength(long indexLength) {
        this.indexLength = indexLength;
    }

    public void setDataRows(long dataRows) {
        this.dataRows = dataRows;
    }

    public long getDataRows() {
        return this.dataRows;
    }
}
