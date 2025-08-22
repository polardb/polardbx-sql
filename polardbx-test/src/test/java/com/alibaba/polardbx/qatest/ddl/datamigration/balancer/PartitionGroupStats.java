package com.alibaba.polardbx.qatest.ddl.datamigration.balancer;

import lombok.Data;

import java.util.TreeMap;

@Data
public class PartitionGroupStats {
    public PartitionGroupStats(String tableGroupName, String partitionGroupName, String subPartitionGroupName,
                               TreeMap<String, String> tableNamesMap, long tableRows, long dataLength,
                               Boolean isSubPartition, String storageInstId) {
        this.tableGroupName = tableGroupName;
        this.partitionGroupName = partitionGroupName;
        this.subPartitionGroupName = subPartitionGroupName;
        this.tableNamesMap = tableNamesMap;
        this.tableRows = tableRows;
        this.dataLength = dataLength;
        this.isSubPartition = isSubPartition;
        this.storageInstId = storageInstId;
    }

    String tableGroupName;
    String partitionGroupName;
    String subPartitionGroupName;
    TreeMap<String, String> tableNamesMap;
    long tableRows;
    long dataLength;
    Boolean isSubPartition;
    String storageInstId;

    public PartitionGroupStats(String tableGroupName, String partitionGroupName, String subPartitionGroupName,
                               Boolean isSubPartition) {
        this.tableGroupName = tableGroupName;
        this.partitionGroupName = partitionGroupName;
        this.subPartitionGroupName = subPartitionGroupName;
        this.isSubPartition = isSubPartition;
        this.tableNamesMap = new TreeMap<>();
        this.tableRows = 0;
        this.dataLength = 0;
    }

    public void appendPhysicalTableStat(String logicalTableName, String physicalTableName, long tableRows,
                                        long tableSize, String storageInstId) {
        this.tableNamesMap.put(logicalTableName, physicalTableName);
        this.tableRows += tableRows;
        this.dataLength += tableSize;
        this.storageInstId = storageInstId;
    }
}
