package com.alibaba.polardbx.qatest.ddl.datamigration.balancer;

import lombok.Data;

import java.util.TreeMap;

@Data
public class TableGroupStats {
    String tableGroupName;
    TreeMap<String, PartitionGroupStats> partitionGroupStatsMap;

    public TableGroupStats(String tableGroupName,
                           TreeMap<String, PartitionGroupStats> partitionGroupStats) {
        this.tableGroupName = tableGroupName;
        this.partitionGroupStatsMap = partitionGroupStats;
    }

    public TableGroupStats(String tableGroupName) {
        this.tableGroupName = tableGroupName;
        this.partitionGroupStatsMap = new TreeMap<>();
    }
}
