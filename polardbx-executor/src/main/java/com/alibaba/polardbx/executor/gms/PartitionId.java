package com.alibaba.polardbx.executor.gms;

import lombok.Data;

/**
 * The unique identifier of a partition.
 */
@Data
public class PartitionId {
    private final String partName;
    private final String logicalSchema;
    private final Long tableId;

    public PartitionId(String partName, String logicalSchema, Long tableId) {
        this.partName = partName;
        this.logicalSchema = logicalSchema;
        this.tableId = tableId;
    }

    public static PartitionId of(String partName, String logicalSchema, Long tableId) {
        return new PartitionId(partName, logicalSchema, tableId);
    }
}
