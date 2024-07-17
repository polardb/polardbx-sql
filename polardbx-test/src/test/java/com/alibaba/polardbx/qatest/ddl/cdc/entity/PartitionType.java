package com.alibaba.polardbx.qatest.ddl.cdc.entity;

public enum PartitionType {
    Hash, HashKey, List, ListColumn, Range, RangeColumn, Single, Broadcast;

    public boolean isSupportAddPartition() {
        return this == Range || this == RangeColumn || this == List || this == ListColumn;
    }

    public boolean isSupportDropPartition() {
        return this == Range || this == RangeColumn || this == List || this == ListColumn;
    }

    public boolean isSupportMovePartition() {
        return this == Range || this == RangeColumn || this == List || this == ListColumn
            || this == Hash || this == HashKey;
    }

    public boolean isSupportAddDropValues() {
        return this == List || this == ListColumn;
    }

    public boolean isPartitionTable() {
        return this != Single && this != Broadcast;
    }
}
