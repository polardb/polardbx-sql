package com.alibaba.polardbx.optimizer.archive;

import com.alibaba.polardbx.gms.partition.TableLocalPartitionRecord;
import com.alibaba.polardbx.gms.ttl.TtlInfoRecord;
import lombok.Data;

@Data
public class TtlSourceInfo {
    protected boolean ttlTable = false;
    protected boolean useRowLevelTtl = false;
    protected TableLocalPartitionRecord tableLocalPartitionRecord;
    protected TtlInfoRecord ttlInfoRecord;

    public TtlSourceInfo() {
    }

    public boolean isTtlTable() {
        return ttlTable;
    }

    public void setTtlTable(boolean ttlTable) {
        this.ttlTable = ttlTable;
    }

    public boolean isUseRowLevelTtl() {
        return useRowLevelTtl;
    }

    public void setUseRowLevelTtl(boolean useRowLevelTtl) {
        this.useRowLevelTtl = useRowLevelTtl;
    }

    public TtlInfoRecord getTtlInfoRecord() {
        return ttlInfoRecord;
    }

    public void setTtlInfoRecord(TtlInfoRecord ttlInfoRecord) {
        this.ttlInfoRecord = ttlInfoRecord;
    }

    public TableLocalPartitionRecord getTableLocalPartitionRecord() {
        return tableLocalPartitionRecord;
    }

    public void setTableLocalPartitionRecord(
        TableLocalPartitionRecord tableLocalPartitionRecord) {
        this.tableLocalPartitionRecord = tableLocalPartitionRecord;
    }
}