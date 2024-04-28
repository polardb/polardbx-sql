package com.alibaba.polardbx.optimizer.utils;

public interface IColumnarTransaction extends ITransaction {
    void setTsoTimestamp(long tsoTimestamp);

    long getSnapshotSeq();

    boolean snapshotSeqIsEmpty();
}
