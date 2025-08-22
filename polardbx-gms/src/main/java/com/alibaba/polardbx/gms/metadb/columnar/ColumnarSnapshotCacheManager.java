package com.alibaba.polardbx.gms.metadb.columnar;

public interface ColumnarSnapshotCacheManager {
    default public FlashbackColumnarManager getFlashbackColumnarManager(long flashbackTso, String logicalSchema,
                                                                String logicalTable, boolean autoPosition) {
        return null;
    }
}
