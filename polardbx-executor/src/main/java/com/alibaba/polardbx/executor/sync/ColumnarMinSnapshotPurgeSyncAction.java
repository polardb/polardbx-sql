package com.alibaba.polardbx.executor.sync;

import com.alibaba.polardbx.executor.gms.ColumnarManager;
import com.alibaba.polardbx.gms.sync.IGmsSyncAction;

public class ColumnarMinSnapshotPurgeSyncAction implements IGmsSyncAction {

    private Long minTso;

    public ColumnarMinSnapshotPurgeSyncAction(Long minTso) {
        this.minTso = minTso;
    }

    @Override
    public Object sync() {
        if (minTso != null && minTso != Long.MIN_VALUE) {
            ColumnarManager.getInstance().purge(minTso);
        }
        return null;
    }

    public Long getMinTso() {
        return this.minTso;
    }

    public void setMinTso(Long minTso) {
        this.minTso = minTso;
    }
}
