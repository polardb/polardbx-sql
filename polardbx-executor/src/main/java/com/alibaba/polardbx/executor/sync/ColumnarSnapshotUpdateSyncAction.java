package com.alibaba.polardbx.executor.sync;

import com.alibaba.polardbx.executor.gms.ColumnarManager;
import com.alibaba.polardbx.gms.sync.IGmsSyncAction;

public class ColumnarSnapshotUpdateSyncAction implements IGmsSyncAction {

    private Long latestTso;

    public ColumnarSnapshotUpdateSyncAction(Long latestTso) {
        this.latestTso = latestTso;
    }

    @Override
    public Object sync() {
        if (latestTso != null) {
            ColumnarManager.getInstance().setLatestTso(latestTso);
        }

        return null;
    }

    public Long getLatestTso() {
        return this.latestTso;
    }

    public void setLatestTso(Long latestTso) {
        this.latestTso = latestTso;
    }
}
