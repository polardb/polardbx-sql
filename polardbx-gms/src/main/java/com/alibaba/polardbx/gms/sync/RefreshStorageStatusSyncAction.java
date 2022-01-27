package com.alibaba.polardbx.gms.sync;

import com.alibaba.polardbx.gms.node.StorageStatusManager;
import com.alibaba.polardbx.gms.node.StorageStatus;
import com.google.common.annotations.VisibleForTesting;

import java.io.Serializable;
import java.util.Map;

/**
 * sync all nodes, and objects of the class should create instance by the injection.
 */
public class RefreshStorageStatusSyncAction implements IGmsSyncAction, Serializable {

    private Map<String, StorageStatus> statusMap;

    public RefreshStorageStatusSyncAction() {

    }

    public RefreshStorageStatusSyncAction(Map<String, StorageStatus> statusMap) {
        this.statusMap = statusMap;
    }

    @Override
    public Object sync() {
        StorageStatusManager manager = StorageStatusManager.getInstance();
        manager.setStorageStatus(statusMap);
        return null;
    }

    public Map<String, StorageStatus> getStatusMap() {
        return statusMap;
    }

    public void setStatusMap(Map<String, StorageStatus> statusMap) {
        this.statusMap = statusMap;
    }
}
