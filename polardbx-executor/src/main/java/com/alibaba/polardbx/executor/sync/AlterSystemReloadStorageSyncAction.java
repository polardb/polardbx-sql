package com.alibaba.polardbx.executor.sync;

import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.gms.ha.impl.StorageHaManager;

import java.util.ArrayList;
import java.util.List;

public class AlterSystemReloadStorageSyncAction implements ISyncAction {

    private List<String> dnIdList = new ArrayList<>();

    public AlterSystemReloadStorageSyncAction(List<String> dnIdList) {
        this.dnIdList = dnIdList;
    }

    public AlterSystemReloadStorageSyncAction() {
        this.dnIdList = dnIdList;
    }

    @Override
    public ResultCursor sync() {
        StorageHaManager.getInstance().reloadStorageInstsBySpecifyingStorageInstIdList(dnIdList);
        return null;
    }

    public List<String> getDnIdList() {
        return dnIdList;
    }

    public void setDnIdList(List<String> dnIdList) {
        this.dnIdList = dnIdList;
    }
}
