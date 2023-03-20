package com.alibaba.polardbx.executor.sync;

import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.gms.ha.impl.StorageHaManager;

import java.util.ArrayList;
import java.util.List;

public class AlterSystemRefreshStorageSyncAction implements ISyncAction {

    private String dnId;
    private String vipAddr;
    private String user;
    private String encPasswd;

    public AlterSystemRefreshStorageSyncAction(String dnId, String vipAddr, String user, String encPasswd) {
        this.dnId = dnId;
        this.vipAddr = vipAddr;
        this.user = user;
        this.encPasswd = encPasswd;
    }

    public AlterSystemRefreshStorageSyncAction() {
    }

    @Override
    public ResultCursor sync() {
        StorageHaManager.getInstance().refreshStorageInstsBySetting(this.dnId, this.vipAddr, this.user, this.encPasswd);
        return null;
    }

    public String getDnId() {
        return dnId;
    }

    public void setDnId(String dnId) {
        this.dnId = dnId;
    }

    public String getVipAddr() {
        return vipAddr;
    }

    public void setVipAddr(String vipAddr) {
        this.vipAddr = vipAddr;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getEncPasswd() {
        return encPasswd;
    }

    public void setEncPasswd(String encPasswd) {
        this.encPasswd = encPasswd;
    }
}
