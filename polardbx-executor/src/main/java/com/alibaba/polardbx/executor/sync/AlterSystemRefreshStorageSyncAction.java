/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.executor.sync;

import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.gms.ha.impl.StorageHaManager;

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
