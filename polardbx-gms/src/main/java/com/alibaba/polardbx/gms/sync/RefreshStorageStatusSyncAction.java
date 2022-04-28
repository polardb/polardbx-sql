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
