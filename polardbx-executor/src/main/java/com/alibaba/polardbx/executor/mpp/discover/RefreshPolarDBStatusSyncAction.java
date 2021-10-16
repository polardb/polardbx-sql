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

package com.alibaba.polardbx.executor.mpp.discover;

import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.mpp.deploy.ServiceProvider;
import com.alibaba.polardbx.gms.node.InternalNodeManager;
import com.alibaba.polardbx.gms.node.PolarDBXStatus;
import com.alibaba.polardbx.gms.node.PolarDBXStatusManager;
import com.alibaba.polardbx.gms.sync.IGmsSyncAction;

import java.io.Serializable;
import java.util.Map;

/**
 * sync all nodes, and objects of the class should create instance by the injection.
 */
public class RefreshPolarDBStatusSyncAction implements IGmsSyncAction, Serializable {

    private Map<String, PolarDBXStatus> polarDBXStatusMap;

    public RefreshPolarDBStatusSyncAction() {

    }

    public RefreshPolarDBStatusSyncAction(Map<String, PolarDBXStatus> polarDBXStatusMap) {
        this.polarDBXStatusMap = polarDBXStatusMap;
    }

    @Override
    public Object sync() {
        InternalNodeManager manager = ServiceProvider.getInstance().getServer().getNodeManager();
        PolarDBXStatusManager dbxStatusManager = manager.getPolarDBXStatusManager();
        if (dbxStatusManager != null) {
            dbxStatusManager.setLearnPolarDBXStatusMap(polarDBXStatusMap);
        }
        return null;
    }

    public Map<String, PolarDBXStatus> getPolarDBXStatusMap() {
        return polarDBXStatusMap;
    }

    public void setPolarDBXStatusMap(Map<String, PolarDBXStatus> polarDBXStatusMap) {
        this.polarDBXStatusMap = polarDBXStatusMap;
    }
}
