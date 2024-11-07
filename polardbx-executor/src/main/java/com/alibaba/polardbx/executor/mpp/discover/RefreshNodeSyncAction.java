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

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.mpp.deploy.ServiceProvider;
import com.alibaba.polardbx.gms.node.NodeStatusManager;
import com.alibaba.polardbx.gms.sync.IGmsSyncAction;

/**
 * sync all nodes, and objects of the class should create instance by the injection.
 */
public class RefreshNodeSyncAction implements IGmsSyncAction {

    private static final Logger logger = LoggerFactory.getLogger(RefreshNodeSyncAction.class);

    private String schemaName;

    public RefreshNodeSyncAction() {

    }

    public RefreshNodeSyncAction(String schemaName) {
        this.schemaName = schemaName;
    }

    @Override
    public Object sync() {
        NodeStatusManager manager = ServiceProvider.getInstance().getServer().getStatusManager();
        if (manager != null) {
            manager.refreshNode();
            logger.info("refreshAllNode end");
        }
        return null;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }
}
