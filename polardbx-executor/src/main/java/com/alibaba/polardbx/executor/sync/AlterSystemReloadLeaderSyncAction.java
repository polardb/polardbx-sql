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
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.mpp.deploy.ServiceProvider;
import com.alibaba.polardbx.gms.node.InternalNode;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;

public class AlterSystemReloadLeaderSyncAction implements ISyncAction {

    private String nodeId;

    public AlterSystemReloadLeaderSyncAction(String nodeId) {
        this.nodeId = nodeId;
    }

    public AlterSystemReloadLeaderSyncAction() {
    }

    @Override
    public ResultCursor sync() {
        boolean bRet = false;
        InternalNode node = ServiceProvider.getInstance().getServer().getLocalNode();
        if (node.getHostPort().equalsIgnoreCase(nodeId)) {
            bRet = ServiceProvider.getInstance().getServer().getStatusManager().forceToBeLeader();
        }
        final ArrayResultCursor result = new ArrayResultCursor("LEADER_RESULT_INFO");
        result.addColumn("TYPE", DataTypes.BooleanType);
        result.initMeta();
        result.addRow(new Object[] {bRet});
        return result;
    }

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }
}
