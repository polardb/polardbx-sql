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

package com.alibaba.polardbx.gms.node;

/**
 * A bridge to access NodeStatusManager in tddl-gms
 * Since NodeStatusManager's implementation is located at tddl-executor,
 * which could not been accessed by tddl-gms
 *
 * @author moyi
 * @since 2021/04
 */
public class LeaderStatusBridge {

    private static final LeaderStatusBridge INSTANCE = new LeaderStatusBridge();

    /**
     * NodeStatusManager registered by CobarServer
     */
    private NodeStatusManager nodeStatusManager;

    public void setUpNodeStatusManager(NodeStatusManager nodeStatusManager) {
        this.nodeStatusManager = nodeStatusManager;
    }

    public static LeaderStatusBridge getInstance() {
        return INSTANCE;
    }

    /**
     * Whether this CN is a leader
     *
     * @return true if this node is leader
     */
    public boolean hasLeaderShip() {
        // the server has not been initialized
        if (nodeStatusManager == null) {
            return false;
        }
        return nodeStatusManager.getLocalNode().isLeader();
    }
}
