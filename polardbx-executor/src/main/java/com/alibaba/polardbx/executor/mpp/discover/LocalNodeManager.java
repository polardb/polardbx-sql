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
import com.alibaba.polardbx.gms.node.AllNodes;
import com.alibaba.polardbx.gms.node.InternalNode;
import com.alibaba.polardbx.gms.node.InternalNodeManager;
import com.alibaba.polardbx.gms.node.Node;
import com.alibaba.polardbx.gms.node.NodeServer;
import com.google.common.collect.ImmutableSet;

import javax.inject.Inject;
import java.util.Set;

public class LocalNodeManager implements InternalNodeManager {

    private static final Logger logger = LoggerFactory.getLogger(LocalNodeManager.class);

    private InternalNode localNode;
    private AllNodes allNodes;

    @Inject
    public LocalNodeManager(InternalNode currentNode) {
        this.localNode = currentNode;
        this.allNodes =
            new AllNodes(ImmutableSet.of(localNode), ImmutableSet.of(), ImmutableSet.of(), ImmutableSet.of(),
                ImmutableSet.of());
    }

    @Override
    public Node getCurrentNode() {
        return localNode;
    }

    @Override
    public AllNodes getAllNodes() {
        return allNodes;
    }

    @Override
    public void removeNode(NodeServer nodeServer, String schema) {
        throw new IllegalStateException();
    }

    @Override
    public synchronized void updateNodes(Set<InternalNode> currentActiveNodes, Set<InternalNode> remoteActiveRowNodes,
                                         Set<InternalNode> remoteActiveColumnarNodes, Set<InternalNode> inactiveNodes,
                                         Set<InternalNode> shuttingDownNodes) {
        if (logger.isDebugEnabled()) {
            logger.debug("updateNodes:activeNodes=" + currentActiveNodes + ",otherActiveNodes=" + remoteActiveRowNodes
                + ",remoteActiveColumnarNodes=" + remoteActiveColumnarNodes + ",inactiveNodes=" + inactiveNodes + ","
                + "shuttingDownNodes=" + shuttingDownNodes);
        }
        allNodes.updateActiveNodes(currentActiveNodes, remoteActiveRowNodes, remoteActiveColumnarNodes, inactiveNodes,
            shuttingDownNodes);
    }
}
