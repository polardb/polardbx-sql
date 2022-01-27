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
import com.alibaba.polardbx.gms.node.NodeState;
import com.google.common.collect.ImmutableSet;

import javax.inject.Inject;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class LocalNodeManager implements InternalNodeManager {

    private static final Logger logger = LoggerFactory.getLogger(LocalNodeManager.class);

    private InternalNode localNode;
    private AllNodes allNodes;

    @Inject
    public LocalNodeManager(InternalNode currentNode) {
        this.localNode = currentNode;
        this.allNodes =
            new AllNodes(ImmutableSet.of(localNode), ImmutableSet.of(), ImmutableSet.of(), ImmutableSet.of());
    }

    @Override
    public Set<InternalNode> getNodes(NodeState state, boolean slaveFirst) {
        switch (state) {
        case ACTIVE:
            if (slaveFirst) {
                Set<InternalNode> otherActiveNodes = getAllNodes().getOtherActiveNodes();
                if (otherActiveNodes != null && !otherActiveNodes.isEmpty()) {
                    return otherActiveNodes;
                }
            }
            return getAllNodes().getActiveNodes();
        case INACTIVE:
            return getAllNodes().getInactiveNodes();
        case SHUTTING_DOWN:
            return getAllNodes().getShuttingDownNodes();
        default:
            throw new IllegalArgumentException("Unknown node state " + state);
        }
    }

    @Override
    public Node getCurrentNode() {
        return localNode;
    }

    @Override
    public Set<Node> getCoordinators() {
        Set<Node> coordinators = new HashSet<>();
        for (Node node : allNodes.getActiveNodes()) {
            if (node.isCoordinator()) {
                coordinators.add(node);
            }
        }
        return coordinators;
    }

    @Override
    public AllNodes getAllNodes() {
        return allNodes;
    }

    @Override
    public void refreshNodes() {
        throw new IllegalStateException();
    }

    @Override
    public void removeNode(NodeServer nodeServer, String schema) {
        throw new IllegalStateException();
    }

    @Override
    public synchronized void updateNodes(Set<InternalNode> activeNodes, Set<InternalNode> otherActiveNodes,
                                         Set<InternalNode> inactiveNodes, Set<InternalNode> shuttingDownNodes) {
        if (logger.isDebugEnabled()) {
            logger.debug("updateNodes:activeNodes=" + activeNodes + ",inactiveNodes=" + inactiveNodes + "," +
                "shuttingDownNodes=" + shuttingDownNodes);
        }
        allNodes.setActiveNodes(activeNodes, otherActiveNodes);
        allNodes.setInactiveNodes(inactiveNodes);
        allNodes.setShuttingDownNodes(shuttingDownNodes);
    }

    @Override
    public List<Node> getAllWorkers(boolean slaveFirst) {
        return allNodes.getAllWorkers(slaveFirst);
    }

    @Override
    public List<Node> getAllCoordinators() {
        return allNodes.getAllCoordinators();
    }
}
