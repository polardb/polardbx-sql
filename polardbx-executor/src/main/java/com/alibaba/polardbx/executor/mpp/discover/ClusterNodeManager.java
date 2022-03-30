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
import com.alibaba.polardbx.executor.mpp.deploy.MppServer;
import com.alibaba.polardbx.executor.mpp.metadata.ForNodeManager;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.gms.node.AllNodes;
import com.alibaba.polardbx.gms.node.InternalNode;
import com.alibaba.polardbx.gms.node.InternalNodeManager;
import com.alibaba.polardbx.gms.node.Node;
import com.alibaba.polardbx.gms.node.NodeServer;
import com.alibaba.polardbx.gms.node.NodeState;
import com.alibaba.polardbx.gms.node.NodeStatusManager;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import io.airlift.http.client.HttpClient;
import io.airlift.json.JsonCodec;

import javax.inject.Inject;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;

public class ClusterNodeManager implements InternalNodeManager {

    private static final Logger logger = LoggerFactory.getLogger(ClusterNodeManager.class);

    private InternalNode localNode;
    private AllNodes allNodes;
    private JsonCodec<InternalNode> nodeInfoCodec;
    private JsonCodec<AllNodes> allNodeInfoCodec;

    private HttpClient httpClient;

    private ScheduledExecutorService nodeStateUpdateExecutor;

    private MppServer mppServer;

    @Inject
    public ClusterNodeManager(InternalNode currentNode,
                              JsonCodec<InternalNode> nodeInfoCodec,
                              JsonCodec<AllNodes> allNodeInfoCodec,
                              @ForNodeManager HttpClient httpClient) {
        this.localNode = currentNode;
        this.allNodes = new AllNodes(ImmutableSet.of(), ImmutableSet.of(), ImmutableSet.of(), ImmutableSet.of());
        this.nodeInfoCodec = nodeInfoCodec;
        this.allNodeInfoCodec = allNodeInfoCodec;
        this.httpClient = httpClient;
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
    @Deprecated
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
        //TODO 向manager主动获取节点信息
    }

    @Override
    public void removeNode(NodeServer nodeServer, String schema) {
        if (nodeServer != null) {
            for (InternalNode node : allNodes.getActiveNodes()) {
                if (node.getNodeServer() != null && node.getNodeServer().equals(nodeServer)) {
                    logger.warn("temp removeNode:" + node);

                    allNodes.getActiveNodes().remove(node);
                    allNodes.getOtherActiveNodes().remove(node);
                    NodeStatusManager manager = ExecUtils.getStatusManager(schema);
                    manager.tempInactiveNode(node);
                    break;
                }
            }
        }
    }

    @Override
    public synchronized void updateNodes(Set<InternalNode> activeNodes, Set<InternalNode> otherActiveNodes,
                                         Set<InternalNode> inactiveNodes, Set<InternalNode> shuttingDownNodes) {
        if (logger.isDebugEnabled()) {
            logger.debug(
                "updateNodes:activeNodes=" + activeNodes + ",otherActiveNodes=" + otherActiveNodes + ",inactiveNodes="
                    + inactiveNodes + "," +
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

    public void setAllNodes(AllNodes allNodes) {
        this.allNodes = allNodes;
    }

    public InternalNode getLocalNode() {
        return localNode;
    }

    public JsonCodec<InternalNode> getNodeInfoCodec() {
        return nodeInfoCodec;
    }

    public JsonCodec<AllNodes> getAllNodeInfoCodec() {
        return allNodeInfoCodec;
    }

    public HttpClient getHttpClient() {
        return httpClient;
    }
}
