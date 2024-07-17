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
import com.alibaba.polardbx.executor.mpp.Threads;
import com.alibaba.polardbx.executor.mpp.deploy.MppServer;
import com.alibaba.polardbx.executor.mpp.deploy.Server;
import com.alibaba.polardbx.executor.mpp.deploy.ServiceProvider;
import com.alibaba.polardbx.gms.node.AllNodes;
import com.alibaba.polardbx.gms.node.InternalNode;
import com.alibaba.polardbx.gms.node.NodeState;
import com.google.common.net.HttpHeaders;
import com.google.common.net.MediaType;
import io.airlift.http.client.HttpUriBuilder;
import io.airlift.http.client.JsonBodyGenerator;
import io.airlift.http.client.Request;

import java.net.URI;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.JsonBodyGenerator.jsonBodyGenerator;
import static io.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static io.airlift.http.client.Request.Builder.prepareDelete;
import static io.airlift.http.client.Request.Builder.preparePost;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

public class DiscoveryManager {

    private static final Logger logger = LoggerFactory.getLogger(DiscoveryManager.class);
    private static DiscoveryManager instance;
    private ScheduledExecutorService nodeStateUpdateExecutor;
    private Map<String, NodeDiscoveryStatus> nodeMap = new ConcurrentHashMap<>();
    private Server server;

    public static DiscoveryManager getInstance() {
        if (instance == null) {
            synchronized (DiscoveryManager.class) {
                if (instance == null) {
                    instance = new DiscoveryManager();
                }
            }
        }
        return instance;
    }

    private DiscoveryManager() {
        if (ServiceProvider.getInstance().clusterMode()) {
            this.nodeStateUpdateExecutor = newSingleThreadScheduledExecutor(
                Threads.threadsNamed("discovery-node-state"));
            this.server = ServiceProvider.getInstance().getServer();
            nodeStateUpdateExecutor.scheduleWithFixedDelay(() -> {
                if (server != null && server.getLocalNode().isLeader()) {
                    try {
                        boolean changed = false;
                        if (nodeMap.size() > 0) {
                            //30s未心跳，认为节点不可用
                            long timeInactive = System.currentTimeMillis() - 30000L;
                            for (String nodeId : nodeMap.keySet()) {
                                NodeDiscoveryStatus status = nodeMap.get(nodeId);
                                if (status != null && status.nodestate == NodeState.ACTIVE
                                    && status.modifyTime < timeInactive) {
                                    status.nodestate = NodeState.INACTIVE;
                                    changed = true;
                                    logger.warn("inactive node:" + status.node);
                                }
                            }
                        }
                        if (changed) {
                            updateNodes();
                        }
                    } catch (Exception e) {
                        logger.error(e);
                    }
                }
            }, 5, 5, TimeUnit.SECONDS);
        }
    }

    public void updateNodes() {
        if (!ServiceProvider.getInstance().clusterMode()) {
            return;
        }
        Set<InternalNode> activeNodes = new HashSet<>();
        Set<InternalNode> inactiveNodes = new HashSet<>();
        Set<InternalNode> shuttingDownNodes = new HashSet<>();
        for (String nodeId : nodeMap.keySet()) {
            NodeDiscoveryStatus status = nodeMap.get(nodeId);
            switch (status.nodestate) {
            case ACTIVE:
                if (!status.node.isInBlacklist()) {
                    activeNodes.add(status.node);
                }
                break;
            case SHUTTING_DOWN:
                shuttingDownNodes.add(status.node);
                break;
            case INACTIVE:
            case TEMP_INACTIVE:
                inactiveNodes.add(status.node);
                break;
            default:
                break;
            }
        }
        ((MppServer) server).getNodeManager().updateNodes(activeNodes, null, inactiveNodes, shuttingDownNodes);
    }

    public boolean notifyNode(InternalNode node) {
        NodeDiscoveryStatus dnode = nodeMap.get(node.getNodeIdentifier());
        if (dnode != null && dnode.nodestate == NodeState.ACTIVE) {
            dnode.modifyTime = System.currentTimeMillis();
            if (!dnode.node.toString().equals(node.toString())) {
                logger.warn("modify node:" + node);
                dnode.node = node;
                updateNodes();
                return true;
            }
        } else if (dnode != null && dnode.nodestate == NodeState.TEMP_INACTIVE) {
            if (logger.isDebugEnabled()) {
                logger.debug("check temp inactive node:" + node);
            }
            if (System.currentTimeMillis() - dnode.modifyTime > 60000L) {
                logger.warn("reactive node:" + node);
                nodeMap.put(node.getNodeIdentifier(), new NodeDiscoveryStatus(node));
                updateNodes();
                return true;
            }
        } else {
            logger.warn("input node:" + node);
            nodeMap.put(node.getNodeIdentifier(), new NodeDiscoveryStatus(node));
            updateNodes();
            return true;
        }
        return false;
    }

    public boolean removeNode(InternalNode node, String type) {
        logger.warn("removeNode:" + node.getNodeIdentifier() + ",type=" + type);
        NodeDiscoveryStatus dnode = nodeMap.get(node.getNodeIdentifier());
        if (dnode != null) {
            if (type.equalsIgnoreCase("temp") && dnode.nodestate == NodeState.ACTIVE) {
                dnode.modifyTime = System.currentTimeMillis();
                dnode.nodestate = NodeState.TEMP_INACTIVE;
                updateNodes();
                return true;
            } else if (type.equalsIgnoreCase("black")) {
                dnode.node.setInBlacklist(true);
                updateNodes();
                return true;
            } else if (type.equalsIgnoreCase("unblack")) {
                dnode.node.setInBlacklist(false);
                updateNodes();
                return true;
            }
        }
        return false;
    }


    class NodeDiscoveryStatus {
        InternalNode node;
        long modifyTime;
        NodeState nodestate;

        public NodeDiscoveryStatus(InternalNode node) {
            this.node = node;
            this.modifyTime = System.currentTimeMillis();
            this.nodestate = NodeState.ACTIVE;
        }
    }
}
