package com.alibaba.polardbx.executor.mpp.discover;

import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.mpp.deploy.ServiceProvider;
import com.alibaba.polardbx.executor.mpp.metadata.ForNodeManager;
import com.alibaba.polardbx.gms.node.AllNodes;
import com.alibaba.polardbx.gms.node.InternalNode;
import com.alibaba.polardbx.gms.node.InternalNodeManager;
import com.alibaba.polardbx.gms.node.Node;
import com.alibaba.polardbx.gms.node.NodeServer;
import com.alibaba.polardbx.gms.node.NodeStatusManager;
import com.google.common.collect.ImmutableSet;
import io.airlift.http.client.HttpClient;
import io.airlift.json.JsonCodec;

import javax.inject.Inject;
import java.util.Set;

public class ClusterNodeManager implements InternalNodeManager {

    private static final Logger logger = LoggerFactory.getLogger(ClusterNodeManager.class);

    private InternalNode localNode;
    private AllNodes allNodes;
    private JsonCodec<InternalNode> nodeInfoCodec;
    private JsonCodec<AllNodes> allNodeInfoCodec;

    private HttpClient httpClient;

    @Inject
    public ClusterNodeManager(InternalNode currentNode,
                              JsonCodec<InternalNode> nodeInfoCodec,
                              JsonCodec<AllNodes> allNodeInfoCodec,
                              @ForNodeManager HttpClient httpClient) {
        this.localNode = currentNode;
        this.allNodes = new AllNodes(
            ImmutableSet.of(), ImmutableSet.of(), ImmutableSet.of(), ImmutableSet.of(), ImmutableSet.of());
        this.nodeInfoCodec = nodeInfoCodec;
        this.allNodeInfoCodec = allNodeInfoCodec;
        this.httpClient = httpClient;
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
        if (nodeServer != null) {
            markInactiveNode(allNodes.getActiveNodes(), nodeServer);
            markInactiveNode(allNodes.getOtherActiveRowNodes(), nodeServer);
            markInactiveNode(allNodes.getOtherActiveColumnarNodes(), nodeServer);
        }
    }

    private void markInactiveNode(Set<InternalNode> nodes, NodeServer nodeServer) {
        for (InternalNode node : nodes) {
            if (node.getNodeServer() != null && node.getNodeServer().equals(nodeServer)) {
                nodes.remove(node);
                NodeStatusManager manager = ServiceProvider.getInstance().getServer().getStatusManager();
                manager.tempInactiveNode(node);
                break;
            }
        }
    }

    @Override
    public synchronized void updateNodes(Set<InternalNode> currentActiveNodes, Set<InternalNode> remoteActiveRowNodes,
                                         Set<InternalNode> remoteActiveColumnarNodes,
                                         Set<InternalNode> inactiveNodes,
                                         Set<InternalNode> shuttingDownNodes) {
        if (logger.isDebugEnabled()) {
            logger.debug(
                "updateNodes:activeNodes=" + currentActiveNodes + ",otherActiveNodes=" + remoteActiveRowNodes
                    + ",remoteActiveColumnarNodes=" + remoteActiveColumnarNodes + ",inactiveNodes="
                    + inactiveNodes + "," +
                    "shuttingDownNodes=" + shuttingDownNodes);
        }
        allNodes.updateActiveNodes(currentActiveNodes, remoteActiveRowNodes, remoteActiveColumnarNodes, inactiveNodes,
            shuttingDownNodes);

        if (remoteActiveColumnarNodes.isEmpty()) {
            DynamicConfig.getInstance().existColumnarNodes(false);
        } else {
            DynamicConfig.getInstance().existColumnarNodes(true);
        }
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
