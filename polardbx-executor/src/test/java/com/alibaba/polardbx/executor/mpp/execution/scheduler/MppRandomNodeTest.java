package com.alibaba.polardbx.executor.mpp.execution.scheduler;

import com.alibaba.polardbx.executor.mpp.deploy.Server;
import com.alibaba.polardbx.executor.mpp.deploy.ServiceProvider;
import com.alibaba.polardbx.executor.mpp.execution.NodeTaskMap;
import com.alibaba.polardbx.gms.node.InternalNode;
import com.alibaba.polardbx.gms.node.InternalNodeManager;
import com.alibaba.polardbx.gms.node.Node;
import com.alibaba.polardbx.gms.node.NodeVersion;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.executor.mpp.execution.scheduler.SimpleNodeSelector.NODE_COMPARATOR;

public class MppRandomNodeTest {

    private final int totalNodes = 8;
    private List<InternalNode> nodeList;
    private Set<InternalNode> nodes;
    private List<InternalNode> sortedNodeList;

    @Before
    public void before() {
        nodeList = new ArrayList<>();
        for (int i = 0; i < totalNodes; i++) {
            InternalNode node = new InternalNode("node" + i, "cluster1", "inst1", "11.11.11.1" + i, 1234, 12345,
                NodeVersion.UNKNOWN, true, true, false, false);
            nodeList.add(node);
        }
        nodes = new HashSet<>(nodeList);
        sortedNodeList = nodeList.stream().sorted(NODE_COMPARATOR).collect(Collectors.toList());
    }

    @Test
    public void testMppNodeGroup() {
        final int limitNode = 4;
        final RandomNodeMode randomNodeMode = RandomNodeMode.GROUP;

        InternalNodeManager nodeManager = Mockito.mock(InternalNodeManager.class);
        NodeTaskMap nodeTaskMap = Mockito.mock(NodeTaskMap.class);

        for (int curNodeIdx = 0; curNodeIdx < totalNodes; curNodeIdx++) {
            try (MockedStatic<ServiceProvider> mockedStatic = Mockito.mockStatic(ServiceProvider.class)) {
                // mock local node
                ServiceProvider serviceProvider = getMockCurrentNodeProvider(nodeList.get(curNodeIdx));
                mockedStatic.when(ServiceProvider::getInstance).thenReturn(serviceProvider);

                ColumnarNodeSelector nodeSelector = new ColumnarNodeSelector(nodeManager, nodeTaskMap, nodes, limitNode,
                    1024, false, randomNodeMode, true, false, new ExecutionContext());

                List<Node> resultNodes = nodeSelector.getOrderedNode();
                Assert.assertEquals(limitNode, resultNodes.size());

                if (curNodeIdx < 4) {
                    for (int i = 0; i < limitNode; i++) {
                        Assert.assertEquals(sortedNodeList.get(i).getNodeIdentifier(),
                            resultNodes.get(i).getNodeIdentifier());
                    }
                } else {
                    for (int i = 0; i < limitNode; i++) {
                        Assert.assertEquals(sortedNodeList.get(i + limitNode).getNodeIdentifier(),
                            resultNodes.get(i).getNodeIdentifier());
                    }
                }
            }
        }
    }

    @Test
    public void testMppNodeGroup2() {
        final int limitNode = 8;
        final RandomNodeMode randomNodeMode = RandomNodeMode.GROUP;

        InternalNodeManager nodeManager = Mockito.mock(InternalNodeManager.class);
        NodeTaskMap nodeTaskMap = Mockito.mock(NodeTaskMap.class);

        for (int curNodeIdx = 0; curNodeIdx < totalNodes; curNodeIdx++) {
            try (MockedStatic<ServiceProvider> mockedStatic = Mockito.mockStatic(ServiceProvider.class)) {
                // mock local node
                ServiceProvider serviceProvider = getMockCurrentNodeProvider(nodeList.get(curNodeIdx));
                mockedStatic.when(ServiceProvider::getInstance).thenReturn(serviceProvider);

                ColumnarNodeSelector nodeSelector = new ColumnarNodeSelector(nodeManager, nodeTaskMap, nodes, limitNode,
                    1024, false, randomNodeMode, true, false, new ExecutionContext());

                List<Node> resultNodes = nodeSelector.getOrderedNode();
                Assert.assertEquals(limitNode, resultNodes.size());

                for (int i = 0; i < limitNode; i++) {
                    Assert.assertEquals(sortedNodeList.get(i).getNodeIdentifier(),
                        resultNodes.get(i).getNodeIdentifier());
                }
            }
        }
    }

    @Test
    public void testMppNodeGroup3() {
        final int limitNode = 2;
        final RandomNodeMode randomNodeMode = RandomNodeMode.GROUP;

        InternalNodeManager nodeManager = Mockito.mock(InternalNodeManager.class);
        NodeTaskMap nodeTaskMap = Mockito.mock(NodeTaskMap.class);

        for (int curNodeIdx = 0; curNodeIdx < totalNodes; curNodeIdx++) {
            try (MockedStatic<ServiceProvider> mockedStatic = Mockito.mockStatic(ServiceProvider.class)) {
                // mock local node
                ServiceProvider serviceProvider = getMockCurrentNodeProvider(nodeList.get(curNodeIdx));
                mockedStatic.when(ServiceProvider::getInstance).thenReturn(serviceProvider);

                ColumnarNodeSelector nodeSelector = new ColumnarNodeSelector(nodeManager, nodeTaskMap, nodes, limitNode,
                    1024, false, randomNodeMode, true, false, new ExecutionContext());

                List<Node> resultNodes = nodeSelector.getOrderedNode();
                Assert.assertEquals(limitNode, resultNodes.size());

                int offset;
                if (curNodeIdx < 2) {
                    offset = 0;
                } else if (curNodeIdx < 4) {
                    offset = 2;
                } else if (curNodeIdx < 6) {
                    offset = 4;
                } else {
                    offset = 6;
                }
                for (int i = 0; i < limitNode; i++) {
                    Assert.assertEquals(sortedNodeList.get(i + offset).getNodeIdentifier(),
                        resultNodes.get(i).getNodeIdentifier());
                }
            }
        }
    }

    @Test
    public void testMppNodeGroup4() {
        final int limitNode = 3;
        final List<InternalNode> nodeGroup0 = nodeList.subList(0, 3);
        final List<InternalNode> nodeGroup1 = nodeList.subList(3, 6);
        final List<InternalNode> nodeGroup2 = Lists.newArrayList(nodeList.get(0), nodeList.get(6), nodeList.get(7));

        final RandomNodeMode randomNodeMode = RandomNodeMode.GROUP;

        InternalNodeManager nodeManager = Mockito.mock(InternalNodeManager.class);
        NodeTaskMap nodeTaskMap = Mockito.mock(NodeTaskMap.class);

        for (int curNodeIdx = 0; curNodeIdx < totalNodes; curNodeIdx++) {
            try (MockedStatic<ServiceProvider> mockedStatic = Mockito.mockStatic(ServiceProvider.class)) {
                InternalNode curNode = nodeList.get(curNodeIdx);
                // mock local node
                ServiceProvider serviceProvider = getMockCurrentNodeProvider(curNode);
                mockedStatic.when(ServiceProvider::getInstance).thenReturn(serviceProvider);

                ColumnarNodeSelector nodeSelector = new ColumnarNodeSelector(nodeManager, nodeTaskMap, nodes, limitNode,
                    1024, false, randomNodeMode, true, false, new ExecutionContext());

                List<Node> resultNodes = nodeSelector.getOrderedNode();
                Assert.assertEquals(limitNode, resultNodes.size());

                List<InternalNode> targetNodeGroup;
                if (curNodeIdx < 3) {
                    targetNodeGroup = nodeGroup0;
                } else if (curNodeIdx < 6) {
                    targetNodeGroup = nodeGroup1;
                } else {
                    targetNodeGroup = nodeGroup2;
                }
                for (int i = 0; i < limitNode; i++) {
                    Assert.assertEquals(String.format("Current node: %s, targetNodeGroup: %s, resultNodeGroup: %s",
                            curNode.getNodeIdentifier(), targetNodeGroup, resultNodes),
                        targetNodeGroup.get(i).getNodeIdentifier(),
                        resultNodes.get(i).getNodeIdentifier());
                }
            }
        }
    }

    @Test
    public void testMppNodeRandom() {
        final int limitNode = 4;
        final int totalNodes = 8;
        final RandomNodeMode randomNodeMode = RandomNodeMode.RANDOM;

        InternalNodeManager nodeManager = Mockito.mock(InternalNodeManager.class);
        NodeTaskMap nodeTaskMap = Mockito.mock(NodeTaskMap.class);
        List<InternalNode> nodeList = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            InternalNode node = new InternalNode("node" + i, "cluster1", "inst1", "11.11.11.1" + i, 1234, 12345,
                NodeVersion.UNKNOWN, true, true, false, false);
            nodeList.add(node);
        }

        Set<InternalNode> nodes = new HashSet<>(nodeList);

        for (int curNodeIdx = 0; curNodeIdx < totalNodes; curNodeIdx++) {
            try (MockedStatic<ServiceProvider> mockedStatic = Mockito.mockStatic(ServiceProvider.class)) {
                // mock local node
                ServiceProvider serviceProvider = getMockCurrentNodeProvider(nodeList.get(curNodeIdx));
                mockedStatic.when(ServiceProvider::getInstance).thenReturn(serviceProvider);

                ColumnarNodeSelector nodeSelector = new ColumnarNodeSelector(nodeManager, nodeTaskMap, nodes, limitNode,
                    1024, false, randomNodeMode, true, false, new ExecutionContext());

                List<Node> resultNodes = nodeSelector.getOrderedNode();
                // since this is random mode, we only check if the size is correct
                Assert.assertEquals(limitNode, resultNodes.size());
            }
        }
    }

    @Test
    public void testMppNodeNone() {
        final int limitNode = 4;
        final int totalNodes = 8;
        final RandomNodeMode randomNodeMode = RandomNodeMode.NONE;

        InternalNodeManager nodeManager = Mockito.mock(InternalNodeManager.class);
        NodeTaskMap nodeTaskMap = Mockito.mock(NodeTaskMap.class);
        List<InternalNode> nodeList = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            InternalNode node = new InternalNode("node" + i, "cluster1", "inst1", "11.11.11.1" + i, 1234, 12345,
                NodeVersion.UNKNOWN, true, true, false, false);
            nodeList.add(node);
        }

        Set<InternalNode> nodes = new HashSet<>(nodeList);

        for (int curNodeIdx = 0; curNodeIdx < totalNodes; curNodeIdx++) {
            try (MockedStatic<ServiceProvider> mockedStatic = Mockito.mockStatic(ServiceProvider.class)) {
                // mock local node
                ServiceProvider serviceProvider = getMockCurrentNodeProvider(nodeList.get(curNodeIdx));
                mockedStatic.when(ServiceProvider::getInstance).thenReturn(serviceProvider);

                ColumnarNodeSelector nodeSelector = new ColumnarNodeSelector(nodeManager, nodeTaskMap, nodes, limitNode,
                    1024, false, randomNodeMode, true, false, new ExecutionContext());

                List<Node> resultNodes = nodeSelector.getOrderedNode();
                // always the same nodes
                Assert.assertEquals(limitNode, resultNodes.size());
                for (int i = 0; i < limitNode; i++) {
                    Assert.assertEquals(nodeList.get(i).getNodeIdentifier(), resultNodes.get(i).getNodeIdentifier());
                }
            }
        }
    }

    @Test
    public void testRandomNodeMode() {
        Assert.assertEquals(RandomNodeMode.RANDOM, RandomNodeMode.getRandomNodeMode("random"));
        Assert.assertEquals(RandomNodeMode.GROUP, RandomNodeMode.getRandomNodeMode("group"));
        Assert.assertEquals(RandomNodeMode.GROUP, RandomNodeMode.getRandomNodeMode("GROUP"));
        Assert.assertEquals(RandomNodeMode.NONE, RandomNodeMode.getRandomNodeMode("none"));
        // allow illegal values
        Assert.assertEquals(RandomNodeMode.RANDOM, RandomNodeMode.getRandomNodeMode("other"));
        Assert.assertEquals(RandomNodeMode.RANDOM, RandomNodeMode.getRandomNodeMode(null));
    }

    private ServiceProvider getMockCurrentNodeProvider(InternalNode internalNode) {
        ServiceProvider serviceProvider = Mockito.mock(ServiceProvider.class);
        Server server = Mockito.mock(Server.class);
        Mockito.when(serviceProvider.getServer()).thenReturn(server);
        Mockito.when(server.getLocalNode()).thenReturn(internalNode);
        return serviceProvider;
    }

}
