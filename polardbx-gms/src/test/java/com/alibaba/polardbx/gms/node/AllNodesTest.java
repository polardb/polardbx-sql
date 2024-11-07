package com.alibaba.polardbx.gms.node;

import com.google.common.collect.ImmutableSet;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class AllNodesTest {

    private AllNodes allNodes;

    @Before
    public void setUp() {
        Set<InternalNode> activeNodes = new HashSet<>();
        InternalNode activeNode = mockNode();
        InternalNode otherActiveRowNode = mockNode();
        InternalNode otherActiveColumnarNode = mockNode();
        InternalNode inactiveNode = mockNode();
        InternalNode shuttingDownNode = mockNode();

        activeNodes.add(activeNode);

        Set<InternalNode> otherActiveRowNodes = ImmutableSet.of(otherActiveRowNode);
        Set<InternalNode> otherActiveColumnarNodes = ImmutableSet.of(otherActiveColumnarNode);
        Set<InternalNode> inactiveNodes = ImmutableSet.of(inactiveNode);
        Set<InternalNode> shuttingDownNodes = ImmutableSet.of(shuttingDownNode);

        allNodes =
            new AllNodes(activeNodes, otherActiveRowNodes, otherActiveColumnarNodes, inactiveNodes, shuttingDownNodes);
    }

    private InternalNode mockNode() {
        InternalNode node = new InternalNode("key", "cluster1", "inst1", "11.11.11.11", 1234, 12345,
            NodeVersion.UNKNOWN, true, true, false, true);
        return node;
    }

    @Test
    public void testGetAllWorkersAllScope() {
        List<InternalNode> workers = allNodes.getAllWorkers(MppScope.ALL);
        assertEquals(3, workers.size());
    }

    @Test
    public void testGetAllWorkersSlaveScope() {
        List<InternalNode> workers = allNodes.getAllWorkers(MppScope.SLAVE);
        assertEquals(1, workers.size());
    }

    @Test
    public void testGetAllWorkersColumnarScope() {
        List<InternalNode> workers = allNodes.getAllWorkers(MppScope.COLUMNAR);
        assertEquals(1, workers.size());
    }

    @Test
    public void testGetAllCoordinators() {
        List<Node> coordinators = allNodes.getAllCoordinators();
        assertEquals(1, coordinators.size());
    }
}
