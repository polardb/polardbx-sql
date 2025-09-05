package com.alibaba.polardbx.executor.mpp.execution.scheduler;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.executor.mpp.execution.NodeTaskMap;
import com.alibaba.polardbx.executor.mpp.metadata.Split;
import com.alibaba.polardbx.executor.mpp.split.OssSplit;
import com.alibaba.polardbx.gms.node.InternalNode;
import com.alibaba.polardbx.gms.node.InternalNodeManager;
import com.alibaba.polardbx.gms.node.Node;
import com.alibaba.polardbx.gms.node.NodeVersion;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.utils.PartitionUtils;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ColumnarNodeSelectorTest {

    private ColumnarNodeSelector nodeSelector;
    private List<Node> candidateNodes;
    private ExecutionContext executionContext;

    @Before
    public void setUp() {
        InternalNodeManager nodeManager = Mockito.mock(InternalNodeManager.class);
        NodeTaskMap nodeTaskMap = Mockito.mock(NodeTaskMap.class);
        InternalNode node1 = new InternalNode("node1", "cluster1", "inst1", "11.11.11.11", 1234, 12345,
            NodeVersion.UNKNOWN, true, true, false, false);
        InternalNode node2 = new InternalNode("node2", "cluster1", "inst1", "10.10.10.10", 1234, 12345,
            NodeVersion.UNKNOWN, true, true, false, false);
        Set<InternalNode> nodes = new HashSet<>(Arrays.asList(node1, node2));
        executionContext = new ExecutionContext();
        nodeSelector =
            new ColumnarNodeSelector(nodeManager, nodeTaskMap, nodes, 2, 1024, false, RandomNodeMode.NONE, true, false,
                executionContext);
        candidateNodes = Arrays.asList(node1, node2); // Fill with mock or real nodes
    }

    @Test
    public void testScheduleOssSplit() {
        nodeSelector.setScheduleByPartition(true);
        OssSplit split = Mockito.mock(OssSplit.class);
        when(split.getPartIndex()).thenReturn(-1);
        when(split.getLogicalSchema()).thenReturn("test");
        when(split.getLogicalTableName()).thenReturn("test");
        when(split.getPhysicalSchema()).thenReturn("test");
        when(split.getPhyTableNameList()).thenReturn(Arrays.asList("test"));

        List<Split> splits = ImmutableList.of(new Split(true, split));
        NodeAssignmentStats assignmentStats = mock(NodeAssignmentStats.class);
        Multimap assignment = HashMultimap.create();
        try (MockedStatic<PartitionUtils> theMock = Mockito.mockStatic(PartitionUtils.class)) {
            when(PartitionUtils.calcPartition("test", "test", "test", "test", executionContext)).thenReturn(1);
            Multimap<Node, Split> result =
                nodeSelector.scheduleOssSplit(splits, candidateNodes, assignmentStats, assignment);

            Assert.assertTrue(result.size() == 1, "assign result should be one");
            result.entries().stream().forEach(entry -> {
                Node node = entry.getKey();
                Assert.assertTrue(node.getNodeIdentifier().equals("node1"), "assign result should be node1");
            });
        }
    }

    @Test
    public void testScheduleOssSplitWithPartition() {
        nodeSelector.setScheduleByPartition(false);
        OssSplit split = Mockito.mock(OssSplit.class);
        when(split.getPartIndex()).thenReturn(1);

        List<Split> splits = ImmutableList.of(new Split(true, split));
        NodeAssignmentStats assignmentStats = mock(NodeAssignmentStats.class);
        Multimap assignment = HashMultimap.create();
        Multimap<Node, Split> result =
            nodeSelector.scheduleOssSplit(splits, candidateNodes, assignmentStats, assignment);

        Assert.assertTrue(result.size() == 1, "assign result should be one");
        result.entries().stream().forEach(entry -> {
            Node node = entry.getKey();
            Assert.assertTrue(node.getNodeIdentifier().equals("node1"), "assign result should be node1");
        });
    }

    @Test
    public void testScheduleOssSplitWithoutPartition() {
        nodeSelector.setScheduleByPartition(false);
        OssSplit split = Mockito.mock(OssSplit.class);
        when(split.getPartIndex()).thenReturn(-1);
        when(split.getDesignatedFile()).thenReturn(Arrays.asList("asdfagagasgdaga.orc"));
        List<Split> splits = ImmutableList.of(new Split(true, split));
        NodeAssignmentStats assignmentStats = mock(NodeAssignmentStats.class);
        Multimap assignment = HashMultimap.create();
        Multimap<Node, Split> result =
            nodeSelector.scheduleOssSplit(splits, candidateNodes, assignmentStats, assignment);
        Assert.assertTrue(result.size() == 1, "assign result should be one");
        result.entries().stream().forEach(entry -> {
            Node node = entry.getKey();
            Assert.assertTrue(node.getNodeIdentifier().equals("node2"), "assign result should be node2");
        });
    }
}
