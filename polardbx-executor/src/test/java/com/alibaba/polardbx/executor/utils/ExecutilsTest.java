package com.alibaba.polardbx.executor.utils;

import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.mpp.deploy.Server;
import com.alibaba.polardbx.executor.mpp.deploy.ServiceProvider;
import com.alibaba.polardbx.gms.node.AllNodes;
import com.alibaba.polardbx.gms.node.GmsNodeManager;
import com.alibaba.polardbx.gms.node.InternalNode;
import com.alibaba.polardbx.gms.node.InternalNodeManager;
import com.alibaba.polardbx.gms.node.MppScope;
import com.alibaba.polardbx.gms.node.NodeVersion;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class ExecutilsTest {

    private AllNodes allNodes;

    @Before
    public void setUp() {
        InternalNode activeNode = mockNode();
        InternalNode otherActiveRowNode = mockNode();
        InternalNode otherActiveColumnarNode = mockNode();
        InternalNode inactiveNode = mockNode();
        InternalNode shuttingDownNode = mockNode();

        Set<InternalNode> activeNodes = ImmutableSet.of(activeNode);
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
    public void testMppScope() {

        try (final MockedStatic<ConfigDataMode> mockConfigDataMode = mockStatic(ConfigDataMode.class);
            final MockedStatic<ServiceProvider> mockServiceProvider = mockStatic(ServiceProvider.class);
        ) {
            ServiceProvider serviceProvider = mock(ServiceProvider.class);
            when(ServiceProvider.getInstance()).thenReturn(serviceProvider);
            Server server = mock(Server.class);
            when(serviceProvider.getServer()).thenReturn(server);

            InternalNodeManager nodeManager = mock(InternalNodeManager.class);
            when(server.getNodeManager()).thenReturn(nodeManager);
            when(nodeManager.getAllNodes()).thenReturn(allNodes);
            when(ConfigDataMode.isMasterMode()).thenReturn(false);
            MppScope scope = ExecUtils.getMppSchedulerScope(false);
            Assert.assertEquals(scope, MppScope.CURRENT);

            when(ConfigDataMode.isMasterMode()).thenReturn(true);
            scope = ExecUtils.getMppSchedulerScope(false);
            Assert.assertEquals(scope, MppScope.COLUMNAR);

            when(ConfigDataMode.isMasterMode()).thenReturn(true);
            scope = ExecUtils.getMppSchedulerScope(true);
            Assert.assertEquals(scope, MppScope.SLAVE);
        }
    }

    @Test
    public void testAllowMppScope() {

        try (final MockedStatic<ConfigDataMode> mockConfigDataMode = mockStatic(ConfigDataMode.class);
            final MockedStatic<ServiceProvider> mockServiceProvider = mockStatic(ServiceProvider.class);
        ) {
            ServiceProvider serviceProvider = mock(ServiceProvider.class);
            when(ServiceProvider.getInstance()).thenReturn(serviceProvider);
            Server server = mock(Server.class);
            when(serviceProvider.getServer()).thenReturn(server);

            InternalNodeManager nodeManager = mock(InternalNodeManager.class);
            when(server.getNodeManager()).thenReturn(nodeManager);
            when(nodeManager.getAllNodes()).thenReturn(allNodes);
            when(ConfigDataMode.isMasterMode()).thenReturn(false);

            Assert.assertTrue(ExecUtils.allowMppMode(new ExecutionContext()));

            when(ConfigDataMode.isMasterMode()).thenReturn(true);
            ExecutionContext context = new ExecutionContext();
            context.getExtraCmds().put("ENABLE_MASTER_MPP", true);
            Assert.assertTrue(ExecUtils.allowMppMode(context));
            context.getExtraCmds().put("ENABLE_MASTER_MPP", false);
            Assert.assertTrue(ExecUtils.allowMppMode(context));
            context.getExtraCmds().put("ENABLE_COLUMNAR_SCHEDULE", true);
            Assert.assertTrue(ExecUtils.allowMppMode(context));

        }
    }

    @Test
    public void testParallelism() {
        try (final MockedStatic<ConfigDataMode> mockConfigDataMode = mockStatic(ConfigDataMode.class);
            final MockedStatic<ServiceProvider> mockServiceProvider = mockStatic(ServiceProvider.class);
        ) {
            ServiceProvider serviceProvider = mock(ServiceProvider.class);
            when(ServiceProvider.getInstance()).thenReturn(serviceProvider);
            Server server = mock(Server.class);
            when(serviceProvider.getServer()).thenReturn(server);

            InternalNodeManager nodeManager = mock(InternalNodeManager.class);
            when(server.getNodeManager()).thenReturn(nodeManager);
            when(nodeManager.getAllNodes()).thenReturn(allNodes);
            when(ConfigDataMode.isMasterMode()).thenReturn(false);

            HashMap<String, String> hashMap = new HashMap<>();
            ParamManager paramManager = new ParamManager(hashMap);
            hashMap.put("MPP_MAX_PARALLELISM", "1");
            Assert.assertEquals(1, ExecUtils.getMppMaxParallelism(paramManager, false));
            hashMap.put("MPP_MAX_PARALLELISM", "-1");
            hashMap.put("POLARDBX_PARALLELISM", "10");
            Assert.assertEquals(10, ExecUtils.getMppMaxParallelism(paramManager, false));
        }
    }

    @Test
    public void testCnCores() {
        GmsNodeManager gmsNodeManager = mock(GmsNodeManager.class);

        try (final MockedStatic<GmsNodeManager> mockGmsNodeManagerStatic = mockStatic(GmsNodeManager.class);
            final MockedStatic<ConfigDataMode> mockConfigDataMode = mockStatic(ConfigDataMode.class);) {
            when(GmsNodeManager.getInstance()).thenReturn(gmsNodeManager);

            HashMap<String, String> hashMap = new HashMap<>();
            ParamManager paramManager = new ParamManager(hashMap);
            when(gmsNodeManager.getReadOnlyNodes()).thenReturn(new ArrayList<>());
            Assert.assertTrue(ExecUtils.getPolarDBXCNCores(paramManager, MppScope.SLAVE) > 0);

            when(gmsNodeManager.getColumnarReadOnlyNodes()).thenReturn(new ArrayList<>());
            Assert.assertTrue(ExecUtils.getPolarDBXCNCores(paramManager, MppScope.COLUMNAR) > 0);
        }

    }

}
