package com.alibaba.polardbx.executor.mpp.discover;

import com.alibaba.polardbx.common.utils.InstanceRole;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.gms.node.GmsNodeManager;
import com.alibaba.polardbx.gms.node.InternalNode;
import com.alibaba.polardbx.gms.node.NodeVersion;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class PolarDBXNodeStatusManagerTest {

    private Pair<InternalNode, GmsNodeManager.GmsNode> masterNodeInfo;
    private Pair<InternalNode, GmsNodeManager.GmsNode> slaveNodeInfo;
    private Pair<InternalNode, GmsNodeManager.GmsNode> columnarNodeInfo;

    @Before
    public void setUp() throws Exception {
        masterNodeInfo = getNodeAndGmsNode("master", false, 1);
        slaveNodeInfo = getNodeAndGmsNode("salve", false, 2);
        columnarNodeInfo = getNodeAndGmsNode("columnar", true, 4);
    }

    private Pair<InternalNode, GmsNodeManager.GmsNode> getNodeAndGmsNode(String key, boolean htap, int instType) {
        InternalNode node = new InternalNode(key, "cluster1", "inst1", "11.11.11.11", 1234, 12345,
            NodeVersion.UNKNOWN, true, true, false, htap);
        GmsNodeManager.GmsNode gmsNode = new GmsNodeManager.GmsNode();
        gmsNode.instType = instType;
        return Pair.of(node, gmsNode);
    }

    @Test
    public void testUpdateHeartbeatStatus() {

        String sql = generateUpdateStatusSql(masterNodeInfo, 1, InstanceRole.MASTER);

        Assert.assertTrue(sql.contains("TIMESTAMPDIFF"));

    }

    @Test
    public void testUpdateInactiveStatus() {

        String sql = generateUpdateStatusSql(masterNodeInfo, 0, InstanceRole.MASTER);

        Assert.assertTrue(!sql.contains("ROLE") && !sql.contains("TIMESTAMPDIFF"));

    }

    @Test
    public void testUpdateSlaveStatus() {

        String sql = generateUpdateStatusSql(slaveNodeInfo, 1, InstanceRole.ROW_SLAVE);

        Assert.assertTrue(sql.contains("ROLE") && !sql.contains("TIMESTAMPDIFF"));

    }

    @Test
    public void testUpdateColumnarStatus() {

        String sql = generateUpdateStatusSql(columnarNodeInfo, 1, InstanceRole.COLUMNAR_SLAVE);

        Assert.assertTrue(sql.contains("ROLE") && !sql.contains("TIMESTAMPDIFF"));

    }

    @Test
    public void testRole() {
        PolarDBXNodeStatusManager polarDBXNodeStatusManager =
            spy(new PolarDBXNodeStatusManager(null, masterNodeInfo.getKey()));

        int role = polarDBXNodeStatusManager.getRole(columnarNodeInfo.getKey());
        Assert.assertEquals(39, role);
        try (final MockedStatic<ConfigDataMode> mockConfigDataMode = mockStatic(ConfigDataMode.class);) {
            when(ConfigDataMode.isColumnarMode()).thenReturn(true);
            role = polarDBXNodeStatusManager.getRole(columnarNodeInfo.getKey());
            Assert.assertEquals(67, role);
        }
    }

    private String generateUpdateStatusSql(Pair<InternalNode, GmsNodeManager.GmsNode> nodeinfo, int status,
                                           InstanceRole role) {
        ClusterNodeManager nodeManager = new ClusterNodeManager(nodeinfo.getKey(), null, null, null);
        PolarDBXNodeStatusManager polarDBXNodeStatusManager =
            spy(new PolarDBXNodeStatusManager(nodeManager, nodeinfo.getKey()));

        Mockito.doNothing().when(polarDBXNodeStatusManager).init();

        GmsNodeManager gmsNodeManager = mock(GmsNodeManager.class);

        try (final MockedStatic<GmsNodeManager> mockGmsNodeManagerStatic = mockStatic(GmsNodeManager.class);
            final MockedStatic<ConfigDataMode> mockConfigDataMode = mockStatic(ConfigDataMode.class);) {
            when(GmsNodeManager.getInstance()).thenReturn(gmsNodeManager);
            when(ConfigDataMode.getInstanceRole()).thenReturn(role);
            when(ConfigDataMode.isRowSlaveMode()).thenReturn(role == InstanceRole.ROW_SLAVE);
            when(gmsNodeManager.getLocalNode()).thenReturn(nodeinfo.getValue());
            String sql = polarDBXNodeStatusManager.updateTableMetaSql(status);
            return sql;
        }
    }
}
