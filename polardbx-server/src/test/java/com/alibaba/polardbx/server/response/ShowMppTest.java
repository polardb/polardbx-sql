package com.alibaba.polardbx.server.response;

import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.gms.node.InternalNode;
import com.alibaba.polardbx.gms.node.MppScope;
import com.alibaba.polardbx.net.packet.RowDataPacket;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class ShowMppTest {
    @Test
    public void testRole() {
        try (MockedStatic<ConfigDataMode> configDataModeMockedStatic = Mockito.mockStatic(ConfigDataMode.class)) {
            configDataModeMockedStatic.when(() -> ConfigDataMode.isMasterMode()).thenReturn(true);
            configDataModeMockedStatic.when(() -> ConfigDataMode.isRowSlaveMode()).thenReturn(false);

            InternalNode node = Mockito.mock(InternalNode.class);
            Mockito.when(node.isLeader()).thenReturn(true);
            Mockito.when(node.getInstId()).thenReturn("");
            Mockito.when(node.getHostPort()).thenReturn("");
            RowDataPacket rowDataPacket = ShowMpp.getRow("utf8", node, MppScope.CURRENT);
            String role = new String(rowDataPacket.fieldValues.get(2));
            Assert.assertEquals("W", role);

            rowDataPacket = ShowMpp.getRow("utf8", node, MppScope.SLAVE);
            role = new String(rowDataPacket.fieldValues.get(2));
            Assert.assertEquals("R", role);

            rowDataPacket = ShowMpp.getRow("utf8", node, MppScope.COLUMNAR);
            role = new String(rowDataPacket.fieldValues.get(2));
            Assert.assertEquals("CR", role);
        }

    }
}
