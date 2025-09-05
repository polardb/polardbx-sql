package com.alibaba.polardbx.server.response;

import com.alibaba.polardbx.gms.config.SqlEngineAlert;
import com.alibaba.polardbx.net.buffer.ByteBufferHolder;
import com.alibaba.polardbx.net.compress.IPacketOutputProxy;
import com.alibaba.polardbx.net.compress.PacketOutputProxyFactory;
import com.alibaba.polardbx.net.packet.EOFPacket;
import com.alibaba.polardbx.net.packet.ResultSetHeaderPacket;
import com.alibaba.polardbx.net.packet.RowDataPacket;
import com.alibaba.polardbx.server.ServerConnection;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ShowSqlEngineAlertTest {

    private ServerConnection mockServerConnection;
    private ByteBufferHolder mockByteBufferHolder;
    private PacketOutputProxyFactory mockPacketOutputProxyFactory;
    private IPacketOutputProxy mockPacketOutputProxy;
    private ResultSetHeaderPacket mockResultSetHeaderPacket;
    private EOFPacket mockEofPacket;
    private RowDataPacket mockRowDataPacket;
    private SqlEngineAlert mockSqlEngineAlert;

    @Before
    public void setUp() throws Exception {
        // 初始化 mock 对象
        mockServerConnection = mock(ServerConnection.class);
        mockByteBufferHolder = mock(ByteBufferHolder.class);
        mockPacketOutputProxyFactory = mock(PacketOutputProxyFactory.class);
        mockPacketOutputProxy = mock(IPacketOutputProxy.class);
        mockResultSetHeaderPacket = mock(ResultSetHeaderPacket.class);
        mockEofPacket = mock(EOFPacket.class);
        mockRowDataPacket = mock(RowDataPacket.class);
        mockSqlEngineAlert = mock(SqlEngineAlert.class);
    }

    @Test
    public void testExecuteWithEofNotDeprecatedAndHasAlertData() {
        // 准备测试数据
        Map<String, String> alertMap = new HashMap<>();
        alertMap.put("testEvent", "testDetail");

        // 设置模拟行为
        when(mockServerConnection.allocate()).thenReturn(mockByteBufferHolder);
        when(mockServerConnection.isCompressProto()).thenReturn(false);
        when(mockPacketOutputProxyFactory.createProxy(mockServerConnection, mockByteBufferHolder))
            .thenReturn(mockPacketOutputProxy);
        when(mockResultSetHeaderPacket.write(mockPacketOutputProxy)).thenReturn(mockPacketOutputProxy);
        when(mockEofPacket.write(mockPacketOutputProxy)).thenReturn(mockPacketOutputProxy);
        when(mockRowDataPacket.write(mockPacketOutputProxy)).thenReturn(mockPacketOutputProxy);
        when(mockSqlEngineAlert.collectAndClear()).thenReturn(alertMap);
        when(mockServerConnection.isEofDeprecated()).thenReturn(false);
        when(mockPacketOutputProxy.getConnection()).thenReturn(mockServerConnection);
        when(mockServerConnection.getPacketHeaderSize()).thenReturn(10);

        try (MockedStatic<SqlEngineAlert> mockedSqlEngineAlert = mockStatic(SqlEngineAlert.class);
            MockedStatic<PacketOutputProxyFactory> mockedPacketOutputProxyFactory = mockStatic(
                PacketOutputProxyFactory.class)) {
            mockedSqlEngineAlert.when(SqlEngineAlert::getInstance).thenReturn(mockSqlEngineAlert);
            mockedPacketOutputProxyFactory.when(PacketOutputProxyFactory::getInstance)
                .thenReturn(mockPacketOutputProxyFactory);
            doNothing().when(mockPacketOutputProxy).write((byte) 0);
            doNothing().when(mockPacketOutputProxy).writeUB2(0);
            doNothing().when(mockPacketOutputProxy).writeUB3(0);
            doNothing().when(mockPacketOutputProxy).writeInt(0);
            doNothing().when(mockPacketOutputProxy).writeFloat(0f);
            doNothing().when(mockPacketOutputProxy).writeUB4(0L);
            doNothing().when(mockPacketOutputProxy).writeLong(0L);
            doNothing().when(mockPacketOutputProxy).writeDouble(0d);
            doNothing().when(mockPacketOutputProxy).writeLength(0L);
            doNothing().when(mockPacketOutputProxy).write(new byte[0]);
            doNothing().when(mockPacketOutputProxy).write(new byte[0], 0, 0);
            doNothing().when(mockPacketOutputProxy).writeWithNull(new byte[0]);
            doNothing().when(mockPacketOutputProxy).writeWithLength(new byte[0]);
            doNothing().when(mockPacketOutputProxy).writeWithLength(new byte[0], (byte) 0);
            doNothing().when(mockPacketOutputProxy).checkWriteCapacity(0);
            doNothing().when(mockPacketOutputProxy).packetBegin();
            doNothing().when(mockPacketOutputProxy).packetEnd();
            doNothing().when(mockPacketOutputProxy).writeArrayAsPacket(new byte[0]);
            doNothing().when(mockPacketOutputProxy).close();

            ShowSqlEngineAlert.execute(mockServerConnection);
        }
        verify(mockPacketOutputProxy, times(7)).packetBegin();
        verify(mockPacketOutputProxy, times(7)).packetEnd();
        verify(mockPacketOutputProxy, times(2)).write(any(byte[].class));
    }
}
