package com.alibaba.polardbx.manager;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.executor.gms.util.ColumnarTransactionUtils;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.gms.engine.FileSystemGroup;
import com.alibaba.polardbx.gms.engine.FileSystemManager;
import com.alibaba.polardbx.manager.response.ShowColumnarRead;
import com.alibaba.polardbx.manager.response.ShowDirectMemory;
import com.alibaba.polardbx.net.buffer.ByteBufferHolder;
import com.alibaba.polardbx.net.compress.IPacketOutputProxy;
import com.alibaba.polardbx.net.compress.PacketOutputProxyFactory;
import com.alibaba.polardbx.net.compress.RawPacketByteBufferOutputProxy;
import com.alibaba.polardbx.net.packet.EOFPacket;
import com.alibaba.polardbx.net.packet.RowDataPacket;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.nio.ByteBuffer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ManagerShowTest {

    @Test
    public void testGetDirectMemory() {
        int size = 1024 * 1024;
        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(size);
        long directMemorySize = ShowDirectMemory.getDirectMemorySize();

        Assert.assertTrue("Got direct memory size: " + directMemorySize, directMemorySize >= size);
        byteBuffer.clear();
    }

    @Test
    public void testShowDirectMemory() {
        ManagerConnection conn = mock(ManagerConnection.class);
        final ByteBufferHolder buffer = new ByteBufferHolder(ByteBuffer.allocate(8 * 1024));
        IPacketOutputProxy proxy = new RawPacketByteBufferOutputProxy(conn, buffer);
        PacketOutputProxyFactory factory = mock(PacketOutputProxyFactory.class);
        RowDataPacket row = mock(RowDataPacket.class);
        EOFPacket eof = mock(EOFPacket.class);

        when(conn.allocate()).thenReturn(buffer);
        when(factory.createProxy(conn, buffer)).thenReturn(proxy);
        when(conn.checkWriteBuffer(any(ByteBufferHolder.class), any(int.class))).thenReturn(buffer);
        when(conn.writeToBuffer(any(byte[].class), any(ByteBufferHolder.class))).then(
            invocation -> {
                byte[] src = invocation.getArgument(0);
                buffer.put(src, 0, src.length);
                return buffer;
            }
        );

        when(row.write(proxy)).thenReturn(proxy);
        when(eof.write(proxy)).thenReturn(proxy);

        byte initialPacketId = 1;
        eof.packetId = initialPacketId++;
        row.packetId = initialPacketId++;

        ShowDirectMemory.execute(conn);
        Assert.assertTrue(buffer.position() > 0);
    }

    @Test
    public void testShowColumnarRead() {
        ManagerConnection conn = mock(ManagerConnection.class);
        final ByteBufferHolder buffer = new ByteBufferHolder(ByteBuffer.allocate(8 * 1024));
        IPacketOutputProxy proxy = new RawPacketByteBufferOutputProxy(conn, buffer);
        PacketOutputProxyFactory factory = mock(PacketOutputProxyFactory.class);
        RowDataPacket row = mock(RowDataPacket.class);
        EOFPacket eof = mock(EOFPacket.class);

        when(conn.allocate()).thenReturn(buffer);
        when(factory.createProxy(conn, buffer)).thenReturn(proxy);
        when(conn.checkWriteBuffer(any(ByteBufferHolder.class), any(int.class))).thenReturn(buffer);
        when(conn.writeToBuffer(any(byte[].class), any(ByteBufferHolder.class))).then(
            invocation -> {
                byte[] src = invocation.getArgument(0);
                buffer.put(src, 0, src.length);
                return buffer;
            }
        );
        when(conn.getResultSetCharset()).thenReturn("utf8");

        when(row.write(proxy)).thenReturn(proxy);
        when(eof.write(proxy)).thenReturn(proxy);

        byte initialPacketId = 1;
        eof.packetId = initialPacketId++;
        row.packetId = initialPacketId++;

        try (MockedStatic<ColumnarTransactionUtils> mockColumnarTransactionUtils = Mockito.mockStatic(
            ColumnarTransactionUtils.class);
            MockedStatic<FileSystemManager> mockFsManager = Mockito.mockStatic(FileSystemManager.class);
            MockedStatic<InstConfUtil> mockInstConfUtil = Mockito.mockStatic(InstConfUtil.class)) {

            mockColumnarTransactionUtils.when(ColumnarTransactionUtils::getLatestTsoFromGms)
                .thenReturn(1L);
            mockInstConfUtil.when(() -> InstConfUtil.getInt(ConnectionParams.COLUMNAR_TSO_UPDATE_DELAY))
                .thenReturn(1000);

            FileSystemGroup fileSystemGroup = Mockito.mock(FileSystemGroup.class);

            mockFsManager.when(
                () -> FileSystemManager.getFileSystemGroup(any())
            ).thenReturn(fileSystemGroup);
            ShowColumnarRead.execute(conn);
            Assert.assertTrue(buffer.position() > 0);
        }
    }
}
