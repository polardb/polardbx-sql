package com.alibaba.polardbx.manager;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.manager.response.ShowDirectMemory;
import com.alibaba.polardbx.net.buffer.ByteBufferHolder;
import com.alibaba.polardbx.net.compress.IPacketOutputProxy;
import com.alibaba.polardbx.net.compress.PacketOutputProxyFactory;
import com.alibaba.polardbx.net.compress.RawPacketByteBufferOutputProxy;
import com.alibaba.polardbx.net.packet.EOFPacket;
import com.alibaba.polardbx.net.packet.RowDataPacket;
import org.junit.Test;

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

        Assert.assertTrue(directMemorySize >= size);
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
}
