package com.alibaba.polardbx.mock;

import org.mockito.Mockito;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketOption;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.util.Set;

public class MockSocketChannel extends SocketChannel {

    public MockSocketChannel(SelectorProvider provider) {
        super(provider);
    }

    @Override
    public SocketChannel bind(SocketAddress local) throws IOException {
        return null;
    }

    @Override
    public <T> SocketChannel setOption(SocketOption<T> name, T value) throws IOException {
        return null;
    }

    @Override
    public <T> T getOption(SocketOption<T> name) throws IOException {
        return null;
    }

    @Override
    public Set<SocketOption<?>> supportedOptions() {
        return null;
    }

    @Override
    public SocketChannel shutdownInput() throws IOException {
        return null;
    }

    @Override
    public SocketChannel shutdownOutput() throws IOException {
        return null;
    }

    public Socket socket() {
        Socket socket = Mockito.mock(Socket.class);
        Mockito.when(socket.isConnected()).thenReturn(true);
        Mockito.when(socket.isClosed()).thenReturn(false);
        InetAddress address = Mockito.mock(InetAddress.class);
        Mockito.when(socket.getInetAddress()).thenReturn(address);
        return socket;
    }

    public boolean isConnected() {
        return false;
    }

    public boolean isConnectionPending() {
        return false;
    }

    public boolean connect(SocketAddress address) throws IOException {
        return false;
    }

    public boolean finishConnect() throws IOException {
        return false;
    }

    @Override
    public SocketAddress getRemoteAddress() {
        return null;
    }

    public int read(ByteBuffer target) throws IOException {
        return 0;
    }

    public long read(ByteBuffer[] targets, int offset, int length) throws IOException {
        return 0;
    }

    public int write(ByteBuffer source) throws IOException {
        return 0;
    }

    public long write(ByteBuffer[] sources, int offset, int length) throws IOException {
        return 0;
    }

    @Override
    public SocketAddress getLocalAddress() {
        return new SocketAddress() {
            @Override
            public int hashCode() {
                return super.hashCode();
            }
        };
    }

    protected void implCloseSelectableChannel() throws IOException {
    }

    protected void implConfigureBlocking(boolean blockingMode) throws IOException {
    }
}
