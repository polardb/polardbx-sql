package com.alibaba.polardbx.mock;

import com.alibaba.polardbx.net.FrontendConnection;
import com.alibaba.polardbx.net.compress.IPacketOutputProxy;

import java.nio.channels.spi.SelectorProvider;

public class MockPacketOutputProxy implements IPacketOutputProxy {

    @Override
    public FrontendConnection getConnection() {
        return new MockFrontendConnection(new MockSocketChannel(SelectorProvider.provider()));
    }

    @Override
    public void write(byte b) {

    }

    @Override
    public void writeUB2(int i) {

    }

    @Override
    public void writeUB3(int i) {

    }

    @Override
    public void writeInt(int i) {

    }

    @Override
    public void writeFloat(float f) {

    }

    @Override
    public void writeUB4(long l) {

    }

    @Override
    public void writeLong(long l) {

    }

    @Override
    public void writeDouble(double d) {

    }

    @Override
    public void writeLength(long l) {

    }

    @Override
    public void write(byte[] src) {

    }

    @Override
    public void write(byte[] src, int off, int len) {

    }

    @Override
    public void writeWithNull(byte[] src) {

    }

    @Override
    public void writeWithLength(byte[] src) {

    }

    @Override
    public void writeWithLength(byte[] src, byte nullValue) {

    }

    @Override
    public int getLength(long length) {
        return 0;
    }

    @Override
    public int getLength(byte[] src) {
        return 0;
    }

    @Override
    public void checkWriteCapacity(int capacity) {

    }

    @Override
    public void packetBegin() {

    }

    @Override
    public void packetEnd() {

    }

    @Override
    public boolean avaliable() {
        return false;
    }

    @Override
    public void writeArrayAsPacket(byte[] src) {

    }

    @Override
    public void close() {

    }
}
