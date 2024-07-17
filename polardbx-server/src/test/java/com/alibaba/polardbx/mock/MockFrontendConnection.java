package com.alibaba.polardbx.mock;

import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.net.FrontendConnection;

import java.nio.channels.SocketChannel;

public class MockFrontendConnection extends FrontendConnection {

    public MockFrontendConnection(SocketChannel channel) {
        super(channel);
    }

    @Override
    public void handleError(ErrorCode errorCode, Throwable t) {
    }

    @Override
    public boolean prepareLoadInfile(String sql) {
        return false;
    }

    @Override
    public void binlogDump(byte[] data) {

    }

    @Override
    public void fieldList(byte[] data) {
    }

    @Override
    public boolean checkConnectionCount() {
        return true;
    }

    @Override
    public void addConnectionCount() {
    }

    @Override
    public boolean isPrivilegeMode() {
        return false;
    }

    @Override
    protected long genConnId() {
        return 0L;
    }
}
