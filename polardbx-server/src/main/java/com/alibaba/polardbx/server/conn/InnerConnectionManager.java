package com.alibaba.polardbx.server.conn;

import com.alibaba.polardbx.common.IInnerConnectionManager;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class InnerConnectionManager implements IInnerConnectionManager {
    private static final InnerConnectionManager INSTANCE = new InnerConnectionManager();

    private static final ConcurrentHashMap<Long, InnerConnection> activeConnections = new ConcurrentHashMap<>();

    private InnerConnectionManager() {
    }

    @Override
    public Connection getConnection() throws SQLException {
        return new InnerConnection();
    }

    @Override
    public Connection getConnection(String schema) throws SQLException {
        return new InnerConnection(schema);
    }

    public static InnerConnectionManager getInstance() {
        return INSTANCE;
    }

    public static ConcurrentHashMap<Long, InnerConnection> getActiveConnections() {
        return activeConnections;
    }
}
