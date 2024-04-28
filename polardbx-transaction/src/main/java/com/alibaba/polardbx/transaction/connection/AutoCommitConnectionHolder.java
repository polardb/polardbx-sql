package com.alibaba.polardbx.transaction.connection;

import com.alibaba.polardbx.common.jdbc.IConnection;
import com.alibaba.polardbx.common.jdbc.IDataSource;
import com.alibaba.polardbx.common.jdbc.MasterSlave;
import com.alibaba.polardbx.transaction.async.AsyncTaskQueue;
import com.alibaba.polardbx.transaction.utils.TransactionAsyncUtils;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class AutoCommitConnectionHolder extends BaseConnectionHolder {

    public AutoCommitConnectionHolder() {
    }

    /**
     *
     */
    @Override
    public IConnection getConnection(String schemaName, String groupName, IDataSource ds) throws SQLException {
        IConnection conn = null;
        try {
            conn = ds.getConnection();
        } finally {
            if (conn != null) {
                this.connections.add(conn);
            }
        }
        heldSchema.add(schemaName);
        return conn;
    }

    @Override
    public IConnection getConnection(String schemaName, String groupName, IDataSource ds, MasterSlave masterSlave)
        throws SQLException {
        IConnection conn = null;
        try {
            conn = ds.getConnection(masterSlave);
        } finally {
            if (conn != null) {
                this.connections.add(conn);
            }
        }
        heldSchema.add(schemaName);
        return conn;
    }

    @Override
    public void tryClose(IConnection conn, String groupName) throws SQLException {
        try {
            conn.close();
        } finally {
            this.connections.remove(conn);
        }
    }

    /**
     * Execute actions concurrently or sequentially, depending on number of tasks
     */
    public void forEachConnection(AsyncTaskQueue asyncQueue, Consumer<IConnection> action) {
        List<Runnable> tasks = new ArrayList<>(connections.size());
        connections.forEach((heldConn) -> tasks.add(() -> action.accept(heldConn)));

        TransactionAsyncUtils.runTasksConcurrently(asyncQueue, tasks);
    }
}
