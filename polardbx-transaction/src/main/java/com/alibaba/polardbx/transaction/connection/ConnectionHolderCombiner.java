package com.alibaba.polardbx.transaction.connection;

import com.alibaba.polardbx.common.jdbc.IConnection;
import com.alibaba.polardbx.common.jdbc.IDataSource;
import com.alibaba.polardbx.optimizer.utils.IConnectionHolder;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

public class ConnectionHolderCombiner extends BaseConnectionHolder {

    private final IConnectionHolder[] chs;

    public ConnectionHolderCombiner(IConnectionHolder... chs) {
        this.chs = chs;
    }

    @Override
    public Set<IConnection> getAllConnection() {
        Set<IConnection> conns = new HashSet<>();
        for (IConnectionHolder ch : chs) {
            conns.addAll(ch.getAllConnection());
        }
        return conns;
    }

    @Override
    public IConnection getConnection(String schemaName, String groupName, IDataSource ds) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void tryClose(IConnection conn, String groupName) throws SQLException {
        throw new UnsupportedOperationException();

    }

    @Override
    public void kill() {
        for (IConnectionHolder ch : chs) {
            ch.kill();
        }
    }

}
