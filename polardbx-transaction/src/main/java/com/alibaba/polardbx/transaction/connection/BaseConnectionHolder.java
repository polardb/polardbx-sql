package com.alibaba.polardbx.transaction.connection;

import com.alibaba.polardbx.common.jdbc.IConnection;
import com.alibaba.polardbx.common.jdbc.IDataSource;
import com.alibaba.polardbx.common.jdbc.MasterSlave;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.group.jdbc.TGroupDirectConnection;
import com.alibaba.polardbx.optimizer.utils.IConnectionHolder;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;

public abstract class BaseConnectionHolder implements IConnectionHolder {

    private final static Logger logger = LoggerFactory.getLogger(BaseConnectionHolder.class);
    protected Collection<IConnection> connections = Collections.synchronizedCollection(new ArrayList<>());
    private final ReentrantLock lock = new ReentrantLock();
    protected volatile boolean killed = false;
    protected Set<String> heldSchema = Collections.synchronizedSet(new HashSet<>());

    public BaseConnectionHolder() {
    }

    @Override
    public Collection<IConnection> getAllConnection() {
        return this.connections;
    }

    @Override
    public void kill() {

        lock.lock();
        try {
            killed = true;
            Collection<IConnection> conns = this.getAllConnection();
            for (IConnection conn : conns) {
                try {
                    if (conn.isClosed()) {
                        continue;
                    }
                    conn.kill();
                } catch (Exception e) {
                    logger.error("connection kill failed, connection is " + conn, e);
                }
            }
        } finally {
            lock.unlock();
        }

    }

    @Override
    public IConnection getConnection(String schemaName, String groupName, IDataSource ds, MasterSlave masterSlave)
        throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void closeAllConnections() {
        lock.lock();
        try {
            Collection<IConnection> conns = this.getAllConnection();
            for (IConnection conn : conns) {
                try {
                    try {
                        if (killed) {
                            // If Phy Conn is killed, this conn should be discard instead of being reusing
                            conn.discard(new Exception("discard current connection"));
                        }
                    } catch (Throwable ex) {
                        logger.info("ignore to discard conn, connection is " + conn, ex);
                    }
                    conn.close();
                } catch (Throwable e) {
                    logger.error("connection close failed, connection is " + conn, e);
                }
            }
            connections.clear();
            heldSchema.clear();
        } finally {
            lock.unlock();
        }

    }

    @Override
    public void handleConnIds(BiConsumer<String, Long> consumer) {
        Lock lock = this.lock;
        lock.lock();
        try {
            for (IConnection connection : getAllConnection()) {
                IConnection realConneciton = connection.getRealConnection();
                if (realConneciton instanceof TGroupDirectConnection) {
                    String group = ((TGroupDirectConnection) realConneciton).getGroupDataSource().getDbGroupKey();
                    long id = -1L;
                    try {
                        id = realConneciton.getId();
                    } catch (Throwable t) {
                        // When we get id from XConnection, an exception may be thrown
                        // if the connection is closed, and we move to the next transaction.
                        break;
                    }

                    consumer.accept(group, id);
                }
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Set<String> getHeldSchemas() {
        return heldSchema;
    }
}
