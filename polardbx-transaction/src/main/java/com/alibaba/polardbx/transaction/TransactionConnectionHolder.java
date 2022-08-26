/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.transaction;

import com.alibaba.polardbx.common.constants.TransactionAttribute;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.IConnection;
import com.alibaba.polardbx.common.jdbc.IDataSource;
import com.alibaba.polardbx.common.jdbc.MasterSlave;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.AsyncUtils;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.common.TopologyHandler;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.utils.IConnectionHolder;
import com.alibaba.polardbx.optimizer.utils.ITransaction;
import com.alibaba.polardbx.optimizer.utils.ITransaction.RW;
import com.alibaba.polardbx.rpc.pool.XConnection;
import com.alibaba.polardbx.transaction.async.AsyncTaskQueue;
import com.alibaba.polardbx.transaction.jdbc.DeferredConnection;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;

import static com.alibaba.polardbx.common.jdbc.ITransactionPolicy.TransactionClass.TSO_TRANSACTION;

/**
 * DRDS 分布式事务连接管理器
 * <p>
 * 若DN节点支持共享ReadView, 则单库内允许读/写连接同时存在, 否则需遵循如下规则:
 * <PRE>
 * 1. 每个库只允许有一个写连接;
 * 2. 在未创建写连接前, 可以有任意个读连接;
 * 3. RC或更低隔离级别下, 创建写连接后, 除非特别指定, 不允许再分配读连接;
 * 4. 如果某个group的写连接正在使用中, 再次获取会报错;
 * </PRE>
 *
 * @since 5.1.28
 */
public class TransactionConnectionHolder implements IConnectionHolder {

    private static class HeldConnection {

        private final IConnection connection;
        private final String schema;
        private final String group;
        private final long connectionId;

        private boolean inuse;
        private boolean participated;

        private HeldConnection(IConnection connection,
                               String schema,
                               String group,
                               long connectionId,
                               boolean inuse,
                               boolean participated) {
            this.connection = connection;
            this.schema = schema;
            this.group = group;
            this.connectionId = connectionId;
            this.inuse = inuse;
            this.participated = participated;
        }
    }

    private final static Logger logger = LoggerFactory.getLogger(TransactionConnectionHolder.class);

    private final Map<String, List<HeldConnection>> heldConns = new HashMap<>();
    private final Map<String, Long> lsnMap = new HashMap<>();
    private final Set<IConnection> connections = new HashSet<>();
    private final ReentrantLock lock;
    private final AbstractTransaction trx;
    private final ExecutionContext executionContext;
    private final int isolationLevel;
    private final boolean reuseReadConn;
    private final TransactionExecutor executor;
    private final boolean consistentReplicaRead;

    public TransactionConnectionHolder(AbstractTransaction trx, ReentrantLock lock, ExecutionContext ctx,
                                       TransactionExecutor executor) {
        this.lock = lock;
        this.trx = trx;
        this.executionContext = ctx;
        this.isolationLevel = executionContext.getTxIsolation();
        this.reuseReadConn = ctx.getParamManager().getBoolean(ConnectionParams.ENABLE_TRX_READ_CONN_REUSE);
        this.consistentReplicaRead = executionContext.getParamManager()
            .getBoolean(ConnectionParams.ENABLE_CONSISTENT_REPLICA_READ);
        this.executor = executor;
    }

    /**
     * Normally when ENABLE_TRX_READ_CONN_REUSE is false, we will hold all connections in the transaction.
     * Otherwise the behavior depends on current isolation level:
     * - for READ-UNCOMMITTED or READ-COMMITTED, only write connections would be hold
     * - for higher isolation levels, both read/write connections wold be hold
     * A special case is, under TSO transaction the session variable `innodb_snapshot_seq` is set along with
     * `XA START` statement and will take effect until `XA END`. As a result, we should also hold every read
     * connections under TSO transaction
     */
    boolean shouldHoldConnection(RW rw) {
        // TODO: It sucks... Remove this once if spilled-out version of insert-select implemented
        if (rw == RW.READ
            && !executionContext.isShareReadView()
            && executionContext.getParamManager().getBoolean(ConnectionParams.FORCE_READ_OUTSIDE_TX)) {
            return false;
        }
        return !reuseReadConn
            || rw == RW.WRITE
            || isolationLevel > Connection.TRANSACTION_READ_COMMITTED
            || trx.getType() == TransactionType.TSO;
    }

    boolean shouldParticipateTransaction(RW rw) {
        TransactionType type = trx.getType();
        return rw != RW.READ || (type != TransactionType.TSO && type != TransactionType.XA);
    }

    public static boolean needReadLsn(
        ITransaction transaction, String schema, MasterSlave masterSlave, boolean consistentReplicaRead) {

        if (schema != null && schema.equalsIgnoreCase("MetaDB")) {
            //MetaDb访问无需获取Lsn
            return false;
        }

        if (!consistentReplicaRead) {
            return false;
        }

        if (!transaction.getTransactionClass().isA(TSO_TRANSACTION)) {
            //非TSO事务无需获取Lsn
            return false;
        }

        return masterSlave != MasterSlave.MASTER_ONLY;
    }

    private IConnection getConnectionWithLsn(String schema, String group, IDataSource ds, RW rw)
        throws SQLException {
        MasterSlave masterSlave = ExecUtils.getMasterSlave(
            true, rw.equals(ITransaction.RW.WRITE), executionContext);

        boolean needReadLsn = needReadLsn(trx, schema, masterSlave, consistentReplicaRead);

        if (!needReadLsn) {
            return ds.getConnection(masterSlave);
        }

        TopologyHandler topology = null;
        if (schema != null) {
            //支持跨库查询
            topology = ExecutorContext.getContext(schema).getTopologyExecutor().getTopology();
        } else {
            topology = executor.getTopology();
        }

        Long masterLsn = lsnMap.get(group);
        if (masterLsn == null) {
            ExecUtils.getLsn(topology, group, lsnMap);
            masterLsn = lsnMap.get(group);
        }
        IConnection slaveConn = new DeferredConnection(ds.getConnection(masterSlave),
            executionContext.getParamManager().getBoolean(ConnectionParams.USING_RDS_RESULT_SKIP));
        slaveConn.executeLater("SET read_lsn = " + masterLsn.toString());
        return slaveConn;
    }

    public IConnection getConnection(String schema, String group, IDataSource ds, RW rw)
        throws SQLException {
        Lock lock = this.lock;
        lock.lock();

        try {
            if (ConfigDataMode.isFastMock()) {
                // Force using a new connection
                IConnection conn = ds.getConnection();
                connections.add(conn);
                return conn;
            }

            if (isForceNewConnection(rw)) {
                IConnection conn = getConnectionWithLsn(schema, group, ds, rw);
                connections.add(conn);
                return conn;
            }

            List<HeldConnection> groupHeldConns = heldConns.computeIfAbsent(group, (k) -> new ArrayList<>());

            HeldConnection freeConn = null;
            boolean hasParticipant = false;
            for (HeldConnection heldConn : groupHeldConns) {
                if (freeConn == null && !heldConn.inuse) {
                    freeConn = heldConn;
                }
                if (heldConn.participated) {
                    hasParticipant = true;
                }
            }
            boolean shouldParticipate = shouldParticipateTransaction(rw);

            if (freeConn != null) {
                // 复用空闲连接
                if (!freeConn.participated && shouldParticipate) {
                    // allow holding multiple read conns before write
                    freeConn.participated = true;
                    trx.commitNonParticipant(group, freeConn.connection);
                    trx.begin(schema, freeConn.group, freeConn.connection);
                } else {
                    trx.reinitializeConnection(schema, freeConn.group, freeConn.connection);
                }
                freeConn.inuse = true;
                return freeConn.connection;
            }

            if (canUseReadConn(rw)) {
                // Using extra connection
                return beginTrxInNewConn(schema, group, ds, rw, groupHeldConns,
                    hasParticipant, shouldParticipate);
            }

            if (hasParticipant) {
                throw new TddlRuntimeException(ErrorCode.ERR_CONCURRENT_TRANSACTION, group,
                    "already hold a write connection");
            }

            if (groupHeldConns.size() >= 1) {
                if (shouldParticipate) {
                    throw new TddlRuntimeException(ErrorCode.ERR_CONCURRENT_TRANSACTION, group,
                        "already hold read connection");
                }
                if (!executionContext.isAutoCommit() || !trx.allowMultipleReadConns()) {
                    throw new TddlRuntimeException(ErrorCode.ERR_CONCURRENT_TRANSACTION, group,
                        "multiple read connections are not allowed");
                }
            }
            return beginTrxInNewConn(schema, group, ds, rw, groupHeldConns, false, shouldParticipate);
        } finally {
            lock.unlock();
        }
    }

    private IConnection beginTrxInNewConn(String schema, String group, IDataSource ds, RW rw,
                                          List<HeldConnection> groupHeldConns,
                                          boolean hasParticipate, boolean shouldParticipate) throws SQLException {
        IConnection conn = new DeferredConnection(getConnectionWithLsn(schema, group, ds, rw),
            executionContext.getParamManager().getBoolean(ConnectionParams.USING_RDS_RESULT_SKIP));
        connections.add(conn);
        HeldConnection heldConn =
            new HeldConnection(conn, schema, group, conn.getId(), true, shouldParticipate);
        groupHeldConns.add(heldConn);
        if (executionContext.getTxIsolation() >= 0 && conn.isWrapperFor(XConnection.class)) {
            // Reset isolation level first before start transaction.
            conn.unwrap(XConnection.class).setTransactionIsolation(executionContext.getTxIsolation());
        }

        beginTrx(shouldParticipate, hasParticipate, schema, group, conn);
        return conn;
    }

    /**
     * Force using a new connection
     */
    private boolean isForceNewConnection(RW rw) {
        return rw == RW.READ
            && !executionContext.isShareReadView()
            && executionContext.getParamManager().getBoolean(ConnectionParams.FORCE_READ_OUTSIDE_TX);
    }

    private void beginTrx(boolean shouldParticipate, boolean hasParticipant, String schema, String group,
                          IConnection conn)
        throws SQLException {
        if (shouldParticipate || hasParticipant) {
            // Needs participate if read after write
            // Using write connection
            // BEGIN / XA START (with ReadView)/ SET snapshot
            trx.begin(schema, group, conn);
        } else {
            trx.beginNonParticipant(group, conn);
        }
    }

    private boolean canUseReadConn(RW rw) {
        boolean allowExtraReadConn = (rw == RW.READ &&
            executionContext.isShareReadView());
        return allowExtraReadConn || !shouldHoldConnection(rw);
    }

    @Override
    public IConnection getConnection(String schema, String group, IDataSource ds) throws SQLException {
        return getConnection(schema, group, ds, RW.READ);
    }

    @Override
    public IConnection getConnection(String schema, String group, IDataSource ds, MasterSlave masterSlave)
        throws SQLException {
        // Ignore masterSlave setting because all connections in transaction should be MASTER_ONLY
        return getConnection(schema, group, ds);
    }

    @Override
    public void tryClose(IConnection conn, String group) throws SQLException {
        Lock lock = this.lock;
        lock.lock();

        try {
            boolean found = false;
            List<HeldConnection> heldConnections = heldConns.get(group);
            if (heldConnections != null) {
                for (HeldConnection heldConn : heldConnections) {
                    if (heldConn.connection == conn) {
                        found = true;
                        heldConn.inuse = false;
                        break;
                    }
                }
            }
            if (!found) {
                try {
                    conn.close();
                } catch (Throwable e) {
                    logger.error("connection close failed, connection is " + conn, e);
                } finally {
                    connections.remove(conn);
                }
            }
        } finally {
            lock.unlock();
        }
    }

    public void handleConnIds(BiConsumer<String, Long> consumer) {
        Lock lock = this.lock;
        lock.lock();
        try {
            for (Map.Entry<String, List<HeldConnection>> entry : heldConns.entrySet()) {
                for (HeldConnection heldConn : entry.getValue()) {
                    consumer.accept(entry.getKey(), heldConn.connectionId);
                }
            }
        } finally {
            lock.unlock();
        }
    }

    public interface Action {
        void execute(String group, IConnection conn, boolean participated);

        default boolean condition(String group, IConnection conn, boolean participated) {
            return true;
        }
    }

    /**
     * Execute actions concurrently or sequentially, depending on number of tasks
     */
    void forEachConnection(AsyncTaskQueue asyncQueue, final Action action) {
        List<Runnable> tasks = new ArrayList<>(heldConns.size());
        for (final List<HeldConnection> groupHeldConns : heldConns.values()) {
            for (HeldConnection heldConn : groupHeldConns) {
                if (action.condition(heldConn.group, heldConn.connection, heldConn.participated)) {
                    tasks.add(() -> action.execute(heldConn.group, heldConn.connection, heldConn.participated));
                }
            }
        }

        if (tasks.size() >= TransactionAttribute.CONCURRENT_COMMIT_LIMIT) {
            // Execute actions (except the last one) in async queue concurrently
            List<Future> futures = new ArrayList<>(tasks.size() - 1);
            for (int i = 0; i < tasks.size() - 1; i++) {
                futures.add(asyncQueue.submit(tasks.get(i)));
            }

            // Execute the last action by this thread
            RuntimeException exception = null;
            try {
                tasks.get(tasks.size() - 1).run();
            } catch (RuntimeException ex) {
                exception = ex;
            }

            AsyncUtils.waitAll(futures);

            if (exception != null) {
                throw exception;
            }
        } else {
            // Execute action by plain loop
            for (Runnable task : tasks) {
                task.run();
            }
        }
    }

    @Override
    public Set<IConnection> getAllConnection() {
        return Collections.unmodifiableSet(connections); // Not thread-safe
    }

    @Override
    public List<String> getHeldGroupsOfSchema(String schema) {
        this.lock.lock();
        try {
            // filter connections belong to this schema
            List<String> groups = new ArrayList<>();
            for (List<HeldConnection> x : this.heldConns.values()) {
                for (HeldConnection y : x) {
                    if (schema.equalsIgnoreCase(y.schema)) {
                        groups.add(y.group);
                    }
                }
            }
            return groups;
        } finally {
            this.lock.unlock();
        }
    }

    @Override
    public void closeAllConnections() {
        Lock lock = this.lock;
        lock.lock();

        try {
            for (IConnection conn : connections) {
                try {
                    if (!conn.isClosed()) {
                        conn.close();
                    }
                } catch (Throwable e) {
                    logger.error("connection close failed, connection is " + conn, e);
                }
            }
            connections.clear();
            heldConns.clear();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void kill() {
        Lock lock = this.lock;
        lock.lock();

        try {
            for (IConnection conn : connections) {
                try {
                    conn.kill();
                } catch (Throwable e) {
                    logger.error("connection kill failed, connection is " + conn, e);
                }
            }
        } finally {
            lock.unlock();
        }
    }
}
