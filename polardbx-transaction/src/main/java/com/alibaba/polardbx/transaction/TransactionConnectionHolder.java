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

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.IConnection;
import com.alibaba.polardbx.common.jdbc.IDataSource;
import com.alibaba.polardbx.common.jdbc.MasterSlave;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.druid.sql.ast.SqlType;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.common.TopologyHandler;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.utils.PhyTableOperationUtil;
import com.alibaba.polardbx.optimizer.utils.IConnectionHolder;
import com.alibaba.polardbx.optimizer.utils.ITransaction;
import com.alibaba.polardbx.optimizer.utils.ITransaction.RW;
import com.alibaba.polardbx.rpc.pool.XConnection;
import com.alibaba.polardbx.transaction.async.AsyncTaskQueue;
import com.alibaba.polardbx.transaction.jdbc.DeferredConnection;
import com.alibaba.polardbx.transaction.utils.TransactionAsyncUtils;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

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

    enum ConnectionState {
        IDLE,
        READING,
        WRITING
    }

    enum ParticipatedState {
        NONE,
        SHARE_READVIEW_READ,
        WRITTEN;

        public boolean participatedTrx() {
            return this == WRITTEN;
        }
    }

    static class WriteHeldConnectionContext {
        private String grpName;
        private HeldConnection defaultWriteConn = null;
        private Map<Long, HeldConnection> grpIdWriteConnMap = new HashMap<>();

        public WriteHeldConnectionContext(String group) {
            this.grpName = group;
        }

        public boolean containWriteConns() {
            return grpIdWriteConnMap.values().size() > 0;
        }

        public List<HeldConnection> allWriteConns() {
            return grpIdWriteConnMap.values().stream().collect(Collectors.toList());
        }

        public boolean containConnId(Long connId) {
            return grpIdWriteConnMap.containsKey(connId);
        }

        public void addNewWriteConnByConnId(HeldConnection newHeldConn, Long connId) {
            grpIdWriteConnMap.put(connId, newHeldConn);
        }

        public HeldConnection getWriteConnByConnId(Long connId) {
            return grpIdWriteConnMap.get(connId);
        }

        public HeldConnection getDefaultWriteConn() {
            return defaultWriteConn;
        }

        public void setDefaultWriteConn(HeldConnection defaultWriteConn) {
            this.defaultWriteConn = defaultWriteConn;
            this.addNewWriteConnByConnId(defaultWriteConn, PhyTableOperationUtil.DEFAULT_WRITE_CONN_ID);
        }
    }

    static class HeldConnection {

        private final IConnection connection;
        private final String schema;
        private final String group;
        private final long connectionId;

        private ConnectionState state;
        private ParticipatedState participated;

        private HeldConnection(IConnection connection,
                               String schema,
                               String group,
                               long connectionId,
                               RW rw,
                               boolean shouldParticipate) {
            this.connection = connection;
            this.schema = schema;
            this.group = group;
            this.connectionId = connectionId;
            setState(rw);
            setParticipated(rw, shouldParticipate);
        }

        void setState(RW rw) {
            this.state = (rw == RW.READ) ? ConnectionState.READING : ConnectionState.WRITING;
        }

        void setParticipated(RW rw, boolean shouldParticipate) {
            if (rw == RW.WRITE) {
                this.participated = ParticipatedState.WRITTEN;
                return;
            }
            if (!shouldParticipate) {
                this.participated = ParticipatedState.NONE;
            } else {
                this.participated = ParticipatedState.SHARE_READVIEW_READ;
            }
        }

        /**
         * read conn -> write conn in use
         */
        void activateWriting() {
            this.participated = ParticipatedState.WRITTEN;
            this.state = ConnectionState.WRITING;
        }

        /**
         * read conn -> read conn with share readview
         */
        void activateShareReadViewReading() {
            this.participated = ParticipatedState.SHARE_READVIEW_READ;
            this.state = ConnectionState.READING;
        }

        void activateReading() {
            this.state = ConnectionState.READING;
        }

        /**
         * when try closing a HeldConnection,
         * just clear its state
         */
        void clearState() {
            this.state = ConnectionState.IDLE;
        }

        boolean isIdle() {
            return this.state == ConnectionState.IDLE;
        }

        boolean isWriting() {
            return this.state == ConnectionState.WRITING;
        }
    }

    private final static Logger logger = LoggerFactory.getLogger(TransactionConnectionHolder.class);

     /**
     * 物理库的写连接集合
     */
    //private final Map<String, HeldConnection> groupHeldWriteConn = new HashMap<>();
     private final Map<String, List<HeldConnection>> groupHeldWriteConn = new HashMap<>();
    /**
     * key: grp
     * val: {
     * key: connId
     * val: HeldConnection( a WriteConn)
     * }
     */
    private final Map<String, WriteHeldConnectionContext> groupWriteHeldConnCtxMap = new HashMap<>();

    /**
     * 物理库的读连接集合
     */
    private final Map<String, List<HeldConnection>> groupHeldReadConns = new HashMap<>();
    private final Map<String, Long> lsnMap = new HashMap<>();
    private final Set<IConnection> connections = new HashSet<>();

    private final ReentrantLock lock;
    private final AbstractTransaction trx;
    private final ExecutionContext executionContext;
    private final int isolationLevel;
    private final boolean reuseReadConn;
    private final TransactionExecutor executor;
    private final boolean consistentReplicaRead;
    private final boolean supportAutoSavepoint;

    public final static String BEFORE_SET_SAVEPOINT = "BEFORE_SET_SAVEPOINT";
    private boolean closed = false;

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
        this.supportAutoSavepoint = ctx.isSupportAutoSavepoint();
    }

    /**
     * Normally when ENABLE_TRX_READ_CONN_REUSE is false, we will hold all connections in the transaction.
     * Otherwise the behavior depends on current isolation level:
     * - for READ-UNCOMMITTED or READ-COMMITTED, only write connections would be hold
     * - for higher isolation levels, both read/write connections would be hold
     * A special case is, under TSO transaction the session variable `innodb_snapshot_seq` is set along with
     * `XA START` statement and will take effect until `XA END`. As a result, we should also hold every read
     * connections under TSO transaction
     */
    boolean shouldHoldConnection(RW rw) {
        return !reuseReadConn
            || rw == RW.WRITE
            || isolationLevel > Connection.TRANSACTION_READ_COMMITTED
            || trx.getType() == TransactionType.TSO;
    }

    /**
     * 满足以下条件之一需要参与事务
     * 1. 非快照读
     * 2. 当前group上共享ReadView的 read-after-write
     * 3. 非TSO或XA事务
     */
    boolean shouldParticipateTransaction(RW rw, boolean hasParticipant) {
        TransactionType type = trx.getType();
        return rw != RW.READ
            || (trx.allowMultipleReadConns() && hasParticipant)
            || (type != TransactionType.TSO && type != TransactionType.XA);
    }

    boolean shouldParticipateTransaction(RW rw, String group, Long connId) {
        return shouldParticipateTransaction(rw, hasParticipant(group, connId));
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

    public IConnection getConnection(String schema, String group, IDataSource ds, RW rw) throws SQLException {
        return getConnection(schema, group, null, ds, rw);
    }

    public IConnection getConnection(String schema, String group, Long grpConnId, IDataSource ds, RW rw)
        throws SQLException {
        Lock lock = this.lock;
        lock.lock();

        try {
            if (ConfigDataMode.isFastMock()) {
                // Force using a new connection
                IConnection conn = ds.getConnection();
                connections.add(conn);
                return wrapWithAutoSavepoint(conn, rw);
            }

//            HeldConnection groupWriteConn = groupHeldWriteConn.get(group);
//            boolean hasParticipant = (groupWriteConn != null);      // 该库上已有写连接

            boolean hasParticipant = hasParticipant(group, grpConnId);
            boolean shouldParticipate = shouldParticipateTransaction(rw, hasParticipant);
            boolean supportGroupMultiWrite = supportGroupMultiWriteConns();
            if (hasParticipant) {

//                // 如果已存在写连接 优先复用写连接
//                if (reuseWriteConn(schema, group, rw, groupWriteConn)) {
//                    return wrapWithAutoSavepoint(groupWriteConn.connection, rw);
//                }

                HeldConnection groupWriteConn = null;
                WriteHeldConnectionContext writeConnCtx =
                    this.groupWriteHeldConnCtxMap.computeIfAbsent(group, (k) -> new WriteHeldConnectionContext(k));
                if (!supportGroupMultiWrite) {
                    //groupWriteConn = writeConns.get(0);
                    groupWriteConn = writeConnCtx.getDefaultWriteConn();
                    if (reuseWriteConn(schema, group, rw, groupWriteConn)) {
                        return wrapWithAutoSavepoint(groupWriteConn.connection, rw);
                    }
                } else {
                    HeldConnection freeWriteConn = findFreeWriteConn(group, grpConnId, writeConnCtx);
                    if (freeWriteConn != null) {
                        // Find free write conn from write conns
                        if (reuseWriteConn(group, schema, rw, freeWriteConn)) {
                            return wrapWithAutoSavepoint(freeWriteConn.connection, rw);
                        }
                    } else {
                        // No find any free conn from write conns, all writeConns are using
                        // , so try to find free conn from read conns
                    }
                }
            }
            // 尝试复用已有的读连接
            List<HeldConnection> groupHeldReadConns =
                this.groupHeldReadConns.computeIfAbsent(group, (k) -> new ArrayList<>());
            HeldConnection freeReadConn = findFreeReadConn(group, groupHeldReadConns);
            if (freeReadConn != null) {
                final IConnection conn =
                    reuseFreeReadConn(schema, group, grpConnId, groupHeldReadConns, freeReadConn, rw,
                        shouldParticipate);
                return wrapWithAutoSavepoint(conn, rw);
            }

            if (!supportGroupMultiWrite) {
                if (canUseExtraReadConn(rw)) {
                    // Using extra connection
                    final IConnection conn = beginTrxInNewConn(schema, group, null, ds, rw, groupHeldReadConns,
                        shouldParticipate);
                    return wrapWithAutoSavepoint(conn, rw);
                }
                if (groupHeldReadConns.size() >= 1) {
                    if (shouldParticipate) {
                        // 已有使用中的读连接不能再开新的写连接
                        throw new TddlRuntimeException(ErrorCode.ERR_CONCURRENT_TRANSACTION, group,
                            "read connection is in use before write");
                    }
                    if (!executionContext.isAutoCommit() || !trx.allowMultipleReadConns()) {
                        throw new TddlRuntimeException(ErrorCode.ERR_CONCURRENT_TRANSACTION, group,
                            "multiple read connections on one group is not allowed");
                    }
                }
            } else {
                final IConnection conn = beginTrxInNewConn(schema, group, grpConnId, ds, rw, groupHeldReadConns,
                    shouldParticipate);
                return wrapWithAutoSavepoint(conn, rw);
            }

            final IConnection conn =
                beginTrxInNewConn(schema, group, grpConnId, ds, rw, groupHeldReadConns, shouldParticipate);
            return wrapWithAutoSavepoint(conn, rw);
        } finally {
            lock.unlock();
        }
    }

    /**
     * @return 复用写连接是否成功
     */
    private boolean reuseWriteConn(String schema, String group, RW rw, HeldConnection groupWriteConn)
        throws SQLException {
        if (groupWriteConn.isIdle()) {
            trx.reinitializeConnection(schema, groupWriteConn.group, groupWriteConn.connection);
            groupWriteConn.setState(rw);
            return true;
        }
        if (!trx.allowMultipleReadConns()) {
            throw new TddlRuntimeException(ErrorCode.ERR_CONCURRENT_TRANSACTION, group,
                "write connection is in use");
        }
        // 共享ReadView下允许写连接与多条读连接同时读
        if (groupWriteConn.isWriting()) {
            logger.error("Write connection is still in use: ");
            throw new TddlRuntimeException(ErrorCode.ERR_CONCURRENT_TRANSACTION, group,
                "write connection is in write state while sharing read view");
        }
        return false;
    }

    private HeldConnection findFreeReadConn(String group, List<HeldConnection> groupReadHeldConns) {
        HeldConnection freeReadConn = null;
        for (HeldConnection heldReadConn : groupReadHeldConns) {
            if (freeReadConn == null && heldReadConn.isIdle()) {
                freeReadConn = heldReadConn;
            }
            if (heldReadConn.participated == ParticipatedState.WRITTEN) {
                throw new TddlRuntimeException(ErrorCode.ERR_CONCURRENT_TRANSACTION, group,
                    "illegal write connection state");
            }
        }
        return freeReadConn;
    }

    private IConnection reuseFreeReadConn(String schema, String group, Long grpConnId,
                                          List<HeldConnection> groupReadHeldConns,
                                          HeldConnection freeReadConn, RW rw, boolean shouldParticipate)
        throws SQLException {
        // readConn to readConn
        if (rw == RW.READ) {
            if (freeReadConn.participated == ParticipatedState.SHARE_READVIEW_READ || !shouldParticipate) {
                // share-readview read to share-readview read
                // or read to read
                trx.reinitializeConnection(schema, freeReadConn.group, freeReadConn.connection);
                freeReadConn.activateReading();
            } else {
                // readConn to share-readview readConn after write
                trx.rollbackNonParticipant(group, freeReadConn.connection);
                trx.begin(schema, freeReadConn.group, freeReadConn.connection);
                freeReadConn.activateShareReadViewReading();
            }
            return freeReadConn.connection;
        }

        // readConn to writeConn
        if (!executionContext.isShareReadView() && groupReadHeldConns.size() > 1) {
            if (!trx.allowMultipleWriteConns()) {
                throw new TddlRuntimeException(ErrorCode.ERR_CONCURRENT_TRANSACTION, group,
                    "already held multiple read connections before write while not sharing read view");
            }
        }

        if (!groupReadHeldConns.remove(freeReadConn)) {
            // should not reach here
            throw new TddlRuntimeException(ErrorCode.ERR_CONCURRENT_TRANSACTION, group,
                "illegal read connection state: found illegal free read connection");
        }
        //groupHeldWriteConn.put(group, freeReadConn);
        addGroupHeldWriteConn(group, grpConnId, freeReadConn);

        if (freeReadConn.participated == ParticipatedState.NONE) {
            trx.rollbackNonParticipant(group, freeReadConn.connection);
            trx.begin(schema, freeReadConn.group, freeReadConn.connection);
        }
        freeReadConn.activateWriting();
        return freeReadConn.connection;
    }

    public boolean isSupportAutoSavepoint() {
        return supportAutoSavepoint;
    }

    private HeldConnection findFreeWriteConn(String group, Long grpConnId, WriteHeldConnectionContext writeConnCtx) {
        if (grpConnId == null || grpConnId.equals(PhyTableOperationUtil.DEFAULT_WRITE_CONN_ID)) {
            return writeConnCtx.getDefaultWriteConn();
        }
        return writeConnCtx.getWriteConnByConnId(grpConnId);
    }

    private void addGroupHeldWriteConn(String grp, Long grpConnId, HeldConnection newHeldConn) {
        WriteHeldConnectionContext ctx =
            groupWriteHeldConnCtxMap.computeIfAbsent(grp, g -> new WriteHeldConnectionContext(g));
        if (grpConnId == null || grpConnId.equals(PhyTableOperationUtil.DEFAULT_WRITE_CONN_ID)) {
            ctx.setDefaultWriteConn(newHeldConn);
        } else {
            ctx.addNewWriteConnByConnId(newHeldConn, grpConnId);
        }
    }

    private boolean hasParticipant(String group, Long connId) {
        //List<HeldConnection> heldConnections = groupHeldWriteConns.get(group);
        WriteHeldConnectionContext ctx = groupWriteHeldConnCtxMap.get(group);
        if (ctx == null || !ctx.containWriteConns()) {
            return false;
        }
        return ctx.containConnId(connId);
    }

    private boolean supportGroupMultiWriteConns() {
        return trx.allowMultipleReadConns() && trx.allowMultipleWriteConns();
    }

    private IConnection wrapWithAutoSavepoint(IConnection conn, RW rw) throws SQLException {
        /* Enable auto savepoint when:
         * 1. user do not turn off auto-savepoint switch;
         * 2. and current statement is a DML;
         * 3. and the connection is a deferred connection.
         * */
        if (this.isSupportAutoSavepoint() && isDML(rw) && conn instanceof DeferredConnection) {
            final DeferredConnection deferredConn = (DeferredConnection) conn;
            final String lastTraceId = deferredConn.getTraceId();
            final String currentTraceId = executionContext.getTraceId();

            if (StringUtils.equals(lastTraceId, BEFORE_SET_SAVEPOINT)) {
                // Some errors happen in this connection before, throw an exception.
                throw new TddlRuntimeException(ErrorCode.ERR_SET_AUTO_SAVEPOINT);
            }

            if (null == lastTraceId) {
                // Send a SAVEPOINT statement.
                deferredConn.setTraceId(BEFORE_SET_SAVEPOINT);
                deferredConn.executeLater("SAVEPOINT " + TStringUtil.backQuote(currentTraceId));
                deferredConn.setTraceId(currentTraceId);
            } else if (!StringUtils.equalsIgnoreCase(lastTraceId, currentTraceId)) {
                // The previous savepoint has not been released, which should not happen.
                throw new TddlRuntimeException(ErrorCode.ERR_SET_AUTO_SAVEPOINT);
            }
        }
        return conn;
    }

    private boolean isDML(RW rw) {
        return rw == RW.WRITE && SqlType.isDML(executionContext.getSqlType());
    }

    private IConnection beginTrxInNewConn(String schema, String group,
                                          Long grpConnId,
                                          IDataSource ds, RW rw,
                                          List<HeldConnection> groupHeldConns,
                                          boolean shouldParticipate) throws SQLException {
        IConnection conn = new DeferredConnection(getConnectionWithLsn(schema, group, ds, rw),
            executionContext.getParamManager().getBoolean(ConnectionParams.USING_RDS_RESULT_SKIP));
        connections.add(conn);
        HeldConnection heldConn =
            new HeldConnection(conn, schema, group, conn.getId(), rw, shouldParticipate);
        if (rw == RW.WRITE && shouldParticipate) {
            //groupHeldWriteConn.put(group, heldConn);
            addGroupHeldWriteConn(group, grpConnId, heldConn);
        } else {
            groupHeldConns.add(heldConn);
        }
        if (executionContext.getTxIsolation() >= 0 && conn.isWrapperFor(XConnection.class)) {
            // Reset isolation level first before start transaction.
            conn.unwrap(XConnection.class).setTransactionIsolation(executionContext.getTxIsolation());
        }

        beginTrx(shouldParticipate, schema, group, conn);
        return conn;
    }

    private void beginTrx(boolean shouldParticipate, String schema, String group,
                          IConnection conn)
        throws SQLException {
        if (shouldParticipate) {
            // Using write connection
            // or read connection with share read view
            // BEGIN / XA START (with ReadView)/ SET snapshot
            trx.begin(schema, group, conn);
        } else {
            trx.beginNonParticipant(group, conn);
        }
    }

    private boolean canUseExtraReadConn(RW rw) {
        boolean allowExtraReadConn = (rw == RW.READ &&
            trx.allowMultipleReadConns());
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
            if (tryCloseHeldWriteConn(conn, group)) {
                return;
            }
            if (tryCloseHeldReadConn(conn, group)) {
                return;
            }
            try {
                conn.close();
            } catch (Throwable e) {
                logger.error("connection close failed, connection is " + conn, e);
            } finally {
                connections.remove(conn);
            }
        } finally {
            lock.unlock();
        }
    }

    private boolean tryCloseHeldReadConn(IConnection conn, String group) {
        List<HeldConnection> heldConnections = groupHeldReadConns.get(group);
        if (heldConnections != null) {
            for (HeldConnection heldConn : heldConnections) {
                if (heldConn.connection == conn) {
                    heldConn.clearState();
                    return true;
                }
            }
        }
        return false;
    }

    private boolean tryCloseHeldWriteConn(IConnection conn, String group) {
        WriteHeldConnectionContext ctx = groupWriteHeldConnCtxMap.get(group);
        if (ctx != null) {
            List<HeldConnection> heldConnections = ctx.allWriteConns();
            if (heldConnections != null) {
                for (HeldConnection heldConn : heldConnections) {
                    if (heldConn.connection == conn) {
                        heldConn.clearState();
                        return true;
                    }
                }
            }
        }
        return false;
    }

//    private boolean tryCloseHeldWriteConn(IConnection conn, String group) {
//        HeldConnection heldWriteConn = groupHeldWriteConn.get(group);
//        if (heldWriteConn != null && heldWriteConn.connection == conn) {
//            heldWriteConn.clearState();
//            return true;
//        }
//        return false;
//    }

    public void handleConnIds(BiConsumer<String, Long> consumer) {
        Lock lock = this.lock;
        lock.lock();
        try {
//            for (Map.Entry<String, HeldConnection> entry : groupHeldWriteConn.entrySet()) {
//                consumer.accept(entry.getKey(), entry.getValue().connectionId);
//            }

            for (Map.Entry<String, WriteHeldConnectionContext> entry : groupWriteHeldConnCtxMap.entrySet()) {
                for (HeldConnection heldConn : entry.getValue().allWriteConns()) {
                    consumer.accept(entry.getKey(), heldConn.connectionId);
                }
            }

            for (Map.Entry<String, List<HeldConnection>> entry : groupHeldReadConns.entrySet()) {
                for (HeldConnection heldConn : entry.getValue()) {
                    consumer.accept(entry.getKey(), heldConn.connectionId);
                }
            }
        } finally {
            lock.unlock();
        }
    }

    public interface Action {
        void execute(String group, IConnection conn, ParticipatedState participated);

        default boolean condition(String group, IConnection conn, ParticipatedState participated) {
            return true;
        }
    }

    /**
     * Execute actions concurrently or sequentially, depending on number of tasks
     */
    void forEachConnection(AsyncTaskQueue asyncQueue, final Action action) {

//        List<Runnable> tasks = new ArrayList<>(groupHeldReadConns.size() + groupHeldWriteConn.size());
//        for (HeldConnection heldConn : groupHeldWriteConn.values()) {
//            if (action.condition(heldConn.group, heldConn.connection, heldConn.participated)) {
//                tasks.add(() -> action.execute(heldConn.group, heldConn.connection, heldConn.participated));
//            }
//        }

        int allWriteConnCount = 0;
        for (final WriteHeldConnectionContext ctx : groupWriteHeldConnCtxMap.values()) {
            allWriteConnCount += ctx.allWriteConns().size();
        }
        List<Runnable> tasks = new ArrayList<>(groupHeldReadConns.size() + allWriteConnCount);
        for (final WriteHeldConnectionContext ctx : groupWriteHeldConnCtxMap.values()) {
            for (HeldConnection heldConn : ctx.allWriteConns()) {
                if (action.condition(heldConn.group, heldConn.connection, heldConn.participated)) {
                    tasks.add(() -> action.execute(heldConn.group, heldConn.connection, heldConn.participated));
                }
            }
        }

        for (final List<HeldConnection> groupHeldConns : groupHeldReadConns.values()) {
            for (HeldConnection heldConn : groupHeldConns) {
                if (action.condition(heldConn.group, heldConn.connection, heldConn.participated)) {
                    tasks.add(() -> action.execute(heldConn.group, heldConn.connection, heldConn.participated));
                }
            }
        }

        TransactionAsyncUtils.runTasksConcurrently(asyncQueue, tasks);
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
            for (List<HeldConnection> readConns : this.groupHeldReadConns.values()) {
                for (HeldConnection readConn : readConns) {
                    if (schema.equalsIgnoreCase(readConn.schema)) {
                        groups.add(readConn.group);
                    }
                }
            }

            for (WriteHeldConnectionContext writeConns : this.groupWriteHeldConnCtxMap.values()) {
                for (HeldConnection readConn : writeConns.allWriteConns()) {
                    if (schema.equalsIgnoreCase(readConn.schema)) {
                        groups.add(readConn.group);
                    }
                }
            }
//            for (HeldConnection writeConn : this.groupHeldWriteConn.values()) {
//                if (schema.equalsIgnoreCase(writeConn.schema)) {
//                    groups.add(writeConn.group);
//                }
//            }
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
            if (closed) {
                return;
            }
            for (IConnection conn : connections) {
                try {
                    if (!conn.isClosed()) {
                        conn.close();
                    }
                } catch (Throwable e) {
                    logger.error("connection close failed, connection is " + conn, e);
                }
            }

            groupWriteHeldConnCtxMap.clear();
            groupHeldReadConns.clear();
            this.closed = true;
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
