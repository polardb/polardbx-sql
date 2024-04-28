package com.alibaba.polardbx.transaction.trx;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.IConnection;
import com.alibaba.polardbx.common.jdbc.IDataSource;
import com.alibaba.polardbx.common.jdbc.ITransactionPolicy;
import com.alibaba.polardbx.common.jdbc.MasterSlave;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.type.TransactionType;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.common.TopologyHandler;
import com.alibaba.polardbx.executor.spi.ITransactionManager;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.executor.utils.GroupingFetchLSN;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.utils.IConnectionHolder;
import com.alibaba.polardbx.rpc.pool.XConnection;
import com.alibaba.polardbx.stats.TransactionStatistics;
import com.alibaba.polardbx.transaction.connection.AutoCommitConnectionHolder;
import com.alibaba.polardbx.transaction.async.AsyncTaskQueue;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.sql.SQLException;
import java.util.Optional;
import java.util.function.Supplier;

import static com.alibaba.polardbx.transaction.connection.TransactionConnectionHolder.needReadLsn;

/**
 * @author mengshi.sunmengshi 2013-12-6 上午11:31:29
 * @since 5.0.0
 */
public class AutoCommitTransaction extends BaseTransaction {

    protected final static Logger logger = LoggerFactory.getLogger(AutoCommitTransaction.class);

    final protected AutoCommitConnectionHolder ch;

    protected final boolean consistentReplicaRead;

    public AutoCommitTransaction(ExecutionContext ec, ITransactionManager manager) {
        super(ec, manager);
        ch = new AutoCommitConnectionHolder();
        this.consistentReplicaRead = executionContext.getParamManager().getBoolean(
            ConnectionParams.ENABLE_CONSISTENT_REPLICA_READ);
    }

    @Override
    public TransactionType getType() {
        return TransactionType.AUTO_COMMIT;
    }

    /**
     * 策略两种：1. 强一致策略，事务中不允许跨机查询。2.弱一致策略，事务中允许跨机查询；
     */
    @Override
    public IConnection getConnection(String schemaName, String groupName, IDataSource ds, RW rw, ExecutionContext ec)
        throws SQLException {
        MasterSlave masterSlave = ExecUtils.getMasterSlave(false, rw.equals(RW.WRITE), ec);
        IConnection connection = getRealConnection(schemaName, groupName, ds, masterSlave);
        return sendLsn(connection, schemaName, groupName, masterSlave, () -> -1L);
    }

    protected IConnection getRealConnection(
        String schemaName, String groupName, IDataSource ds, MasterSlave masterSlave)
        throws SQLException {
        if (groupName == null) {
            throw new IllegalArgumentException("group name is null");
        }
        if (ds == null) {
            throw new IllegalArgumentException("DataSource is null");
        }

        lock.lock();
        try {
            if (isClosed()) {
                throw new TddlRuntimeException(ErrorCode.ERR_QUERY_CANCLED, groupName);
            }

            if (!begun) {
                begun = true;
            }

            IConnection conn;
            conn = this.getConnectionHolder().getConnection(schemaName, groupName, ds, masterSlave);
            return conn;
        } finally {
            lock.unlock();
        }
    }

    protected IConnection sendLsn(
        IConnection connection, String schemaName, String group, MasterSlave masterSlave, Supplier<Long> tso)
        throws SQLException {
        boolean needReadLsn = needReadLsn(this, schemaName, masterSlave, consistentReplicaRead);
        if (needReadLsn) {

            TopologyHandler topology;
            if (schemaName != null) {
                topology = ExecutorContext.getContext(schemaName).getTopologyExecutor().getTopology();
            } else {
                topology = ((com.alibaba.polardbx.transaction.TransactionManager) manager).getTransactionExecutor()
                    .getTopology();
            }
            long masterLsn = GroupingFetchLSN.getInstance().fetchLSN(topology, group, null, tso.get());
            connection.executeLater(String.format("SET read_lsn = %d", masterLsn));
        }
        return connection;
    }

    @Override
    public void commit() {
        long commitStartTime = System.nanoTime();
        if (logger.isDebugEnabled()) {
            logger.debug("commit");
        }
        // do nothing

        stat.setIfUnknown(TransactionStatistics.Status.COMMIT);
        stat.commitTime = System.nanoTime() - commitStartTime;
        close();
    }

    @Override
    public void rollback() {
        long commitStartTime = System.nanoTime();
        if (logger.isDebugEnabled()) {
            logger.debug("rollback");
        }
        stat.setIfUnknown(TransactionStatistics.Status.ROLLBACK);
        stat.commitTime = System.nanoTime() - commitStartTime;
        Optional.ofNullable(OptimizerContext.getTransStat(statisticSchema))
            .ifPresent(s -> s.countRollback.incrementAndGet());
    }

    @Override
    public void tryClose(IConnection conn, String groupName) throws SQLException {
        lock.lock();

        try {
            this.getConnectionHolder().tryClose(conn, groupName);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public IConnectionHolder getConnectionHolder() {
        return this.ch;
    }

    @Override
    public void close() {
        // When the auto-commit mode has been disabled, The Transaction will invoke tryClose()
        // after the connection lifetime is finish, The tryClose() function will execute rollback
        // and remove the connection from The Connections. So The Connections usually is empty in
        // this function.
        //We should execute rollback  before recycle the connections into pool. The reasons as followed:
        //1. releases any database locks currently held for TSO_READONLY&AUTO_COMMIT_SINGLE_SHARD;
        //2. check the connection is used by other users or not, thus the rollback will failed. we will
        //disard the connection.
        AsyncTaskQueue asyncQueue = getManager().getTransactionExecutor().getAsyncQueue();
        try {
            ch.forEachConnection(asyncQueue, (conn) -> {
                try {
                    rollbackBeforeTryClose(conn, null, null);
                } catch (SQLException ignored) {
                    // ignore
                }
            });
        } catch (Throwable t) {
            logger.error("Rollback for auto commit trx failed.", t);
        }

        if (isClosed()) {
            return;
        }

        lock.lock();
        try {
            if (isClosed()) {
                return;
            }

            try {
                super.close();
            } catch (Throwable t) {
                logger.error("BaseTransaction closed failed.", t);
            }
            this.getConnectionHolder().closeAllConnections();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void tryClose() throws SQLException {
        this.close();
    }

    @Override
    public void savepoint(String savepoint) {

    }

    @Override
    public void rollbackTo(String savepoint) {
        throw new TddlRuntimeException(ErrorCode.ERR_UNKNOWN_SAVEPOINT, savepoint);
    }

    @Override
    public void release(String savepoint) {
        throw new TddlRuntimeException(ErrorCode.ERR_UNKNOWN_SAVEPOINT, savepoint);
    }

    @Override
    public ITransactionPolicy.TransactionClass getTransactionClass() {
        return ITransactionPolicy.TransactionClass.AUTO_COMMIT;
    }

    /**
     * force rollback the current connection, and releases the resources currently held.
     */
    public void rollbackBeforeTryClose(
        IConnection conn, @Nullable String groupName, @Nullable ReleaseCallback tryClose) throws SQLException {
        if (conn.isClosed() || conn.isWrapperFor(XConnection.class)) {
            //ignore for XConnection
        } else {
            if (!DynamicConfig.getInstance().enableExtremePerformance()) {
                try {
                    conn.forceRollback();
                } catch (Throwable e) {
                    logger.error("Cleanup single shard readonly transaction failed!", e);
                }
            }
        }
        if (tryClose != null) {
            tryClose.apply(conn, groupName);
        }
    }

    public interface ReleaseCallback {
        void apply(IConnection t, String groupName) throws SQLException;
    }
}
