package com.alibaba.polardbx.transaction.trx;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.IConnection;
import com.alibaba.polardbx.common.jdbc.IDataSource;
import com.alibaba.polardbx.common.jdbc.ITransactionPolicy;
import com.alibaba.polardbx.common.type.TransactionType;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.spi.ITransactionManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.utils.IConnectionHolder;
import com.alibaba.polardbx.transaction.connection.CrossGroupConnectionHolder;

import java.sql.SQLException;
import java.util.Collection;

/**
 * @author mengshi.sunmengshi 2013-12-6 上午11:31:29
 * @since 5.0.0
 */
public class CobarStyleTransaction extends BaseTransaction {

    protected final static Logger logger = LoggerFactory.getLogger(CobarStyleTransaction.class);

    private IConnectionHolder ch = null;

    public CobarStyleTransaction(ExecutionContext ec, ITransactionManager manager) {
        super(ec, manager);

        ch = new CrossGroupConnectionHolder();
    }

    @Override
    public TransactionType getType() {
        return TransactionType.COBAR_STYLE;
    }

    @Override
    public IConnection getConnection(String schemaName, String groupName, IDataSource ds, RW rw, ExecutionContext ec)
        throws SQLException {
        if (groupName == null) {
            throw new IllegalArgumentException("group name is null");
        }

        lock.lock();
        try {

            if (isClosed()) {
                throw new TddlRuntimeException(ErrorCode.ERR_QUERY_CANCLED, groupName);
            }

            if (!begun) {
                begun = true;
            }

            IConnection conn = null;
            conn = this.getConnectionHolder().getConnection(schemaName, groupName, ds);
            conn.setAutoCommit(false);
            return conn;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void commit() {
        if (logger.isDebugEnabled()) {
            logger.debug("commit");
        }

        lock.lock();

        try {
            Collection<IConnection> conns = this.getConnectionHolder().getAllConnection();
            for (IConnection conn : conns) {
                try {
                    conn.commit();
                } catch (SQLException e) {
                    throw GeneralUtil.nestedException(e);
                }
            }
            close();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void rollback() {

        if (logger.isDebugEnabled()) {
            logger.debug("rollback");
        }

        lock.lock();
        try {
            Collection<IConnection> conns = this.getConnectionHolder().getAllConnection();
            for (IConnection conn : conns) {
                try {
                    conn.rollback();
                } catch (SQLException e) {
                    throw GeneralUtil.nestedException(e);
                }
            }
            close();
        } finally {
            lock.unlock();
        }
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
        if (isClosed()) {
            return;
        }
        lock.lock();

        try {
            super.close();
            this.getConnectionHolder().closeAllConnections();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void tryClose() throws SQLException {

    }

    @Override
    public void savepoint(String savepoint) {
        throw new UnsupportedOperationException("savepoint is not supported in cobar style transaction");
    }

    @Override
    public void rollbackTo(String savepoint) {
        throw new UnsupportedOperationException("savepoint is not supported in cobar style transaction");
    }

    @Override
    public void release(String savepoint) {
        throw new UnsupportedOperationException("savepoint is not supported in cobar style transaction");
    }

    @Override
    public ITransactionPolicy.TransactionClass getTransactionClass() {
        return ITransactionPolicy.TransactionClass.COBAR_STYLE;
    }
}
