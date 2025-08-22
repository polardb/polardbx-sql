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

package com.alibaba.polardbx.transaction.trx;

import com.alibaba.polardbx.common.type.TransactionType;
import com.alibaba.polardbx.group.jdbc.TSavepoint;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.IConnection;
import com.alibaba.polardbx.common.jdbc.IDataSource;
import com.alibaba.polardbx.common.jdbc.ITransactionPolicy;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.spi.ITransactionManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.utils.ExplainResult;
import com.alibaba.polardbx.optimizer.utils.IConnectionHolder;
import com.alibaba.polardbx.transaction.connection.AutoCommitConnectionHolder;
import com.alibaba.polardbx.transaction.connection.ConnectionHolderCombiner;
import com.alibaba.polardbx.transaction.SavepointAction;
import com.alibaba.polardbx.transaction.connection.StrictConnectionHolder;

import java.sql.SQLException;
import java.util.Collection;

/**
 * @author mengshi.sunmengshi 2013-12-6 上午11:31:29
 * @since 5.0.0
 */
public class AllowReadTransaction extends BaseTransaction {

    protected final static Logger logger = LoggerFactory.getLogger(AllowReadTransaction.class);

    /**
     * 当前进行事务的节点
     */
    private String writeGroupName = null;
    private IConnection writeConnection = null;
    private final IConnectionHolder ch;

    private final IConnectionHolder readConnectionHolder;
    private final StrictConnectionHolder writeConnectionHolder;

    public AllowReadTransaction(ExecutionContext ec, ITransactionManager manager) {
        super(ec, manager);
        readConnectionHolder = new AutoCommitConnectionHolder(ec);
        writeConnectionHolder = new StrictConnectionHolder();

        this.ch = new ConnectionHolderCombiner(readConnectionHolder, writeConnectionHolder);
    }

    @Override
    public TransactionType getType() {
        return TransactionType.ALLOW_READ;
    }

    /**
     * 策略两种：1. 强一致策略，事务中不允许跨机查询。2.弱一致策略，事务中允许跨机查询；
     *
     * @param ds 当ALLOW_READ的情况下，strongConsistent =
     * true时，会创建事务链接，而如果sConsistent=false则会创建非事务链接
     */
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

            if (rw == RW.READ) {
                if (writeGroupName != null && groupName.equals(writeGroupName)) {
                    return this.writeConnectionHolder.getConnection(schemaName, groupName, ds);
                }

                return readConnectionHolder.getConnection(schemaName, groupName, ds);
            }

            IConnection conn = null;
            if (writeGroupName == null) {
                writeGroupName = groupName;
            }
            // 已经有事务链接了
            if (writeGroupName.equalsIgnoreCase(groupName)
                || ExplainResult.USE_LAST_DATA_NODE.equals(groupName)) {
                conn = this.writeConnectionHolder.getConnection(schemaName, writeGroupName, ds);
            } else {
                if (inventoryMode != null && inventoryMode.isInventoryHint()) {
                    inventoryMode.resetInventoryMode();
                }
                throw new TddlRuntimeException(ErrorCode.ERR_ACCROSS_DB_TRANSACTION, writeGroupName, groupName);
            }

            this.writeConnection = conn;
            return conn;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void commit() {
        lock.lock();
        try {
            checkCanContinue();

            if (logger.isDebugEnabled()) {
                logger.debug("commit");
            }
            Collection<IConnection> conns = this.writeConnectionHolder.getAllConnection();

            if (!(inventoryMode != null && inventoryMode.isCommitOnSuccess())) {
                for (IConnection conn : conns) {
                    try {
                        conn.commit();
                    } catch (SQLException e) {
                        throw GeneralUtil.nestedException(e);
                    }
                }
            }
            close();
        } finally {
            lock.unlock();
        }

    }

    @Override
    public void rollback() {
        lock.lock();

        try {
            if (logger.isDebugEnabled()) {
                logger.debug("rollback");
            }
            Collection<IConnection> conns = this.writeConnectionHolder.getAllConnection();

            if (!(inventoryMode != null && inventoryMode.isCommitOnSuccess())) {
                for (IConnection conn : conns) {
                    try {
                        conn.rollback();
                    } catch (SQLException e) {
                        throw GeneralUtil.nestedException(e);
                    }
                }
            }

            close();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public IConnectionHolder getConnectionHolder() {
        return this.ch;
    }

    @Override
    public void tryClose(IConnection conn, String groupName) throws SQLException {
        lock.lock();

        try {
            if (conn == this.writeConnection) {
                this.writeConnectionHolder.tryClose(conn, groupName);
            } else {
                this.readConnectionHolder.tryClose(conn, groupName);
            }
        } finally {
            lock.unlock();
        }

    }

    @Override
    public void close() {
        if (isClosed()) {
            return;
        }

        lock.lock();
        try {
            super.close();
            this.writeConnectionHolder.closeAllConnections();
            this.readConnectionHolder.closeAllConnections();
        } finally {
            lock.unlock();
        }

    }

    @Override
    public void tryClose() throws SQLException {
        if (isClosed()) {
            return;
        }

        lock.lock();
        try {
            this.readConnectionHolder.closeAllConnections();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void savepoint(String savepoint) {
        if (logger.isDebugEnabled()) {
            logger.debug("savepoint " + savepoint);
        }
        lock.lock();
        try {
            Collection<IConnection> conns = this.writeConnectionHolder.getAllConnection();

            SavepointAction action = new SavepointAction.SetSavepoint(new TSavepoint(savepoint));
            if (conns.isEmpty()) {
                this.writeConnectionHolder.addSavepointAction(action);
                return;
            }

            for (IConnection conn : conns) {
                try {
                    action.doAction(conn);
                } catch (SQLException e) {
                    throw GeneralUtil.nestedException(e);
                }
            }

        } finally {
            lock.unlock();
        }
    }

    @Override
    public void rollbackTo(String savepoint) {
        if (logger.isDebugEnabled()) {
            logger.debug("rollbackTo " + savepoint);
        }
        lock.lock();
        try {
            Collection<IConnection> conns = this.writeConnectionHolder.getAllConnection();
            SavepointAction action = new SavepointAction.RollbackTo(new TSavepoint(savepoint));

            if (conns.isEmpty()) {
                this.writeConnectionHolder.addSavepointAction(action);
                return;
            }

            for (IConnection conn : conns) {
                try {
                    action.doAction(conn);
                } catch (SQLException e) {
                    throw GeneralUtil.nestedException(e);
                }
            }

        } finally {
            lock.unlock();
        }

    }

    @Override
    public void release(String savepoint) {
        if (logger.isDebugEnabled()) {
            logger.debug("release " + savepoint);
        }
        lock.lock();
        try {
            Collection<IConnection> conns = this.writeConnectionHolder.getAllConnection();
            SavepointAction action = new SavepointAction.ReleaseSavepoint(new TSavepoint(savepoint));

            if (conns.isEmpty()) {
                this.writeConnectionHolder.addSavepointAction(action);
                return;
            }

            for (IConnection conn : conns) {

                try {
                    action.doAction(conn);
                } catch (SQLException e) {
                    throw GeneralUtil.nestedException(e);
                }
            }

        } finally {
            lock.unlock();
        }
    }

    @Override
    public ITransactionPolicy.TransactionClass getTransactionClass() {
        return ITransactionPolicy.TransactionClass.ALLOW_READ_CROSS_DB;
    }
}
