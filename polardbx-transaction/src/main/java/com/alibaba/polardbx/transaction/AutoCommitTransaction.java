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
import com.alibaba.polardbx.common.jdbc.ITransactionPolicy;
import com.alibaba.polardbx.common.jdbc.MasterSlave;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.spi.ITransactionManager;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.utils.IConnectionHolder;

import java.sql.SQLException;

/**
 * @author mengshi.sunmengshi 2013-12-6 上午11:31:29
 * @since 5.0.0
 */
public class AutoCommitTransaction extends BaseTransaction {

    protected final static Logger logger = LoggerFactory.getLogger(AutoCommitTransaction.class);

    final protected AutoCommitConnectionHolder ch;

    public AutoCommitTransaction(ExecutionContext ec, ITransactionManager manager) {
        super(ec, manager);
        ch = new AutoCommitConnectionHolder();
    }

    /**
     * 策略两种：1. 强一致策略，事务中不允许跨机查询。2.弱一致策略，事务中允许跨机查询；
     */
    @Override
    public IConnection getConnection(String schemaName, String groupName, IDataSource ds, RW rw, ExecutionContext ec)
        throws SQLException {
        MasterSlave masterSlave = ExecUtils.getMasterSlave(false, rw.equals(RW.WRITE), ec);
        return getSelfConnection(schemaName, groupName, ds, masterSlave);
    }

    protected IConnection getSelfConnection(
        String schemaName, String groupName, IDataSource ds, MasterSlave masterSlave)
        throws SQLException {
        if (groupName == null) {
            throw new IllegalArgumentException("group name is null");
        }
        if(ds==null){
            throw new IllegalArgumentException("DataSource is null");
        }

        lock.lock();
        try {
            if (isClosed()) {
                throw new TddlRuntimeException(ErrorCode.ERR_QUERY_CANCLED, groupName);
            }

            if (!begun) {
                beginTransaction();
                begun = true;
            }

            IConnection conn;
            conn = this.getConnectionHolder().getConnection(schemaName, groupName, ds, masterSlave);
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
        // do nothing

        close();
    }

    @Override
    public void rollback() {

        if (logger.isDebugEnabled()) {
            logger.debug("rollback");
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
            if (isClosed()) {
                return;
            }

            super.close();
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
}
