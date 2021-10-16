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

package com.alibaba.polardbx.optimizer.utils;

import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.IConnection;
import com.alibaba.polardbx.common.jdbc.IDataSource;
import com.alibaba.polardbx.common.jdbc.ITransactionPolicy;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;

import java.sql.SQLException;

/**
 * 事务对象
 *
 * @author mengshi.sunmengshi 2013-11-27 下午4:00:49
 * @since 5.0.0
 */
public interface ITransaction {

    enum RW {
        READ, WRITE
    }

    enum State {
        RUNNING, PREPARED
    }

    long getId();

    void commit();

    void rollback();

    ExecutionContext getExecutionContext();

    void setExecutionContext(ExecutionContext executionContext);

    IConnectionHolder getConnectionHolder();

    void tryClose(IConnection conn, String groupName) throws SQLException;

    void tryClose() throws SQLException;

    IConnection getConnection(String schemaName, String group, IDataSource ds, RW rw) throws SQLException;

    IConnection getConnection(String schemaName, String group, IDataSource ds, RW rw, ExecutionContext ec)
        throws SQLException;

    boolean isClosed();

    void close();

    void kill() throws SQLException;

    void savepoint(String savepoint);

    void rollbackTo(String savepoint);

    void release(String savepoint);

    void clearTrxContext();

    void setCrucialError(ErrorCode errorCode);

    void checkCanContinue();

    /**
     * Whether it is a distributed transaction
     *
     * @return true for XA/TSO/2PC transaction
     */
    boolean isDistributed();

    /**
     * Whether it is a strong-consistent distributed transaction
     *
     * @return true for XA/TSO transaction
     */
    boolean isStrongConsistent();

    State getState();

    ITransactionPolicy.TransactionClass getTransactionClass();

    long getStartTime();

    boolean isBegun();

    default InventoryMode getInventoryMode() {
        return null;
    }

    void setInventoryMode(InventoryMode inventoryMode);

    ITransactionManagerUtil getTransactionManagerUtil();
}
