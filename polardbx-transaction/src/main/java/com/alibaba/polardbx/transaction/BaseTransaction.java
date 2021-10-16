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

import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.IConnection;
import com.alibaba.polardbx.common.jdbc.IDataSource;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.spi.ITransactionManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.utils.ITransaction;
import com.alibaba.polardbx.optimizer.utils.ITransactionManagerUtil;
import com.alibaba.polardbx.optimizer.utils.InventoryMode;
import com.alibaba.polardbx.rpc.pool.XConnection;

import java.sql.SQLException;
import java.util.concurrent.locks.ReentrantLock;

import static com.alibaba.polardbx.common.jdbc.ITransactionPolicy.TransactionClass.SUPPORT_INVENTORY_TRANSACTION;

/**
 * Base class of non-distributed transaction
 */
public abstract class BaseTransaction implements ITransaction {

    private final static Logger logger = LoggerFactory.getLogger(BaseTransaction.class);

    protected final ReentrantLock lock = new ReentrantLock();
    protected long id;
    protected ExecutionContext executionContext;
    protected final int isolationLevel;
    protected final boolean autoCommit;
    private volatile boolean closed;
    private ErrorCode errorCode;

    protected final ITransactionManager manager;

    // DRDS instance system time
    protected long startTime;

    protected boolean begun;

    protected InventoryMode inventoryMode;

    public BaseTransaction(ExecutionContext executionContext, ITransactionManager manager) {
        super();
        this.executionContext = executionContext;
        this.isolationLevel = executionContext.getTxIsolation();
        this.autoCommit = executionContext.isAutoCommit();
        this.manager = manager;
        this.id = manager.generateTxid(executionContext);
        // Register to manager so that we can see it in SHOW TRANS
        manager.register(this);
    }

    @Override
    public IConnection getConnection(String schemaName, String group, IDataSource ds, RW rw) throws SQLException {
        return getConnection(schemaName, group, ds, rw, this.executionContext);
    }

    @Override
    public void setExecutionContext(ExecutionContext executionContext) {
        this.executionContext = executionContext;
    }

    @Override
    public long getId() {
        return id; // 返回事务 ID
    }

    @Override
    public void close() {
        manager.unregister(id);
        this.closed = true;
    }

    @Override
    public boolean isClosed() {
        return this.closed;
    }

    @Override
    public void kill() throws SQLException {
        lock.lock();
        try {
            this.getConnectionHolder().kill();
            // kill是异步操作, 如果立马执行close会产生并发问题，由外部进行close延迟处理
            // this.getConnectionHolder().closeAllConnections();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void clearTrxContext() {
    }

    /**
     * If crucial error is set, current transaction can't continue or be
     * committed.
     */
    @Override
    public void setCrucialError(ErrorCode errorCode) {
        this.errorCode = errorCode;
    }

    /**
     * Check whether the transaction is destined to abort before commission.
     */
    @Override
    public void checkCanContinue() {
        // For now, only ERR_GLOBAL_SECONDARY_INDEX_CONTINUE_AFTER_WRITE_FAIL
        // could happen here.
        if (errorCode != null) {
            throw new TddlRuntimeException(errorCode);
        }
    }

    @Override
    public ExecutionContext getExecutionContext() {
        return executionContext;
    }

    @Override
    public State getState() {
        // For 1PC transactions there is only one state
        return State.RUNNING;
    }

    @Override
    public boolean isDistributed() {
        return false;
    }

    @Override
    public boolean isStrongConsistent() {
        return false;
    }

    protected static void discard(String group, IConnection conn, Throwable e) {
        if (ConfigDataMode.isFastMock()) {
            return;
        }
        try {
            if (conn.isWrapperFor(XConnection.class)) {
                conn.unwrap(XConnection.class).setLastException(new Exception("discard"));
            } else {
                throw new NotSupportException("xproto required");
            }
        } catch (Throwable ex) {
            logger.error("Failed to discard connection on group " + group, ex);
        }
    }

    /**
     * Transaction Starts
     */
    protected void beginTransaction() {
        // Record the transaction start time
        startTime = System.currentTimeMillis();
    }

    public TransactionManager getManager() {
        return (TransactionManager) manager;
    }

    @Override
    public long getStartTime() {
        return startTime;
    }

    @Override
    public boolean isBegun() {
        return begun;
    }

    @Override
    public InventoryMode getInventoryMode() {
        return inventoryMode;
    }

    @Override
    public void setInventoryMode(InventoryMode inventoryMode) {
        if (inventoryMode != null && !getTransactionClass().isA(SUPPORT_INVENTORY_TRANSACTION)) {
            throw new UnsupportedOperationException("Don't support the Inventory Hint on current Transaction Policy!");
        } else {
            this.inventoryMode = inventoryMode;
        }
    }

    @Override
    public ITransactionManagerUtil getTransactionManagerUtil() {
        return this.manager;
    }
}
