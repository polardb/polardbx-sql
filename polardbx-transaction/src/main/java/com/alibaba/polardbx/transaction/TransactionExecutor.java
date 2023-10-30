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

import com.alibaba.polardbx.common.IdGenerator;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.IDataSource;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.type.TransactionType;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.convertor.ConvertorHelper;
import com.alibaba.polardbx.common.utils.extension.Activate;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.TopologyExecutor;
import com.alibaba.polardbx.executor.common.StorageInfoManager;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.BaseQueryOperation;
import com.alibaba.polardbx.optimizer.core.rel.BaseTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.SingleTableOperation;
import com.alibaba.polardbx.optimizer.utils.ITransaction;
import com.alibaba.polardbx.transaction.async.AsyncTaskQueue;
import com.alibaba.polardbx.transaction.log.GlobalTxLogManager;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlKind;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * DRDS 事务执行器。
 *
 * @since 5.1.28
 */
@Activate(order = 1)
public class TransactionExecutor extends TopologyExecutor {

    protected final static Logger logger = LoggerFactory.getLogger(TransactionExecutor.class);

    private AsyncTaskQueue asyncQueue;

    private boolean xaAvailable;
    private boolean tsoAvailable;

    @Override
    protected void doInit() {
        super.doInit();

        this.handler.setTopologyChanger(topologyHandler -> initSystemTables());

        asyncQueue = new AsyncTaskQueue(handler.getSchemaName(), executorService);

    }

    public AsyncTaskQueue getAsyncQueue() {
        return asyncQueue;
    }

    public void initSystemTables() {
        if (ConfigDataMode.isFastMock()) {
            xaAvailable = true;
            return;
        }

        long nextMillis = new ParamManager(handler.getCp()).getInt(ConnectionParams.PURGE_TRANS_INTERVAL) * 1000L * 2;
        long initTxid = IdGenerator.assembleId(System.currentTimeMillis() + nextMillis, 0, 0);
        boolean xaAvailable = true;
        List<String> transGroupList = getGroupList();
        for (String group : transGroupList) {
            IDataSource dataSource = getGroupExecutor(group).getDataSource();

            if (ConfigDataMode.isMasterMode()) {
                // Only master inst need doing init system tables

                // initialize system tables if not exists
                GlobalTxLogManager.createTables(dataSource, initTxid);
            }

            // check whether XA available (aka. MySQL version >= 5.7)
            String version = StorageInfoManager.getMySqlVersion(dataSource);
            if (version.startsWith("5.6") || version.startsWith("5.5")) {
                xaAvailable = false;
            }
        }
        this.xaAvailable = xaAvailable;
    }

    public void checkTsoTransaction() {
        boolean tsoAvailable = true;
        for (String group : getGroupList()) {
            IDataSource dataSource = getGroupExecutor(group).getDataSource();

            if (!StorageInfoManager.checkSupportTso(dataSource)) {
                tsoAvailable = false;
            }
        }
        this.tsoAvailable = tsoAvailable;
    }

    public List<String> getGroupList() {
        return handler.getAllTransGroupList();
    }

    @Override
    public Cursor execByExecPlanNode(RelNode relNode, ExecutionContext executionContext) {
        ITransaction trx = executionContext.getTransaction();

        if (!(trx instanceof AbstractTransaction)) {
            return super.execByExecPlanNode(relNode, executionContext);
        }

        if (relNode instanceof BaseQueryOperation) {
            BaseQueryOperation plan = (BaseQueryOperation) relNode;
            if (plan.getKind() == SqlKind.SELECT) {
                return executeQuery(plan, executionContext);
            } else if (plan.getKind() == SqlKind.INSERT || plan.getKind() == SqlKind.REPLACE
                || plan.getKind() == SqlKind.UPDATE || plan.getKind() == SqlKind.DELETE) {
                return executePut(plan, executionContext);
            }
        }

        return super.execByExecPlanNode(relNode, executionContext);
    }

    protected Cursor executeQuery(BaseQueryOperation plan, ExecutionContext executionContext) {
        return super.execByExecPlanNode(plan, executionContext);
    }

    private Cursor executePut(BaseQueryOperation plan, ExecutionContext executionContext) {
        AbstractTransaction transaction = (AbstractTransaction) executionContext.getTransaction();

        try {
            if (transaction.getType() == TransactionType.XA) {
                return executePutXA(plan, executionContext);
            } else if (transaction.getType() == TransactionType.TSO
                || transaction.getType() == TransactionType.TSO_2PC_OPT) {
                return executePutTSO(plan, executionContext);
            } else {
                throw new RuntimeException("impossible");
            }
        } catch (TddlRuntimeException ex) {
            if (ex.getErrorCodeType() == ErrorCode.ERR_EXECUTE_ON_MYSQL
                && ex.getCause() instanceof SQLException
                && ex.getErrorCode() == ErrorCode.ER_LOCK_DEADLOCK.getCode()) {
                // Prevent this transaction from committing
                transaction.setCrucialError(ErrorCode.ERR_TRANS_DEADLOCK, ex.getMessage());
            }
            throw ex;
        }
    }

    private Cursor executePutXA(BaseQueryOperation plan, ExecutionContext executionContext) {
        if (!xaAvailable) {
            throw new TddlRuntimeException(ErrorCode.ERR_TRANS_UNSUPPORTED, "XA Transaction need MySQL 5.7 or above");
        }

        return super.execByExecPlanNode(plan, executionContext);
    }

    private Cursor executePutTSO(BaseQueryOperation plan, ExecutionContext executionContext) {
        if (!tsoAvailable) {
            throw new TddlRuntimeException(ErrorCode.ERR_TRANS_UNSUPPORTED, "TSO Transaction not supported on storage");
        }

        return super.execByExecPlanNode(plan, executionContext);
    }

    public boolean isXaAvailable() {
        return xaAvailable;
    }
}
