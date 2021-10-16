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

package com.alibaba.polardbx.server.response;

import com.alibaba.polardbx.ErrorCode;
import com.alibaba.polardbx.config.SchemaConfig;
import com.alibaba.polardbx.server.ServerConnection;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.spi.ITransactionManager;
import com.alibaba.polardbx.matrix.jdbc.TDataSource;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.transaction.TransactionManager;
import com.alibaba.polardbx.transaction.log.GlobalTxLogManager;

public abstract class AbstractTransHandler {

    protected final ServerConnection c;

    protected String schema;
    protected ExecutorContext executorContext;
    protected TransactionManager transactionManager;
    protected GlobalTxLogManager globalTxLogManager;

    public AbstractTransHandler(ServerConnection c) {
        this.c = c;
    }

    public void execute() {
        schema = c.getSchema();

        if (schema == null) {
            c.writeErrMessage(ErrorCode.ER_NO_DB_ERROR, "No database selected");
            return;
        }

        SchemaConfig schemaConfig = c.getSchemaConfig();
        if (schemaConfig == null) {
            c.writeErrMessage(ErrorCode.ER_BAD_DB_ERROR, "Unknown database '" + schema + "'");
            return;
        }

        TDataSource ds = schemaConfig.getDataSource();
        if (!ds.isInited()) {
            try {
                ds.init();
            } catch (Throwable e) {
                c.handleError(ErrorCode.ERR_HANDLE_DATA, e);
                return;
            }
        }

        executorContext = ds.getConfigHolder().getExecutorContext();
        ITransactionManager tm = executorContext.getTransactionManager();
        if (!(tm instanceof TransactionManager)) {
            c.writeErrMessage(ErrorCode.ERR_TRANS, "Unsupported for '" + schema + "'");
            return;
        }

        transactionManager = ((TransactionManager) tm);
        globalTxLogManager = transactionManager.getGlobalTxLogManager();

        OptimizerContext.setContext(ds.getConfigHolder().getOptimizerContext());
        doExecute();
    }

    protected abstract void doExecute();
}
