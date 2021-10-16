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

import com.alibaba.polardbx.CobarServer;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.config.SchemaConfig;
import com.alibaba.polardbx.executor.mpp.deploy.ServiceProvider;
import com.alibaba.polardbx.matrix.jdbc.TConnection;
import com.alibaba.polardbx.matrix.jdbc.TDataSource;
import com.alibaba.polardbx.net.FrontendConnection;
import com.alibaba.polardbx.net.NIOProcessor;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.utils.ITransaction;
import com.alibaba.polardbx.server.ServerConnection;
import com.alibaba.polardbx.transaction.AbstractTransaction;
import com.alibaba.polardbx.transaction.TransactionManager;

import java.text.MessageFormat;
import java.util.Collection;

/**
 * @version 1.0
 */
public class KillTimeoutTrxLocalAction implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(KillTimeoutTrxLocalAction.class);

    private final String db;

    public KillTimeoutTrxLocalAction(String db) {
        this.db = db;
    }

    @Override
    public void run() {
        final SchemaConfig config = CobarServer.getInstance().getConfig().getSchemas().get(db);
        if (null == config) {
            return;
        }

        final TDataSource ds = config.getDataSource();
        if (null == ds.getConfigHolder() ||
            null == ds.getConfigHolder().getExecutorContext()) {
            return;
        }
        final TransactionManager tm =
            (TransactionManager) ds.getConfigHolder().getExecutorContext().getTransactionManager();
        if (null == tm) {
            return;
        }
        final Collection<ITransaction> transactions = tm.getTransactions().values();

        long currentTimeMs = System.currentTimeMillis();
        for (ITransaction transaction : transactions) {
            if (!transaction.isDistributed()) {
                continue;
            }
            final AbstractTransaction tx = (AbstractTransaction) transaction;
            // Ignore trx without start time(which is not actual started).
            if (tx.getStartTime() != 0) {
                final TConnection connection = (TConnection) tx.getExecutionContext().getConnection();
                final long timeoutTime = tx.getMaxTime() * 1000;

                final String trxId = Long.toHexString(tx.getId());
                final String type = tx.getType().toString();
                final long duration = currentTimeMs - tx.getStartTime();
                final String state = tx.getState().toString();

                if (duration > timeoutTime && timeoutTime > 1000) {
                    final String user = connection.getUser().contains("@") ?
                        connection.getUser().substring(0, connection.getUser().indexOf("@")) : connection.getUser();
                    final long processId = connection.getId();

                    logger.warn(MessageFormat.format("Kill TSO/XA/2PC trx which is timeout,"
                            + " user:[{0}] trx_id:[{1}] type:[{2}] duration:[{3,number,#}] state:[{4}] conn_id:[{5,number,#}]",
                        user, trxId, type, duration, state, processId));

                    // Find front connection.
                    final NIOProcessor[] processors = CobarServer.getInstance().getProcessors();
                    for (NIOProcessor p : processors) {
                        final FrontendConnection fc = p.getFrontends().get(processId);
                        if (fc != null) {
                            if (fc instanceof ServerConnection
                                && (TStringUtil.equals(user, fc.getUser()) || ((ServerConnection) fc)
                                .isAdministrator(user))) {
                                TConnection tc = ((ServerConnection) fc).getTddlConnection();
                                if (tc != null) {
                                    ExecutionContext executionContext = tc.getExecutionContext();
                                    if (ServiceProvider.getInstance().getServer() != null &&
                                        executionContext.getTraceId() != null) {
                                        ServiceProvider.getInstance().getServer().getQueryManager()
                                            .cancelQuery(executionContext.getTraceId());
                                    }
                                }
                                fc.close();
                            }
                            break;
                        }
                    }
                }
            }
        }
    }
}
