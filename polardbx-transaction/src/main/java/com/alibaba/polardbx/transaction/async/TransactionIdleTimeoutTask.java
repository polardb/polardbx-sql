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

package com.alibaba.polardbx.transaction.async;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.spi.ITransactionManager;
import com.alibaba.polardbx.executor.sync.ISyncAction;
import com.alibaba.polardbx.optimizer.utils.ITransaction;
import com.alibaba.polardbx.optimizer.utils.OptimizerHelper;
import com.alibaba.polardbx.transaction.TransactionLogger;

import java.util.Collection;

/**
 * @author yaozhili
 */
public class TransactionIdleTimeoutTask implements Runnable {

    private static Class killSyncActionClass;

    static {
        // 只有server支持，这里是暂时改法，后续要将这段逻辑解耦
        try {
            killSyncActionClass =
                Class.forName("com.alibaba.polardbx.server.response.KillSyncAction");
        } catch (ClassNotFoundException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, e, e.getMessage());
        }
    }

    @Override
    public void run() {
        long currentTime = System.nanoTime();
        for (String schema : OptimizerHelper.getServerConfigManager().getLoadedSchemas()) {
            final ITransactionManager tm = ExecutorContext.getContext(schema).getTransactionManager();
            Collection<ITransaction> transactions = tm.getTransactions().values();
            for (ITransaction tran : transactions) {
                long lastActiveTime = tran.getLastActiveTime();
                if (lastActiveTime <= 0) {
                    continue;
                }

                long timeout = tran.getIdleTimeout();
                if (tran.isRwTransaction() && tran.getIdleRWTimeout() > 0L) {
                    timeout = tran.getIdleRWTimeout();
                } else if (!tran.isRwTransaction() && tran.getIdleROTimeout() > 0L) {
                    timeout = tran.getIdleROTimeout();
                }

                if (timeout == 0L) {
                    // 0 means never timeout.
                    continue;
                }

                // Convert second to nanosecond.
                timeout = (long) (timeout * 1e9);
                if (currentTime - lastActiveTime > timeout) {
                    long connId = tran.getExecutionContext().getConnId();
                    ISyncAction killSyncAction;
                    try {
                        TransactionLogger.warn(
                            "Kill idle timeout trx " + Long.toHexString(tran.getId()) + ", conn id " + connId);
                        killSyncAction =
                            (ISyncAction) killSyncActionClass
                                .getConstructor(String.class, Long.TYPE, Boolean.TYPE, Boolean.TYPE, ErrorCode.class)
                                // KillSyncAction(String user, long id, boolean killQuery, boolean skipValidation, ErrorCode cause)
                                .newInstance("", connId, false, true, ErrorCode.ERR_TRANS_IDLE_TIMEOUT);
                    } catch (Exception e) {
                        throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, e, e.getMessage());
                    }
                    // No need to sync to other CN.
                    killSyncAction.sync();
                }
            }
        }
    }
}
