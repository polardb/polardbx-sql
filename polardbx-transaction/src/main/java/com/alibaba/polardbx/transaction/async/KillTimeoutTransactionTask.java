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
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.transaction.TransactionExecutor;

/**
 * @version 1.0
 */
public class KillTimeoutTransactionTask implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(DeadlockDetectionTask.class);

    private final String db;

    private final TransactionExecutor executor;

    private static Class killTimeoutLocalActionClass;

    static {
        try {
            killTimeoutLocalActionClass =
                Class.forName("com.alibaba.polardbx.server.response.KillTimeoutTrxLocalAction");
        } catch (ClassNotFoundException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, e, e.getMessage());
        }
    }

    public KillTimeoutTransactionTask(String db, TransactionExecutor executor) {
        this.db = db;
        this.executor = executor;
    }

    @Override
    public void run() {
        logger.debug("Start kill timeout trx.");
        try {
            final Runnable killTimeoutLocalAction;
            try {
                killTimeoutLocalAction =
                    (Runnable) killTimeoutLocalActionClass.getConstructor(String.class).newInstance(db);
            } catch (Exception e) {
                throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, e, e.getMessage());
            }
            killTimeoutLocalAction.run();
        } catch (Throwable ex) {
            logger.error("Failed to do kill timeout", ex);
        }
    }
}
