/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.server.response;

import com.alibaba.polardbx.CobarServer;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.thread.NamedThreadFactory;
import com.alibaba.polardbx.config.SchemaConfig;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.gms.ha.impl.StorageHaManager;
import com.alibaba.polardbx.matrix.config.MatrixConfigHolder;
import com.alibaba.polardbx.matrix.jdbc.TDataSource;
import com.alibaba.polardbx.optimizer.utils.ITransaction;
import com.alibaba.polardbx.transaction.TransactionManager;

import java.util.Collection;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class OnDnChangeLeaderAction {
    private static final Logger logger = LoggerFactory.getLogger(OnDnChangeLeaderAction.class);

    private static final ThreadPoolExecutor executor = new ThreadPoolExecutor(
        1, // corePoolSize
        1, // maximumPoolSize
        10, // keepAliveTime
        TimeUnit.SECONDS,
        new LinkedBlockingQueue<>(1),
        new NamedThreadFactory("SwitchoverDirtyReadRefresher", true),
        new ThreadPoolExecutor.DiscardPolicy());

    protected static Collection<SchemaConfig> schemas() {
        return CobarServer.getInstance().getConfig().getSchemas().values();
    }

    protected static void task() {
        final long startNanos = System.nanoTime();
        for (final SchemaConfig config : schemas()) {
            final TDataSource ds = config.getDataSource();
            if (null == ds) {
                continue;
            }
            final MatrixConfigHolder holder = ds.getConfigHolder();
            if (null == holder) {
                continue;
            }
            final ExecutorContext executorContext = holder.getExecutorContext();
            if (null == executorContext) {
                continue;
            }
            final TransactionManager tm = (TransactionManager) executorContext.getTransactionManager();
            if (null == tm) {
                continue;
            }
            for (final ITransaction transaction : tm.getTransactions().values()) {
                if (transaction != null) {
                    transaction.releaseDirtyReadConnections();
                }
            }
        }
        final long durationNanos = System.nanoTime() - startNanos;
        logger.info("Dirty read connections refresh in " + durationNanos / 1000000.f + "ms");
    }

    public static void onDnLeaderChanging(final boolean fasterHaCheck) {
        if (fasterHaCheck) {
            StorageHaManager.getInstance().adjustStorageHaTaskPeriod(200);
        }
        try {
            executor.submit(OnDnChangeLeaderAction::task);
        } catch (RejectedExecutionException ignore) {
        }
    }
}
