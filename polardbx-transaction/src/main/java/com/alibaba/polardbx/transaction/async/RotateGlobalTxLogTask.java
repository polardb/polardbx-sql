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

import com.alibaba.polardbx.common.IdGenerator;
import com.alibaba.polardbx.common.jdbc.IDataSource;
import com.alibaba.polardbx.common.utils.AsyncUtils;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.transaction.TransactionExecutor;
import com.alibaba.polardbx.transaction.TransactionLogger;
import com.alibaba.polardbx.transaction.log.GlobalTxLogManager;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

public class RotateGlobalTxLogTask implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(RotateGlobalTxLogTask.class);

    private final int beforeSeconds;
    private final int nextSeconds;

    private final AsyncTaskQueue asyncQueue;
    private final TransactionExecutor executor;

    private int purgedCount;

    /**
     * RotateGlobalTxLogTask will keep the global tx log in [now() - beforeSeconds, now() + nextSeconds].
     */
    public RotateGlobalTxLogTask(TransactionExecutor executor, int beforeSeconds, int nextSeconds,
                                 AsyncTaskQueue asyncQueue) {
        this.beforeSeconds = beforeSeconds;
        this.asyncQueue = asyncQueue;
        this.executor = executor;
        this.nextSeconds = nextSeconds;
    }

    public int getPurgedCount() {
        return purgedCount;
    }

    @Override
    public void run() {
        if (ConfigDataMode.isFastMock()) {
            return;
        }

        boolean hasLeadership = ExecUtils.hasLeadership(asyncQueue.getSchema());

        if (!hasLeadership) {
            TransactionLogger.getLogger().info("Skip rotate task since I am not the leader");
            return;
        }

        TransactionLogger.getLogger().info("Rotate task starts");
        long startTime = System.nanoTime();

        try {
            purgedCount = doRotate();
        } catch (Exception ex) {
            logger.error("Rotate global tx log failed", ex);
        }

        double duration = (System.nanoTime() - startTime) / 1e9;
        TransactionLogger.getLogger()
            .info("Rotate tx log task completed. " + purgedCount + " trans purged. Cost " + duration + " secs");
    }

    private int doRotate() {
        final long nowTimeMillis = System.currentTimeMillis();
        final long beforeTimeMillis = nowTimeMillis - beforeSeconds * 1000L;
        final long beforeTxid = IdGenerator.assembleId(beforeTimeMillis, 0, 0);
        final long nextTimeMillis = nowTimeMillis + nextSeconds * 1000L;
        final long nextTxid = IdGenerator.assembleId(nextTimeMillis, 0, 0);
        List<String> groups = executor.getGroupList();
        AtomicInteger purgedCount = new AtomicInteger();

        if (!groups.isEmpty()) {
            List<Future> futures = new ArrayList<>(groups.size());
            for (String group : groups) {
                // Rotate each group simultaneously
                futures.add(asyncQueue.submit(() -> {
                    try {
                        IDataSource dataSource = executor.getGroupExecutor(group).getDataSource();
                        int count = GlobalTxLogManager.rotate(dataSource, beforeTxid, nextTxid);
                        purgedCount.addAndGet(count);
                    } catch (Exception e) {
                        logger.error("Rotate transaction log failed on group " + group, e);
                    }
                }));
            }
            AsyncUtils.waitAll(futures);
        }

        return purgedCount.get();
    }
}
