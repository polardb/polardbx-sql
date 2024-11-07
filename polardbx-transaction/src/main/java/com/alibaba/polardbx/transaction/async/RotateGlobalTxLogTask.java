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
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.AsyncUtils;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.logger.MDC;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.group.jdbc.TGroupDataSource;
import com.alibaba.polardbx.transaction.TransactionExecutor;
import com.alibaba.polardbx.transaction.TransactionLogger;
import com.alibaba.polardbx.transaction.TransactionManager;
import com.alibaba.polardbx.transaction.log.GlobalTxLogManager;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class RotateGlobalTxLogTask implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(RotateGlobalTxLogTask.class);

    private final Calendar startTime;
    private final Calendar endTime;

    private final int beforeSeconds;
    private final int nextSeconds;

    private final AsyncTaskQueue asyncQueue;
    private final TransactionExecutor executor;

    private int purgedCount;

    private static Lock globalLock = new ReentrantLock();

    /**
     * Purge trans log created before {@param beforeSeconds}, and split the last partition into
     * [current_max, {@param nextSeconds}) and [{@param nextSeconds}, unlimited) if necessary.
     */
    public RotateGlobalTxLogTask(TransactionExecutor executor, Calendar startTime, Calendar endTime,
                                 int beforeSeconds, int nextSeconds, AsyncTaskQueue asyncQueue) {
        this.startTime = startTime;
        this.endTime = endTime;
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

        final String schema = asyncQueue.getSchema();

        final Map savedMdcContext = MDC.getCopyOfContextMap();
        try {
            MDC.put(MDC.MDC_KEY_APP, schema.toLowerCase());

            boolean hasLeadership = ExecUtils.hasLeadership(schema);

            if (!hasLeadership) {
                TransactionLogger.warn("Skip rotate task since I am not the leader");
                return;
            }

            if (!isInAllowedInterval()) {
                TransactionLogger.warn("Skip rotate task since not in allowed interval");
                return;
            }

            if (0 != DynamicConfig.getInstance().getTrxLogMethod()
                && DynamicConfig.getInstance().isSkipLegacyLogTableClean()) {
                TransactionLogger.warn("Skip legacy log rotate task");
                return;
            }

            if (TransactionManager.getInstance(schema).isFirstRecover()) {
                // Wait until XA recover task finishes handling the trx log.
                try {
                    Thread.sleep(30 * 1000L);
                } catch (InterruptedException e) {
                    logger.error("Interrupted when waiting XA recover task", e);
                }

                if (null == TransactionManager.getInstance(schema) ||
                    TransactionManager.getInstance(schema).isFirstRecover()) {
                    // Still not finished, or db is dropped, skip rotating trx log.
                    logger.warn("Wait XA recover task timeout, skip this round of trx log rotating.");
                    return;
                }
            }

            TransactionLogger.warn(asyncQueue.getSchema() + ": Rotate task starts");
            long begin = System.nanoTime();

            try {
                purgedCount = doRotate();
            } catch (Exception ex) {
                logger.error(asyncQueue.getSchema() + ": Rotate global tx log failed", ex);
            }

            double duration = (System.nanoTime() - begin) / 1e9;
            TransactionLogger
                .warn(asyncQueue.getSchema() + ": Rotate tx log task completed. "
                    + purgedCount + " trans purged. Cost " + duration + " secs");
        } finally {
            MDC.setContextMap(savedMdcContext);
        }
    }

    private boolean isInAllowedInterval() {
        final Calendar currentTime = Calendar.getInstance();
        final Calendar start = (Calendar) currentTime.clone();
        final Calendar end = (Calendar) currentTime.clone();
        start.set(Calendar.HOUR_OF_DAY, startTime.get(Calendar.HOUR_OF_DAY));
        start.set(Calendar.MINUTE, startTime.get(Calendar.MINUTE));
        start.set(Calendar.SECOND, startTime.get(Calendar.SECOND));
        end.set(Calendar.HOUR_OF_DAY, endTime.get(Calendar.HOUR_OF_DAY));
        end.set(Calendar.MINUTE, endTime.get(Calendar.MINUTE));
        end.set(Calendar.SECOND, endTime.get(Calendar.SECOND));
        return (currentTime.after(start) && currentTime.before(end));
    }

    private int doRotate() {
        final long nowTimeMillis = System.currentTimeMillis();
        final long beforeTimeMillis = nowTimeMillis - beforeSeconds * 1000L;
        final long beforeTxid = IdGenerator.assembleId(beforeTimeMillis, 0, 0);
        final long nextTimeMillis = nowTimeMillis + nextSeconds * 1000L;
        final long nextTxid = IdGenerator.assembleId(nextTimeMillis, 0, 0);
        List<String> groups = executor.getGroupList();
        AtomicInteger purgedCount = new AtomicInteger();

        ConcurrentLinkedQueue<IDataSource> reorderedQueue = new ConcurrentLinkedQueue<>();
        int numberOfDn = ExecUtils.reorderGroupsByDnId(groups, asyncQueue.getSchema(), reorderedQueue);

        // Keep parallelism >= number of DN.
        int parallelism = InstConfUtil.getInt(ConnectionParams.TRX_LOG_CLEAN_PARALLELISM);
        if (-1 == parallelism) {
            // Default parallelism is number of DN.
            parallelism = numberOfDn;
        }
        // Keep parallelism <= number of groups.
        parallelism = Integer.min(reorderedQueue.size(), parallelism);

        if (0 != DynamicConfig.getInstance().getTrxLogMethod()) {
            parallelism = 1;
        }

        StringBuilder sb = new StringBuilder();
        sb.append("Ready to rotate trx log for schema ")
            .append(asyncQueue.getSchema())
            .append(", parallelism is ")
            .append(parallelism)
            .append(", rotate order is [ ");
        reorderedQueue.forEach(ds -> sb.append(((TGroupDataSource) ds).getDbGroupKey()).append(", "));
        sb.append(" ].");
        logger.warn(sb.toString());
        TransactionLogger.warn(sb.toString());

        if (!groups.isEmpty()) {
            if (0 == DynamicConfig.getInstance().getTrxLogMethod()) {
                List<Future> futures = new ArrayList<>(parallelism);
                for (int i = 0; i < parallelism; i++) {
                    futures.add(asyncQueue.submitRandomBucket(() -> {
                        while (true) {
                            IDataSource datasource = reorderedQueue.poll();

                            if (null == datasource) {
                                // All tasks are done.
                                break;
                            }

                            try {
                                int count = GlobalTxLogManager.rotateWithTimeout(datasource, beforeTxid, nextTxid);
                                purgedCount.addAndGet(count);
                            } catch (Exception e) {
                                logger.error("Rotate transaction log failed on group "
                                    + ((TGroupDataSource) datasource).getFullDbGroupKey(), e);
                            }
                        }
                    }));
                }
                AsyncUtils.waitAll(futures);
            } else {
                // Legacy method is disabled, set global parallelism to 1.
                while (true) {
                    IDataSource datasource = reorderedQueue.poll();

                    if (null == datasource) {
                        // All tasks are done.
                        break;
                    }

                    globalLock.lock();
                    try {
                        int count = GlobalTxLogManager.rotateWithTimeout(datasource, beforeTxid, nextTxid);
                        purgedCount.addAndGet(count);
                        Thread.sleep(1000);
                    } catch (Exception e) {
                        logger.error("Rotate transaction log failed on group "
                            + ((TGroupDataSource) datasource).getFullDbGroupKey(), e);
                    } finally {
                        globalLock.unlock();
                    }
                }
            }
        }

        return purgedCount.get();
    }
}
