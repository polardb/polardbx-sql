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

import com.alibaba.polardbx.common.DefaultSchema;
import com.alibaba.polardbx.common.async.AsyncTask;
import com.alibaba.polardbx.common.eventlogger.EventLogger;
import com.alibaba.polardbx.common.eventlogger.EventType;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.logger.MDC;
import com.alibaba.polardbx.common.utils.thread.ServerThreadPool;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.optimizer.utils.ITimestampOracle;
import com.alibaba.polardbx.transaction.TransactionExecutor;
import com.alibaba.polardbx.transaction.TransactionLogger;
import com.alibaba.polardbx.transaction.log.GlobalTxLogManager;

import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 执行异步 Recover/Purge 任务的队列
 */
public class AsyncTaskQueue {

    private static final Logger logger = LoggerFactory.getLogger(AsyncTaskQueue.class);

    private static final Timer timer = new Timer("trans-async-task-timer", true);

    private final ServerThreadPool executor;
    private final String schema;

    public AsyncTaskQueue(String schema, ServerThreadPool executor) {
        this.executor = executor;
        this.schema = schema;
    }

    public Future<?> submit(Runnable runnable) {
        return executor.submit(schema, null, AsyncTask.build(runnable));
    }

    public Future<?> submitRandomBucket(Runnable runnable) {
        return executor.submit(null, null, AsyncTask.build(runnable));
    }

    public TimerTask scheduleXARecoverTask(TransactionExecutor te, int interval, boolean supportAsyncCommit) {
        final XARecoverTask recoverTask = new XARecoverTask(te, supportAsyncCommit);
        final ScheduleAsyncTask task = ScheduleAsyncTask.build(recoverTask);

        TimerTask timerTask = new TimerTask() {

            @Override
            public void run() {
                if (!task.schedule()) {
                    logger.warn("Ignore re-submit XA recover task");
                    return;
                }

                try {
                    executor.submit(schema, null, task);
                } catch (Throwable e) {
                    task.cancel();
                    logger.error("Submit XA recover task failed", e);
                }
            }
        };

        timer.scheduleAtFixedRate(timerTask, 0, interval * 1000L);
        TransactionLogger.warn("Scheduled XA transaction recovery task");

        return timerTask;
    }

    public TimerTask scheduleAutoCleanTask(final int interval, long delay,
                                           final Runnable rotateGlobalTxLogTask) {

        final ScheduleAsyncTask task = ScheduleAsyncTask.build(rotateGlobalTxLogTask);

        final TimerTask timerTask = new TimerTask() {

            @Override
            public void run() {
                if (!task.schedule()) {
                    logger.warn("Ignore re-submit auto rotate task");
                    return;
                }

                try {
                    executor.submit(schema, null, task);
                } catch (Throwable e) {
                    task.cancel();
                    logger.error("Submit auto rotate task failed", e);
                }
            }

            @Override
            public boolean cancel() {
                try {
                    // Cancel the async task in case that
                    // it is already submitted but not yet executed.
                    task.cancel();
                } catch (Throwable t) {
                    // Ignore.
                }
                final boolean returnVal = super.cancel();
                // Release space of cancelled timer task.
                timer.purge();
                return returnVal;
            }
        };

        timer.scheduleAtFixedRate(timerTask, delay, interval * 1000L);

        return timerTask;
    }

    public TimerTask scheduleDeadlockDetectionTask(final int interval, final Runnable detectTask) {
        final ScheduleAsyncTask task = ScheduleAsyncTask.build(detectTask);

        final TimerTask timerTask = new TimerTask() {

            @Override
            public void run() {
                if (!task.schedule()) {
                    logger.warn("Ignore re-submit deadlock detection task");
                    return;
                }

                try {
                    executor.submit(schema, null, task);
                } catch (Throwable e) {
                    task.cancel();
                    logger.error("Submit deadlock detection failed", e);
                }
            }

            @Override
            public boolean cancel() {
                try {
                    // Cancel the async task in case that
                    // it is already submitted but not yet executed.
                    task.cancel();
                } catch (Throwable t) {
                    // Ignore.
                    logger.error("Submit deadlock detection task failed", t);
                }
                final boolean returnVal = super.cancel();
                // Release space of cancelled timer task.
                timer.purge();
                return returnVal;
            }
        };

        timer.scheduleAtFixedRate(timerTask, 0, interval);

        TransactionLogger.warn(schema + ": Scheduled deadlock detection task.");
        EventLogger.log(EventType.DEAD_LOCK_DETECTION, String.format(
            "Deadlock detection task for schema:%s is online",
            schema
        ));

        return timerTask;
    }

    public TimerTask scheduleKillTimeoutTransactionTask(TransactionExecutor te, int intervalInMs) {
        final KillTimeoutTransactionTask killTask = new KillTimeoutTransactionTask(schema, te);
        final ScheduleAsyncTask task = ScheduleAsyncTask.build(killTask);

        TimerTask timerTask = new TimerTask() {

            @Override
            public void run() {
                if (!task.schedule()) {
                    logger.debug("Ignore re-submit deadlock detection task");
                    return;
                }

                try {
                    executor.submit(schema, null, task);
                } catch (Throwable e) {
                    task.cancel();
                    logger.error("Submit deadlock detection task failed", e);
                }
            }
        };

        timer.scheduleAtFixedRate(timerTask, 0, intervalInMs);
        TransactionLogger.warn("Scheduled kill timeout transaction task");
        EventLogger.log(EventType.DEAD_LOCK_DETECTION, String.format(
            "Kill timeout transaction task for schema:%s is online",
            schema
        ));

        return timerTask;
    }

    public TimerTask scheduleMdlDeadlockDetectionTask(TransactionExecutor te, int intervalInMs,
                                                      int mdlWaitTimeoutInSec) {
        final MdlDeadlockDetectionTask detectTask = new MdlDeadlockDetectionTask(schema, te, mdlWaitTimeoutInSec);
        final ScheduleAsyncTask task = ScheduleAsyncTask.build(detectTask);

        TimerTask timerTask = new TimerTask() {

            @Override
            public void run() {
                if (!task.schedule()) {
                    logger.debug("Ignore re-submit MDL deadlock detection task");
                    return;
                }

                try {
                    executor.submit(schema, null, task);
                } catch (Throwable e) {
                    task.cancel();
                    logger.error("Submit MDL deadlock detection task failed", e);
                }
            }
        };

        timer.scheduleAtFixedRate(timerTask, 0, intervalInMs);
        TransactionLogger.warn("Scheduled MDL deadlock detection task");
        EventLogger.log(EventType.DEAD_LOCK_DETECTION, String.format(
            "MDL Deadlock detection task for schema:%s is online",
            schema
        ));

        return timerTask;
    }

    public TimerTask scheduleTsoHeartbeatTask(ITimestampOracle tso, int intervalMs) {
        final TsoHeartbeatTask heartbeatTask = new TsoHeartbeatTask(this, tso);
        final ScheduleAsyncTask task = ScheduleAsyncTask.build(heartbeatTask);

        TimerTask timerTask = new TimerTask() {

            @Override
            public void run() {
                if (!task.schedule()) {
                    logger.warn("Ignore re-submit TSO heartbeat task");
                    return;
                }

                try {
                    executor.submit(schema, null, task);
                } catch (Throwable e) {
                    task.cancel();
                    logger.error("Submit TSO heartbeat task failed", e);
                }
            }
        };

        timer.scheduleAtFixedRate(timerTask, 0, intervalMs);
        TransactionLogger.info("Scheduled TSO heartbeat task");

        return timerTask;
    }

    public TimerTask scheduleTsoPurgeTask(int intervalMs) {
        final PurgeTsoTimerTask purgeTsoTimerTask = new PurgeTsoTimerTask(this);
        final ScheduleAsyncTask task = ScheduleAsyncTask.build(purgeTsoTimerTask);

        TimerTask timerTask = new TimerTask() {

            @Override
            public void run() {
                if (!task.schedule()) {
                    logger.warn("Ignore re-submit TSO purge task");
                    return;
                }

                try {
                    executor.submit(schema, null, task);
                } catch (Throwable e) {
                    task.cancel();
                    logger.error("Submit TSO purge task failed", e);
                }
            }
        };

        timer.scheduleAtFixedRate(timerTask, 0, intervalMs);
        TransactionLogger.info("Scheduled TSO purge task");

        return timerTask;
    }

    public TimerTask scheduleTransactionStatisticsTask(final long interval, final Runnable rawTask) {
        final ScheduleAsyncTask task = ScheduleAsyncTask.build(rawTask);

        final TimerTask timerTask = new TimerTask() {

            @Override
            public void run() {
                if (!task.schedule()) {
                    logger.warn("Ignore re-submit transaction statistics task");
                    return;
                }

                try {
                    executor.submit(schema, null, task);
                } catch (Throwable e) {
                    task.cancel();
                    logger.error("Submit transaction statistics task failed", e);
                }
            }

            @Override
            public boolean cancel() {
                try {
                    // Cancel the async task in case that
                    // it is already submitted but not yet executed.
                    task.cancel();
                } catch (Throwable t) {
                    // Ignore.
                    logger.error("Cancel transaction statistics task failed", t);
                }
                final boolean returnVal = super.cancel();
                // Release space of cancelled timer task.
                timer.purge();
                return returnVal;
            }
        };

        timer.scheduleAtFixedRate(timerTask, 0, interval);

        TransactionLogger.info(schema + ": Scheduled transaction statistics task.");

        return timerTask;
    }

    public TimerTask scheduleTransactionIdleTimeoutTask(final int interval, final Runnable rawTask) {
        final ScheduleAsyncTask task = ScheduleAsyncTask.build(rawTask);

        final TimerTask timerTask = new TimerTask() {

            @Override
            public void run() {
                if (!task.schedule()) {
                    logger.warn("Ignore re-submit idle trx timeout task");
                    return;
                }

                try {
                    executor.submit(schema, null, task);
                } catch (Throwable e) {
                    task.cancel();
                    logger.error("Submit idle trx timeout failed", e);
                }
            }

            @Override
            public boolean cancel() {
                try {
                    // Cancel the async task in case that
                    // it is already submitted but not yet executed.
                    task.cancel();
                } catch (Throwable t) {
                    // Ignore.
                    logger.error("Submit idle trx timeout task failed", t);
                }
                final boolean returnVal = super.cancel();
                // Release space of cancelled timer task.
                timer.purge();
                return returnVal;
            }
        };

        timer.scheduleAtFixedRate(timerTask, 0, interval * 1000L);

        TransactionLogger.info(schema + ": Scheduled idle trx timeout task.");

        return timerTask;
    }

    String getSchema() {
        return schema;
    }

    static class ScheduleAsyncTask extends AsyncTask {

        private final AtomicBoolean scheduled = new AtomicBoolean();

        ScheduleAsyncTask(Runnable task, Map mdcContext, String schema) {
            super(task, mdcContext, schema);
        }

        public static ScheduleAsyncTask build(Runnable task) {
            final Map mdcContext = MDC.getCopyOfContextMap();
            final String schema = DefaultSchema.getSchemaName();
            return new ScheduleAsyncTask(task, mdcContext, schema);
        }

        public boolean schedule() {
            return scheduled.compareAndSet(false, true);
        }

        public void cancel() {
            scheduled.lazySet(false);
        }

        @Override
        public void run() {
            if (ConfigDataMode.isFastMock()) {
                return;
            }
            try {
                if (scheduled.get()) {
                    super.run();
                }
            } finally {
                scheduled.lazySet(false);
            }
        }
    }
}
