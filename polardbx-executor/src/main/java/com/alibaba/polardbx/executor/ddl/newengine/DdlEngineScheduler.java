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

package com.alibaba.polardbx.executor.ddl.newengine;

import com.alibaba.polardbx.executor.ddl.newengine.meta.DdlJobManager;
import com.google.common.collect.Sets;
import com.alibaba.polardbx.common.async.AsyncCallableTask;
import com.alibaba.polardbx.common.async.AsyncTask;
import com.alibaba.polardbx.common.ddl.newengine.DdlState;
import com.alibaba.polardbx.common.eventlogger.EventLogger;
import com.alibaba.polardbx.common.eventlogger.EventType;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.thread.ServerThreadPool;
import com.alibaba.polardbx.executor.ddl.newengine.meta.DdlJobManager;
import com.alibaba.polardbx.executor.ddl.newengine.sync.DdlRequest;
import com.alibaba.polardbx.executor.ddl.newengine.utils.DdlHelper;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.config.impl.MetaDbInstConfigManager;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineRecord;
import com.google.common.collect.Sets;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static com.alibaba.polardbx.common.ddl.newengine.DdlConstants.DDL_DISPATCHER_NAME;
import static com.alibaba.polardbx.common.ddl.newengine.DdlConstants.DDL_SCHEDULER_NAME;
import static com.alibaba.polardbx.common.ddl.newengine.DdlConstants.DEFAULT_LOGICAL_DDL_PARALLELISM;
import static com.alibaba.polardbx.common.ddl.newengine.DdlConstants.DEFAULT_PAUSED_DDL_RESCHEDULE_INTERVAL_IN_MINUTES;
import static com.alibaba.polardbx.common.ddl.newengine.DdlConstants.MEDIAN_WAITING_TIME;
import static com.alibaba.polardbx.common.ddl.newengine.DdlConstants.MORE_WAITING_TIME;
import static com.alibaba.polardbx.common.properties.ConnectionProperties.LOGICAL_DDL_PARALLELISM;

public class DdlEngineScheduler {

    private static final Logger LOGGER = LoggerFactory.getLogger(DdlEngineScheduler.class);

    private static final DdlEngineScheduler INSTANCE = new DdlEngineScheduler();

    private final Set<String> activeSchemas = ConcurrentHashMap.newKeySet();

    private final DdlJobManager schedulerManager = new DdlJobManager();

    /**
     * Instance-level DDL request dispatcher.
     */
    private final ExecutorService ddlDispatcher = DdlHelper.createSingleThreadPool(DDL_DISPATCHER_NAME);
    private final BlockingQueue<DdlRequest> ddlRequestTransitQueue = new LinkedBlockingQueue<>();

    /**
     * Database-level DDL request schedulers.
     */
    private final ExecutorService ddlSchedulers = DdlHelper.createThreadPool(DDL_SCHEDULER_NAME);
    private final Map<String, Future> ddlFutures = new ConcurrentHashMap<>();
    private final Map<String, DdlJobScheduler> ddlJobSchedulerMap = new ConcurrentHashMap<>();
    private final Map<String, BlockingQueue<DdlEngineRecord>> ddlJobDeliveryQueues = new ConcurrentHashMap<>();

    /**
     * DDL Job Executors
     */
    private final Map<String, ServerThreadPool> ddlExecutors = new ConcurrentHashMap<>();

    /**
     * Job executing version number, increments version number by 1 for each Job executed
     */
    private final AtomicLong performVersion = new AtomicLong(0L);
    private final AtomicBoolean suspending = new AtomicBoolean(false);

    private DdlEngineScheduler() {
        if (DdlHelper.isRunnable()) {
            ddlDispatcher.submit(AsyncTask.build(new DdlJobDispatcher()));
        }
    }

    public static DdlEngineScheduler getInstance() {
        return INSTANCE;
    }

    public void register(String schemaName, ServerThreadPool executor) {
        if (DdlHelper.isRunnable(schemaName)) {
            String lowerCaseSchemaName = schemaName.toLowerCase();

            ddlJobDeliveryQueues.put(lowerCaseSchemaName, new LinkedBlockingQueue<>());

            ddlExecutors.put(lowerCaseSchemaName, executor);

            activeSchemas.add(lowerCaseSchemaName);

            DdlEngineDagExecutorMap.register(lowerCaseSchemaName);

            DdlJobScheduler ddlJobScheduler = new DdlJobScheduler(schemaName);
            ddlJobSchedulerMap.put(lowerCaseSchemaName, ddlJobScheduler);
            Future future = ddlSchedulers.submit(AsyncTask.build(ddlJobScheduler));
            ddlFutures.put(lowerCaseSchemaName, future);
        }
    }

    public void deregister(String schemaName) {
        if (DdlHelper.isRunnable(schemaName)) {
            String lowerCaseSchemaName = schemaName.toLowerCase();

            DdlEngineDagExecutorMap.deregister(lowerCaseSchemaName);

            activeSchemas.remove(lowerCaseSchemaName);

            ddlExecutors.remove(lowerCaseSchemaName);

            ddlJobDeliveryQueues.remove(lowerCaseSchemaName);

            Future future = ddlFutures.remove(lowerCaseSchemaName);
            boolean cancelled = future.cancel(true);
            if (!cancelled) {
                // Wait a few seconds.
                DdlHelper.waitToContinue(MORE_WAITING_TIME);
            }
            ddlJobSchedulerMap.remove(schemaName.toLowerCase());
        }
    }

    public void notify(DdlRequest ddlRequest) {
        StringBuilder message = new StringBuilder();
        message.append(". DDL Job Request: schema - ").append(ddlRequest.getSchemaName());
        message.append(", jobIds - ");
        for (Long jobId : ddlRequest.getJobIds()) {
            message.append(jobId).append(",");
        }
        offerQueue(ddlRequestTransitQueue, ddlRequest, message.toString());
    }

    public void suspend() {
        suspending.set(true);
    }

    public void resume() {
        suspending.set(false);
    }

    public long getPerformVersion() {
        return performVersion.get();
    }

    public synchronized boolean compareAndExecute(long expectVersion, Supplier<?> supplier) throws TimeoutException {
        try {
            if (expectVersion != performVersion.get()) {
                return false;
            }

            suspending.set(true);
            long startTime = System.currentTimeMillis();
            while (true) {
                boolean allIdle = ddlJobSchedulerMap.values().stream().map(DdlJobScheduler::isIdle)
                    .reduce(true, (a, b) -> a && b);

                if (allIdle) {
                    if (expectVersion == performVersion.get()) {
                        supplier.get();
                        return true;
                    } else {
                        return false;
                    }
                } else {
                    DdlHelper.waitToContinue(MORE_WAITING_TIME);
                }

                if (System.currentTimeMillis() - startTime >= MORE_WAITING_TIME * 3) {
                    throw new TimeoutException("wait job transfering to idle time out.");
                }
            }
        } finally {
            suspending.set(false);
        }
    }

    private class DdlJobDispatcher implements Runnable {

        @Override
        public void run() {
            if (!DdlHelper.isRunnable()) {
                return;
            }
            while (true) {
                // Process DDL jobs only on the leader node.
                if (!ExecUtils.hasLeadership(null)) {
                    DdlHelper.waitToContinue(MORE_WAITING_TIME);
                    continue;
                }
                try {
                    // Take DDL requests from the instance-level queue, then
                    // dispatch to schema-level queues for scheduling.
                    DdlRequest ddlRequest = ddlRequestTransitQueue.poll(MORE_WAITING_TIME, TimeUnit.MILLISECONDS);
                    if (ddlRequest != null) {
                        processRequest(ddlRequest);
                    } else {
                        // No DDL request received before timeout, so
                        // try to fetch from the DDL job queue directly.
                        processQueue();
                        processPaused();
                    }
                } catch (InterruptedException e) {
                    try {
                        LOGGER.error("DDL Request Transit Queue was interrupted. Caused by: " + e.getMessage(), e);
                    } catch (Throwable ignored) {
                    }
                } catch (Throwable t) {
                    try {
                        // Log it without throwing.
                        LOGGER.error(" DDL Request transit queue hit unexpected failure. Caused by: "
                            + (t instanceof InterruptedException ? "interrupted" : t.getMessage()), t);
                    } catch (Throwable ignored) {
                    }
                }
            }
        }

        private void processRequest(DdlRequest ddlRequest) {
            List<DdlEngineRecord> records = schedulerManager.fetchRecords(ddlRequest.getJobIds());
            dispatch(records);
        }

        private void processQueue() {
            // No job request came before timeout. Let's try to poll on the
            // job queue directly to see if any job was left to handle.
            List<DdlEngineRecord> records =
                schedulerManager.fetchRecords(Sets.union(DdlState.RUNNABLE, DdlState.FINISHED));
            dispatch(records);
        }

        private void processPaused() {
            List<DdlEngineRecord> records =
                schedulerManager.fetchRecords(DdlState.TERMINATED, DEFAULT_PAUSED_DDL_RESCHEDULE_INTERVAL_IN_MINUTES);

            for (DdlEngineRecord record : records) {
                try {
                    DdlState currentState = DdlState.valueOf(record.state);
                    if (currentState == DdlState.PAUSED) {
                        DdlState pausedPolicy = DdlState.valueOf(record.pausedPolicy);
                        if (pausedPolicy == currentState) {
                            continue;
                        }
                        schedulerManager.compareAndSetDdlState(record.jobId, currentState, pausedPolicy);
                    }
                    if (currentState == DdlState.ROLLBACK_PAUSED) {
                        DdlState rollbackPausedPolicy = DdlState.valueOf(record.rollbackPausedPolicy);
                        if (rollbackPausedPolicy == currentState) {
                            continue;
                        }
                        schedulerManager.compareAndSetDdlState(record.jobId, currentState, rollbackPausedPolicy);
                    }
                } catch (Throwable t) {
                    EventLogger.log(EventType.DDL_WARN, "reschedule paused ddl error: " + t.getMessage());
                    LOGGER.error("reschedule paused ddl error: " + t.getMessage(), t);
                }
            }
        }

        private void dispatch(List<DdlEngineRecord> records) {
            for (DdlEngineRecord record : records) {
                try {
                    BlockingQueue deliveryQueue = ddlJobDeliveryQueues.get(record.schemaName.toLowerCase());
                    if (deliveryQueue == null) {
                        deliveryQueue = new LinkedBlockingQueue();
                        ddlJobDeliveryQueues.put(record.schemaName.toLowerCase(), deliveryQueue);
                    }
                    uniqueOffer(record.schemaName.toLowerCase(), deliveryQueue, record);
                } catch (Throwable t) {
                    LOGGER.error("Failed to dispatch to queue. Caused by: " + t.getMessage(), t);
                }
            }
        }

    }

    private class DdlJobScheduler implements Runnable {

        private final String schemaName;

        private final BlockingQueue<DdlEngineRecord> ddlJobDeliveryQueue;
        private int maxParallelism;
        private final Semaphore semaphore;
        private final ExecutorCompletionService completionService;
        private volatile boolean scheduleSuspended;

        DdlJobScheduler(String schemaName) {
            this.schemaName = schemaName;
            this.ddlJobDeliveryQueue = ddlJobDeliveryQueues.get(schemaName.toLowerCase());
            String parallelismStr = MetaDbInstConfigManager.getInstance().getInstProperty(LOGICAL_DDL_PARALLELISM);
            if (StringUtils.isNotEmpty(parallelismStr)) {
                try {
                    this.maxParallelism = Integer.valueOf(parallelismStr);
                } catch (Throwable t) {
                    EventLogger.log(EventType.DDL_WARN,
                        "parse LOGICAL_DDL_PARALLELISM failed. strVal:" + parallelismStr);
                    this.maxParallelism = DEFAULT_LOGICAL_DDL_PARALLELISM;
                }
            } else {
                this.maxParallelism = DEFAULT_LOGICAL_DDL_PARALLELISM;
            }

            this.semaphore = new Semaphore(maxParallelism);
            this.completionService = new ExecutorCompletionService(ddlExecutors.get(schemaName.toLowerCase()));
        }

        @Override
        public void run() {
            if (!DdlHelper.isRunnable()) {
                return;
            }

            // We need to know which schema the DDL job scheduler is being associated with.
            DdlHelper.renameCurrentThread(DDL_SCHEDULER_NAME, schemaName);

            while (true) {
                if (!isSchemaAvailable(schemaName)) {
                    return;
                }

                if (!ExecUtils.hasLeadership(null)) {
                    // Let's wait for a while when no leadership or
                    // queue failure occurred, then continue.
                    DdlHelper.waitToContinue(MORE_WAITING_TIME);
                    continue;
                }

                if (suspending.get()) {
                    scheduleSuspended = true;
                    DdlHelper.waitToContinue(MEDIAN_WAITING_TIME);
                    continue;
                } else {
                    scheduleSuspended = false;
                }

                try {
                    // Take current schema-specific DDL job.
                    DdlEngineRecord record = ddlJobDeliveryQueue.poll(1, TimeUnit.SECONDS);
                    if (record == null) {
                        continue;
                    }
                    // Process received DDL request.
                    schedule(record);
                    // Continue next round right now.
                    continue;
                } catch (Throwable t) {
                    try {
                        // Log it without throwing.
                        LOGGER.error("Failed to process this round of DDL job in " + schemaName + ". Caused by: "
                            + (t instanceof InterruptedException ? "interrupted" : t.getMessage()), t);
                    } catch (Throwable ignored) {
                    }
                    if (t instanceof InterruptedException && !isSchemaAvailable(schemaName)) {
                        // The thread has been cancelled because the schema is already
                        // unavailable, so we should exit to avoid multiple job schedulers
                        // associated with the same schema when consecutively dropping
                        // and creating a database with the same name.
                        return;
                    }
                }
            }
        }

        public boolean isIdle() {
            return suspending.get() && scheduleSuspended && semaphore.availablePermits() == maxParallelism;
        }

        private void schedule(DdlEngineRecord record) throws InterruptedException {
            if (record == null || !ExecUtils.hasLeadership(null)) {
                return;
            }
            //current DDL JOB running
            if (DdlEngineDagExecutorMap.contains(record.schemaName, record.jobId)) {
                return;
            }
            semaphore.acquire();
            try {
                completionService.submit(AsyncCallableTask.build(
                    new DdlJobExecutor(semaphore, maxParallelism, record.schemaName, record.jobId))
                );
            } catch (Throwable e) {
                //submit DDL task error
                semaphore.release();
                EventLogger.log(EventType.DDL_WARN, String.format(
                    "submit DDL JOB error. jobId:%s. semaphore count:%s, max parallelism: %s",
                    record.jobId,
                    maxParallelism
                ));
                LOGGER.error(String.format(
                    "submit DDL JOB error. jobId:%s. semaphore count:%s, max parallelism: %s",
                    record.jobId,
                    maxParallelism
                ), e);
            }
        }
    }

    private class DdlJobExecutor implements Callable<Boolean> {

        private final Semaphore semaphore;
        private final int maxParallelism;
        private final String schemaName;
        private final Long jobId;

        DdlJobExecutor(Semaphore semaphore, int maxParallelism, String schemaName, Long jobId) {
            this.semaphore = semaphore;
            this.maxParallelism = maxParallelism;
            this.schemaName = schemaName;
            this.jobId = jobId;
        }

        @Override
        public Boolean call() {
            try {
                if (!DdlHelper.isRunnable() || !ExecUtils.hasLeadership(null)) {
                    return null;
                }
                try {
                    // Perform the DDL job.
                    DdlHelper.getServerConfigManager().restoreDDL(schemaName, jobId);
                } catch (Throwable t) {
                    LOGGER.error("Failed to perform the DDL job '" + jobId + "'. Caused by: " + (
                        t instanceof NullPointerException ? "NPE" : t.getMessage()), t);
                }
                return true;
            } finally {
                semaphore.release();
                performVersion.incrementAndGet();
                FailPoint.inject(() -> {
                    int availablePermits = semaphore.availablePermits();
                    if (availablePermits > maxParallelism) {
                        FailPoint.throwException(String.format(
                            "semaphore leaks. maxParallelism:%s, availablePermits:%s",
                            maxParallelism, availablePermits));
                    }
                });
            }
        }

    }

    private boolean offerQueue(BlockingQueue queue, Object object, String message) {
        boolean successful = false;
        String errMsg = null;
        Throwable throwable = null;

        try {
            if (queue.offer(object)) {
                successful = true;
            } else {
                errMsg = "No enough space in the queue";
            }
        } catch (Throwable t) {
            errMsg = "Failed to put the object in the queue. Caused by: " + t.getMessage();
            throwable = t;
        }

        if (!successful) {
            if (TStringUtil.isNotEmpty(message)) {
                errMsg += message;
            }
            if (throwable != null) {
                LOGGER.error(errMsg, throwable);
            } else {
                LOGGER.error(errMsg);
            }
        }

        return successful;
    }

    private void uniqueOffer(String schemaName, BlockingQueue queue, DdlEngineRecord record) {
        try {
            if (queue == null) {
                throw new TddlNestableRuntimeException("DDL Scheduler Queue is null");
            }
            synchronized (queue) {
                if (queue.contains(record)) {
                    return;
                }
                boolean success = queue.offer(record);
                if (!success) {
                    LOGGER.error(String.format(
                        "No enough space in the queue. schemaName:%s, jobId:%s", schemaName, record.jobId));
                }
            }
        } catch (Throwable t) {
            LOGGER.error(String.format(
                "Failed to put the object in the queue. schemaName:%s, jobId:%s", schemaName, record.jobId), t);
        }
    }

    private boolean isSchemaAvailable(String schemaName) {
        boolean available = activeSchemas.contains(schemaName.toLowerCase());
        if (!available) {
            // The schema activity was registered prior to the DDL job scheduler.
            // If it's unavailable, then means that the schema has been unloaded
            // or dropped, so we should release current DDL job scheduler thread.
            DdlHelper.resetCurrentThreadName(DDL_SCHEDULER_NAME);
            return false;
        }
        return true;
    }

    private void interruptDdlJobs(){
        if(CollectionUtils.isEmpty(activeSchemas)){
            return;
        }
        for(String schemaName: activeSchemas){
            List<DdlEngineDagExecutorMap.DdlEngineDagExecutionInfo> executionInfoList =
                DdlEngineDagExecutorMap.getAllDdlJobCaches(schemaName);
            if(CollectionUtils.isEmpty(executionInfoList)){
                return;
            }
            for(DdlEngineDagExecutorMap.DdlEngineDagExecutionInfo info: executionInfoList){
                DdlEngineDagExecutor dagExecutor = DdlEngineDagExecutorMap.get(schemaName, info.jobId);
                if(dagExecutor != null && !dagExecutor.isInterrupted()){
                    dagExecutor.interrupt();
                    EventLogger.log(EventType.DDL_INTERRUPT, String.format(
                        "interrupt DDL JOB %s for losing CN leadership", dagExecutor.getJobId()));
                }
            }
        }
    }

}
