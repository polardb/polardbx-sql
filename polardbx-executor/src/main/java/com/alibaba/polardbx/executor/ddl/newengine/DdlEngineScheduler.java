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

import com.alibaba.polardbx.common.async.AsyncCallableTask;
import com.alibaba.polardbx.common.async.AsyncTask;
import com.alibaba.polardbx.common.ddl.newengine.DdlState;
import com.alibaba.polardbx.common.eventlogger.EventLogger;
import com.alibaba.polardbx.common.eventlogger.EventType;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.utils.LoggerUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.thread.ExecutorUtil;
import com.alibaba.polardbx.common.utils.thread.NamedThreadFactory;
import com.alibaba.polardbx.common.utils.thread.ServerThreadPool;
import com.alibaba.polardbx.executor.changeset.ChangeSetApplyExecutorMap;
import com.alibaba.polardbx.executor.ddl.newengine.meta.DdlEngineSchedulerManager;
import com.alibaba.polardbx.executor.ddl.newengine.meta.DdlJobManager;
import com.alibaba.polardbx.executor.ddl.newengine.sync.DdlRequest;
import com.alibaba.polardbx.executor.ddl.newengine.utils.DdlHelper;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.config.impl.MetaDbInstConfigManager;
import com.alibaba.polardbx.gms.lease.LeaseManager;
import com.alibaba.polardbx.gms.lease.impl.LeaseManagerImpl;
import com.alibaba.polardbx.gms.metadb.lease.LeaseRecord;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineRecord;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import com.google.common.collect.Sets;
import org.apache.calcite.rel.metadata.BuiltInMetadata;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static com.alibaba.polardbx.common.ddl.newengine.DdlConstants.DDL_ARCHIVE_CLEANER_NAME;
import static com.alibaba.polardbx.common.ddl.newengine.DdlConstants.DDL_DISPATCHER_NAME;
import static com.alibaba.polardbx.common.ddl.newengine.DdlConstants.DDL_LEADER_ELECTION_NAME;
import static com.alibaba.polardbx.common.ddl.newengine.DdlConstants.DDL_LEADER_KEY;
import static com.alibaba.polardbx.common.ddl.newengine.DdlConstants.DDL_LEADER_TTL_IN_MILLIS;
import static com.alibaba.polardbx.common.ddl.newengine.DdlConstants.DDL_SCHEDULER_NAME;
import static com.alibaba.polardbx.common.ddl.newengine.DdlConstants.DEFAULT_LOGICAL_DDL_PARALLELISM;
import static com.alibaba.polardbx.common.ddl.newengine.DdlConstants.DEFAULT_PAUSED_DDL_RESCHEDULE_INTERVAL_IN_MINUTES;
import static com.alibaba.polardbx.common.ddl.newengine.DdlConstants.DEFAULT_RUNNING_DDL_RESCHEDULE_INTERVAL_IN_MINUTES;
import static com.alibaba.polardbx.common.ddl.newengine.DdlConstants.MEDIAN_WAITING_TIME;
import static com.alibaba.polardbx.common.ddl.newengine.DdlConstants.MORE_WAITING_TIME;
import static com.alibaba.polardbx.common.ddl.newengine.DdlConstants.ROLLBACK_DDL_WAIT_TIMES;
import static com.alibaba.polardbx.common.properties.ConnectionProperties.LOGICAL_DDL_PARALLELISM;
import static com.alibaba.polardbx.gms.topology.SystemDbHelper.DEFAULT_DB_NAME;

public class DdlEngineScheduler {

    private static final Logger LOGGER = LoggerFactory.getLogger(DdlEngineScheduler.class);

    private static final DdlEngineScheduler INSTANCE = new DdlEngineScheduler();

    /**
     * Instance-level DDL request dispatcher.
     */
    private final ExecutorService ddlDispatcherThread = DdlHelper.createSingleThreadPool(DDL_DISPATCHER_NAME);
    private final BlockingQueue<DdlRequest> ddlRequestTransitQueue = new LinkedBlockingQueue<>();
    private final BlockingQueue<DdlEngineRecord> ddlJobDeliveryQueue = new LinkedBlockingQueue<>();

    /**
     * Database-level DDL request schedulers.
     */
    private final ExecutorService ddlSchedulerThread = DdlHelper.createSingleThreadPool(DDL_SCHEDULER_NAME);

    private final ScheduledExecutorService archiveCleanerThread = ExecutorUtil.createScheduler(1,
        new NamedThreadFactory(DDL_ARCHIVE_CLEANER_NAME),
        new ThreadPoolExecutor.DiscardPolicy());

    private final ScheduledExecutorService ddlLeaderElectionThread = ExecutorUtil.createScheduler(1,
        new NamedThreadFactory(DDL_LEADER_ELECTION_NAME),
        new ThreadPoolExecutor.DiscardPolicy());

    private final DdlJobScheduler ddlJobScheduler = new DdlJobScheduler();
    private Map<String, DdlJobSchedulerConfig> activeSchemaDdlConfig = new ConcurrentHashMap<>();

    /**
     * Job executing version number, increments version number by 1 for each Job executed
     */
    private final AtomicLong performVersion = new AtomicLong(0L);
    private final AtomicBoolean suspending = new AtomicBoolean(false);

    private final AtomicReference<LeaseRecord> ddlLeaderLease = new AtomicReference<>();

    private DdlEngineScheduler() {
        if (DdlHelper.isRunnable()) {
            ddlDispatcherThread.submit(AsyncTask.build(new DdlJobDispatcher()));
            ddlSchedulerThread.submit(AsyncTask.build(ddlJobScheduler));

            archiveCleanerThread.scheduleAtFixedRate(
                AsyncTask.build(new DdlArchiveCleaner()),
                0L,
                4L,
                TimeUnit.HOURS
            );
            ddlLeaderElectionThread.scheduleAtFixedRate(
                AsyncTask.build(new DdlLeaderElectionRunner()),
                0L,
                DDL_LEADER_TTL_IN_MILLIS / 2,
                TimeUnit.MILLISECONDS
            );
        }
    }

    public static DdlEngineScheduler getInstance() {
        return INSTANCE;
    }

    public void register(String schemaName, ServerThreadPool executor) {
        if (DdlHelper.isRunnable(schemaName)) {
            String lowerCaseSchemaName = schemaName.toLowerCase();
            DdlEngineDagExecutorMap.register(lowerCaseSchemaName);
            ChangeSetApplyExecutorMap.register(lowerCaseSchemaName);
            synchronized (activeSchemaDdlConfig) {
                activeSchemaDdlConfig.put(lowerCaseSchemaName,
                    new DdlJobSchedulerConfig(lowerCaseSchemaName, executor));
            }
        }
    }

    public void deregister(String schemaName) {
        if (DdlHelper.isRunnable(schemaName)) {
            String lowerCaseSchemaName = schemaName.toLowerCase();
            DdlEngineDagExecutorMap.deregister(lowerCaseSchemaName);
            ChangeSetApplyExecutorMap.deregister(lowerCaseSchemaName);
            synchronized (activeSchemaDdlConfig) {
                activeSchemaDdlConfig.remove(lowerCaseSchemaName);
            }
        }
    }

    public void notify(DdlRequest ddlRequest) {
        StringBuilder message = new StringBuilder();
        message.append(". DDL Job Request: schema - ").append(ddlRequest.getSchemaName());
        message.append(", jobIds - ");
        for (Long jobId : ddlRequest.getJobIds()) {
            for (long waitTimes = 0; waitTimes < ROLLBACK_DDL_WAIT_TIMES; ++waitTimes) {
                if (!DdlEngineDagExecutorMap.contains(ddlRequest.getSchemaName(), jobId)) {
                    break;
                }
                SQLRecorderLogger.ddlEngineLogger.info(
                    String.format("waiting for ddl job:%s.%s stop, times:%s",
                        ddlRequest.getSchemaName(), jobId, waitTimes));
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
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
                boolean allIdle = ddlJobScheduler.isIdle();

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

    private class DdlArchiveCleaner implements Runnable {

        private final DdlJobManager ddlJobManager = new DdlJobManager();

        @Override
        public void run() {
            if (!DdlHelper.isRunnable()) {
                return;
            }
            if (!ExecUtils.hasLeadership(null)) {
                return;
            }
            //15 days
            long daysInMinuts = 15 * 24 * 60;
            int count = ddlJobManager.cleanUpArchive(daysInMinuts);
            SQLRecorderLogger.ddlEngineLogger.info(String.format("clean up DDL archive data, count:[%s]", count));
        }
    }

    private class DdlLeaderElectionRunner implements Runnable {

        private final LeaseManager leaseManager = new LeaseManagerImpl();

        @Override
        public void run() {
            try {
                SQLRecorderLogger.ddlEngineLogger.debug(
                    "current ddlLeaderLease info:" + (ddlLeaderLease.get() == null ? "empty" :
                        ddlLeaderLease.get().info()));
                if (DdlHelper.hasDdlLeadership()) {
                    Optional<LeaseRecord> optionalLeaseRecord = leaseManager.extend(DDL_LEADER_KEY);
                    if (optionalLeaseRecord.isPresent()) {
                        ddlLeaderLease.set(optionalLeaseRecord.get());
                        SQLRecorderLogger.ddlEngineLogger.debug(
                            "success extend DDL_LEADER:" + optionalLeaseRecord.get().info());
                    }
                } else {
                    Optional<LeaseRecord> optionalLeaseRecord =
                        leaseManager.acquire(DEFAULT_DB_NAME, DDL_LEADER_KEY, DDL_LEADER_TTL_IN_MILLIS);
                    if (optionalLeaseRecord.isPresent()) {
                        ddlLeaderLease.set(optionalLeaseRecord.get());
                        SQLRecorderLogger.ddlEngineLogger.info(
                            "success acquire DDL_LEADER:" + optionalLeaseRecord.get().info());
                    }
                }
            } catch (Throwable t) {
                SQLRecorderLogger.ddlEngineLogger.error("DDL Leader election error", t);
            }
        }
    }

    private class DdlJobDispatcher implements Runnable {

        private final DdlJobManager ddlJobManager = new DdlJobManager();

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
                        processRunning();
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
            List<DdlEngineRecord> records = ddlJobManager.fetchRecords(ddlRequest.getJobIds());
            dispatch(records);
        }

        private void processQueue() {
            // No job request came before timeout. Let's try to poll on the
            // job queue directly to see if any job was left to handle.
            List<DdlEngineRecord> records =
                ddlJobManager.fetchRecords(Sets.union(Sets.newHashSet(DdlState.QUEUED), DdlState.FINISHED));
            dispatch(records);
        }

        private void processRunning() {
            // No job request came before timeout. Let's try to poll on the
            // job queue directly to see if any job was left to handle.
            List<DdlEngineRecord> records =
                ddlJobManager.fetchRecords(
                    Sets.newHashSet(DdlState.RUNNING, DdlState.ROLLBACK_RUNNING, DdlState.ROLLBACK_TO_READY),
                    DEFAULT_RUNNING_DDL_RESCHEDULE_INTERVAL_IN_MINUTES);
            dispatch(records);
        }

        private void processPaused() {
            List<DdlEngineRecord> records =
                ddlJobManager.fetchRecords(DdlState.TERMINATED,
                    DdlHelper.getInstConfigAsInt(LOGGER, ConnectionProperties.PAUSED_DDL_RESCHEDULE_INTERVAL_IN_MINUTES,
                        DEFAULT_PAUSED_DDL_RESCHEDULE_INTERVAL_IN_MINUTES));
            for (DdlEngineRecord record : records) {
                try {
                    DdlState currentState = DdlState.valueOf(record.state);
                    if (currentState == DdlState.PAUSED) {
                        DdlState pausedPolicy = DdlState.valueOf(record.pausedPolicy);
                        if (pausedPolicy == currentState) {
                            continue;
                        }
                        ddlJobManager.compareAndSetDdlState(record.jobId, currentState, pausedPolicy);
                    }
                    if (currentState == DdlState.ROLLBACK_PAUSED) {
                        DdlState rollbackPausedPolicy = DdlState.valueOf(record.rollbackPausedPolicy);
                        if (rollbackPausedPolicy == currentState) {
                            continue;
                        }
                        ddlJobManager.compareAndSetDdlState(record.jobId, currentState, rollbackPausedPolicy);
                    }
                } catch (Throwable t) {
                    EventLogger.log(EventType.DDL_WARN, "reschedule paused ddl error: " + t.getMessage());
                    LOGGER.error("reschedule paused ddl error: " + t.getMessage(), t);
                }
            }
        }

        private void dispatch(List<DdlEngineRecord> records) {
            for (DdlEngineRecord record : records) {
                // Avoid schedule subjobs
                if (record.isSubJob() && !record.isRollBackToReady() && !record.isSkipSubjob()) {
                    continue;
                }
                try {
                    uniqueOffer(record);
                } catch (Throwable t) {
                    LOGGER.error("Failed to dispatch to queue. Caused by: " + t.getMessage(), t);
                }
            }
        }

        private void uniqueOffer(DdlEngineRecord record) {
            if (record == null || (record.isSubJob() && !record.isRollBackToReady() && !record.isSkipSubjob())) {
                return;
            }
            String schemaName = record.schemaName.toLowerCase();
            try {
                synchronized (ddlJobDeliveryQueue) {
                    if (ddlJobDeliveryQueue.contains(record)) {
                        return;
                    }
                    if (DdlEngineDagExecutorMap.contains(schemaName, record.jobId)) {
                        return;
                    }
                    // double check ddl record exits
                    List<DdlEngineRecord> records
                        = ddlJobManager.fetchRecords(Collections.singletonList(record.jobId));
                    if (records == null || records.isEmpty()) {
                        return;
                    }
                    boolean success = ddlJobDeliveryQueue.offer(record);
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

    }

    private class DdlJobSchedulerConfig {
        private String schemaName;
        private int maxParallelism;
        private final Semaphore semaphore;
        private final ExecutorCompletionService completionService;

        public DdlJobSchedulerConfig(String schemaName, ServerThreadPool ddlExecutor) {
            this.schemaName = schemaName;
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
            this.completionService = new ExecutorCompletionService(ddlExecutor);
        }
    }

    private class DdlJobScheduler implements Runnable {

        private volatile boolean scheduleSuspended;

        private DdlJobScheduler() {
        }

        @Override
        public void run() {
            if (!DdlHelper.isRunnable()) {
                return;
            }

            Thread.currentThread().setName(DDL_SCHEDULER_NAME);

            while (true) {
                try {
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
                        LOGGER.error("Failed to schedule DDL job. Caused by: "
                            + (t instanceof InterruptedException ? "interrupted" : t.getMessage()), t);
                        EventLogger.log(EventType.DDL_WARN, "Failed to schedule DDL job. Caused by: "
                            + (t instanceof InterruptedException ? "interrupted" : t.getMessage()));
                    } catch (Throwable ignored) {
                    }
                }
            }
        }

        public boolean isIdle() {
            boolean idleFlag = suspending.get() && scheduleSuspended;
            if (idleFlag == false) {
                return false;
            }
            synchronized (activeSchemaDdlConfig) {
                for (Map.Entry<String, DdlJobSchedulerConfig> entry : activeSchemaDdlConfig.entrySet()) {
                    DdlJobSchedulerConfig schedulerConfig = entry.getValue();
                    if (schedulerConfig.semaphore.availablePermits() != schedulerConfig.maxParallelism) {
                        //there are executing DDLs
                        return false;
                    }
                }
            }
            return true;
        }

        private void schedule(DdlEngineRecord record) throws InterruptedException {
            if (record == null || !ExecUtils.hasLeadership(null)
                || (record.isSubJob() && !record.isRollBackToReady() && !record.isSkipSubjob())) {
                return;
            }
            //current DDL JOB running
            if (DdlEngineDagExecutorMap.contains(record.schemaName, record.jobId)) {
                return;
            }
            final String schemaName = StringUtils.lowerCase(record.schemaName);
            final DdlJobSchedulerConfig schedulerConfig = activeSchemaDdlConfig.get(schemaName);
            if (schedulerConfig == null) {
                LOGGER.debug(String.format("schema:%s is not active for DDL JOB:%s", schemaName, record.jobId));
                return;
            }
            final Semaphore semaphore = schedulerConfig.semaphore;
            final ExecutorCompletionService completionService = schedulerConfig.completionService;
            final int maxParallelism = schedulerConfig.maxParallelism;

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
                LoggerUtil.buildMDC(schemaName);
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

    public AtomicReference<LeaseRecord> getDdlLeaderLease() {
        return this.ddlLeaderLease;
    }
}
