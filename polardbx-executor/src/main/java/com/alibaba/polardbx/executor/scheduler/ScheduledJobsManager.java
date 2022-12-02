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

package com.alibaba.polardbx.executor.scheduler;

import com.alibaba.polardbx.common.TddlConstants;
import com.alibaba.polardbx.common.async.AsyncCallableTask;
import com.alibaba.polardbx.common.async.AsyncTask;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.scheduler.FiredScheduledJobState;
import com.alibaba.polardbx.common.scheduler.SchedulePolicy;
import com.alibaba.polardbx.gms.module.ModuleInfo;
import com.alibaba.polardbx.gms.node.LeaderStatusBridge;
import com.alibaba.polardbx.gms.scheduler.ScheduledJobExecutorType;
import com.alibaba.polardbx.common.scheduler.SchedulerJobStatus;
import com.alibaba.polardbx.common.scheduler.SchedulerType;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.thread.ExecutorUtil;
import com.alibaba.polardbx.common.utils.thread.NamedThreadFactory;
import com.alibaba.polardbx.executor.scheduler.executor.ScheduleJobStarter;
import com.alibaba.polardbx.executor.ddl.newengine.DdlPlanScheduler;
import com.alibaba.polardbx.executor.ddl.newengine.utils.DdlHelper;
import com.alibaba.polardbx.executor.scheduler.executor.SchedulerExecutor;
import com.alibaba.polardbx.executor.sync.FetchRunningScheduleJobsSyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.gms.scheduler.ExecutableScheduledJob;
import com.alibaba.polardbx.gms.scheduler.ScheduledJobsAccessorDelegate;
import com.alibaba.polardbx.gms.scheduler.ScheduledJobsRecord;
import com.alibaba.polardbx.optimizer.view.VirtualViewType;
import com.cronutils.descriptor.CronDescriptor;
import com.cronutils.model.Cron;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.parser.CronParser;
import com.google.common.collect.Maps;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.glassfish.jersey.internal.guava.Sets;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.alibaba.polardbx.common.properties.ConnectionParams.SCHEDULER_CLEAN_UP_INTERVAL_HOURS;
import static com.alibaba.polardbx.common.properties.ConnectionParams.SCHEDULER_MAX_WORKER_COUNT;
import static com.alibaba.polardbx.common.properties.ConnectionParams.SCHEDULER_SCAN_INTERVAL_SECONDS;
import static com.alibaba.polardbx.common.properties.ConnectionParams.SCHEDULER_MIN_WORKER_COUNT;
import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.INTERRUPTED;
import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.QUEUED;
import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.SKIPPED;
import static com.alibaba.polardbx.common.scheduler.SchedulePolicy.FIRE;
import static com.alibaba.polardbx.common.scheduler.SchedulePolicy.SKIP;
import static com.alibaba.polardbx.common.scheduler.SchedulePolicy.WAIT;
import static com.alibaba.polardbx.executor.scheduler.ScheduledJobsTrigger.restoreTrigger;
import static com.cronutils.model.CronType.QUARTZ;

/**
 * https://yuque.antfin-inc.com/coronadb/design/dugt95
 *
 * @author guxu
 */
public final class ScheduledJobsManager implements ModuleInfo {

    private static final Logger logger = LoggerFactory.getLogger(ScheduledJobsManager.class);

    private final static ScheduledJobsManager INSTANCE = new ScheduledJobsManager();

    public static ScheduledJobsManager getINSTANCE() {
        return INSTANCE;
    }

    private long lastFireTimestamp = -1;
    private int firedJobNum = 0;
    private long lastTriggerTimestamp = -1;
    private int triggerJobNum = 0;

    /**
     * periodically scan & trigger JOBs
     */
    private final ScheduledThreadPoolExecutor scannerThread =
        ExecutorUtil.createScheduler(1,
            new NamedThreadFactory("Scheduled-Jobs-Scanner-Thread", true),
            new ThreadPoolExecutor.DiscardPolicy());

    /**
     * periodically clean up JOB execution records
     */
    private final ScheduledThreadPoolExecutor cleanerThread =
        ExecutorUtil.createScheduler(1,
            new NamedThreadFactory("Scheduled-Jobs-Cleaner-Thread", true),
            new ThreadPoolExecutor.DiscardPolicy());

    /**
     * periodically clean up JOB execution records
     */
    private final ScheduledThreadPoolExecutor safeExitThread =
        ExecutorUtil.createScheduler(1,
            new NamedThreadFactory("Scheduled-Jobs-SafeExit-Thread", true),
            new ThreadPoolExecutor.DiscardPolicy());

    /**
     * execute JOBs
     */
    private final ExecutorService workersThreadPool =
        new ThreadPoolExecutor(
            InstConfUtil.getInt(SCHEDULER_MIN_WORKER_COUNT),
            InstConfUtil.getInt(SCHEDULER_MAX_WORKER_COUNT),
            1L,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(128),
            new NamedThreadFactory("Scheduled-Jobs-Worker-Thread", true),
            new ThreadPoolExecutor.DiscardPolicy()
        );

    private static final Map<Long, Long> executingMap = new ConcurrentHashMap<>();

    private ScheduledJobsManager() {
        scannerThread.scheduleWithFixedDelay(
            AsyncTask.build(
                () -> {
                    new ScheduledJobsScanner().run();
                    new FiredScheduledJobsScanner(workersThreadPool).run();
                }
            ),
            5L,
            InstConfUtil.getLong(SCHEDULER_SCAN_INTERVAL_SECONDS),
            TimeUnit.SECONDS
        );

        cleanerThread.scheduleWithFixedDelay(
            AsyncTask.build(new ScheduledJobsCleaner()),
            0L,
            InstConfUtil.getLong(SCHEDULER_CLEAN_UP_INTERVAL_HOURS),
            TimeUnit.HOURS
        );

        safeExitThread.scheduleWithFixedDelay(
            AsyncTask.build(new ScheduledJobsSafeExitChecker()),
            0L,
            InstConfUtil.getLong(SCHEDULER_SCAN_INTERVAL_SECONDS),
            TimeUnit.SECONDS
        );

        ScheduleJobStarter.launchAll();
    }

    public static Map<Long, Long> getExecutingMap() {
        return executingMap;
    }

    // module info interface start

    /**
     *
     */
    @Override
    public String state() {
        return ModuleInfo.buildStateByArgs(
            SCHEDULER_MIN_WORKER_COUNT,
            SCHEDULER_MAX_WORKER_COUNT,
            SCHEDULER_SCAN_INTERVAL_SECONDS,
            SCHEDULER_CLEAN_UP_INTERVAL_HOURS
        );
    }

    @Override
    public String status(long since) {
        if (!LeaderStatusBridge.getInstance().hasLeadership()) {
            return "";
        }
        SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyyy-mm-dd hh:mm:ss");
        return "fired num:" + firedJobNum +
            ",lastFiredTime:" + DATE_FORMAT.format(new Date(lastFireTimestamp)) +
            ",trigger num:" + triggerJobNum +
            ",lastTriggerTime:" + DATE_FORMAT.format(new Date(lastTriggerTimestamp));
    }

    @Override
    public String resources() {
        return "scannerThread 1 cleanerThread 1 safeExitThread 1 workersThreadPool";
    }

    @Override
    public String scheduleJobs() {
        if (!LeaderStatusBridge.getInstance().hasLeadership()) {
            return "";
        }
        List<ScheduledJobsRecord> jobs = this.queryScheduledJobsRecord();
        StringBuilder stringBuilder = new StringBuilder();
        jobs.stream().forEach(j -> stringBuilder.append(j.toString()).append(";"));
        return stringBuilder.toString();
    }

    @Override
    public String workload() {
        return executingMap.toString();
    }

    @Override
    public String views() {
        return VirtualViewType.SCHEDULE_JOBS.name() + ",metadb.scheduled_jobs,metadb.fired_scheduled_jobs";
    }
    // module info interface end

    /**
     * Record Cleaner
     */
    private static class ScheduledJobsCleaner implements Runnable {

        @Override
        public void run() {
            if (!hasLeadership()) {
                return;
            }
            try {
                logger.info("start cleaning fired_scheduled_jobs");
                ScheduledJobsAccessorDelegate<Integer> delegate = new ScheduledJobsAccessorDelegate<Integer>() {
                    @Override
                    protected Integer invoke() {
                        //240 hours == 10 days
                        //720 hours == 30 days
                        long hours = InstConfUtil.getLong(ConnectionParams.SCHEDULER_RECORD_KEEP_HOURS);
                        firedScheduledJobsAccessor.setFiredJobsTimeout(240L);
                        return firedScheduledJobsAccessor.cleanup(hours);
                    }
                };
                int count = delegate.execute();
                logger.info("cleaned fired_scheduled_jobs count: " + count);
            } catch (Throwable t) {
                logger.error("clean fired_scheduled_jobs error", t);
            }
        }
    }

    /**
     * safe exit
     */
    private static class ScheduledJobsSafeExitChecker implements Runnable {

        @Override
        public void run() {
            if (!hasLeadership()) {
                return;
            }
            try {
                logger.info("start attaching error jobs");
                ScheduledJobsAccessorDelegate<Integer> delegate = new ScheduledJobsAccessorDelegate<Integer>() {
                    @Override
                    protected Integer invoke() {
                        List<ExecutableScheduledJob> runningJobs = firedScheduledJobsAccessor.getRunningJobs();
                        // TODO timeout control?
                        List<List<Map<String, Object>>> results =
                            SyncManagerHelper.sync(new FetchRunningScheduleJobsSyncAction(),
                                TddlConstants.INFORMATION_SCHEMA);
                        Map<Long, Set<Long>> executingJobs = merge(results);
                        if (!hasLeadership()) {
                            return -1;
                        }

                        int count = 0;
                        for (ExecutableScheduledJob job : runningJobs) {
                            Long scheduleId = job.getScheduleId();
                            Long fireTime = job.getFireTime();

                            if (!hasLeadership()) {
                                return -1;
                            }

                            if (executingJobs.containsKey(scheduleId) && executingJobs.get(scheduleId)
                                .contains(fireTime)) {
                                continue;
                            }

                            /**
                             * try safe exit
                             */
                            SchedulerExecutor esj = SchedulerExecutor.createSchedulerExecutor(job);
                            if (esj.needInterrupted().getKey() && esj.safeExit()) {
                                //mark as fail
                                ScheduledJobsManager.updateState(scheduleId, fireTime, INTERRUPTED,
                                    "interrupted by safe exit checker", "");
                                count++;
                            }
                        }
                        return count;
                    }

                    /**
                     * merge running jobs
                     * @param results schedule id -> Set<firetime>
                     * @return
                     */
                    private Map<Long, Set<Long>> merge(List<List<Map<String, Object>>> results) {
                        Map<Long, Set<Long>> runningJobs = Maps.newConcurrentMap();
                        for (List<Map<String, Object>> nodeRs : results) {
                            for (Map<String, Object> row : nodeRs) {
                                Long scheduleId = (Long) row.get("SCHEDULE_ID");
                                Long fireTime = (Long) row.get("FIRE_TIME");
                                if (!runningJobs.containsKey(scheduleId)) {
                                    runningJobs.put(scheduleId, Sets.newHashSet());
                                }
                                runningJobs.get(scheduleId).add(fireTime);
                            }
                        }

                        for (Map.Entry<Long, Long> entry : ScheduledJobsManager.getExecutingMap().entrySet()) {
                            Long scheduleId = entry.getKey();
                            Long fireTime = entry.getValue();

                            if (!runningJobs.containsKey(scheduleId)) {
                                runningJobs.put(scheduleId, Sets.newHashSet());
                            }
                            runningJobs.get(scheduleId).add(fireTime);
                        }
                        return runningJobs;
                    }
                };
                int count = delegate.execute();
                logger.info("change interrupted fired_scheduled_jobs from fake running state count: " + count);
            } catch (Throwable t) {
                logger.error("change interrupted fired_scheduled_jobs from fake running state error", t);
            }
        }
    }

    /**
     * 1. Scan & Lock System Table: scheduled_jobs
     * 2. Fire a Job if necessary
     */
    private static class ScheduledJobsScanner implements Runnable {

        @Override
        public void run() {
            if (!hasLeadership()) {
                return;
            }
            try {
                ScheduledJobsAccessorDelegate delegate = new ScheduledJobsAccessorDelegate<Integer>() {
                    @Override
                    protected Integer invoke() {
                        List<ScheduledJobsRecord> scheduledJobsRecordList = scheduledJobsAccessor.scan();
                        if (CollectionUtils.isEmpty(scheduledJobsRecordList)) {
                            return 0;
                        }
                        int fireCount = 0;
                        for (ScheduledJobsRecord record : scheduledJobsRecordList) {
                            try {
                                //won't fire scheduledJobs if it's status is 'DISABLED'
                                if (StringUtils.equalsIgnoreCase(record.getStatus(),
                                    SchedulerJobStatus.DISABLED.name())) {
                                    continue;
                                }
                                if (!hasLeadership()) {
                                    return fireCount;
                                }
                                ScheduledJobsTrigger trigger = restoreTrigger(record, scheduledJobsAccessor,
                                    firedScheduledJobsAccessor);
                                if (trigger == null) {
                                    //schedule type not supported yet
                                    continue;
                                }
                                if (trigger.fire()) {
                                    ScheduledJobsManager.getINSTANCE().firedJobNum++;
                                    ScheduledJobsManager.getINSTANCE().lastFireTimestamp = System.currentTimeMillis();
                                    fireCount++;
                                }
                            } catch (Throwable t) {
                                logger.error(String.format("process scheduled job:[%s] error", record.getScheduleId()),
                                    t);
                            }
                        }
                        return fireCount;
                    }
                };
                delegate.execute();
            } catch (Throwable t) {
                logger.error("ScheduledJobsScanner error", t);
            }
        }
    }

    /**
     * 1. Scan System Table: fired_scheduled_jobs
     * 2. Invoke SchedulerExecutor to execute it
     */
    private static class FiredScheduledJobsScanner implements Runnable {

        private final ExecutorService executorService;

        FiredScheduledJobsScanner(ExecutorService executorService) {
            this.executorService = executorService;
        }

        @Override
        public void run() {
            if (!hasLeadership()) {
                return;
            }
            try {
                List<ExecutableScheduledJob> executableScheduledJobList = getExecutableScheduledJobList();
                if (CollectionUtils.isEmpty(executableScheduledJobList)) {
                    return;
                }
                for (ExecutableScheduledJob record : executableScheduledJobList) {
                    if (!hasLeadership()) {
                        return;
                    }
                    if (!StringUtils.equalsIgnoreCase(record.getState(), QUEUED.name())) {
                        continue;
                    }
                    //won't execute scheduledJobs if it's status is 'DISABLED'
                    if (StringUtils.equalsIgnoreCase(record.getStatus(), SchedulerJobStatus.DISABLED.name())) {
                        continue;
                    }
                    executorService.submit(AsyncCallableTask.<Boolean>build(new SchedulerExecutorRunner(record)));
                }
            } catch (Throwable t) {
                logger.error("FiredScheduledJobsScanner error", t);
            }
        }
    }

    /**
     * 执行一次定时任务
     */
    private static class SchedulerExecutorRunner implements Callable<Boolean> {

        private final ExecutableScheduledJob executableScheduledJob;
        private final long scheduleId;
        private final long fireTime;

        SchedulerExecutorRunner(ExecutableScheduledJob executableScheduledJob) {
            this.executableScheduledJob = executableScheduledJob;
            this.scheduleId = executableScheduledJob.getScheduleId();
            this.fireTime = executableScheduledJob.getFireTime();
        }

        @Override
        public Boolean call() throws Exception {
            if (!hasLeadership()) {
                return false;
            }
            String schedulePolicy = executableScheduledJob.getSchedulePolicy();
            if (StringUtils.equalsIgnoreCase(schedulePolicy, WAIT.name())) {
                return executeByWaitPolicy();
            } else if (StringUtils.equalsIgnoreCase(schedulePolicy, SKIP.name())) {
                return executeBySkipPolicy();
            } else if (StringUtils.equalsIgnoreCase(schedulePolicy, FIRE.name())) {
                return executeByFirePolicy();
            } else {
                return executeByWaitPolicy();
            }
        }

        /**
         * schedule policy: wait
         * if exists executing job, wait for it
         */
        private boolean executeByWaitPolicy() {
            boolean hasExecuting = !putExecutingJobIfAbsent(scheduleId, fireTime);
            if (hasExecuting) {
                return false;
            }
            try {
                SchedulerExecutor schedulerExecutor = SchedulerExecutor.createSchedulerExecutor(executableScheduledJob);
                if (schedulerExecutor == null) {
                    return false;
                }
                if (schedulerExecutor.needInterrupted().getKey()) {
                    return false;
                }
                ScheduledJobsManager.getINSTANCE().triggerJobNum++;
                ScheduledJobsManager.getINSTANCE().lastTriggerTimestamp = System.currentTimeMillis();

                return schedulerExecutor.execute();
            } finally {
                remove(scheduleId);
            }
        }

        /**
         * schedule policy: skip
         * if exists executing job, skip current job
         */
        private boolean executeBySkipPolicy() {
            Long value = get(scheduleId);
            if (value == null) {
                return executeByWaitPolicy();
            }
            if (value == this.fireTime) {
                return false;
            } else {
                casStateWithStartTime(scheduleId, fireTime, QUEUED, SKIPPED, 0L);
                return false;
            }
        }

        /**
         * schedule policy: fire
         */
        private boolean executeByFirePolicy() {
            try {
                put(scheduleId, fireTime);
                SchedulerExecutor schedulerExecutor = SchedulerExecutor.createSchedulerExecutor(executableScheduledJob);
                if (schedulerExecutor == null) {
                    return false;
                }
                if (schedulerExecutor.needInterrupted().getKey()) {
                    return false;
                }
                ScheduledJobsManager.getINSTANCE().triggerJobNum++;
                ScheduledJobsManager.getINSTANCE().lastTriggerTimestamp = System.currentTimeMillis();

                return schedulerExecutor.execute();
            } finally {
                remove(scheduleId);
            }
        }
    }

    private static boolean hasLeadership() {
        return ExecUtils.hasLeadership(null);
    }

    /**
     * @return whether it was absent
     */
    public static boolean putExecutingJobIfAbsent(long scheduleId, long fireTime) {
        synchronized (executingMap) {
            boolean hasExecuting = executingMap.containsKey(scheduleId);
            if (hasExecuting) {
                return false;
            }
            executingMap.put(scheduleId, fireTime);
            return true;
        }
    }

    public static void put(long scheduleId, long fireTime) {
        synchronized (executingMap) {
            executingMap.put(scheduleId, fireTime);
        }
    }

    public static long remove(long scheduleId) {
        synchronized (executingMap) {
            return executingMap.remove(scheduleId);
        }
    }

    public static Long get(long scheduleId) {
        return executingMap.get(scheduleId);
    }

    /******************************************** dao methods ***********************************************/

    public static boolean updateState(long schedulerId,
                                      long fireTime,
                                      FiredScheduledJobState newState,
                                      String remark,
                                      String result) {
        return new ScheduledJobsAccessorDelegate<Boolean>() {
            @Override
            protected Boolean invoke() {
                return firedScheduledJobsAccessor.updateState(schedulerId, fireTime, newState, remark, result);
            }
        }.execute();
    }

    public static boolean casStateWithStartTime(long schedulerId,
                                                long fireTime,
                                                FiredScheduledJobState currentState,
                                                FiredScheduledJobState newState,
                                                long startTime) {
        return new ScheduledJobsAccessorDelegate<Boolean>() {
            @Override
            protected Boolean invoke() {
                return firedScheduledJobsAccessor
                    .compareAndSetStateWithStartTime(schedulerId, fireTime, currentState, newState, startTime);
            }
        }.execute();
    }

    public static boolean casStateWithFinishTime(long schedulerId,
                                                 long fireTime,
                                                 FiredScheduledJobState currentState,
                                                 FiredScheduledJobState newState,
                                                 long finishTime,
                                                 String remark) {
        return new ScheduledJobsAccessorDelegate<Boolean>() {
            @Override
            protected Boolean invoke() {
                return firedScheduledJobsAccessor
                    .compareAndSetStateWithFinishTime(schedulerId, fireTime, currentState, newState, finishTime,
                        remark);
            }
        }.execute();
    }

    public static ScheduledJobsRecord queryScheduledJobById(long scheduleId) {
        return new ScheduledJobsAccessorDelegate<ScheduledJobsRecord>() {
            @Override
            protected ScheduledJobsRecord invoke() {
                return scheduledJobsAccessor.queryById(scheduleId);
            }
        }.execute();
    }

    public static List<ScheduledJobsRecord> queryScheduledJobsRecord() {
        return new ScheduledJobsAccessorDelegate<List<ScheduledJobsRecord>>() {
            @Override
            protected List<ScheduledJobsRecord> invoke() {
                return scheduledJobsAccessor.query();
            }
        }.execute();
    }

    public static List<ExecutableScheduledJob> queryScheduledRunningJobsRecord() {
        return new ScheduledJobsAccessorDelegate<List<ExecutableScheduledJob>>() {
            @Override
            protected List<ExecutableScheduledJob> invoke() {
                return firedScheduledJobsAccessor.getRunningJobs();
            }
        }.execute();
    }

    public static List<ExecutableScheduledJob> getExecutableScheduledJobList() {
        return new ScheduledJobsAccessorDelegate<List<ExecutableScheduledJob>>() {
            @Override
            protected List<ExecutableScheduledJob> invoke() {
                return firedScheduledJobsAccessor.getQueuedJobs();
            }
        }.execute();
    }

    public static List<ExecutableScheduledJob> getScheduledJobResult(long scheduleId) {
        return new ScheduledJobsAccessorDelegate<List<ExecutableScheduledJob>>() {
            @Override
            protected List<ExecutableScheduledJob> invoke() {
                return firedScheduledJobsAccessor.queryByScheduleId(scheduleId);
            }
        }.execute();
    }


    public static int createScheduledJob(ScheduledJobsRecord record) {
        if (record == null) {
            return 0;
        }
        return new ScheduledJobsAccessorDelegate<Integer>() {
            @Override
            protected Integer invoke() {
                return scheduledJobsAccessor.insert(record);
            }
        }.execute();
    }

    private void fireAtOnce() {
        scannerThread.submit(
            AsyncTask.build(new FiredScheduledJobsScanner(workersThreadPool))
        );
    }

    public static int dropScheduledJob(long scheduleId) {
        return new ScheduledJobsAccessorDelegate<Integer>() {
            @Override
            protected Integer invoke() {
                int count = scheduledJobsAccessor.deleteById(scheduleId);
                firedScheduledJobsAccessor.deleteById(scheduleId);
                return count;
            }
        }.execute();
    }

    public static int fireScheduledJob(long scheduleId) {
        return new ScheduledJobsAccessorDelegate<Integer>() {
            @Override
            protected Integer invoke() {
                ScheduledJobsTrigger trigger = restoreTrigger(scheduledJobsAccessor.queryById(scheduleId),
                    scheduledJobsAccessor,
                    firedScheduledJobsAccessor);
                if (trigger == null) {
                    //schedule type not supported yet
                    return 0;
                }
                if (trigger.fireOnceNow()) {
                    ScheduledJobsManager.getINSTANCE().fireAtOnce();
                    return 1;
                }
                return 0;
            }
        }.execute();
    }

    public static int pauseScheduledJob(long scheduleId) {
        return new ScheduledJobsAccessorDelegate<Integer>() {
            @Override
            protected Integer invoke() {
                int count = scheduledJobsAccessor.disableById(scheduleId);
                return count;
            }
        }.execute();
    }

    public static int continueScheduledJob(long scheduleId) {
        return new ScheduledJobsAccessorDelegate<Integer>() {
            @Override
            protected Integer invoke() {
                int count = scheduledJobsAccessor.enableById(scheduleId);
                return count;
            }
        }.execute();
    }

    public static int dropScheduledJob(String schemaName, String tableName) {
        return new ScheduledJobsAccessorDelegate<Integer>() {
            @Override
            protected Integer invoke() {
                List<ScheduledJobsRecord> recordList = scheduledJobsAccessor.query(schemaName, tableName);
                int count = 0;
                if (CollectionUtils.isEmpty(recordList)) {
                    return count;
                }
                for (ScheduledJobsRecord record : recordList) {
                    count += scheduledJobsAccessor.deleteById(record.getScheduleId());
                    firedScheduledJobsAccessor.deleteById(record.getScheduleId());
                }
                return count;
            }
        }.execute();
    }

    public static ScheduledJobsRecord createQuartzCronJob(
        String tableSchema,
        String tableGroupName,
        String tableName,
        ScheduledJobExecutorType executorType,
        String scheduleExpr,
        String timeZone,
        SchedulePolicy schedulePolicy) {
        ScheduledJobsRecord record = new ScheduledJobsRecord();
        record.setTableSchema(tableSchema);
        record.setTableGroupName(tableGroupName);
        record.setTableName(tableName);
        record.setScheduleName(executorType + ":" + tableSchema + "." + tableName);
        CronParser quartzCronParser = new CronParser(CronDefinitionBuilder.instanceDefinitionFor(QUARTZ));
        Cron cron = quartzCronParser.parse(scheduleExpr);
        cron.validate();
        CronDescriptor descriptor = CronDescriptor.instance(Locale.US);
        record.setScheduleComment(descriptor.describe(cron));
        record.setExecutorType(executorType.name());
        record.setStatus(SchedulerJobStatus.ENABLED.name());
        record.setScheduleType(SchedulerType.QUARTZ_CRON.name());
        record.setScheduleExpr(scheduleExpr);
        record.setTimeZone(timeZone);
        record.setSchedulePolicy(schedulePolicy.name());
        return record;
    }

    public static List<ScheduledJobsRecord> getScheduledJobResultByScheduledType(String executorType) {
        return new ScheduledJobsAccessorDelegate<List<ScheduledJobsRecord>>() {
            @Override
            protected List<ScheduledJobsRecord> invoke() {
                return scheduledJobsAccessor.queryByExecutorType(executorType);
            }
        }.execute();
    }
}