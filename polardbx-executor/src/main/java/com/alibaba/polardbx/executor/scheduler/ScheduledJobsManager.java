package com.alibaba.polardbx.executor.scheduler;

import com.alibaba.polardbx.common.async.AsyncCallableTask;
import com.alibaba.polardbx.common.async.AsyncTask;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.scheduler.FiredScheduledJobState;
import com.alibaba.polardbx.common.scheduler.SchedulePolicy;
import com.alibaba.polardbx.common.scheduler.ScheduledJobExecutorType;
import com.alibaba.polardbx.common.scheduler.SchedulerJobStatus;
import com.alibaba.polardbx.common.scheduler.SchedulerType;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.thread.ExecutorUtil;
import com.alibaba.polardbx.common.utils.thread.NamedThreadFactory;
import com.alibaba.polardbx.executor.scheduler.executor.SchedulerExecutor;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.gms.config.impl.MetaDbInstConfigManager;
import com.alibaba.polardbx.gms.scheduler.ExecutableScheduledJob;
import com.alibaba.polardbx.gms.scheduler.ScheduledJobsAccessorDelegate;
import com.alibaba.polardbx.gms.scheduler.ScheduledJobsRecord;
import com.cronutils.descriptor.CronDescriptor;
import com.cronutils.model.Cron;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.parser.CronParser;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.QUEUED;
import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.SKIPPED;
import static com.alibaba.polardbx.common.scheduler.SchedulePolicy.FIRE;
import static com.alibaba.polardbx.common.scheduler.SchedulePolicy.SKIP;
import static com.alibaba.polardbx.common.scheduler.SchedulePolicy.WAIT;
import static com.alibaba.polardbx.executor.scheduler.ScheduledJobsTrigger.restoreTrigger;
import static com.cronutils.model.CronType.QUARTZ;

/**
 * https://yuque.antfin-inc.com/coronadb/design/dugt95
 * @author guxu
 */
public final class ScheduledJobsManager {

    private static final Logger logger = LoggerFactory.getLogger(ScheduledJobsManager.class);

    private final static ScheduledJobsManager INSTANCE = new ScheduledJobsManager();

    public static ScheduledJobsManager getINSTANCE() {
        return INSTANCE;
    }

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
     * execute JOBs
     */
    private final ExecutorService workersThreadPool =
        new ThreadPoolExecutor(
            getInstConfigAsInt(ConnectionProperties.SCHEDULER_MIN_WORKER_COUNT, 32),
            getInstConfigAsInt(ConnectionProperties.SCHEDULER_MAX_WORKER_COUNT, 64),
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
                ()->{
                    new ScheduledJobsScanner().run();
                    new FiredScheduledJobsScanner(workersThreadPool).run();
                }
            ),
            5L,
            getInstConfigAsLong(ConnectionProperties.SCHEDULER_SCAN_INTERVAL_SECONDS, 60L),
            TimeUnit.SECONDS
        );

        cleanerThread.scheduleWithFixedDelay(
            AsyncTask.build(new ScheduledJobsCleaner()),
            0L,
            getInstConfigAsLong(ConnectionProperties.SCHEDULER_CLEAN_UP_INTERVAL_HOURS, 3L),
            TimeUnit.HOURS
        );
    }

    /**
     * Record Cleaner
     */
    private static class ScheduledJobsCleaner implements Runnable{

        @Override
        public void run() {
            if(!hasLeadership()){
                return;
            }
            try {
                logger.info("start cleaning fired_scheduled_jobs");
                ScheduledJobsAccessorDelegate<Integer> delegate = new ScheduledJobsAccessorDelegate<Integer>(){
                    @Override
                    protected Integer invoke() {
                        //240 hours == 10 days
                        //720 hours == 30 days
                        long hours = getINSTANCE().getInstConfigAsLong(
                            ConnectionProperties.SCHEDULER_RECORD_KEEP_HOURS, 720L);
                        firedScheduledJobsAccessor.setFiredJobsTimeout(240L);
                        return firedScheduledJobsAccessor.cleanup(hours);
                    }
                };
                int count = delegate.execute();
                logger.info("cleaned fired_scheduled_jobs count: " + count);
            }catch (Throwable t){
                logger.error("clean fired_scheduled_jobs error", t);
            }
        }
    }

    /**
     * 1. Scan & Lock System Table: scheduled_jobs
     * 2. Fire a Job if necessary
     */
    private static class ScheduledJobsScanner implements Runnable{

        @Override
        public void run() {
            if(!hasLeadership()){
                return;
            }
            try {
                ScheduledJobsAccessorDelegate delegate = new ScheduledJobsAccessorDelegate<Integer>(){
                    @Override
                    protected Integer invoke() {
                        List<ScheduledJobsRecord> scheduledJobsRecordList = scheduledJobsAccessor.scan();
                        if(CollectionUtils.isEmpty(scheduledJobsRecordList)){
                            return 0;
                        }
                        int fireCount = 0;
                        for(ScheduledJobsRecord record: scheduledJobsRecordList){
                            try {
                                if(StringUtils.equalsIgnoreCase(record.getStatus(), SchedulerJobStatus.DISABLED.name())){
                                    continue;
                                }
                                if(!hasLeadership()){
                                    return fireCount;
                                }
                                ScheduledJobsTrigger trigger = restoreTrigger(record, scheduledJobsAccessor,
                                    firedScheduledJobsAccessor);
                                if(trigger==null){
                                    //schedule type not supported yet
                                    continue;
                                }
                                if(trigger.fire()){
                                    fireCount++;
                                }
                            }catch (Throwable t){
                                logger.error(String.format("process scheduled job:[%s] error", record.getScheduleId()), t);
                            }
                        }
                        return fireCount;
                    }
                };
                delegate.execute();
            }catch (Throwable t){
                logger.error("ScheduledJobsScanner error", t);
            }
        }
    }

    /**
     * 1. Scan System Table: fired_scheduled_jobs
     * 2. Invoke SchedulerExecutor to execute it
     */
    private static class FiredScheduledJobsScanner implements Runnable{

        private final ExecutorService executorService;

        FiredScheduledJobsScanner(ExecutorService executorService){
            this.executorService = executorService;
        }

        @Override
        public void run() {
            if(!hasLeadership()){
                return;
            }
            try {
                List<ExecutableScheduledJob> executableScheduledJobList = getExecutableScheduledJobList();
                if(CollectionUtils.isEmpty(executableScheduledJobList)){
                    return;
                }
                for(ExecutableScheduledJob record: executableScheduledJobList){
                    if(!StringUtils.equalsIgnoreCase(record.getState(), QUEUED.name())){
                        continue;
                    }
                    if(!hasLeadership()){
                        return;
                    }
                    executorService.submit(AsyncCallableTask.<Boolean>build(new SchedulerExecutorRunner(record)));
                }
            }catch (Throwable t){
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

        SchedulerExecutorRunner(ExecutableScheduledJob executableScheduledJob){
            this.executableScheduledJob = executableScheduledJob;
            this.scheduleId = executableScheduledJob.getScheduleId();
            this.fireTime = executableScheduledJob.getFireTime();
        }

        @Override
        public Boolean call() throws Exception {
            if(!hasLeadership()){
                return false;
            }
            String schedulePolicy = executableScheduledJob.getSchedulePolicy();
            if(StringUtils.equalsIgnoreCase(schedulePolicy, WAIT.name())){
                return executeByWaitPolicy();
            }else if(StringUtils.equalsIgnoreCase(schedulePolicy, SKIP.name())){
                return executeBySkipPolicy();
            }else if(StringUtils.equalsIgnoreCase(schedulePolicy, FIRE.name())){
                return executeByFirePolicy();
            }else {
                return executeByWaitPolicy();
            }
        }

        /**
         * schedule policy: wait
         * if exists executing job, wait for it
         */
        private boolean executeByWaitPolicy(){
            boolean hasExecuting = !putExecutingJobIfAbsent(scheduleId, fireTime);
            if(hasExecuting){
                return false;
            }
            try {
                SchedulerExecutor schedulerExecutor = SchedulerExecutor.createSchedulerExecutor(executableScheduledJob);
                if(schedulerExecutor == null){
                    return false;
                }
                return schedulerExecutor.execute();
            }finally {
                remove(scheduleId);
            }
        }

        /**
         * schedule policy: skip
         * if exists executing job, skip current job
         */
        private boolean executeBySkipPolicy(){
            Long value = get(scheduleId);
            if(value == null){
                return executeByWaitPolicy();
            }
            if(value == this.fireTime){
                return false;
            }else {
                casStateWithStartTime(scheduleId, fireTime, QUEUED, SKIPPED, 0L);
                return false;
            }
        }

        /**
         * schedule policy: fire
         */
        private boolean executeByFirePolicy(){
            try {
                put(scheduleId, fireTime);
                SchedulerExecutor schedulerExecutor = SchedulerExecutor.createSchedulerExecutor(executableScheduledJob);
                if(schedulerExecutor == null){
                    return false;
                }
                return schedulerExecutor.execute();
            }finally {
                remove(scheduleId);
            }
        }
    }

    private static boolean hasLeadership(){
        return ExecUtils.hasLeadership(null);
    }

    /**
     * @return whether it was absent
     */
    public static boolean putExecutingJobIfAbsent(long scheduleId, long fireTime){
        synchronized (executingMap){
            boolean hasExecuting = executingMap.containsKey(scheduleId);
            if(hasExecuting){
                return false;
            }
            executingMap.put(scheduleId, fireTime);
            return true;
        }
    }

    public static void put(long scheduleId, long fireTime){
        synchronized (executingMap){
            executingMap.put(scheduleId, fireTime);
        }
    }

    public static long remove(long scheduleId){
        synchronized (executingMap){
            return executingMap.remove(scheduleId);
        }
    }

    public static Long get(long scheduleId){
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
                    .compareAndSetStateWithFinishTime(schedulerId, fireTime, currentState, newState, finishTime, remark);
            }
        }.execute();
    }

    public static ScheduledJobsRecord queryScheduledJobById(long scheduleId){
        return new ScheduledJobsAccessorDelegate<ScheduledJobsRecord>(){
            @Override
            protected ScheduledJobsRecord invoke() {
                return scheduledJobsAccessor.queryById(scheduleId);
            }
        }.execute();
    }

    public static List<ScheduledJobsRecord> queryScheduledJobsRecord(){
        return new ScheduledJobsAccessorDelegate<List<ScheduledJobsRecord>>(){
            @Override
            protected List<ScheduledJobsRecord> invoke() {
                return scheduledJobsAccessor.query();
            }
        }.execute();
    }

    public static List<ExecutableScheduledJob> getExecutableScheduledJobList(){
        return new ScheduledJobsAccessorDelegate<List<ExecutableScheduledJob>>(){
                @Override
                protected List<ExecutableScheduledJob> invoke() {
                    return firedScheduledJobsAccessor.getQueuedJobs();
                }
            }.execute();
    }

    public static List<ExecutableScheduledJob> getScheduledJobResult(long scheduleId){
        return new ScheduledJobsAccessorDelegate<List<ExecutableScheduledJob>>(){
            @Override
            protected List<ExecutableScheduledJob> invoke() {
                return firedScheduledJobsAccessor.queryByScheduleId(scheduleId);
            }
        }.execute();
    }

    public static int createScheduledJob(ScheduledJobsRecord record){
        if(record==null){
            return 0;
        }
        return new ScheduledJobsAccessorDelegate<Integer>(){
            @Override
            protected Integer invoke() {
                return scheduledJobsAccessor.insert(record);
            }
        }.execute();
    }

    public static int dropScheduledJob(long scheduleId){
        return new ScheduledJobsAccessorDelegate<Integer>(){
            @Override
            protected Integer invoke() {
                int count = scheduledJobsAccessor.deleteById(scheduleId);
                firedScheduledJobsAccessor.deleteById(scheduleId);
                return count;
            }
        }.execute();
    }

    public static int dropScheduledJob(String schemaName, String tableName){
        return new ScheduledJobsAccessorDelegate<Integer>(){
            @Override
            protected Integer invoke() {
                List<ScheduledJobsRecord> recordList = scheduledJobsAccessor.query(schemaName, tableName);
                int count = 0;
                if(CollectionUtils.isEmpty(recordList)){
                    return count;
                }
                for(ScheduledJobsRecord record: recordList){
                    count += scheduledJobsAccessor.deleteById(record.getScheduleId());
                    firedScheduledJobsAccessor.deleteById(record.getScheduleId());
                }
                return count;
            }
        }.execute();
    }

    public static ScheduledJobsRecord createQuartzCronJob(
        String  tableSchema,
        String  tableName,
        ScheduledJobExecutorType executorType,
        String  scheduleExpr,
        String  timeZone,
        SchedulePolicy  schedulePolicy){
        ScheduledJobsRecord record = new ScheduledJobsRecord();
        record.setTableSchema(tableSchema);
        record.setTableName(tableName);
        record.setScheduleName(tableSchema + "." + tableName);
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

    int getInstConfigAsInt(String key, int defaultVal){
        String val = MetaDbInstConfigManager.getInstance().getInstProperty(key);
        if(StringUtils.isEmpty(val)){
            return defaultVal;
        }
        try {
            return Integer.valueOf(val);
        }catch (Exception e){
            logger.error(String.format("parse param:[%s=%s] error", key, val), e);
            return defaultVal;
        }
    }

    long getInstConfigAsLong(String key, long defaultVal){
        String val = MetaDbInstConfigManager.getInstance().getInstProperty(key);
        if(StringUtils.isEmpty(val)){
            return defaultVal;
        }
        try {
            return Long.valueOf(val);
        }catch (Exception e){
            logger.error(String.format("parse param:[%s=%s] error", key, val), e);
            return defaultVal;
        }
    }

}