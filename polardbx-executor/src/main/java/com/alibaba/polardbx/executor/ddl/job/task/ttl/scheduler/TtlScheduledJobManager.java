package com.alibaba.polardbx.executor.ddl.job.task.ttl.scheduler;

import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.scheduler.FiredScheduledJobState;
import com.alibaba.polardbx.common.utils.CaseInsensitive;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.exception.TtlJobRuntimeException;
import com.alibaba.polardbx.executor.scheduler.ScheduledJobsManager;
import com.alibaba.polardbx.executor.scheduler.executor.TtlArchivedDataScheduledJob;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.scheduler.ExecutableScheduledJob;
import com.alibaba.polardbx.gms.scheduler.FiredScheduledJobsAccessor;
import com.alibaba.polardbx.optimizer.ttl.TtlConfigUtil;

import java.sql.Connection;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.QUEUED;
import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.RUNNING;

/**
 * @author chenghui.lch
 */
public class TtlScheduledJobManager extends AbstractLifecycle {
    protected static final TtlScheduledJobManager instance = new TtlScheduledJobManager();
    protected volatile FiredTtlJobInfo firedTtlJobInfo = null;

    protected static class FiredTtlJobInfo {
        protected List<ExecutableScheduledJob> queuedTtlJobListOrderByFireTime = new ArrayList<>();
        protected List<ExecutableScheduledJob> runningTtlJobListOrderByFireTime = new ArrayList<>();
        protected Set<String> runningJobTableNameSet = new TreeSet<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);

        public FiredTtlJobInfo(List<ExecutableScheduledJob> latestQueuedJobs,
                               List<ExecutableScheduledJob> latestRunningJobs) {
            this.queuedTtlJobListOrderByFireTime = latestQueuedJobs;
            this.runningTtlJobListOrderByFireTime = latestRunningJobs;
            for (int i = 0; i < runningTtlJobListOrderByFireTime.size(); i++) {
                ExecutableScheduledJob job = runningTtlJobListOrderByFireTime.get(i);
                String dbName = job.getTableSchema();
                String tbName = job.getTableName();
                String dbAndTb = buildFullTblName(dbName, tbName);
                if (!runningJobTableNameSet.contains(dbAndTb)) {
                    runningJobTableNameSet.add(dbAndTb);
                }
            }
        }

        public List<ExecutableScheduledJob> getQueuedTtlJobListOrderByFireTime() {
            return queuedTtlJobListOrderByFireTime;
        }

        public List<ExecutableScheduledJob> getRunningTtlJobListOrderByFireTime() {
            return runningTtlJobListOrderByFireTime;
        }

        public Set<String> getRunningJobTableNameSet() {
            return runningJobTableNameSet;
        }
    }

    private static String buildFullTblName(String dbName, String tbName) {
        return String.format("%s.%s", dbName, tbName).toLowerCase();
    }

    public static TtlScheduledJobManager getInstance() {
        if (!instance.isInited()) {
            synchronized (instance) {
                if (!instance.isInited()) {
                    instance.init();
                }
            }
        }
        return instance;
    }

    protected TtlScheduledJobManager() {
    }

    public synchronized void reloadTtlScheduledJobInfos() {
        try (Connection conn = MetaDbDataSource.getInstance().getConnection()) {
            FiredScheduledJobsAccessor accessor = new FiredScheduledJobsAccessor();
            accessor.setConnection(conn);
            List<ExecutableScheduledJob> queuedJobs = accessor.getQueuedTtlJobs();
            List<ExecutableScheduledJob> runningJobs = accessor.getRunningTtlJobs();
            FiredTtlJobInfo jobInfo = new FiredTtlJobInfo(queuedJobs, runningJobs);
            this.firedTtlJobInfo = jobInfo;
        } catch (Throwable ex) {
            throw new TtlJobRuntimeException(ex);
        }
    }

    public boolean updateStateCasWithStartTime(long schedulerId,
                                               long fireTime,
                                               FiredScheduledJobState currentState,
                                               FiredScheduledJobState newState,
                                               long startTime) {

        boolean casSuccess =
            ScheduledJobsManager.casStateWithStartTime(schedulerId, fireTime, QUEUED, RUNNING, startTime);
        return casSuccess;
    }

    /**
     * Check if current ttl job allowed to running
     */
    public synchronized boolean applyForRunning(TtlArchivedDataScheduledJob job) {

        /**
         * Fetch the latest queued ttl jobs and running ttl jobs
         */
        reloadTtlScheduledJobInfos();

        FiredTtlJobInfo firedTtlJobInfo = this.firedTtlJobInfo;
        List<ExecutableScheduledJob> queuedTtlJobListOrderByFireTime =
            firedTtlJobInfo.getQueuedTtlJobListOrderByFireTime();
        List<ExecutableScheduledJob> runningTtlJobListOrderByFireTime =
            firedTtlJobInfo.getRunningTtlJobListOrderByFireTime();

        if (queuedTtlJobListOrderByFireTime.isEmpty()) {
            /**
             * No found target fired jobs
             */
            return false;
        }

        int maxTtlScheduledJobParallelism = TtlConfigUtil.getTtlScheduledJobMaxParallelism();
        if (runningTtlJobListOrderByFireTime.size() >= maxTtlScheduledJobParallelism) {
            return false;
        }

        boolean findTargetScheduleId = false;
        int emptyJobListSize = maxTtlScheduledJobParallelism - runningTtlJobListOrderByFireTime.size();
        int queuedJobListSize = queuedTtlJobListOrderByFireTime.size();
        int queueSize = Math.min(queuedJobListSize, emptyJobListSize);
        for (int i = 0; i < queueSize; i++) {
            ExecutableScheduledJob nextQueueJob = queuedTtlJobListOrderByFireTime.get(i);
            if (nextQueueJob.getScheduleId() == job.getScheduleId()) {
                findTargetScheduleId = true;
                break;
            }
        }
        if (!findTargetScheduleId) {
            return false;
        }

        /**
         * Update Ttl Schedule Job State
         */
        ExecutableScheduledJob jobRec = job.getExecutableScheduledJob();
        long startTime = ZonedDateTime.now().toEpochSecond();
        updateStateCasWithStartTime(jobRec.getScheduleId(), jobRec.getFireTime(), QUEUED, RUNNING, startTime);
        return true;
    }

    @Override
    protected void doInit() {
        super.doInit();
        reloadTtlScheduledJobInfos();
    }

    @Override
    protected void doDestroy() {
        super.doDestroy();
    }
}
