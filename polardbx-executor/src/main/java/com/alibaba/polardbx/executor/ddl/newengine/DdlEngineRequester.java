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

import com.alibaba.polardbx.common.ddl.newengine.DdlState;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.ddl.job.task.basic.SubJobTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.meta.DdlEngineSchedulerManager;
import com.alibaba.polardbx.executor.ddl.newengine.meta.DdlJobManager;
import com.alibaba.polardbx.executor.ddl.newengine.sync.DdlRequest;
import com.alibaba.polardbx.executor.ddl.newengine.sync.DdlRequestSyncAction;
import com.alibaba.polardbx.executor.ddl.newengine.sync.DdlResponse;
import com.alibaba.polardbx.executor.ddl.newengine.sync.DdlResponse.Response;
import com.alibaba.polardbx.executor.ddl.newengine.utils.DdlHelper;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineRecord;
import com.alibaba.polardbx.gms.sync.GmsSyncManagerHelper;
import com.alibaba.polardbx.optimizer.context.DdlContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.common.ddl.newengine.DdlConstants.LESS_WAITING_TIME;
import static com.alibaba.polardbx.common.ddl.newengine.DdlConstants.MORE_WAITING_TIME;
import static com.alibaba.polardbx.common.ddl.newengine.DdlType.ALTER_TABLEGROUP;
import static com.alibaba.polardbx.common.ddl.newengine.DdlType.MOVE_DATABASE;
import static com.alibaba.polardbx.common.properties.ConnectionParams.ENABLE_ROLLBACK_TO_READY;

public class DdlEngineRequester {

    private static final Logger LOGGER = LoggerFactory.getLogger(DdlEngineRequester.class);

    /**
     * Keep ddl result for 12 hours, and set a capacity to avoid too much memory footprint
     */
    private final static Cache<Long, Response> RESPONSES = CacheBuilder.newBuilder()
        .expireAfterAccess(Duration.ofHours(48))
        .maximumSize(1024)
        .build();

    private final DdlJob ddlJob;
    private final DdlContext ddlContext;
    private final DdlJobManager ddlJobManager;
    private final ExecutionContext executionContext;

    private static final DdlEngineSchedulerManager schedulerManager = new DdlEngineSchedulerManager();

    public DdlEngineRequester(DdlJob ddlJob, ExecutionContext ec, DdlContext dc) {
        this.ddlJob = ddlJob;
        this.ddlContext = dc;
        this.ddlJobManager = new DdlJobManager();
        this.executionContext = ec;
    }

    /**
     * Create a requester from existed job
     */
    public static DdlEngineRequester create(DdlJob ddlJob, ExecutionContext executionContext) {
        if (ddlJob != null && executionContext != null && executionContext.getDdlContext() != null) {
            return new DdlEngineRequester(ddlJob, executionContext, executionContext.getDdlContext());
        } else {
            throw DdlHelper.logAndThrowError(LOGGER, "The DDL job and contexts must not be null");
        }
    }

    /**
     * Execute the subjob
     */
    public long executeSubJob(long parentJobId, long parentTaskId, boolean forRollback) {
        ddlContext.setResources(ddlJob.getExcludeResources());
        // Create a new job and put it in the queue.
        return ddlJobManager.storeSubJob(parentJobId, parentTaskId, ddlJob, ddlContext, forRollback);
    }

    public void execute() {
        ddlContext.setResources(ddlJob.getExcludeResources());

        // Create a new job and put it in the queue.
        ddlJobManager.storeJob(ddlJob, ddlContext);

        // Request the leader to perform the job.
        DdlRequest ddlRequest = notifyLeader(ddlContext.getSchemaName(), Lists.newArrayList(ddlContext.getJobId()));

        // Wait for response from the leader, then respond to the client.
        if (ddlContext.isAsyncMode()) {
            return;
        }
        respond(ddlRequest, ddlJobManager, executionContext, true, false, ddlContext.isEnableTrace());
    }

    public static DdlRequest notifyLeader(String schemaName, List<Long> jobId) {
        // Build a new DDL request.
        DdlRequest ddlRequest = DdlJobManager.buildRequest(jobId, schemaName);

        // Notify the leader of new DDL request.
        if (ExecUtils.hasLeadership(null)) {
            // Notify myself without sync.
            DdlEngineScheduler.getInstance().notify(ddlRequest);
        } else {
            try {
                // Fetch the leader key for specific sync.
                String leaderKey = ExecUtils.getLeaderKey(null);
                // Notify the leader via Sync Action.
                GmsSyncManagerHelper.sync(new DdlRequestSyncAction(ddlRequest), schemaName, leaderKey);
            } catch (Exception e) {
                // Log only since the sync failure doesn't affect the leader to perform the DDL job.
                LOGGER.error("Hit sync failure (" + e.getMessage() + ") when sending a DDL request to the leader. "
                    + "The DDL will still be performed by the leader later. ", e);
            }
        }

        return ddlRequest;
    }

    public static void respond(DdlRequest ddlRequest,
                               DdlJobManager ddlJobManager,
                               ExecutionContext executionContext,
                               boolean checkResponseInMemory,
                               boolean rollbackOpt,
                               boolean forceCheckResInMemory) {
        DdlResponse ddlResponse =
            waitForComplete(ddlRequest.getJobIds(), ddlJobManager, checkResponseInMemory, rollbackOpt,
                forceCheckResInMemory);

        Response response = ddlResponse.getResponse(ddlRequest.getJobIds().get(0));

        switch (response.getResponseType()) {
        case ERROR:
            String errContent = response.getResponseContent();
            if (TStringUtil.isEmpty(errContent)) {
                errContent = "The DDL job has been cancelled or interrupted";
            }
            throw GeneralUtil.nestedException(errContent);
        case WARNING:
            List<ExecutionContext.ErrorMessage> warnings =
                (List<ExecutionContext.ErrorMessage>) response.getWarning();
            executionContext.getExtraDatas().put(ExecutionContext.FAILED_MESSAGE, warnings);
            break;
        case SUCCESS:
        default:
            break;
        }

        if (response.getTracer() != null && executionContext.getTracer() != null) {
            executionContext.getTracer().trace(response.getTracer().getOperations());
        }
    }

    public static DdlResponse waitForComplete(List<Long> jobIds,
                                              DdlJobManager ddlJobManager,
                                              boolean checkResponseInMemory,
                                              boolean rollbackOpt,
                                              boolean forceCheckResInMemory) {
        DdlResponse ddlResponse = new DdlResponse();

        // Wait until the response is received or the job(s) failed.
        final int checkInterval = MORE_WAITING_TIME;
        int totalWaitingTime = 0;

        while (true) {
            // Check if we have received response(s) from leader.
            if (checkResponseInMemory && checkResponse(ddlResponse, jobIds)) {
                break;
            }

            // Wait for a moment since leader is probably performing the job(s).
            totalWaitingTime += DdlHelper.waitToContinue(LESS_WAITING_TIME);
            if (Thread.interrupted()) {
                exit();
            }

            // Only a worker checks if the job(s) are paused or failed, but leader
            // wasn't able to respond to the worker.
            if (totalWaitingTime > checkInterval && !forceCheckResInMemory) {
                // Check if the job(s) have been pended.
                if (ddlJobManager.checkRecords(ddlResponse, jobIds, rollbackOpt)) {
                    // Double check to avoid miss message
                    DdlHelper.waitToContinue(LESS_WAITING_TIME);
                    if (checkResponseInMemory) {
                        checkResponse(ddlResponse, jobIds);
                    }
                    break;
                }
                // Reset for next check.
                totalWaitingTime = 0;
            }
        }

        return ddlResponse;
    }

    private static boolean checkResponse(DdlResponse ddlResponse, List<Long> jobIds) {
        for (Long jobId : jobIds) {
            Response response = RESPONSES.getIfPresent(jobId);
            if (response != null) {
                ddlResponse.addResponse(jobId, response);
            }
        }

        if (ddlResponse.getResponses().size() >= jobIds.size()) {
            // Already collected all the responses.
            return true;
        }

        // No response yet.
        return false;
    }

    public static void addResponses(Map<Long, Response> responses) {
        RESPONSES.putAll(responses);
    }

    public static List<Response> getResponse() {
        return Lists.newArrayList(RESPONSES.asMap().values());
    }

    public static void removeResponses(List<Long> jobIds) {
        jobIds.stream().forEach(jobId -> RESPONSES.invalidate(jobId));
    }

    public static void pauseJob(Long jobId, ExecutionContext executionContext) {
        if (jobId == null) {
            return;
        }
        DdlJobManager ddlJobManager = new DdlJobManager();
        DdlEngineRecord record = ddlJobManager.fetchRecordByJobId(jobId);
        pauseJob(record, false, false, executionContext);
    }

    public static int pauseJob(DdlEngineRecord record, boolean enableOperateSubJob,
                               boolean enableContinueRunningSubJob, ExecutionContext executionContext) {
        if (record == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR, "The ddl job does not exist");
        }

        if (record.isSubJob() && !enableOperateSubJob) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR, "Operation on subjob is not allowed");
        }

        List<Long> pausedJobs = new ArrayList<>();
        List<String> traceIds = new ArrayList<>();

        if (enableOperateSubJob && enableContinueRunningSubJob) {
            pauseJob(record, true, pausedJobs, traceIds, true, executionContext);
        } else {
            pauseJob(record, true, pausedJobs, traceIds, false, executionContext);
        }

        Collections.reverse(pausedJobs);
        DdlEngineRequester.notifyLeader(executionContext.getSchemaName(), pausedJobs);

        return pausedJobs.size();
    }

    private static void pauseJob(DdlEngineRecord record, boolean subJob, List<Long> pausedJobs, List<String> traceIds,
                                 Boolean continueRunningSubJob, ExecutionContext executionContext) {
        DdlState before = DdlState.valueOf(record.state);
        DdlState after = DdlState.PAUSE_JOB_STATE_TRANSFER.get(before);

        String errMsg =
            String.format("Only RUNNING/ROLLBACK_RUNNING/QUEUED jobs can be paused, but job %s is in %s state",
                record.jobId, before);

        if (before == DdlState.PAUSED || before == DdlState.ROLLBACK_PAUSED ||
            before == DdlState.COMPLETED || before == DdlState.ROLLBACK_COMPLETED) {
            buildWarning(errMsg, executionContext);
            return;
        }

        if (!(before == DdlState.RUNNING || before == DdlState.ROLLBACK_RUNNING || before == DdlState.QUEUED)) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR, errMsg);
        }

        if ((MOVE_DATABASE.name().equalsIgnoreCase(record.ddlType)
            || ALTER_TABLEGROUP.name().equalsIgnoreCase(record.ddlType)) && before == DdlState.RUNNING
            && executionContext.getParamManager().getBoolean(ENABLE_ROLLBACK_TO_READY)) {
            // support cancel, cdc task not done
            DdlState rebalanceAfter = DdlState.ROLLBACK_TO_READY;
            if (record.isSupportCancel() && schedulerManager.tryPauseDdl(record.jobId, before, rebalanceAfter)) {
                // revert ddl to first task
                LOGGER.info(String.format("revert job %d", record.jobId));
                pausedJobs.add(record.jobId);
                traceIds.add(record.traceId);

                // 中断子任务
                DdlHelper.interruptJobs(record.schemaName, Collections.singletonList(record.jobId));
                DdlHelper.killActivePhyDDLs(record.schemaName, record.traceId);
            }
            return;
        }

        if (schedulerManager.tryPauseDdl(record.jobId, before, after)) {
            LOGGER.info(String.format("pause job %d", record.jobId));

            pausedJobs.add(record.jobId);
            traceIds.add(record.traceId);

            // 先中断父任务
            DdlHelper.interruptJobs(record.schemaName, Collections.singletonList(record.jobId));
            DdlHelper.killActivePhyDDLs(record.schemaName, record.traceId);

            if (subJob) {
                pauseSubJobs(record.jobId, pausedJobs, traceIds, continueRunningSubJob, executionContext);
            }
        }
    }

    private static void pauseSubJobs(long jobId, List<Long> pausedJobs, List<String> traceIds,
                                     Boolean continueRunningSubJob, ExecutionContext executionContext) {
        List<SubJobTask> subJobs = schedulerManager.fetchSubJobsRecursive(jobId, continueRunningSubJob);

        List<Long> subJobIds = GeneralUtil.emptyIfNull(subJobs)
            .stream().flatMap(x -> x.fetchAllSubJobs().stream()).collect(Collectors.toList());

        if (CollectionUtils.isEmpty(subJobIds)) {
            return;
        }

        List<DdlEngineRecord> records = schedulerManager.fetchRecords(subJobIds);

        for (DdlEngineRecord record : GeneralUtil.emptyIfNull(records)) {
            pauseJob(record, false, pausedJobs, traceIds, false, executionContext);
        }
    }

    private static void buildWarning(String warnMsg, ExecutionContext executionContext) {
        List<ExecutionContext.ErrorMessage> warnings = new ArrayList<>(1);
        warnings.add(new ExecutionContext.ErrorMessage(ErrorCode.ERR_DDL_JOB_WARNING.getCode(), null, warnMsg));
        executionContext.getExtraDatas().put(ExecutionContext.FAILED_MESSAGE, warnings);
    }

    private static void exit() {
        throw new TddlRuntimeException(ErrorCode.ERR_USER_CANCELED, "Query was canceled");
    }

}
