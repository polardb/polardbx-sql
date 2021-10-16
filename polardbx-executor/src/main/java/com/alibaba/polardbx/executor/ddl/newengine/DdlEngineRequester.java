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
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.meta.DdlJobManager;
import com.alibaba.polardbx.executor.ddl.newengine.sync.DdlInterruptSyncAction;
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

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static com.alibaba.polardbx.common.ddl.newengine.DdlConstants.LESS_WAITING_TIME;
import static com.alibaba.polardbx.common.ddl.newengine.DdlConstants.MORE_WAITING_TIME;

public class DdlEngineRequester {

    private static final Logger LOGGER = LoggerFactory.getLogger(DdlEngineRequester.class);

    /**
     * Keep ddl result for 2 hours, and set a capacity to avoid too much memory footprint
     */
    private final static Cache<Long, Response> RESPONSES = CacheBuilder.newBuilder()
        .expireAfterAccess(Duration.ofHours(12))
        .maximumSize(1024)
        .build();

    private final DdlJob ddlJob;
    private final DdlContext ddlContext;
    private final DdlJobManager ddlJobManager;
    private final ExecutionContext executionContext;

    private DdlEngineRequester(DdlJob ddlJob, ExecutionContext executionContext) {
        this.ddlJob = ddlJob;
        this.ddlContext = executionContext.getDdlContext();
        this.ddlJobManager = new DdlJobManager();
        this.executionContext = executionContext;
    }

    public static DdlEngineRequester create(DdlJob ddlJob, ExecutionContext executionContext) {
        if (ddlJob != null && executionContext != null && executionContext.getDdlContext() != null) {
            return new DdlEngineRequester(ddlJob, executionContext);
        } else {
            throw DdlHelper.logAndThrowError(LOGGER, "The DDL job and contexts must not be null");
        }
    }

    public void execute() {
        ddlContext.setResources(ddlJob.getExcludeResources());

        // Create a new job and put it in the queue.
        ddlJobManager.storeJob(ddlJob, ddlContext);

        // Request the leader to perform the job.
        DdlRequest ddlRequest = notifyLeader();

        // Wait for response from the leader, then respond to the client.
        if (ddlContext.isAsyncMode()) {
            return;
        }
        respond(ddlRequest, ddlJobManager, executionContext, true);
    }

    private DdlRequest notifyLeader() {
        // Build a new DDL request.
        String schemaName = ddlContext.getSchemaName();
        DdlRequest ddlRequest = ddlJobManager.buildRequest(ddlContext);

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
                               boolean checkResponseInMemory) {
        DdlResponse ddlResponse = waitForComplete(ddlRequest.getJobIds(), ddlJobManager, checkResponseInMemory);

        Response response = ddlResponse.getResponse(ddlRequest.getJobIds().get(0));

        switch (response.getResponseType()) {
        case ERROR:
            String errContent = (String) response.getResponseContent();
            if (TStringUtil.isEmpty(errContent)) {
                errContent = "The DDL job has been cancelled or interrupted";
            }
            throw GeneralUtil.nestedException(errContent);
        case WARNING:
            List<ExecutionContext.ErrorMessage> warnings =
                (List<ExecutionContext.ErrorMessage>) response.getWarning();
            executionContext.getExtraDatas().put(ExecutionContext.FailedMessage, warnings);
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
                                              boolean checkResponseInMemory) {
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
                pauseJobThenExit(jobIds, ddlJobManager);
            }

            // Only a worker checks if the job(s) are paused or failed, but leader
            // wasn't able to respond to the worker.
            if (totalWaitingTime > checkInterval) {
                // Check if the job(s) have been pended.
                if (ddlJobManager.checkRecords(ddlResponse, jobIds)) {
                    // Double check to avoid miss message
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

    private static void pauseJobThenExit(List<Long> jobIds, DdlJobManager ddlJobManager) {
        List<DdlEngineRecord> records = ddlJobManager.fetchRecords(jobIds);
        int countDone = 0;
        for (DdlEngineRecord record : records) {
            if (DdlState.RUNNING == DdlState.valueOf(record.state)) {
                if (ddlJobManager.tryUpdateDdlState(
                    record.schemaName,
                    record.jobId,
                    DdlState.RUNNING,
                    DdlState.PAUSED)) {
                    countDone++;
                    DdlRequest ddlRequest = new DdlRequest(record.schemaName, Lists.newArrayList(record.jobId));
                    GmsSyncManagerHelper.sync(new DdlInterruptSyncAction(ddlRequest), record.schemaName);
                }
            } else if (DdlState.ROLLBACK_RUNNING == DdlState.valueOf(record.state)) {
                if (ddlJobManager.tryUpdateDdlState(
                    record.schemaName,
                    record.jobId,
                    DdlState.ROLLBACK_RUNNING,
                    DdlState.ROLLBACK_PAUSED)) {
                    countDone++;
                    DdlRequest ddlRequest = new DdlRequest(record.schemaName, Lists.newArrayList(record.jobId));
                    GmsSyncManagerHelper.sync(new DdlInterruptSyncAction(ddlRequest), record.schemaName);
                }
            }
        }

        if (countDone == 0) {
            throw new TddlRuntimeException(
                ErrorCode.ERR_GMS_GENERIC,
                "No job has been paused. Use 'show full ddl' to check DDL info");
        }

        throw new TddlRuntimeException(
            ErrorCode.ERR_GMS_GENERIC,
            String.format("%s jobs paused. Use 'show full ddl' to check DDL info", countDone));
    }

}
