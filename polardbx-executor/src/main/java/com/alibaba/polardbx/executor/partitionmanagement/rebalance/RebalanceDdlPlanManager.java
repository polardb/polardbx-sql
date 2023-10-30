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

package com.alibaba.polardbx.executor.partitionmanagement.rebalance;

import com.alibaba.polardbx.common.ddl.newengine.DdlPlanState;
import com.alibaba.polardbx.common.ddl.newengine.DdlState;
import com.alibaba.polardbx.common.eventlogger.EventLogger;
import com.alibaba.polardbx.common.eventlogger.EventType;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.executor.balancer.action.ActionUtils;
import com.alibaba.polardbx.executor.ddl.job.task.CostEstimableDdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.meta.DdlJobManager;
import com.alibaba.polardbx.executor.ddl.newengine.meta.DdlPlanAccessorDelegate;
import com.alibaba.polardbx.executor.ddl.newengine.meta.DdlPlanManager;
import com.alibaba.polardbx.executor.ddl.newengine.utils.TaskHelper;
import com.alibaba.polardbx.executor.gsi.GsiBackfillManager;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineRecord;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineTaskRecord;
import com.alibaba.polardbx.gms.scheduler.DdlPlanRecord;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.gms.topology.SystemDbHelper.DEFAULT_DB_NAME;

public class RebalanceDdlPlanManager {

    protected static final Logger LOGGER = SQLRecorderLogger.ddlEngineLogger;
    private DdlJobManager ddlJobManager = new DdlJobManager();
    private DdlPlanManager ddlPlanManager = new DdlPlanManager();
    private GsiBackfillManager gsiBackfillManager = new GsiBackfillManager(DEFAULT_DB_NAME);

    public RebalanceDdlPlanManager() {
    }

    public void process(final DdlPlanRecord ddlPlanRecord) {
        switch (DdlPlanState.valueOf(ddlPlanRecord.getState())) {
        case INIT:
            onInit(ddlPlanRecord);
            break;
        case EXECUTING:
            onExecuting(ddlPlanRecord);
            break;
        case SUCCESS:
            onSuccess(ddlPlanRecord);
            break;
        case TERMINATED:
            onTerminated(ddlPlanRecord);
            break;
        }
    }

    protected void onInit(final DdlPlanRecord ddlPlanRecord) {
        ddlPlanManager.submitNewRebalanceJobIfAssertTrue(ddlPlanRecord.getPlanId(), record -> {
            if (record.getJobId() > 0) {
                throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR, "already exist executing rebalance DDL");
            }
            String clusterLock = ActionUtils.genRebalanceClusterName();
            boolean ok =
                ddlJobManager.getResourceManager().checkResource(Sets.newHashSet(), Sets.newHashSet(clusterLock));
            if (!ok) {
                throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR, "already exist executing rebalance DDL");
            }
        });
    }

    protected void onExecuting(final DdlPlanRecord ddlPlanRecord) {
        final long jobId = ddlPlanRecord.getJobId();
        DdlEngineRecord ddlEngineRecord = ddlJobManager.fetchRecordByJobId(jobId);
        if (ddlEngineRecord == null) {
            ddlEngineRecord = ddlJobManager.fetchArchiveRecordByJobId(jobId);
        }
        switch (DdlState.valueOf(ddlEngineRecord.state)) {
        case QUEUED:
        case RUNNING:
        case ROLLBACK_RUNNING:
            //do nothing
            break;
        case PAUSED:
            onDdlJobPaused(ddlPlanRecord, ddlEngineRecord);
            break;
        case ROLLBACK_PAUSED:
            onDdlJobRollbackPaused(ddlPlanRecord, ddlEngineRecord);
            break;
        case ROLLBACK_COMPLETED:
            onDdlJobRollbackCompleted(ddlPlanRecord.getPlanId(), jobId);
            break;
        case COMPLETED:
            onDdlJobCompleted(ddlPlanRecord.getPlanId(), jobId);
            break;
        }
    }

    protected void onSuccess(final DdlPlanRecord ddlPlanRecord) {
        //do nothing
    }

    protected void onTerminated(final DdlPlanRecord ddlPlanRecord) {
        //do nothing
    }

    /*****************************************************************************************/

    /**
     * submit a new rebalance DDL JOB
     */
    protected void onDdlJobRollbackCompleted(long ddlPlanId, long originJobId) {
        ddlPlanManager.submitNewRebalanceJobIfAssertTrue(ddlPlanId, record -> {
            long jobId = record.getJobId();
            if (jobId != originJobId) {
                throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR, "already rescheduled rebalance DDL");
            }

            String clusterLock = ActionUtils.genRebalanceClusterName();
            boolean ok =
                ddlJobManager.getResourceManager().checkResource(Sets.newHashSet(), Sets.newHashSet(clusterLock));
            if (!ok) {
                throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR, "already exist executing rebalance DDL");
            }

            //archive finished rows
            try {
                Pair<Long, Long> pair = getBackFillCount(jobId, true);
                CostEstimableDdlTask.CostInfo formerCostInfo = TaskHelper.decodeCostInfo(record.getExtras());
                CostEstimableDdlTask.CostInfo costInfo =
                    CostEstimableDdlTask.createCostInfo(pair.getKey(), pair.getValue());
                CostEstimableDdlTask.CostInfo newCostInfo =
                    CostEstimableDdlTask.CostInfo.combine(formerCostInfo, costInfo);
                ddlPlanManager.updateCostInfo(ddlPlanId, newCostInfo);
            } catch (Exception e) {
                LOGGER.error("update cost info for ddl_plan error", e);
            }
        });
    }

    protected void onDdlJobPaused(DdlPlanRecord ddlPlanRecord, DdlEngineRecord ddlEngineRecord) {
        if (needRetry(ddlPlanRecord.getRetryCount(), ddlPlanRecord.getGmtModified())) {
            DdlState newState = DdlState.tryParse(ddlEngineRecord.pausedPolicy, DdlState.PAUSED);
            if (newState == ddlJobManager.compareAndSetDdlState(ddlEngineRecord.jobId, DdlState.PAUSED, newState)) {
                LOGGER.info(String.format("update DDL JOB state:[%s] from PAUSED to %s", ddlEngineRecord.jobId,
                    newState.name()));
                EventLogger.log(EventType.DDL_WARN,
                    String.format("FAILED TO CONTINUE DDL PLAN:[%s]", ddlPlanRecord.getPlanId()));
                ddlPlanManager.incrementRetryCount(ddlPlanRecord.getPlanId());
            }
        }
    }

    protected void onDdlJobRollbackPaused(DdlPlanRecord ddlPlanRecord, DdlEngineRecord ddlEngineRecord) {
        if (needRetry(ddlPlanRecord.getRetryCount(), ddlPlanRecord.getGmtModified())) {
            DdlState newState = DdlState.tryParse(ddlEngineRecord.rollbackPausedPolicy, DdlState.ROLLBACK_PAUSED);
            if (newState == ddlJobManager.compareAndSetDdlState(ddlEngineRecord.jobId, DdlState.ROLLBACK_PAUSED,
                newState)) {
                LOGGER.info(
                    String.format("update DDL JOB state:[%s] from ROLLBACK_PAUSED to %s", ddlEngineRecord.jobId,
                        newState.name()));
                EventLogger.log(EventType.DDL_WARN,
                    String.format("FAILED TO CONTINUE DDL PLAN:[%s]", ddlPlanRecord.getPlanId()));
                ddlPlanManager.incrementRetryCount(ddlPlanRecord.getPlanId());
            }
        }
    }

    protected void onDdlJobCompleted(long ddlPlanId, long jobId) {
        new DdlPlanAccessorDelegate<Boolean>() {
            @Override
            protected Boolean invoke() {
                DdlPlanRecord record = ddlPlanAccessor.queryForUpdate(ddlPlanId);

                //archive finished rows
                try {
                    Pair<Long, Long> pair = getBackFillCount(jobId, true);
                    CostEstimableDdlTask.CostInfo formerCostInfo = TaskHelper.decodeCostInfo(record.getExtras());
                    CostEstimableDdlTask.CostInfo costInfo =
                        CostEstimableDdlTask.createCostInfo(pair.getKey(), pair.getValue());
                    CostEstimableDdlTask.CostInfo newCostInfo =
                        CostEstimableDdlTask.CostInfo.combine(formerCostInfo, costInfo);
                    ddlPlanAccessor.updateExtra(ddlPlanId, TaskHelper.encodeCostInfo(newCostInfo));
                } catch (Exception e) {
                    LOGGER.error("update cost info for ddl_plan error", e);
                }

                return ddlPlanAccessor.updateState(
                    ddlPlanId,
                    DdlPlanState.SUCCESS,
                    record.getResult(),
                    jobId
                );
            }
        }.execute();
        LOGGER.info(String.format("schedule ddl_plan:[%s] SUCCESS", ddlPlanId));
    }

    public void updateRebalanceScheduleState(long jobId, DdlPlanState ddlPlanState, String result) {
        new DdlPlanAccessorDelegate<Boolean>() {
            @Override
            protected Boolean invoke() {
                return ddlPlanAccessor.updateStateByJobId(
                    ddlPlanState,
                    result,
                    jobId
                );
            }
        }.execute();
        LOGGER.info(String.format("update schedule by job_id:[%s] %s", jobId, ddlPlanState));
    }

    /*****************************************************************************************/

    private boolean needRetry(int retryCount, Date gmtModified) {
        if (retryCount == 0) {
            return true;
        }
        int minutes;
        switch (retryCount) {
        case 1:
            minutes = 5;
            break;
        case 2:
            minutes = 15;
            break;
        case 3:
            minutes = 30;
            break;
        case 4:
            minutes = 60;
            break;
        case 5:
            minutes = 120;
            break;
        case 6:
            minutes = 180;
            break;
        default:
            minutes = 360;
            break;
        }
        LocalDateTime lastModified = LocalDateTime.ofInstant(gmtModified.toInstant(), ZoneId.systemDefault());
        if (lastModified.plusMinutes(minutes).isBefore(LocalDateTime.now())) {
            return true;
        }
        return false;
    }

    /**
     * left: finished Rows
     * right: total Rows
     */
    private Pair<Long, Long> getBackFillCount(long jobId, boolean archive) {
        long successRowCount = 0L;
        long totalRowCount = 0L;

        List<DdlEngineTaskRecord> allTasks = archive ?
            ddlJobManager.fetchAllSuccessiveTaskByJobIdInArchive(jobId) :
            ddlJobManager.fetchAllSuccessiveTaskByJobId(jobId);
        List<DdlEngineTaskRecord> rootDdlJobTaskList =
            allTasks.stream().filter(e -> e.getJobId() == jobId).collect(Collectors.toList());
        //1. calculate total row count
        for (DdlEngineTaskRecord record : rootDdlJobTaskList) {
            if (StringUtils.isEmpty(record.getCost())) {
                continue;
            }
            CostEstimableDdlTask.CostInfo costInfo = TaskHelper.decodeCostInfo(record.getCost());
            totalRowCount += costInfo.rows;
        }

        //2. calculate finished row count
        List<DdlEngineTaskRecord> backFillTaskList =
            allTasks.stream().filter(e -> StringUtils.containsIgnoreCase(e.getName(), "BackFill"))
                .collect(Collectors.toList());
        List<Long> backFillIdList = backFillTaskList.stream().map(e -> e.taskId).collect(Collectors.toList());
        List<GsiBackfillManager.BackFillAggInfo> backFillAggInfoList =
            gsiBackfillManager.queryBackFillAggInfoById(backFillIdList);

        for (GsiBackfillManager.BackFillAggInfo aggInfo : backFillAggInfoList) {
            successRowCount += aggInfo.getSuccessRowCount();
        }

        return Pair.of(successRowCount, totalRowCount);
    }

}