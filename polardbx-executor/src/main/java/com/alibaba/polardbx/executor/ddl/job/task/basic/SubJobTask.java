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

package com.alibaba.polardbx.executor.ddl.job.task.basic;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.ddl.newengine.DdlState;
import com.alibaba.polardbx.common.ddl.newengine.DdlTaskState;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.ddl.job.MockDdlJob;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.CostEstimableDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.ddl.newengine.DdlEngineDagExecutor;
import com.alibaba.polardbx.executor.ddl.newengine.DdlEngineDagExecutorMap;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlExceptionAction;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.meta.DdlEngineSchedulerManager;
import com.alibaba.polardbx.executor.ddl.newengine.meta.DdlJobManager;
import com.alibaba.polardbx.executor.ddl.newengine.utils.DdlHelper;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.executor.utils.failpoint.FailPointKey;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineRecord;
import com.alibaba.polardbx.optimizer.context.DdlContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.alibaba.polardbx.common.ddl.newengine.DdlConstants.SUB_JOB_RETRY_ERRER_MESSAGE;
import static com.alibaba.polardbx.common.ddl.newengine.DdlConstants.TRANSIENT_SUB_JOB_ID;

/**
 * The delegator that execute a ddl statement as a subjob.
 * <p>
 * Sub job will be executed as a special ddl job, which
 * 1. Not visible via `show ddl` command, but could be shown via `show full ddl`
 * 2. Observe the same exception handling strategy as a normal job
 * 3. Could be recover/rollback through recover/rollback the parent job
 * 4. Is not rollback-able after executing completed
 *
 * @author moyi
 * @since 2021/11
 */
@TaskName(name = "SubJobTask")
@Getter
@Setter
public final class SubJobTask extends BaseDdlTask implements CostEstimableDdlTask {

    private String ddlStmt;
    /**
     * -1: TransientDdlJob
     * 0: not submitted yet
     * \d{19}: subJob id
     */
    private long subJobId;

    private String rollbackDdlStmt;
    /**
     * -1: TransientDdlJob
     * 0: not submitted yet
     * \d{19}: subJob id
     */
    private long rollbackSubJobId;

    private boolean parentAcquireResource;

    public SubJobTask(String schemaName, String ddlStmt, String rollbackDdlStmt) {
        this(schemaName, ddlStmt, 0, rollbackDdlStmt, 0);
    }

    @JSONCreator
    public SubJobTask(String schemaName,
                      String ddlStmt,
                      long subJobId,
                      String rollbackDdlStmt,
                      long rollbackSubJobId) {
        super(schemaName);
        this.ddlStmt = ddlStmt;
        this.rollbackDdlStmt = rollbackDdlStmt;
        this.subJobId = subJobId;
        this.rollbackSubJobId = rollbackSubJobId;
        setExceptionAction(DdlExceptionAction.ROLLBACK);
    }

    /**
     * 1. subJob == -1          skip
     * 2. subJob == 0           submit & execute
     * 3. subJob == \d{19}      stateTransfer & execute
     */
    @Override
    protected void beforeTransaction(ExecutionContext executionContext) {
        if (subJobId == TRANSIENT_SUB_JOB_ID) {
            LOGGER.info("subjob is transient, skip execution");
            return;
        }
        if (subJobId == 0) {
            submitSubJob(executionContext.getDdlContext(), executionContext.getParamManager());
        } else {
            // Has been submitted, should recover the job
            Pair<DdlState, Boolean> stateChange = stateTransfer(subJobId, DdlState.RECOVER_JOB_STATE_TRANSFER);
            if (stateChange == null) {
                LOGGER.error("Subjob is not exist during execution");
                throw DdlHelper.logAndThrowError(LOGGER, String.format("Subjob %d not exist", subJobId));
            } else if (!stateChange.getValue()) {
                throw DdlHelper.logAndThrowError(LOGGER, String.format("Recover subjob %d failed", subJobId));
            }
        }

        DdlContext subJobDdlContext = executeSubJob(subJobId);

        if (subJobDdlContext.getState() == DdlState.COMPLETED) {
            LOGGER.info(String.format("Execute subjob %d success: %s", subJobId, subJobDdlContext.getDdlStmt()));
            return;
        } else {
            if (subJobDdlContext.getState() == DdlState.RUNNING) {
                DdlEngineSchedulerManager scheduler = new DdlEngineSchedulerManager();
                List<DdlEngineRecord> records =
                    scheduler.fetchRecords(Collections.singletonList(subJobId));

                if (!CollectionUtils.isEmpty(records)) {
                    DdlEngineRecord record = records.get(0);
                    if (DdlState.PAUSED.name().equalsIgnoreCase(record.state)
                        || DdlState.ROLLBACK_TO_READY.name().equalsIgnoreCase(record.state)) {
                        this.setExceptionAction(DdlExceptionAction.PAUSE);
                    }
                    // skip subjob
                    if (DdlState.ROLLBACK_TO_READY.name().equalsIgnoreCase(record.state) && record.isSkipSubjob()) {
                        LOGGER.info(String.format("subjob is skipped, SUBJOB:%s", record.jobId));
                        return;
                    }
                }
            }
            final String errMsg = subJobDdlContext.getErrorMessage();
            if (errMsg != null) {
                throw DdlHelper.logAndThrowError(LOGGER, errMsg);
            } else {
                throw DdlHelper.logAndThrowError(LOGGER,
                    String.format("Execute subjob %d failed with state %s", subJobDdlContext.getJobId(),
                        subJobDdlContext.getState()));
            }
        }
    }

    /**
     * 1. subJob == -1                  skip
     * 2. subJob == 0                   skip
     * 3. subJob executing              stateTransfer & rollback subJob
     * 4. subJob finished               submit rollbackStmt & execute rollbackStmt
     * 5. rollbackSubJob == -1          skip
     * 6. rollbackSubJob executing      stateTransfer & continue rollback rollbackSubJob
     */
    @Override
    protected void beforeRollbackTransaction(ExecutionContext executionContext) {
        if (subJobId == TRANSIENT_SUB_JOB_ID) {
            LOGGER.info("Subjob is transient, skip rollback");
            return;
        }
        if (subJobId == 0) {
            LOGGER.info("Subjob is not exist, skip rollback");
            return;
        }

        // Try to rollback the subjob
        Pair<DdlState, Boolean> stateChange = stateTransfer(subJobId, DdlState.ROLLBACK_JOB_STATE_TRANSFER);
        if (stateChange == null) {
            LOGGER.error("Subjob is not exist during rollback");
            throw DdlHelper.logAndThrowError(LOGGER, String.format("Subjob %s not exists", subJobId));
        } else if (stateChange.getValue()) {
            // Rollback the unfinished sub job
            DdlEngineDagExecutor ddlEngineDagExecutor = DdlEngineDagExecutorMap.get(schemaName, subJobId);
            if (ddlEngineDagExecutor != null) {
                ddlEngineDagExecutor.interrupt();
            }
            DdlContext subJobDdlContext = executeSubJob(subJobId);

            if (subJobDdlContext.getState() == DdlState.ROLLBACK_COMPLETED) {
                LOGGER.info(String.format("Rollback subjob %d success: %s", subJobId, subJobDdlContext.getDdlStmt()));
                return;
            } else {
                throw DdlHelper.logAndThrowError(LOGGER,
                    String.format("Rollback subjob %d failed with state %s", subJobId, subJobDdlContext.getState()));
            }

        } else if (DdlState.COMPLETED == (stateChange.getKey())) {
            // Subjob already finished, we could ignore it, or submit another job to rollback.
            if (StringUtils.isEmpty(rollbackDdlStmt)) {
                LOGGER.info(String.format("Subjob %d already completed, needn't rollback: %s", subJobId, state));
                return;
            } else {
                LOGGER.info(String.format("Subjob %d already completed, submit a reversed job to rollback: %s",
                    subJobId, rollbackDdlStmt));
                createReversedSubJob(executionContext.getDdlContext(), executionContext.getParamManager());
                return;
            }
        } else if (DdlState.ROLLBACK_COMPLETED == (stateChange.getKey())) {
            // Subjob already rollbacked, ignore it
        } else {
            throw DdlHelper.logAndThrowError(LOGGER, String.format("Subjob %d in state %s, could not rollback",
                subJobId, stateChange.getKey()));
        }
    }

    //todo submitSubJob可以和submitRollbackSubJob合并
    private void submitSubJob(DdlContext ddlContext, ParamManager paramManager) {
        if (FailPoint.isKeyEnable(FailPointKey.FP_HIJACK_DDL_JOB)
            && org.apache.commons.lang3.StringUtils.equalsIgnoreCase(ddlStmt, FailPointKey.FP_INJECT_SUBJOB)) {
            submitMockSubJob(ddlContext);
        }
        int count = 0;
        while (!ddlContext.isInterrupted() && !subJobSubmitted()) {
            try {
                subJobId = DdlHelper.getServerConfigManager()
                    .submitSubDDL(schemaName, ddlContext, getJobId(), getTaskId(), false, ddlStmt, paramManager);
                setState(DdlTaskState.DIRTY);
                LOGGER.info(String.format("Create subjob %d", subJobId));
                if (subJobId == 0L) {
                    throw new TddlNestableRuntimeException("submit subjob error");
                }
            } catch (Exception e) {
                if (StringUtils.containsIgnoreCase(e.getMessage(), SUB_JOB_RETRY_ERRER_MESSAGE)) {
                    LOGGER.warn(String.format("submit subjob error, retry %d times", count++), e);
                    continue;
                }
                throw e;
            }
        }
    }

    private void submitRollbackSubJob(DdlContext ddlContext, ParamManager paramManager) {
        if (FailPoint.isKeyEnable(FailPointKey.FP_HIJACK_DDL_JOB)
            && org.apache.commons.lang3.StringUtils.equalsIgnoreCase(rollbackDdlStmt, FailPointKey.FP_INJECT_SUBJOB)) {
            submitMockSubJob(ddlContext);
        }
        int count = 0;
        while (!ddlContext.isInterrupted() && !rollbackSubJobSubmitted()) {
            try {
                rollbackSubJobId = DdlHelper.getServerConfigManager()
                    .submitSubDDL(schemaName, ddlContext, getJobId(), getTaskId(), true, rollbackDdlStmt, paramManager);
                LOGGER.info(String.format("Create rollback subjob %d", rollbackSubJobId));
                if (rollbackSubJobId == 0L) {
                    throw new TddlNestableRuntimeException("submit rollback subjob error");
                }
            } catch (Exception e) {
                if (StringUtils.containsIgnoreCase(e.getMessage(), SUB_JOB_RETRY_ERRER_MESSAGE)) {
                    LOGGER.warn(String.format("submit rollback subjob error, retry %d times", count++), e);
                    continue;
                }
                throw e;
            }
        }
    }

    private boolean submitMockSubJob(DdlContext ddlContext) {
        if (FailPoint.isKeyEnable(FailPointKey.FP_HIJACK_DDL_JOB)
            && org.apache.commons.lang3.StringUtils.equalsIgnoreCase(ddlStmt, FailPointKey.FP_INJECT_SUBJOB)) {
            DdlJob mockSubJob = new MockDdlJob(5, 5, 30, false).create();
            subJobId = new DdlJobManager().storeSubJob(this, mockSubJob, ddlContext, false);
            LOGGER.info(String.format("Create mock subjob %d for task %d/%d", subJobId, getJobId(), getTaskId()));
            return true;
        }
        return false;
    }

    /**
     * @return DdlState
     */
    private DdlContext executeSubJob(long subJobId) {
        if (subJobId == 0L) {
            throw new TddlNestableRuntimeException("SubJob not submitted yet");
        }
        if (subJobId == TRANSIENT_SUB_JOB_ID) {
            DdlContext transientDdlContext = new DdlContext();
            transientDdlContext.unSafeSetDdlState(DdlState.COMPLETED);
            return transientDdlContext;
        }
        DdlEngineRecord subJobRecord = new DdlJobManager().fetchRecordByJobId(subJobId);
        if (subJobRecord == null || !subJobRecord.isSubJob()) {
            throw new TddlNestableRuntimeException(String.format("SubJob %s doesn't exist", subJobId));
        }
        return DdlHelper.getServerConfigManager().restoreDDL(subJobRecord.schemaName, subJobRecord.jobId);
    }

    // TODO: fetch and cas should be in a transaction
    private Pair<DdlState, Boolean> stateTransfer(long jobId, Map<DdlState, DdlState> stateMap) {
        DdlEngineSchedulerManager scheduler = new DdlEngineSchedulerManager();
        List<DdlEngineRecord> records = scheduler.fetchRecords(Arrays.asList(jobId));
        if (CollectionUtils.isEmpty(records)) {
            return null;
        }
        DdlEngineRecord record = records.get(0);

        DdlState before = DdlState.valueOf(record.state);
        DdlState after = stateMap.get(before);
        // State transfer not exists, consider it as an exception
        if (after == null) {
            return Pair.of(before, false);
        }
        // State already be transferred
        if (after == before) {
            return Pair.of(before, true);
        }
        return Pair.of(before, scheduler.tryUpdateDdlState(record.schemaName, record.jobId, before, after));
    }

    /**
     * Create a reversed job to rollback:
     * Eg. For move database a to b, the reversed job is move database b to a.
     */
    private void createReversedSubJob(DdlContext ddlContext, ParamManager paramManager) {
        if (rollbackSubJobId == TRANSIENT_SUB_JOB_ID) {
            LOGGER.info("rollbackSubjob is transient, skip rollback");
            return;
        }
        if (rollbackSubJobId == 0) {
            // Create a new job if not exists
            submitRollbackSubJob(ddlContext, paramManager);
        } else {
            // Try to recover existed job
            Pair<DdlState, Boolean> stateChange = stateTransfer(rollbackSubJobId, DdlState.RECOVER_JOB_STATE_TRANSFER);
            if (stateChange == null) {
                throw DdlHelper.logAndThrowError(LOGGER, String.format("Subjob %d not exist", rollbackSubJobId));
            } else if (!stateChange.getValue()) {
                throw DdlHelper.logAndThrowError(LOGGER, String.format("Recover subjob %d failed", rollbackSubJobId));
            }
        }

        DdlContext subJobDdlContext = executeSubJob(rollbackSubJobId);
        if (subJobDdlContext.getState() == DdlState.COMPLETED) {
            LOGGER.info(String.format("Subjob %d completed", rollbackSubJobId));
        } else {
            throw DdlHelper.logAndThrowError(LOGGER, String.format("Subjob %d failed with state %s",
                rollbackSubJobId, subJobDdlContext.getState()));
        }
    }

    private ExecutionContext copyExecutionContextForSubJob(ExecutionContext context) {
        DdlContext copiedDdlContext = context.getDdlContext().copy();
        ExecutionContext copiedExecutionContext = context.copy();
        copiedExecutionContext.setDdlContext(copiedDdlContext);
        return copiedExecutionContext;
    }

    @Override
    public String getDescription() {
        return ddlStmt;
    }

    @Override
    public String remark() {
        String costInfoStr = "";
        if (costInfo != null) {
            costInfoStr = String.format("|estimated rows:%s, estimated size:%s", costInfo.rows, costInfo.dataSize);
        }
        return String.format(
            "|subJobId:%s, rollbackSubJobId:%s, ddlStmt:%s, rollbackDdlStmt:%s%s",
            subJobId,
            rollbackSubJobId,
            ddlStmt,
            rollbackDdlStmt,
            costInfoStr
        );
    }

    public List<Long> fetchAllSubJobs() {
        List<Long> res = new ArrayList<>();
        if (subJobId != 0 && subJobId != TRANSIENT_SUB_JOB_ID) {
            res.add(subJobId);
        }
        if (rollbackSubJobId != 0 && rollbackSubJobId != TRANSIENT_SUB_JOB_ID) {
            res.add(rollbackSubJobId);
        }
        return res;
    }

    public static String getTaskName() {
        return "SubJobTask";
    }

    public boolean subJobSubmitted() {
        return subJobId == TRANSIENT_SUB_JOB_ID || subJobId > 0L;
    }

    public boolean rollbackSubJobSubmitted() {
        return rollbackSubJobId == TRANSIENT_SUB_JOB_ID || rollbackSubJobId > 0L;
    }

    private transient volatile CostInfo costInfo;

    @Override
    public void setCostInfo(CostInfo costInfo) {
        this.costInfo = costInfo;
    }

    @Override
    public CostInfo getCostInfo() {
        return costInfo;
    }

    @Override
    public List<String> explainInfo() {
        return new ArrayList<>();

    }
}
