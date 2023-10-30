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

package com.alibaba.polardbx.repo.mysql.handler.ddl.newengine;

import com.alibaba.polardbx.common.ddl.newengine.DdlConstants;
import com.alibaba.polardbx.common.ddl.newengine.DdlPlanState;
import com.alibaba.polardbx.common.ddl.newengine.DdlState;
import com.alibaba.polardbx.common.ddl.newengine.DdlTaskState;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowCursor;
import com.alibaba.polardbx.executor.ddl.job.task.basic.SubJobTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.spec.AlterTableRollbacker;
import com.alibaba.polardbx.executor.ddl.newengine.DdlEngineRequester;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlExceptionAction;
import com.alibaba.polardbx.executor.ddl.newengine.meta.DdlEngineAccessorDelegate;
import com.alibaba.polardbx.executor.ddl.newengine.utils.DdlHelper;
import com.alibaba.polardbx.executor.partitionmanagement.rebalance.RebalanceDdlPlanManager;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineRecord;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineTaskRecord;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalDal;
import org.apache.calcite.sql.SqlCancelDdlJob;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.common.ddl.newengine.DdlPlanState.SUCCESS;
import static com.alibaba.polardbx.common.ddl.newengine.DdlPlanState.TERMINATED;
import static com.alibaba.polardbx.common.ddl.newengine.DdlType.ALTER_TABLEGROUP;
import static com.alibaba.polardbx.common.ddl.newengine.DdlType.MOVE_DATABASE;
import static com.alibaba.polardbx.common.ddl.newengine.DdlType.REBALANCE;

public class DdlEngineCancelJobsHandler extends DdlEngineJobsHandler {

    public DdlEngineCancelJobsHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor doHandle(final LogicalDal logicalPlan, ExecutionContext executionContext) {
        SqlCancelDdlJob command = (SqlCancelDdlJob) logicalPlan.getNativeSqlNode();
        return doCancel(command.isAll(), command.getJobIds(), executionContext);
    }

    public Cursor doCancel(boolean isAll, List<Long> jobIds, ExecutionContext executionContext) {
        boolean enableOperateSubJob =
            executionContext.getParamManager().getBoolean(ConnectionParams.ENABLE_OPERATE_SUBJOB);
        boolean cancelSubJob =
            executionContext.getParamManager().getBoolean(ConnectionParams.CANCEL_SUBJOB);
        List<DdlEngineRecord> records =
            fetchRecords(executionContext.getSchemaName(), isAll, jobIds);
        for (DdlEngineRecord record : records) {
            if (REBALANCE.name().equalsIgnoreCase(record.ddlType)) {
                // update ddl plan state
                DdlPlanState afterState;
                if (record.ddlStmt.toLowerCase().contains("drain_node")) {
                    // fail
                    afterState = TERMINATED;
                } else {
                    // success
                    afterState = SUCCESS;
                }
                String message = String.format("update state:[%s] by rollback the rebalance ddl", afterState.name());
                RebalanceDdlPlanManager rebalanceDdlPlanManager = new RebalanceDdlPlanManager();
                rebalanceDdlPlanManager.updateRebalanceScheduleState(record.jobId, afterState, message);
            }
        }

        records.stream().forEach(record -> {
            DdlState state = DdlState.valueOf(record.state);

            if (!(state == DdlState.RUNNING || state == DdlState.PAUSED)) {
                String errMsg = String.format("Only RUNNING/PAUSED jobs can be cancelled, but job %s is in %s state. ",
                    record.jobId, record.state);
                if (StringUtils.equalsIgnoreCase(record.state, DdlState.ROLLBACK_PAUSED.name())) {
                    errMsg += String.format("You may want to try command: continue ddl %s", record.jobId);
                }
                throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC, errMsg);
            }

            if (!record.isSupportCancel() || !AlterTableRollbacker.checkIfRollbackable(record.ddlStmt)) {
                String detail = (AlterTableRollbacker.checkIfRollbackable(record.ddlStmt) ? "original DDL itself" :
                    "the DDL operations") + " cannot be rolled back";
                String errMsg = "Cancel/rollback is not supported for job %s because %s%s. Please try: continue ddl %s";

                if (state == DdlState.RUNNING) {
                    // Pause the job first.
                    DdlEnginePauseJobsHandler pauseJobsHandler = new DdlEnginePauseJobsHandler(repo);
                    pauseJobsHandler.doPause(isAll, jobIds, executionContext);
                    record.state = DdlState.PAUSED.name();
                }

                Optional<DdlEngineTaskRecord> phyDdlTaskRecord = checkIfAllShardsNotDone(record);

                if (phyDdlTaskRecord != null && record.isSupportCancel()) {
                    // There is no shard done, so we can roll the job back right now.
                    if (phyDdlTaskRecord.isPresent()) {
                        // The physical DDL task is still DIRTY, so we should set the task to READY for rollback.
                        new DdlEngineAccessorDelegate<Integer>() {
                            @Override
                            protected Integer invoke() {
                                phyDdlTaskRecord.get().state = DdlTaskState.READY.name();
                                phyDdlTaskRecord.get().exceptionAction = DdlExceptionAction.ROLLBACK.name();
                                return engineTaskAccessor.updateTask(phyDdlTaskRecord.get());
                            }
                        }.execute();
                    } else {
                        // The physical ddl task has been set already, so nothing to do.
                    }
                } else {
                    // Otherwise, there is at least one shard done, so raise an exception to explain it.
                    if (state == DdlState.RUNNING) {
                        throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                            String.format(errMsg, record.jobId, detail, ", so the DDL job has been paused instead",
                                record.jobId));
                    }
                    if (state == DdlState.PAUSED) {
                        throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                            String.format(errMsg, record.jobId, detail, "", record.jobId));
                    }
                }
            }
        });

        int countDone = 0;
        for (DdlEngineRecord record : records) {
            if (record.isSubJob() && !enableOperateSubJob) {
                throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR, "Operation on subjob is not allowed");
            }

            List<Long> rollbackJobs = new ArrayList<>();
            List<String> traceIds = new ArrayList<>();

            cancelJob(record, cancelSubJob, rollbackJobs, traceIds);

            countDone += rollbackJobs.size();
        }

        DdlHelper.waitToContinue(DdlConstants.MEDIAN_WAITING_TIME);
        DdlEngineRequester.notifyLeader(executionContext.getSchemaName(), jobIds);

        boolean asyncMode = executionContext.getParamManager().getBoolean(ConnectionParams.PURE_ASYNC_DDL_MODE);
        if (!asyncMode && CollectionUtils.isNotEmpty(records) && CollectionUtils.size(records) == 1) {
            DdlEngineRecord record = records.get(0);
            respond(record.schemaName, record.jobId, executionContext, false, true);
        }

        return new AffectRowCursor(new int[] {countDone});
    }

    private void cancelJob(DdlEngineRecord record, boolean subJob, List<Long> rollbackJobs, List<String> traceIds) {
        if (MOVE_DATABASE.name().equalsIgnoreCase(record.ddlType)
            || ALTER_TABLEGROUP.name().equalsIgnoreCase(record.ddlType)) {
            if (!record.isSupportCancel()) {
                return;
            }
        }
        if (DdlState.RUNNING == DdlState.valueOf(record.state)) {
            if (schedulerManager.tryUpdateDdlState(
                record.schemaName,
                record.jobId,
                DdlState.RUNNING,
                DdlState.ROLLBACK_RUNNING)) {

                rollbackJobs.add(record.jobId);
                traceIds.add(record.traceId);

                if (subJob) {
                    cancelSubJobs(record.jobId, rollbackJobs, traceIds);
                }

                DdlHelper.interruptJobs(record.schemaName, rollbackJobs);
                DdlHelper.killActivePhyDDLs(record.schemaName, traceIds);
            }
        } else if (DdlState.PAUSED == DdlState.valueOf(record.state)) {
            if (schedulerManager.tryUpdateDdlState(
                record.schemaName,
                record.jobId,
                DdlState.PAUSED,
                DdlState.ROLLBACK_RUNNING)) {

                rollbackJobs.add(record.jobId);

                DdlHelper.interruptJobs(record.schemaName, rollbackJobs);
            }
        }
    }

    private void cancelSubJobs(long jobId, List<Long> rollbackJobs, List<String> traceIds) {
        List<SubJobTask> subJobs = schedulerManager.fetchSubJobsRecursive(jobId, false);

        List<Long> subJobIds = GeneralUtil.emptyIfNull(subJobs)
            .stream().flatMap(x -> x.fetchAllSubJobs().stream()).collect(Collectors.toList());

        if (CollectionUtils.isEmpty(subJobIds)) {
            return;
        }

        List<DdlEngineRecord> records = schedulerManager.fetchRecords(subJobIds);

        for (DdlEngineRecord record : GeneralUtil.emptyIfNull(records)) {
            cancelJob(record, false, rollbackJobs, traceIds);
        }
    }

    private Optional<DdlEngineTaskRecord> checkIfAllShardsNotDone(DdlEngineRecord record) {
        boolean allShardsNotDone = true;

        List<DdlEngineTaskRecord> taskRecords = fetchTasks(record.jobId);

        Optional<DdlEngineTaskRecord> phyDdlTaskRecord = taskRecords.stream().filter(
            tr -> TStringUtil.containsIgnoreCase(tr.name, "PhyDdlTask") && DdlTaskState.DIRTY.name()
                .equalsIgnoreCase(tr.state)).findFirst();

        if (phyDdlTaskRecord.isPresent() && TStringUtil.isNotEmpty(phyDdlTaskRecord.get().extra)) {
            String[] shards = phyDdlTaskRecord.get().extra.split(DdlConstants.SEMICOLON);
            for (String shard : shards) {
                String[] flags = shard.split(DdlConstants.COLON);
                if (flags.length > 3) {
                    if (Boolean.valueOf(flags[2])) {
                        allShardsNotDone = false;
                        break;
                    }
                } else {
                    allShardsNotDone = false;
                    break;
                }
            }
        }

        return allShardsNotDone ? phyDdlTaskRecord : null;
    }

}
