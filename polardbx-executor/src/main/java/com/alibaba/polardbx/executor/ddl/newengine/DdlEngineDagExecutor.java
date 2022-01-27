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
import com.alibaba.polardbx.common.ddl.newengine.DdlConstants;
import com.alibaba.polardbx.common.ddl.newengine.DdlState;
import com.alibaba.polardbx.common.ddl.newengine.DdlTaskState;
import com.alibaba.polardbx.common.eventlogger.EventLogger;
import com.alibaba.polardbx.common.eventlogger.EventType;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.thread.ServerThreadPool;
import com.alibaba.polardbx.common.utils.timezone.InternalTimeZone;
import com.alibaba.polardbx.common.utils.timezone.TimeZoneUtils;
import com.alibaba.polardbx.executor.ddl.newengine.dag.TaskScheduler;
import com.alibaba.polardbx.executor.ddl.newengine.job.AbstractDdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.meta.DdlJobManager;
import com.alibaba.polardbx.executor.ddl.newengine.sync.DdlResponse;
import com.alibaba.polardbx.executor.ddl.newengine.sync.DdlResponse.Response;
import com.alibaba.polardbx.executor.ddl.newengine.sync.DdlResponseSyncAction;
import com.alibaba.polardbx.executor.ddl.newengine.utils.DdlHelper;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineRecord;
import com.alibaba.polardbx.gms.sync.GmsSyncManagerHelper;
import com.alibaba.polardbx.optimizer.context.DdlContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.statis.SQLRecord;
import com.alibaba.polardbx.optimizer.statis.SQLTracer;
import com.alibaba.polardbx.statistics.ExecuteSQLOperation;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.alibaba.polardbx.executor.ddl.newengine.DdlEngineDagExecutorMap.DdlJobResult;
import static com.alibaba.polardbx.executor.ddl.newengine.sync.DdlResponse.ResponseType;
import static com.alibaba.polardbx.executor.utils.failpoint.FailPointKey.FP_DDL_INTERNAL_MAX_PARALLELISM;
import static com.alibaba.polardbx.executor.utils.failpoint.FailPointKey.FP_DDL_RESTORE_JOB_SUSPEND;
import static com.alibaba.polardbx.executor.utils.failpoint.FailPointKey.FP_DDL_TASK_SUSPEND_WHEN_FAILED;
import static com.alibaba.polardbx.executor.utils.failpoint.FailPointKey.FP_EACH_DDL_TASK_BACK_AND_FORTH;
import static com.alibaba.polardbx.executor.utils.failpoint.FailPointKey.FP_EACH_DDL_TASK_EXECUTE_TWICE;
import static com.alibaba.polardbx.executor.utils.failpoint.FailPointKey.FP_PAUSE_AFTER_DDL_TASK_EXECUTION;
import static com.alibaba.polardbx.executor.utils.failpoint.FailPointKey.FP_ROLLBACK_AFTER_DDL_TASK_EXECUTION;

/**
 * execute ddl tasks
 * will obey the DAG dependency
 * <p>
 * DdlState transition in this class, In parentheses are final states:
 * * QUEUED -> RUNNING -> (COMPLETE)
 * *             \------> (PAUSED)
 * *             \------> ROLLBACK_RUNNING -> (ROLLBACK_COMPLETED)
 * *                             \----------> (ROLLBACK_PAUSED)
 * DdlTasks can check ddlContext.isInterrupted() to know whether they should terminate
 *
 * @author guxu
 */
public class DdlEngineDagExecutor {

    private static final Logger LOGGER = SQLRecorderLogger.ddlEngineLogger;
    private static final Logger ROOT_LOGGER = LoggerFactory.getLogger(DdlEngineDagExecutor.class);

    private static final String ERROR_MSG = "Failed to execute the DDL task. Caused by: %s";
    private String errorMessage;

    private final ExecutionContext executionContext;

    private final DdlJob ddlJob;
    private final DdlContext ddlContext;
    private final DdlJobManager ddlJobManager = new DdlJobManager();

    private final ServerThreadPool executor;
    private final ConcurrentLinkedQueue<Future> futures;
    private final ConcurrentLinkedQueue<TaskExecutionInfo> taskExecutionQueue = new ConcurrentLinkedQueue<>();
    private final TaskScheduler taskScheduler;
    private final AtomicReference<TaskScheduler> reveredTaskSchedulerReference = new AtomicReference<>();
    /**
     * max parallelism for DAG scheduler.
     */
    private int maxParallelism = 1;
    private final Semaphore semaphore;
    /**
     * DdlState might be changed by:
     * 1. DDL commands like 'pause ddl ...' 、 'cancel ddl ...' and so on.
     * 2. directly update ddl_engine table in MetaDB.（shouldn't happen, but have to take into consideration）
     * <p>
     * once DdlState inconsistency detected, simply shutdown current execution gracefully.
     * a new execution may restart later if DdlState is 'RUNNING' or 'ROLLBACK_RUNNING'.
     */
    private AtomicReference<Optional<DdlState>> stateInconsistencyDetector = new AtomicReference<>();
    private final boolean interruptWhileLosingLeader;
    private final LocalDateTime beginTs = LocalDateTime.now();
    private LocalDateTime lastRecordTs;

    DdlEngineDagExecutor(Long jobId, ExecutionContext executionContext) {
        FailPoint.injectSuspendFromHint(FP_DDL_RESTORE_JOB_SUSPEND, executionContext);
        final Pair<DdlJob, DdlContext> ddlJobDdlContextPair = ddlJobManager.restoreJob(jobId);
        this.ddlJob = ddlJobDdlContextPair.getKey();
        this.ddlContext = ddlJobDdlContextPair.getValue();
        if (DdlState.TERMINATED.contains(ddlContext.getState())) {
            throw new TddlNestableRuntimeException(String.format(
                "DDL JOB is not runnable. jobId:[%s], state:[%s]",
                jobId, ddlContext.getState().name()));
        }
        prepareExecutionContext(executionContext, ddlContext);
        this.executionContext = executionContext;
        interruptWhileLosingLeader =
            executionContext.getParamManager().getBoolean(ConnectionParams.INTERRUPT_DDL_WHILE_LOSING_LEADER);
        this.executor = executionContext.getExecutorService();
        this.futures = new ConcurrentLinkedQueue<>();
        this.taskScheduler = this.ddlJob.createTaskScheduler();
        this.maxParallelism = this.ddlJob.getMaxParallelism();
        FailPoint.injectFromHint(FP_DDL_INTERNAL_MAX_PARALLELISM, executionContext, (k, v) -> {
            try {
                maxParallelism = Integer.parseInt(v);
            } catch (Throwable e) {
            }
        });
        this.semaphore = new Semaphore(maxParallelism);
    }

    /**
     * 在同一个进程内，能够确保同一个JOB不会被并发调度，然后并发执行1次以上
     */
    public static void restoreAndRun(String schemaName, Long jobId, ExecutionContext executionContext) {
        if (jobId == null) {
            throw DdlHelper.logAndThrowError(LOGGER, "The DDL record must not be null");
        }
        // Restore Job Information from GMS
        boolean restoreSuccess = DdlEngineDagExecutorMap.restore(schemaName, jobId, executionContext);
        if (!restoreSuccess) {
            return;
        }
        try {
            DdlEngineDagExecutorMap.get(schemaName, jobId).run();
        } catch (Throwable t) {
            DdlHelper.errorLogDual(LOGGER, ROOT_LOGGER, t);
            EventLogger.log(EventType.DDL_WARN, String.format(
                "run DDL JOB error. errMessage:%s",
                t.getMessage()
            ));
            throw t;
        } finally {
            DdlEngineDagExecutorMap.get(schemaName, jobId).interrupt();
            DdlEngineDagExecutorMap.remove(schemaName, jobId);
        }
    }

    private void run() {
        try {
            LOGGER.info(String.format("start run() DDL JOB: [%s]", ddlContext.getJobId()));
            // Start the job state machine.
            if (ddlContext.getState() == DdlState.QUEUED) {
                onQueued();
            }
            if (ddlContext.getState() == DdlState.RUNNING) {
                onRunning();
            }
            if (ddlContext.getState() == DdlState.ROLLBACK_RUNNING) {
                onRollingBack();
            }

            // Handle the terminated states.
            switch (ddlContext.getState()) {
            case ROLLBACK_PAUSED:
            case PAUSED:
                onTerminated();
                break;
            case ROLLBACK_COMPLETED:
            case COMPLETED:
                onFinished();
                break;
            default:
                break;
            }
        }finally {
            LocalDateTime endTs = LocalDateTime.now();
            DdlHelper.errorLogDual(LOGGER, ROOT_LOGGER, String.format(
                "DDL JOB:[%s] exit with state: %s. beginTs:%s, endTs:%s, cost:%sms",
                ddlContext.getJobId(),
                ddlContext.getState().name(),
                beginTs,
                endTs,
                endTs.toInstant(ZoneOffset.ofHours(8)).toEpochMilli() - beginTs.toInstant(ZoneOffset.ofHours(8)).toEpochMilli()
            ));
        }
    }

    private void onQueued() {
        if(hasFailureOnState(DdlState.QUEUED)){
            return;
        }
        // Start performing the job.
        updateDdlState(DdlState.QUEUED, DdlState.RUNNING);
    }

    private void onRunning() {
        LOGGER.info(String.format(
            "onRunning DDL [%s] JobId:[%s] Task graph:\n%s\n",
            TStringUtil.quoteString(ddlContext.getDdlStmt()),
            ddlContext.getJobId(),
            ddlJob.visualizeTasks()));
        final TaskScheduler executingTaskScheduler = taskScheduler;
        // assert: must have some task we can execute
        FailPoint.assertTrue(() ->
            executingTaskScheduler.hasMoreExecutable() && !executingTaskScheduler.isAllTaskDone());
        // Execute the tasks.
        while (true) {
            if (hasFailureOnState(DdlState.RUNNING)) {
                if (waitForAllTasksToStop(50L, TimeUnit.MILLISECONDS)) {
                    LOGGER.info(String.format("JobId:[%s], all tasks stopped", ddlContext.getJobId()));
                    return;
                } else {
                    continue;
                }
            }
            if (executingTaskScheduler.isAllTaskDone()) {
                updateDdlState(DdlState.RUNNING, DdlState.COMPLETED);
                return;
            }
            if (executingTaskScheduler.hasMoreExecutable()) {
                // fetch & execute next batch
                submitDdlTask(executingTaskScheduler.pollBatch(), true, executingTaskScheduler);
                continue;
            }
            //get some rest
            sleep(50L);
        }
    }

    private void onRollingBack() {
        if(!allowRollback()){
            updateDdlState(DdlState.ROLLBACK_RUNNING, DdlState.ROLLBACK_PAUSED);
            return;
        }
        // Load tasks reversely if needed.
        reverseTaskDagForRollback();

        final TaskScheduler reveredTaskScheduler = reveredTaskSchedulerReference.get();
        ddlContext.setInterruptedAsFalse();

        // Rollback the tasks.
        while (true) {
            if (hasFailureOnState(DdlState.ROLLBACK_RUNNING)) {
                if (waitForAllTasksToStop(50L, TimeUnit.MILLISECONDS)) {
                    LOGGER.info(String.format("JobId:[%s], all tasks stoped", ddlContext.getJobId()));
                    return;
                } else {
                    continue;
                }
            }
            if (reveredTaskScheduler.isAllTaskDone()) {
                updateDdlState(DdlState.ROLLBACK_RUNNING, DdlState.ROLLBACK_COMPLETED);
                return;
            }
            if (reveredTaskScheduler.hasMoreExecutable()) {
                // fetch & execute next batch
                submitDdlTask(reveredTaskScheduler.pollBatch(), false, reveredTaskScheduler);
                continue;
            }
            //get some rest
            sleep(50L);
        }
    }

    /**
     * need to wait for all tasks to stop if the DDL JOB's state is changed
     * <p>
     * here's a question:
     * whether we should force executing tasks shutdown?
     * since executing tasks may block for a long time
     * the question is basically: should we trust DdlTask implementors?
     * <p>
     * In my opinion, we can follow the JSR-51
     * DdlTasks could check DdlState periodically (through ddlContext.isInterrupted())
     * see: https://docs.oracle.com/javase/1.5.0/docs/guide/misc/threadPrimitiveDeprecation.html
     */
    private boolean waitForAllTasksToStop(long timeout, TimeUnit unit) {
        try {
            ddlContext.setInterruptedAsTrue();

//            //interrupt all task threads
//            if (CollectionUtils.isNotEmpty(futures)) {
//                for (Future f : futures) {
//                    if (f.isCancelled() || f.isDone()) {
//                        continue;
//                    }
//                    f.cancel(true);
//                    System.err.println("cancel foobar");
//                    DdlHelper.errorLogDual(LOGGER, ROOT_LOGGER, "cancel foobar22222");
//                }
//            }

            //wait for all executing tasks to finish
            if (semaphore.tryAcquire(maxParallelism, timeout, unit)) {
                semaphore.release(maxParallelism);
                return true;
            }
            return false;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new TddlNestableRuntimeException(e);
        }
    }

    private void onTerminated() {
        LOGGER.warn(String.format("execute DDL JOB error, JobId: [%s], final DDL State:[%s], task graph:\n%s\n",
            ddlContext.getJobId(), ddlContext.getState(), ddlJob.visualizeTasks()));
        LOGGER.info(String.format("Task Execution Sequence:\n%s", genTaskExecutionSequence()));
        EventLogger.log(EventType.DDL_PAUSED, String.format(
            "execute DDL JOB error, schemaName:[%s], JobId:[%s], State:[%s], errorMessage:[%s]",
            ddlContext.getSchemaName(),
            ddlContext.getJobId(),
            ddlContext.getState().name(),
            errorMessage
        ));
        // Build a response for failure.
        Response response = buildResponse(ResponseType.ERROR, errorMessage);

        // Save the response as the DDL result for show.
        ddlJobManager.saveResult(ddlContext.getJobId(), response);

        // Save the result in memory as the last result.
        saveLastResult(response);

        // Respond to the worker.
        respond(response);
    }

    private void onFinished() {
        FailPoint.inject(() -> LOGGER.info(String.format(
            "finish executing/rollingBack DDL JOB, JobId: [%s], task graph:\n%s\n",
            ddlContext.getJobId(), ddlJob.visualizeTasks())));
        saveDagToTrace();
        // Build a response.
        Response response;
        if (ddlContext.getState() == DdlState.ROLLBACK_COMPLETED) {
            if (ddlContext.isUsingWarning()) {
                response = buildResponse(ResponseType.WARNING, "");
                response.setWarning(executionContext.getExtraDatas().get(ExecutionContext.FailedMessage));
            } else {
                // Still should report the original error.
                response = buildResponse(ResponseType.ERROR, errorMessage);
            }
        } else {
            response = buildResponse(ResponseType.SUCCESS, "SUCCESS");
        }

        response.setTracer(executionContext.getTracer());

        // Save the result in memory as the last result.
        saveLastResult(response);

        // Respond to the worker.
        respond(response);

        // Clean the job up.
        ddlJobManager.removeJob(ddlContext.getSchemaName(), ddlContext.getJobId());
    }

    private Response buildResponse(ResponseType type, String responseContent) {
        return new Response(
            ddlContext.getJobId(),
            ddlContext.getSchemaName(),
            ddlContext.getObjectName(),
            ddlContext.getDdlType().name(),
            type,
            responseContent
        );
    }

    private void saveLastResult(Response response) {
        DdlJobResult lastResult = new DdlJobResult();
        lastResult.jobId = ddlContext.getJobId();
        lastResult.schemaName = ddlContext.getSchemaName();
        lastResult.objectName = ddlContext.getObjectName();
        lastResult.ddlType = ddlContext.getDdlType().name();
        lastResult.response = response;
        DdlEngineDagExecutorMap.setLastDdlJobResult(lastResult.schemaName, lastResult);
    }

    private void saveDagToTrace() {
        if (executionContext.isEnableDdlTrace()) {
            ExecuteSQLOperation op = new ExecuteSQLOperation("",
                "",
                ddlJob.visualizeTasks(),
                System.nanoTime());
            op.setParams(new Parameters(null, false));
            executionContext.getTracer().trace(op);
        }
    }

    /**
     * execute/rollback tasks in parallel
     * notice: max parallelism is determined by 'maxParallelism' field
     *
     * @param executeElseRollback true means execute, false means rollback
     */
    private void submitDdlTask(List<DdlTask> taskList, boolean executeElseRollback, final TaskScheduler scheduler) {
        if (CollectionUtils.isEmpty(taskList)) {
            return;
        }

        for (DdlTask task : taskList) {
            try {
                semaphore.acquire();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new TddlNestableRuntimeException(e);
            }
            Future f = executor.submit(AsyncCallableTask.build(() -> {
                try {
                    //check the job state, it may has been changed by other failed task or user command
                    if (executeElseRollback && hasFailureOnState(DdlState.RUNNING)) {
                        return false;
                    } else if (!executeElseRollback && hasFailureOnState(DdlState.ROLLBACK_RUNNING)) {
                        return false;
                    }

                    // execute task
                    boolean isSuccess = executeElseRollback ?
                        executeTask(task) :
                        rollbackTask(task);
                    FailPoint.injectFromHint(FP_EACH_DDL_TASK_EXECUTE_TWICE, executionContext, (k, v) -> {
                        if (executeElseRollback) {
                            executeTask(task);
                        } else {
                            rollbackTask(task);
                        }
                    });
                    FailPoint.injectFromHint(FP_EACH_DDL_TASK_BACK_AND_FORTH, executionContext, (k, v) -> {
                        if (executeElseRollback && allowRollback()) {
                            rollbackTask(task);
                            ((AbstractDdlTask) task).updateTaskStateInNewTxn(DdlTaskState.READY);
                            executeTask(task);
                        }
                    });
                    if (isSuccess) {
                        // mark current task as done
                        scheduler.markAsDone(task);
                    } else {
                        scheduler.markAsFail(task);
                    }
                    return isSuccess;
                } catch (Throwable e) {
                    // may throw exception again in the catch clause of executeTask()/rollbackTask()
                    // typically while DdlState is inconsistent
                    scheduler.markAsFail(task);
                    return false;
                } finally {
                    semaphore.release();
                }
            }));
            futures.add(f);
        }
    }

    /**
     * execute a task
     */
    private boolean executeTask(DdlTask task) {
        int recoveryRetryTimes = 0;
        while (DdlTaskState.needToCallExecute(task.getState())) {
            try {
                if(hasFailureOnState(DdlState.RUNNING)){
                    return false;
                }
                task.execute(executionContext);
                recordTaskExecutionInfo(taskExecutionQueue, task, true, null);
            } catch (Throwable e) {
                recordTaskExecutionInfo(taskExecutionQueue, task, true, e);
                FailPoint.injectSuspend(FP_DDL_TASK_SUSPEND_WHEN_FAILED);
                //concurrent tasks may fail at the same time
                //only allow first task to set the DdlState
                if (hasFailureOnState(DdlState.RUNNING)) {
                    DdlHelper.errorLogDual(LOGGER, ROOT_LOGGER, String.format(
                        "concurrent failed execute task found. current state: [%s]. ",
                        ddlContext.getState()));
//                    errorMessage = errorMessage + ";" + String.format("Caused by: %s", e.getMessage());
                    DdlHelper.errorLogDual(LOGGER, ROOT_LOGGER, errorMessage, e);
                    return false;
                }
                switch (task.getExceptionAction()) {
                case TRY_RECOVERY_THEN_PAUSE:
                    String errMsg = String.format(ERROR_MSG, e.getMessage());
                    DdlHelper.errorLogDual(LOGGER, ROOT_LOGGER, errMsg, e);
                    if (++recoveryRetryTimes <= DdlConstants.RECOVER_MAX_RETRY_TIMES) {
                        DdlHelper.errorLogDual(LOGGER, ROOT_LOGGER,
                            String.format("fail to execute task:[%s], current recovery times:[%d/%d]. try again...",
                                task.getName(), recoveryRetryTimes, DdlConstants.RECOVER_MAX_RETRY_TIMES));
                        continue;
                    } else {
                        DdlHelper.errorLogDual(LOGGER, ROOT_LOGGER,
                            String.format("fail to execute task:[%s], try pause DDL JOB:[%s]", task.getName(),
                                ddlContext.getJobId()));
                        errorMessage = errMsg;
                        updateDdlState(DdlState.RUNNING, DdlState.PAUSED);
                        return false;
                    }
                case ROLLBACK:
                    errorMessage = String.format(ERROR_MSG, e.getMessage());
                    DdlHelper.errorLogDual(LOGGER, ROOT_LOGGER, errorMessage, e);
                    DdlHelper.errorLogDual(LOGGER, ROOT_LOGGER,
                        String.format("fail to execute task:[%s], try rollback DDL JOB:[%s]", task.getName(),
                            ddlContext.getJobId()));
                    if (!allowRollback()) {
                        updateDdlState(DdlState.RUNNING, DdlState.PAUSED);
                        return false;
                    }
                    updateDdlState(DdlState.RUNNING, DdlState.ROLLBACK_RUNNING);
                    return false;
                case TRY_RECOVERY_THEN_ROLLBACK:
                    errMsg = String.format(ERROR_MSG, e.getMessage());
                    DdlHelper.errorLogDual(LOGGER, ROOT_LOGGER, errMsg, e);
                    if (++recoveryRetryTimes <= DdlConstants.RECOVER_MAX_RETRY_TIMES) {
                        DdlHelper.errorLogDual(LOGGER, ROOT_LOGGER,
                            String.format("fail to execute task:[%s], current recovery times:[%d/%d]. try again...",
                                task.getName(), recoveryRetryTimes, DdlConstants.RECOVER_MAX_RETRY_TIMES));
                        continue;
                    } else {
                        DdlHelper.errorLogDual(LOGGER, ROOT_LOGGER,
                            String.format("fail to execute task:[%s], try rollback DDL JOB:[%s]", task.getName(),
                                ddlContext.getJobId()));
                        errorMessage = errMsg;
                        if (!allowRollback()) {
                            updateDdlState(DdlState.RUNNING, DdlState.PAUSED);
                            return false;
                        }
                        updateDdlState(DdlState.RUNNING, DdlState.ROLLBACK_RUNNING);
                        return false;
                    }
                case PAUSE:
                default:
                    errorMessage = String.format(ERROR_MSG, e.getMessage());
                    DdlHelper.errorLogDual(LOGGER, ROOT_LOGGER, errorMessage, e);
                    DdlHelper.errorLogDual(LOGGER, ROOT_LOGGER,
                        String.format("fail to execute task:[%s], try pause DDL JOB:[%s]", task.getName(),
                            ddlContext.getJobId()));
                    updateDdlState(DdlState.RUNNING, DdlState.PAUSED);
                    return false;
                }
            } finally {
                FailPoint.injectFromHint(FP_ROLLBACK_AFTER_DDL_TASK_EXECUTION, executionContext, (k, v) -> {
                    if (task.getState() == DdlTaskState.SUCCESS && StringUtils.equalsIgnoreCase(task.getName(), v)) {
                        updateDdlState(DdlState.RUNNING, DdlState.ROLLBACK_RUNNING);
                    }
                });
                FailPoint.injectFromHint(FP_PAUSE_AFTER_DDL_TASK_EXECUTION, executionContext, (k, v) -> {
                    if (task.getState() == DdlTaskState.SUCCESS && StringUtils.equalsIgnoreCase(task.getName(), v)) {
                        updateDdlState(DdlState.RUNNING, DdlState.PAUSED);
                    }
                });
            }
        }
        return DdlTaskState.success(task.getState());
    }

    /**
     * rollback a task
     */
    private boolean rollbackTask(DdlTask task) {
        int rollbackRetryTimes = 0;

        // Start rolling back the job's tasks one by one reversely.
        while (DdlTaskState.needToCallRollback(task.getState())) {
            try {
                if(hasFailureOnState(DdlState.ROLLBACK_RUNNING)){
                    return false;
                }
                task.rollback(executionContext);
                recordTaskExecutionInfo(taskExecutionQueue, task, false, null);
            } catch (Throwable e) {
                recordTaskExecutionInfo(taskExecutionQueue, task, false, null);
                FailPoint.injectSuspend(FP_DDL_TASK_SUSPEND_WHEN_FAILED);
                //concurrent tasks may fail at the same time
                //only allow first task to set the DdlState
                if (hasFailureOnState(DdlState.ROLLBACK_RUNNING)) {
                    DdlHelper.errorLogDual(LOGGER, ROOT_LOGGER, String.format("concurrent failed rollback task found. "
                        + "current state: [%s]. ", ddlContext.getState()));
//                    errorMessage = errorMessage + ";" + String.format("Caused by: %s", e.getMessage());
                    DdlHelper.errorLogDual(LOGGER, ROOT_LOGGER, errorMessage, e);
                    return false;
                }
                String errMsg = String.format(ERROR_MSG, e.getMessage());
                DdlHelper.errorLogDual(LOGGER, ROOT_LOGGER, errMsg, e);
                if (++rollbackRetryTimes <= DdlConstants.ROLLBACK_MAX_RETRY_TIMES) {
                    DdlHelper.errorLogDual(LOGGER, ROOT_LOGGER,
                        String.format("fail to rollback task:[%s], current retry times:[%d/%d]. try again...",
                            task.getName(), rollbackRetryTimes, DdlConstants.ROLLBACK_MAX_RETRY_TIMES));
                    continue;
                } else {
                    DdlHelper.errorLogDual(LOGGER, ROOT_LOGGER, String.format(
                        "fail to rollback taskName:[%s], taskId:[%s], jobId:[%s]",
                        task.getName(), task.getTaskId(), task.getJobId()));
                    errorMessage = errMsg;
                    updateDdlState(DdlState.ROLLBACK_RUNNING, DdlState.ROLLBACK_PAUSED);
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * check current DdlEngineDagExecutor is alive
     * exceptions in onRunning()/onRollingBack() may cause DdlEngineDagExecutor dead.   e.g. OOM
     */
    private boolean deamonThreadAlive(){
        DdlEngineDagExecutor dagExecutor = DdlEngineDagExecutorMap.get(ddlContext.getSchemaName(), ddlContext.getJobId());
        if(dagExecutor == this){
            return true;
        }
        return false;
    }

    private void respond(Response response) {
        // Get local server key.
        String localServerKey = DdlHelper.getLocalServerKey();

        // Build a DDL response.
        DdlResponse ddlResponse = new DdlResponse();
        ddlResponse.addResponse(ddlContext.getJobId(), response);

        if (TStringUtil.equals(ddlContext.getResponseNode(), localServerKey)) {
            // Respond to current server, so no sync is needed.
            DdlEngineRequester.addResponses(ddlResponse.getResponses());
        } else {
            try {
                // Respond to the worker via sync.
                DdlResponseSyncAction responseSyncAction =
                    new DdlResponseSyncAction(ddlContext.getResponseNode(), ddlResponse);
                if (TStringUtil.isNotEmpty(ddlContext.getResponseNode())) {
                    GmsSyncManagerHelper
                        .sync(responseSyncAction, ddlContext.getSchemaName(), ddlContext.getResponseNode());
                } else {
                    GmsSyncManagerHelper.sync(responseSyncAction, ddlContext.getSchemaName());
                }
            } catch (Throwable t) {
                StringBuilder errMsg = new StringBuilder();
                errMsg.append("Failed to notify the worker of the DDL response: \n");
                for (Map.Entry<Long, Response> resp : ddlResponse.getResponses().entrySet()) {
                    errMsg.append(resp.getKey()).append(" - ");
                    errMsg.append(resp.getValue().getResponseType()).append(":");
                    errMsg.append(resp.getValue().getResponseContent()).append("\n");
                }
                errMsg.append("in ").append(ddlContext.getSchemaName()).append(". Caused by: ").append(t.getMessage());
                DdlHelper.errorLogDual(LOGGER, ROOT_LOGGER, errMsg.toString(), t);
            }
        }
    }

    /**
     * current DDL JOB may not support cancel/rollback any more
     */
    private boolean allowRollback() {
        List<DdlEngineRecord> records = ddlJobManager.fetchRecords(Lists.newArrayList(ddlContext.getJobId()));
        FailPoint.assertTrue(() -> CollectionUtils.isNotEmpty(records) && CollectionUtils.size(records) == 1);
        DdlEngineRecord record = records.get(0);
        boolean allowRollback = record.isSupportCancel();
        if (!allowRollback) {
            String errMsg = String.format(
                "try to cancel DDL JOB: [%s], but cancel command is not supported at this stage.",
                ddlContext.getJobId()
            );
            EventLogger.log(EventType.DDL_WARN, errMsg);
            DdlHelper.errorLogDual(LOGGER, ROOT_LOGGER, errMsg);
        }
        return allowRollback;
    }

    /**
     * @throws TddlNestableRuntimeException when CAS operation fails
     */
    private void updateDdlState(final DdlState expect, final DdlState newState) {
        if(!deamonThreadAlive()){
            return;
        }
        DdlState originState = compareAndSetDdlState(expect, newState);
        //fatal: current DdlEngineDagExecutor's state is inconsistent with which in GMS
        if (originState != expect) {
            stateInconsistencyDetector.compareAndSet(null, Optional.ofNullable(originState));
            ddlContext.setInterruptedAsTrue();
            String errMsg = String.format(
                "fail to update DDL State. JobId:[%s]. expect state:[%s], actual state:[%s], new state:[%s]",
                ddlContext.getJobId(), expect, originState, newState);
            EventLogger.log(EventType.DDL_WARN, errMsg);
            DdlHelper.errorLogDual(LOGGER, ROOT_LOGGER, errMsg);
            throw new TddlNestableRuntimeException(errMsg);
        }
        ddlContext.unSafeSetDdlState(newState);
        LOGGER.info(String.format(
            "update DDL State from [%s] to [%s]. JobId:[%s]",
            expect.name(), newState.name(), ddlContext.getJobId()));
    }

    /**
     * compare-and-set DdlState
     *
     * @return old state
     */
    private DdlState compareAndSetDdlState(final DdlState expect, final DdlState newState) {
        return ddlJobManager.compareAndSetDdlState(ddlContext.getJobId(), expect, newState);
    }

    private void reverseTaskDagForRollback() {
        try {
            LOGGER.info(String.format(
                "wait for all executing tasks to finish. JobId:[%s], running task count is around:[%d]",
                ddlContext.getJobId(), maxParallelism - semaphore.availablePermits()));
            //wait for all executing tasks to finish
            semaphore.acquire(maxParallelism);
            semaphore.release(maxParallelism);
            LOGGER.info(String.format("start to reverse DAG to rollback tasks. JobId:[%s]", ddlContext.getJobId()));

            boolean success =
                reveredTaskSchedulerReference.compareAndSet(null, ddlJob.createReversedTaskScheduler());
            if (!success) {
                DdlHelper.errorLogDual(LOGGER, ROOT_LOGGER, String.format(
                    "Fail to reverse DAG since it had been reversed. JobId:[%s]", ddlContext.getJobId()));
            }
            LOGGER.info(String.format(
                "onRollingBack DDL JobId:[%s]. task graph:\n%s\n", ddlContext.getJobId(), ddlJob.visualizeTasks()));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new TddlNestableRuntimeException(e);
        }
    }

    private boolean hasFailureOnState(DdlState expect) {
        touchLastRecordTs();
        if (currentStateOutOfDate()) {
            DdlHelper.errorLogDual(LOGGER, ROOT_LOGGER, String.format(
                "DDL state out of date. state in memory:[%s], state in MetaDB:[%s]. Quit Current Job",
                ddlContext.getState().name(), getStateInconsistencyDetectorForLog()));
            LOGGER.info(String.format("JobId:[%s], task count is around:[%d]. waiting for all tasks to stop...... ",
                ddlContext.getJobId(), maxParallelism - semaphore.availablePermits()));
            return true;
        }
        if (ddlContext.getState() != expect) {
            DdlHelper.errorLogDual(LOGGER, ROOT_LOGGER, String.format(
                "DDL %s error. current state:[%s].", expect.name(), ddlContext.getState().name()));
            LOGGER.info(String.format(
                "JobId:[%s], task count is around:[%d]. waiting for all tasks to stop...... ",
                ddlContext.getJobId(), maxParallelism - semaphore.availablePermits()));
            return true;
        }
        if (ddlContext.isInterrupted()) {
            return true;
        }
        if(interruptWhileLosingLeader && !ExecUtils.hasLeadership(null)){
            ddlContext.setInterruptedAsTrue();
            DdlHelper.errorLogDual(LOGGER, ROOT_LOGGER, String.format(
                "interrupt DDL JOB %s for losing CN leadership.", ddlContext.getJobId()));
            EventLogger.log(EventType.DDL_INTERRUPT, String.format(
                "interrupt DDL JOB %s for losing CN leadership.", ddlContext.getJobId()));
            return true;
        }
        if (!deamonThreadAlive()){
            return true;
        }
        return false;
    }

    private boolean currentStateOutOfDate() {
        return stateInconsistencyDetector.get() != null;
    }

    private String getStateInconsistencyDetectorForLog() {
        if (stateInconsistencyDetector.get() == null) {
            return "<nothing inconsistent>";
        }
        if (stateInconsistencyDetector.get().isPresent()) {
            return stateInconsistencyDetector.get().get().name();
        } else {
            return "<DdlState is null in MetaDB>";
        }
    }

    private void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new TddlNestableRuntimeException(e);
        }
    }

    public String visualize() {
        return ddlJob.visualizeTasks();
    }

    public static void prepareExecutionContext(ExecutionContext context, DdlContext ddlContext) {
        context.setDdlContext(ddlContext);
        context.setServerVariables(ddlContext.getServerVariables());
        context.setUserDefVariables(ddlContext.getUserDefVariables());
        context.setExtraServerVariables(ddlContext.getExtraServerVariables());
        context.setExtraCmds(ddlContext.getExtraCmds());
        context.setEncoding(ddlContext.getEncoding());
        prepareTimezone(context, ddlContext);

        Map<String, Object> dataPassed = ddlContext.getDataPassed();

        Object connId = dataPassed.get(SQLRecord.CONN_ID);
        if (connId != null) {
            context.setConnId((Long) connId);
        }

        Object testMode = dataPassed.get(DdlConstants.TEST_MODE);
        if (testMode != null) {
            context.setTestMode((Boolean) testMode);
        }

        Object clientIP = dataPassed.get(SQLRecord.CLIENT_IP);
        if (clientIP != null) {
            context.setClientIp((String) clientIP);
        }

        Object traceId = dataPassed.get(SQLRecord.TRACE_ID);
        if (traceId != null) {
            context.setTraceId((String) traceId);
        }

        Object txId = dataPassed.get(SQLRecord.TX_ID);
        if (txId != null) {
            context.setTxId((Long) txId);
        }

        if (ddlContext.getSchemaName() != null) {
            context.setSchemaName(ddlContext.getSchemaName());
        }

        if (ddlContext.isEnableTrace()) {
            context.setEnableDdlTrace(true);
            context.setTracer(new SQLTracer());
        }
    }

    private static void prepareTimezone(ExecutionContext context, DdlContext ddlContext) {

        if (!StringUtils.isEmpty(ddlContext.getTimeZone())) {
            context.setTimeZone(TimeZoneUtils.convertFromMySqlTZ(ddlContext.getTimeZone()));
            return;
        }

        String timeZoneStr = (String) context.getServerVariables().get("time_zone");
        if (StringUtils.isEmpty(timeZoneStr)) {
            // No Find any time_zone variables
            timeZoneStr = GeneralUtil.getPropertyString(context.getExtraCmds(),
                ConnectionProperties.LOGICAL_DB_TIME_ZONE,
                "null").replace("\"", "");
        }
        InternalTimeZone tz;
        if (timeZoneStr.equalsIgnoreCase("null")) {
            tz = InternalTimeZone.defaultTimeZone;
        } else {
            tz = TimeZoneUtils.convertFromMySqlTZ(timeZoneStr);
        }
        if (tz != null) {
            context.setTimeZone(tz);
        }
    }

    public long getJobId() {
        return ddlContext.getJobId();
    }

    public String getSchemaName() {
        return ddlContext.getSchemaName();
    }

    public String getResources() {
        return Joiner.on(",").join(ddlContext.getResources());
    }

    public DdlState getDdlState() {
        return ddlContext.getState();
    }

    public void interrupt() {
        ddlContext.setInterruptedAsTrue();
    }

    public boolean isInterrupted() {
        return ddlContext.isInterrupted();
    }

    private static void recordTaskExecutionInfo(ConcurrentLinkedQueue<TaskExecutionInfo> taskExecutionQueue,
                                                DdlTask ddlTask,
                                                boolean executeOrElseRollback,
                                                Throwable throwable) {
        TaskExecutionInfo taskExecutionInfo = new TaskExecutionInfo(ddlTask, executeOrElseRollback, throwable);
        taskExecutionQueue.offer(taskExecutionInfo);
    }

    private String genTaskExecutionSequence() {
        StringBuilder dag = new StringBuilder();
        dag.append("digraph G {\n");
        Iterator<TaskExecutionInfo> taskExecutionInfoIterator = taskExecutionQueue.iterator();
        List<Integer> ids = new ArrayList<>(taskExecutionQueue.size());
        for (int i = 1; taskExecutionInfoIterator.hasNext(); i++) {
            ids.add(i);
            dag.append(String.valueOf(i) + " ");
            dag.append(taskExecutionInfoIterator.next().executionInfo + "\n");
        }
        dag.append(Joiner.on("->").join(ids));
        dag.append("\n");
        dag.append("}\n");
        return dag.toString();
    }

    public static class TaskExecutionInfo {
        private Long jobId;
        private Long taskId;
        private String taskName;
        private boolean executeOrElseRollback;
        private Throwable throwable;
        private String executionInfo;

        public TaskExecutionInfo(DdlTask ddlTask, boolean executeOrElseRollback, Throwable throwable) {
            this.jobId = ddlTask.getJobId();
            this.taskId = ddlTask.getTaskId();
            this.taskName = ddlTask.getName();
            this.executeOrElseRollback = executeOrElseRollback;
            this.throwable = throwable;
            this.executionInfo = ddlTask.executionInfo();
        }

        @Override
        public String toString() {
            return executionInfo;
        }
    }

    private void touchLastRecordTs() {
        synchronized (this) {
            LocalDateTime now = LocalDateTime.now();
            if (lastRecordTs == null) {
                lastRecordTs = now;
                return;
            }
            if (now.minusHours(3L).isAfter(lastRecordTs)) {
                lastRecordTs = now;
                EventLogger.log(EventType.DDL_WARN, String.format(
                    "Found slow DDL JOB %s. schemaName:%s, begin at:%s, current time is:%s",
                    ddlContext.getJobId(),
                    ddlContext.getSchemaName(),
                    beginTs,
                    now
                ));
            }
        }
    }

}
