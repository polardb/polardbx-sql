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

package com.alibaba.polardbx.executor.ddl.newengine.job;

import com.alibaba.polardbx.common.ddl.newengine.DdlTaskState;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.ddl.job.task.BaseValidateTask;
import com.alibaba.polardbx.executor.ddl.newengine.meta.DdlEngineAccessorDelegate;
import com.alibaba.polardbx.executor.ddl.newengine.serializable.SerializableClassMapper;
import com.alibaba.polardbx.executor.ddl.newengine.utils.DdlJobManagerUtils;
import com.alibaba.polardbx.executor.ddl.newengine.utils.TaskHelper;
import com.alibaba.polardbx.executor.handler.HandlerCommon;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineTaskRecord;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import org.apache.calcite.rel.RelNode;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;

import static com.alibaba.polardbx.executor.utils.failpoint.FailPointKey.FP_EACH_DDL_TASK_FAIL_ONCE;
import static com.alibaba.polardbx.executor.utils.failpoint.FailPointKey.FP_FAIL_ON_DDL_TASK_NAME;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public abstract class AbstractDdlTask extends HandlerCommon implements DdlTask {

    protected static final Logger LOGGER = SQLRecorderLogger.ddlEngineLogger;

    protected transient Long jobId;
    protected transient Long taskId;
    protected String schemaName;
    protected transient volatile DdlTaskState state = DdlTaskState.READY;
    protected transient DdlExceptionAction exceptionAction = DdlExceptionAction.DEFAULT_ACTION;
    protected transient volatile Long beginExecuteTs;
    protected transient volatile Long endExecuteTs;

    protected transient volatile Long beginRollbackTs;
    protected transient volatile Long endRollbackTs;

    private transient volatile int injectTimes = 0;

    public AbstractDdlTask(final String schemaName) {
        this.schemaName = schemaName;
    }

    @Override
    public void execute(ExecutionContext executionContext) {
        beginExecuteTs = System.nanoTime();
        beforeTransaction(executionContext);
        final DdlTask currentTask = this;
        DdlEngineAccessorDelegate delegate = new DdlEngineAccessorDelegate<Integer>() {

            @Override
            protected Integer invoke() {
                int result = 0;
                duringTransaction(getConnection(), executionContext);
                DdlEngineTaskRecord taskRecord = TaskHelper.toDdlEngineTaskRecord(currentTask);
                taskRecord.setState(DdlTaskState.SUCCESS.name());
                result += engineTaskAccessor.updateTask(taskRecord);

                //inject exceptions
                injectOnce(executionContext);
                FailPoint.injectFromHint(FP_FAIL_ON_DDL_TASK_NAME, executionContext, (k, v) -> {
                    if (StringUtils.equalsIgnoreCase(currentTask.getName(), v)) {
                        FailPoint.throwException(String.format("injected failure at: [%s]", getName()));
                    }
                });

                if (result <= 0) {
                    throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_UNEXPECTED, "update task status error");
                }
                return result;
            }
        };
        delegate.execute();
        //will not execute this if there's a failure
        onExecutionSuccess(executionContext);
        currentTask.setState(DdlTaskState.SUCCESS);
        endExecuteTs = System.nanoTime();
    }

    protected abstract void beforeTransaction(final ExecutionContext executionContext);

    /**
     * The real execution flow that a subclass should implement.
     */
    protected abstract void duringTransaction(Connection metaDbConnection, final ExecutionContext executionContext);

    protected abstract void onExecutionSuccess(final ExecutionContext executionContext);

    @Override
    public void rollback(ExecutionContext executionContext) {
        beginRollbackTs = System.nanoTime();
        final DdlTask currentTask = this;
        beforeRollbackTransaction(executionContext);
        DdlEngineAccessorDelegate delegate = new DdlEngineAccessorDelegate<Integer>() {

            @Override
            protected Integer invoke() {
                int result = 0;
                duringRollbackTransaction(getConnection(), executionContext);
                DdlEngineTaskRecord taskRecord = TaskHelper.toDdlEngineTaskRecord(currentTask);
                taskRecord.setState(DdlTaskState.ROLLBACK_SUCCESS.name());
                result += engineTaskAccessor.updateTask(taskRecord);

                //inject exceptions
                injectOnce(executionContext);
                FailPoint.injectFromHint(FP_FAIL_ON_DDL_TASK_NAME, executionContext, (k, v) -> {
                    if (StringUtils.equalsIgnoreCase(currentTask.getName(), v)) {
                        FailPoint.throwException(String.format("injected failure at: [%s]", getName()));
                    }
                });

                if (result <= 0) {
                    throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_UNEXPECTED, "update task status error");
                }
                return result;
            }
        };
        delegate.execute();
        onRollbackSuccess(executionContext);
        currentTask.setState(DdlTaskState.ROLLBACK_SUCCESS);
        endRollbackTs = System.nanoTime();
    }

    protected abstract void beforeRollbackTransaction(final ExecutionContext executionContext);

    /**
     * The real execution flow that a subclass should implement.
     */
    protected abstract void duringRollbackTransaction(Connection metaDbConnection,
                                                      final ExecutionContext executionContext);

    protected abstract void onRollbackSuccess(final ExecutionContext executionContext);

    /**
     * update Task State in a new transaction
     */
    public void updateTaskStateInNewTxn(DdlTaskState state) {
        final DdlTask currentTask = this;
        DdlEngineAccessorDelegate delegate = new DdlEngineAccessorDelegate<Integer>() {
            @Override
            protected Integer invoke() {
                currentTask.setState(state);
                DdlEngineTaskRecord taskRecord = TaskHelper.toDdlEngineTaskRecord(currentTask);
                return engineTaskAccessor.updateTask(taskRecord);
            }
        };
        delegate.execute();
    }

    @Override
    public void updateProgress(int progress, Connection connection) {
        DdlJobManagerUtils.updateProgress(jobId, progress, connection);
    }

    @Override
    public void updateSupportedCommands(boolean supportContinue,
                                        boolean supportCancel,
                                        Connection connection) {
        if (connection != null) {
            DdlJobManagerUtils.updateSupportedCommands(jobId, supportContinue, supportCancel, connection);
        } else {
            DdlJobManagerUtils.updateSupportedCommands(jobId, supportContinue, supportCancel);
        }
    }

    @Override
    public Long getJobId() {
        return this.jobId;
    }

    @Override
    public void setJobId(final Long jobId) {
        this.jobId = jobId;
    }

    @Override
    public Long getTaskId() {
        return taskId;
    }

    @Override
    public void setTaskId(final Long taskId) {
        this.taskId = taskId;
    }

    @Override
    public String getName() {
        return SerializableClassMapper.getNameByTaskClass(this.getClass());
    }

    @Override
    public String getSchemaName() {
        return this.schemaName;
    }

    @Override
    public void setSchemaName(final String schemaName) {
        this.schemaName = schemaName;
    }

    @Override
    public DdlExceptionAction getExceptionAction() {
        return exceptionAction;
    }

    @Override
    public void setExceptionAction(DdlExceptionAction exceptionAction) {
        this.exceptionAction = exceptionAction;
    }

    @Override
    public DdlTask onExceptionTryRecoveryThenPause() {
        return onExceptionTry(DdlExceptionAction.TRY_RECOVERY_THEN_PAUSE);
    }

    @Override
    public DdlTask onExceptionTryRollback() {
        return onExceptionTry(DdlExceptionAction.ROLLBACK);
    }

    @Override
    public DdlTask onExceptionTryRecoveryThenRollback() {
        return onExceptionTry(DdlExceptionAction.TRY_RECOVERY_THEN_ROLLBACK);
    }

    private DdlTask onExceptionTry(DdlExceptionAction exceptionAction) {
        this.exceptionAction = exceptionAction;
        return this;
    }

    public void injectOnce(ExecutionContext executionContext) {
        FailPoint.injectFromHint(FP_EACH_DDL_TASK_FAIL_ONCE, executionContext, (k, v) -> {
            if (injectTimes == 1 || this instanceof BaseValidateTask) {
                return;
            }
            injectTimes++;
            FailPoint.throwException(String.format("injected failure at: [%s]", getName()));
        });
    }

    @Override
    public void setState(DdlTaskState ddlTaskState) {
        this.state = ddlTaskState;
    }

    @Override
    public DdlTaskState getState() {
        return state;
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        return null;
    }

    @Override
    public String toString() {
        return String.valueOf(this.getTaskId());
    }

    @Override
    public String nodeInfo() {
        return String.format(
            "%s [shape=record  %s label=\"{%s|taskId:%s|onException:%s|state:%s%s%s%s}\"];",
            this.taskId,
            color(state),
            this.getName(),
            this.taskId,
            this.exceptionAction.name(),
            state.name(),
            cost("|execute cost:%s", beginExecuteTs, endExecuteTs),
            cost("|rollback cost:%s", beginRollbackTs, endRollbackTs),
            remark()
        );
    }

    @Override
    public String executionInfo() {
        return String.format(
            "[shape=record  %s label=\"{%s|taskId:%s|onException:%s|state:%s%s%s%s}\"];",
            color(state),
            this.getName(),
            this.taskId,
            this.exceptionAction.name(),
            state.name(),
            cost("|execute cost:%s", beginExecuteTs, endExecuteTs),
            cost("|rollback cost:%s", beginRollbackTs, endRollbackTs),
            remark()
        );
    }

    /**
     * Description info for logging and explain
     */
    public String getDescription() {
        return "";
    }

    protected String remark() {
        return "";
    }

    private String cost(String format, Long begin, Long end) {
        if (begin == null || end == null) {
            return "";
        } else {
            return String.format(
                format,
                MILLISECONDS.convert(end - begin, NANOSECONDS) + "ms");
        }
    }

    private String color(DdlTaskState ddlTaskState) {
        if (ddlTaskState == DdlTaskState.SUCCESS) {
            return "fillcolor=\"#90ee90\" style=filled";
        } else if (ddlTaskState == DdlTaskState.DIRTY) {
            return "fillcolor=\"#fff68f\" style=filled";
        } else if (ddlTaskState == DdlTaskState.ROLLBACK_SUCCESS) {
            return "fillcolor=\"#f08080\" style=filled";
        }
        return "";
    }
}
