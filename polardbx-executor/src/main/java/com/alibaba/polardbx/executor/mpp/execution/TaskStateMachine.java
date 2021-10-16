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

/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.polardbx.executor.mpp.execution;

import com.google.common.util.concurrent.ListenableFuture;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import org.joda.time.DateTime;

import javax.annotation.concurrent.ThreadSafe;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.alibaba.polardbx.executor.mpp.execution.TaskState.TERMINAL_TASK_STATES;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class TaskStateMachine implements StateMachineBase<TaskState> {
    private static final Logger log = LoggerFactory.getLogger(TaskStateMachine.class);

    private final DateTime createdTime = DateTime.now();
    private long driverStartTime = System.currentTimeMillis();
    private long driverEndTime = -1;
    private long createToDriverFinishTime = -1;
    private long endTime = -1;

    private final TaskId taskId;
    private final StateMachine<TaskState> taskState;
    private final LinkedBlockingQueue<Throwable> failureCauses = new LinkedBlockingQueue<>();
    private final AtomicLong dataFinishTime = new AtomicLong();
    private final AtomicLong dataFlushTime = new AtomicLong();

    public TaskStateMachine(TaskId taskId, Executor executor) {
        this.taskId = requireNonNull(taskId, "taskId is null");
        taskState = new StateMachine<>("task " + taskId, executor, TaskState.RUNNING, TERMINAL_TASK_STATES);
        if (log.isDebugEnabled()) {
            taskState.addStateChangeListener(newState -> {
                log.debug(String.format("Task %s is %s", TaskStateMachine.this.taskId, newState));
            });
        }
    }

    public DateTime getCreatedTime() {
        return createdTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public TaskId getTaskId() {
        return taskId;
    }

    public TaskState getState() {
        return taskState.get();
    }

    public ListenableFuture<TaskState> getStateChange(TaskState currentState) {
        requireNonNull(currentState, "currentState is null");
        checkArgument(!currentState.isDone(), "Current state is already done");

        ListenableFuture<TaskState> future = taskState.getStateChange(currentState);
        TaskState state = taskState.get();
        if (state.isDone()) {
            return immediateFuture(state);
        }
        return future;
    }

    public LinkedBlockingQueue<Throwable> getFailureCauses() {
        return failureCauses;
    }

    public void flushing() {
        taskState
            .setIf(TaskState.FLUSHING, currentState -> currentState != TaskState.FLUSHING && !currentState.isDone());
    }

    public void finished() {
        transitionToDoneState(TaskState.FINISHED);
    }

    public void cancel() {
        transitionToDoneState(TaskState.CANCELED);
    }

    public void abort() {
        transitionToDoneState(TaskState.ABORTED);
    }

    public void recordDriverEndTime(long time) {
        if (driverEndTime == -1) {
            driverEndTime = System.currentTimeMillis() - time;
        }
        if (createToDriverFinishTime == -1) {
            createToDriverFinishTime = System.currentTimeMillis() - driverStartTime;
        }
    }

    public long getDriverEndTime() {
        if (Long.MAX_VALUE / 100000000 <= driverEndTime) {
            return -1;
        }
        return driverEndTime * 100000000 + createToDriverFinishTime;
    }

    @Override
    public void failed(Throwable cause) {
        failureCauses.add(cause);
        transitionToDoneState(TaskState.FAILED);
    }

    private void transitionToDoneState(TaskState doneState) {
        requireNonNull(doneState, "doneState is null");
        // cut Object[]
        if (!doneState.isDone()) {
            checkArgument(doneState.isDone(), "doneState %s is not a done state", doneState);
        }
        endTime = System.currentTimeMillis();
        taskState.setIf(doneState, currentState -> !currentState.isDone());
    }

    @Override
    public boolean isDone() {
        return taskState.get().isDone();
    }

    @Override
    public void addStateChangeListener(StateMachine.StateChangeListener<TaskState> stateChangeListener) {
        taskState.addStateChangeListener(stateChangeListener);
    }

    @Override
    public String toString() {
        return toStringHelper(this)
            .add("taskId", taskId)
            .add("taskState", taskState)
            .add("failureCauses", failureCauses)
            .toString();
    }

    public long getPullDataTime() {

        if (dataFlushTime.get() == 0) {
            return 0;
        }

        if (dataFinishTime.get() == 0) {
            return System.currentTimeMillis() - dataFlushTime.get();
        }

        return dataFinishTime.get() - dataFlushTime.get();
    }

    public void recordDataFinishTime() {
        dataFinishTime.compareAndSet(0, System.currentTimeMillis());
    }

    public void recordDataFushTime() {
        dataFlushTime.compareAndSet(0, System.currentTimeMillis());
    }
}
