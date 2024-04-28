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

import com.alibaba.polardbx.common.utils.bloomfilter.BloomFilterInfo;
import com.alibaba.polardbx.executor.mpp.OutputBuffers;
import com.alibaba.polardbx.executor.mpp.execution.buffer.BufferResult;
import com.alibaba.polardbx.executor.mpp.planner.PlanFragment;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;

import java.net.URI;
import java.util.List;
import java.util.Optional;

public interface TaskManager {
    /**
     * Gets all of the currently tracked tasks.  This will included
     * uninitialized, running, and completed tasks.
     */
    List<TaskInfo> getAllTaskInfo();

    /**
     * Gets the info for the specified task.  If the task has not been created
     * yet, an uninitialized task is created and the info is returned.
     * <p>
     * NOTE: this design assumes that only tasks that will eventually exist are
     * queried.
     */
    TaskInfo getTaskInfo(TaskId taskId);

    /**
     * Gets the status for the specified task.
     */
    TaskStatus getTaskStatus(TaskId taskId);

    /**
     * Gets future info for the task after the state changes from
     * {@code current state}. If the task has not been created yet, an
     * uninitialized task is created and the future is returned.  If the task
     * is already in a final state, the info is returned immediately.
     * <p>
     * NOTE: this design assumes that only tasks that will eventually exist are
     * queried.
     */
    ListenableFuture<TaskInfo> getTaskInfo(TaskId taskId, TaskState currentState);

    /**
     * Gets future status for the task after the state changes from
     * {@code current state}. If the task has not been created yet, an
     * uninitialized task is created and the future is returned.  If the task
     * is already in a final state, the status is returned immediately.
     * <p>
     * NOTE: this design assumes that only tasks that will eventually exist are
     * queried.
     */
    ListenableFuture<TaskStatus> getTaskStatus(TaskId taskId, TaskState currentState);

    /**
     * Updates the task plan, sources and output buffers.  If the task does not
     * already exist, is is created and then updated.
     */
    TaskInfo updateTask(SessionRepresentation session, TaskId taskId, Optional<PlanFragment> fragment,
                        List<TaskSource> sources,
                        OutputBuffers outputBuffers,
                        Optional<List<BloomFilterInfo>> bloomFilterInfos, URI uri);

    /**
     * Cancels a task.  If the task does not already exist, is is created and then
     * canceled.
     */
    TaskInfo cancelTask(TaskId taskId);

    /**
     * Aborts a task.  If the task does not already exist, is is created and then
     * aborted.
     */
    TaskInfo abortTask(TaskId taskId);

    /**
     * Gets results from a task either immediately or in the future.  If the
     * task or buffer has not been created yet, an uninitialized task is
     * created and a future is returned.
     * <p>
     * NOTE: this design assumes that only tasks and buffers that will
     * eventually exist are queried.
     */
    ListenableFuture<BufferResult> getTaskResults(TaskId taskId, boolean preferLocal,
                                                  OutputBuffers.OutputBufferId bufferId, long startingSequenceId,
                                                  DataSize maxSize);

    /**
     * Aborts a result buffer for a task.  If the task or buffer has not been
     * created yet, an uninitialized task is created and a the buffer is
     * aborted.
     * <p>
     * NOTE: this design assumes that only tasks and buffers that will
     * eventually exist are queried.
     */
    void abortTaskResults(TaskId taskId, OutputBuffers.OutputBufferId bufferId);

    List<SqlTask> getAllTasks();
}
