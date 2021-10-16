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

import com.alibaba.polardbx.executor.mpp.util.ImmutableCollectors;

import java.util.Set;
import java.util.stream.Stream;

public enum TaskState {
    /**
     * Task is planned but has not been scheduled yet.  A task will
     * be in the planned state until, the dependencies of the task
     * have begun producing output.
     */
    PLANNED(false),
    /**
     * Task is running.
     */
    RUNNING(false),
    /***
     * Task complete execute but output buffer not consumed
     */
    FLUSHING(false),
    /**
     * Task has finished executing and all output has been consumed.
     */
    FINISHED(true),
    /**
     * Task was canceled by a user.
     */
    CANCELED(true),
    /**
     * Task was aborted due to a failure in the query.  The failure
     * was not in this task.
     */
    ABORTED(true),
    /**
     * Task execution failed.
     */
    FAILED(true);

    public static TaskState findFromString(String name) {
        if ("PLANNED".equals(name)) {
            return PLANNED;
        } else if ("RUNNING".equals(name)) {
            return RUNNING;
        } else if ("FINISHED".equals(name)) {
            return FINISHED;
        } else if ("CANCELED".equals(name)) {
            return CANCELED;
        } else if ("ABORTED".equals(name)) {
            return ABORTED;
        } else if ("FAILED".equals(name)) {
            return FAILED;
        }
        return null;
    }

    public static final Set<TaskState> TERMINAL_TASK_STATES =
        Stream.of(TaskState.values()).filter(TaskState::isDone).collect(
            ImmutableCollectors.toImmutableSet());

    private final boolean doneState;

    TaskState(boolean doneState) {
        this.doneState = doneState;
    }

    /**
     * Is this a terminal state.
     */
    public boolean isDone() {
        return doneState;
    }

    public boolean isException() {
        return this == CANCELED || this == ABORTED || this == FAILED;
    }
}
