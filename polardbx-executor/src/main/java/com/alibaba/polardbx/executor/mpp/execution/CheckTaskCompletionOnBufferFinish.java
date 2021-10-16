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

package com.alibaba.polardbx.executor.mpp.execution;

import com.alibaba.polardbx.executor.mpp.execution.buffer.BufferState;

import java.lang.ref.WeakReference;

public class CheckTaskCompletionOnBufferFinish
    implements StateMachine.StateChangeListener<BufferState> {
    private final WeakReference<SqlTaskExecution> sqlTaskExecutionReference;

    public CheckTaskCompletionOnBufferFinish(SqlTaskExecution sqlTaskExecution) {
        // we are only checking for completion of the task, so don't hold up GC if the task is dead
        this.sqlTaskExecutionReference = new WeakReference<>(sqlTaskExecution);
    }

    @Override
    public void stateChanged(BufferState newState) {
        if (newState == BufferState.FINISHED) {
            SqlTaskExecution sqlTaskExecution = sqlTaskExecutionReference.get();
            if (sqlTaskExecution != null) {
                sqlTaskExecution.checkTaskCompletion();
            }
        }
    }
}