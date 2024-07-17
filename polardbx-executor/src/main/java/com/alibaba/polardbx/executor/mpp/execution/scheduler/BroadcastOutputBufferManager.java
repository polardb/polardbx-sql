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
package com.alibaba.polardbx.executor.mpp.execution.scheduler;

import com.alibaba.polardbx.executor.mpp.OutputBuffers;
import com.alibaba.polardbx.executor.mpp.execution.StageId;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

@ThreadSafe
class BroadcastOutputBufferManager
    implements OutputBufferManager {
    private final Consumer<OutputBuffers> outputBufferTarget;
    private final Map<Integer, Boolean> outputNoMoreBuffers;

    /**
     * required child output size under remote partition wise join
     * otherwise, zero
     */
    private final int bufferCount;

    @GuardedBy("this")
    private OutputBuffers outputBuffers =
        OutputBuffers.createInitialEmptyOutputBuffers(OutputBuffers.BufferType.BROADCAST);

    public BroadcastOutputBufferManager(Map<Integer, Boolean> outputNoMoreBuffers,
                                        int bufferCount,
                                        Consumer<OutputBuffers> outputBufferTarget) {
        this.outputBufferTarget = requireNonNull(outputBufferTarget, "outputBufferTarget is null");
        this.outputNoMoreBuffers = requireNonNull(outputNoMoreBuffers, "outputNoMoreBuffers is null");
        this.bufferCount = bufferCount;
        outputBufferTarget.accept(outputBuffers);
    }

    @Override
    public void addOutputBuffers(StageId stageId, List<OutputBuffers.OutputBufferId> newBuffers,
                                 boolean noMoreBuffers) {
        OutputBuffers newOutputBuffers;
        synchronized (this) {
            if (outputBuffers.isNoMoreBufferIds()) {
                // a stage can move to a final state (e.g., failed) while scheduling, so ignore
                // the new buffers
                return;
            }

            OutputBuffers originalOutputBuffers = outputBuffers;

            // Note: it does not matter which partition id the task is using, in broadcast all tasks read from the same partition
            for (OutputBuffers.OutputBufferId newBuffer : newBuffers) {
                outputBuffers = outputBuffers.withBuffer(newBuffer, OutputBuffers.BROADCAST_PARTITION_ID);
            }

            Boolean stageNoMoreBuffersFlag = outputNoMoreBuffers.get(stageId.getId());
            checkArgument(stageNoMoreBuffersFlag != null, "noMoreBuffersFlag for stage:%s is null", stageId.getId());
            if (noMoreBuffers && stageNoMoreBuffersFlag.equals(false)) {
                outputNoMoreBuffers.put(stageId.getId(), true);
            }

            // only set no more buffers flag
            boolean notFinish = false;
            for (Boolean isNoMoreBuffers : outputNoMoreBuffers.values()) {
                if (!isNoMoreBuffers) {
                    notFinish = true;
                    break;
                }
            }
            if (!notFinish) {
                outputBuffers = outputBuffers.withNoMoreBufferIds();
            }

            // don't update if nothing changed
            if (outputBuffers == originalOutputBuffers) {
                return;
            }
            newOutputBuffers = this.outputBuffers;
        }
        outputBufferTarget.accept(newOutputBuffers);
    }

    public int getBufferCount() {
        return bufferCount;
    }
}
