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
import com.google.common.collect.ImmutableMap;

import javax.annotation.concurrent.ThreadSafe;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkArgument;

@ThreadSafe
public class PartitionedOutputBufferManager
    implements OutputBufferManager {
    private final Map<OutputBuffers.OutputBufferId, Integer> outputBuffers;

    public PartitionedOutputBufferManager(Set<StageId> parentStages, boolean needSetStageId, int partitionCount,
                                          Consumer<OutputBuffers> outputBufferTarget) {
        checkArgument(partitionCount >= 1, "partitionCount must be at least 1");

        ImmutableMap.Builder<OutputBuffers.OutputBufferId, Integer> partitions = ImmutableMap.builder();
        for (StageId stageId : parentStages) {
            for (int partition = 0; partition < partitionCount; partition++) {
                if (needSetStageId) {
                    partitions.put(new OutputBuffers.OutputBufferId(stageId.getId(), partition), partition);
                } else {
                    partitions.put(new OutputBuffers.OutputBufferId(partition), partition);
                }
            }
        }

        OutputBuffers outputBuffers =
            OutputBuffers.createInitialEmptyOutputBuffers(OutputBuffers.BufferType.PARTITIONED)
                .withBuffers(partitions.build())
                .withNoMoreBufferIds();
        outputBufferTarget.accept(outputBuffers);

        this.outputBuffers = outputBuffers.getBuffers();
    }

    @Override
    public void addOutputBuffers(StageId stageId, List<OutputBuffers.OutputBufferId> newBuffers,
                                 boolean noMoreBuffers) {
        // All buffers are created in the constructor, so just validate that this isn't
        // a request to add a new buffer
        for (OutputBuffers.OutputBufferId newBuffer : newBuffers) {
            Integer existingBufferId = outputBuffers.get(newBuffer);
            if (existingBufferId == null) {
                throw new IllegalStateException("Unexpected new output buffer " + newBuffer);
            }
            if (newBuffer.getId() != existingBufferId) {
                throw new IllegalStateException("newOutputBuffers has changed the assignment for task " + newBuffer);
            }
        }
    }
}
