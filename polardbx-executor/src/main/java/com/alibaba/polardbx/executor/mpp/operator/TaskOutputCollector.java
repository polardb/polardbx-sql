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

package com.alibaba.polardbx.executor.mpp.operator;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.ChunkConverter;
import com.alibaba.polardbx.executor.chunk.Converters;
import com.alibaba.polardbx.executor.mpp.execution.buffer.ClientBuffer;
import com.alibaba.polardbx.executor.mpp.execution.buffer.OutputBuffer;
import com.alibaba.polardbx.executor.mpp.execution.buffer.PagesSerde;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class TaskOutputCollector extends OutputCollector {

    private final OutputBuffer outputBuffer;
    private final PagesSerde serde;
    private ListenableFuture<?> blocked = NOT_BLOCKED;
    private boolean finished;
    private ChunkConverter chunkConverter;

    public TaskOutputCollector(
        List<DataType> inputType,
        List<DataType> outputType,
        OutputBuffer outputBuffer, PagesSerde serde, ExecutionContext context) {

        this.outputBuffer = outputBuffer;
        this.serde = serde;
        this.chunkConverter = Converters.createChunkConverter(inputType, outputType, context);
    }

    @Override
    public void finish() {
        finished = true;
    }

    @Override
    public boolean isFinished() {
        if (blocked != NOT_BLOCKED && blocked.isDone()) {
            blocked = NOT_BLOCKED;
        }

        return finished && blocked == NOT_BLOCKED;
    }

    @Override
    public ListenableFuture<?> consumeIsBlocked() {
        if (blocked != NOT_BLOCKED && blocked.isDone()) {
            blocked = NOT_BLOCKED;
        }
        return blocked;
    }

    @Override
    public void doOutput(Chunk page) {
        requireNonNull(page, "page is null");
        if (page.getPositionCount() == 0) {
            return;
        }
        ClientBuffer buffer = outputBuffer.getClientBuffer(0);
        ListenableFuture<?> future = outputBuffer.enqueue(Lists.newArrayList(
            serde.serialize(buffer != null && buffer.isPreferLocal(), chunkConverter.apply(page))));
        if (!future.isDone()) {
            this.blocked = future;
        }
    }

    @Override
    public boolean needsInput() {
        return !finished && consumeIsBlocked().isDone();
    }
}
