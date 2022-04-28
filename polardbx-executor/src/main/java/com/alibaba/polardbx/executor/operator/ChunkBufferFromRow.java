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

package com.alibaba.polardbx.executor.operator;

import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.ChunkBuilder;

public class ChunkBufferFromRow extends AbstractChunkBuffer {
    final ChunkBuilder chunkBuilder;

    public ChunkBufferFromRow(ChunkBuilder chunkBuilder) {
        super();
        this.chunkBuilder = chunkBuilder;
    }

    @Override
    void flushToBuffer(boolean force) {
        if (force) {
            flush();
        } else {
            tryFlush();
        }
    }

    @Override
    boolean allIsInBuffer() {
        return chunkBuilder.isEmpty();
    }

    public void addRow(Chunk srcChunk, int srcPosition) {
        chunkBuilder.declarePosition();
        for (int channel = 0; channel < chunkBuilder.getTypes().size(); ++channel) {
            chunkBuilder.appendTo(
                srcChunk.getBlock(channel), channel, srcPosition);
        }
        tryFlush();
    }

    private void flush() {
        if (chunkBuilder.isEmpty()) {
            return;
        }
        Chunk chunk = chunkBuilder.build();
        chunkBuilder.reset();
        bufferChunks.add(chunk);
    }

    private void tryFlush() {
        if (chunkBuilder.isFull()) {
            flush();
        }
    }

}
