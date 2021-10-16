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

import com.google.common.collect.AbstractIterator;
import com.alibaba.polardbx.optimizer.chunk.Chunk;

import java.util.Iterator;
import java.util.LinkedList;

/**
 * TODO: add memoryContext
 * TODO: support revoke memory
 * not thread-safe
 *
 */
public abstract class AbstractChunkBuffer {

    protected final LinkedList<Chunk> bufferChunks;

    public AbstractChunkBuffer() {
        this.bufferChunks = new LinkedList<Chunk>();
    }

    public Iterator<Chunk> getChunksAndDeleteAfterRead() {
        return new AbstractIterator<Chunk>() {

            @Override
            protected Chunk computeNext() {
                if (bufferChunks.isEmpty()) {
                    return endOfData();
                }
                return nextChunk();
            }
        };
    }

    /**
     * build and flush all the remaining chunk to the buffer
     *
     * @param force if is false, maybe batching
     */
    abstract void flushToBuffer(boolean force);

    /**
     * check if all the chunks(include the chunk source) has benn flush to the buffer
     *
     * @return if all is flushed
     */
    abstract boolean allIsInBuffer();

    boolean hasNextChunk() {
        return !bufferChunks.isEmpty();
    }

    public Chunk nextChunk() {
        if (!bufferChunks.isEmpty()) {
            return bufferChunks.remove(0);
        } else {
            return null;
        }
    }

    public void addChunk(Chunk chunk) {
        bufferChunks.add(chunk);
    }

    public boolean produceIsFinished() {
        return bufferChunks.isEmpty() && this.allIsInBuffer();
    }
}
