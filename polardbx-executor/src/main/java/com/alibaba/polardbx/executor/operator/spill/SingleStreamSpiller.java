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

package com.alibaba.polardbx.executor.operator.spill;

import com.alibaba.polardbx.common.exception.NotSupportException;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.alibaba.polardbx.executor.chunk.Chunk;

import java.io.Closeable;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Future;

import static com.google.common.collect.Iterators.singletonIterator;

public interface SingleStreamSpiller extends Closeable {

    ListenableFuture<?> NOT_BLOCKED = Futures.immediateFuture(null);

    /**
     * Initiate spilling of pages stream. Returns completed future once spilling has finished.
     * Next spill can be initiated as soon as previous one completes.
     */
    ListenableFuture<?> spill(Iterator<Chunk> page);

    /**
     * Initiate spilling of single page. Returns completed future once spilling has finished.
     * Next spill can be initiated as soon as previous one completes.
     */
    default ListenableFuture<?> spill(Chunk page) {
        return spill(singletonIterator(page));
    }

    /**
     * Returns list of previously spilled Chunks as a single stream. Chunks are in the same order
     * as they were spilled. Method requires the issued spill request to be completed.
     */
    Iterator<Chunk> getSpilledChunks();

    /**
     * 返回的Iterator最多读取maxChunkNum个Chunk
     */
    default Iterator<Chunk> getSpilledChunks(long maxChunkNum){
        throw new NotSupportException();
    };

    /**
     * Initiates read of previously spilled pages. The returned {@link Future} will be complete once all pages are read.
     */
    ListenableFuture<List<Chunk>> getAllSpilledChunks();

    /**
     * Flush all buffers into disk
     */
    void flush();

    void reset();

    /**
     * Close releases/removes all underlying resources used during spilling
     * like for example all created temporary files.
     */
    @Override
    void close();
}