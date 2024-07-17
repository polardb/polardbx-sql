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
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.google.common.util.concurrent.ListenableFuture;

import java.io.Closeable;
import java.util.Iterator;
import java.util.List;

public interface Spiller extends Closeable {
    /**
     * Initiate spilling of chunks stream. Returns completed future once spilling has finished.
     */
    ListenableFuture<?> spill(Iterator<Chunk> chunkIterator, boolean append);

    /**
     * Returns list of previously spilled Chunks streams.
     */
    List<Iterator<Chunk>> getSpills();

    /**
     * 返回的每一个Iterator最多读取maxChunkNum个Chunk
     */
    default List<Iterator<Chunk>> getSpills(long maxChunkNum) {
        throw new NotSupportException();
    }

    /**
     * flush
     */
    default void flush() {
        throw new NotSupportException();
    }

    /**
     * Close releases/removes all underlying resources used during spilling
     * like for example all created temporary files.
     */
    @Override
    void close();
}
