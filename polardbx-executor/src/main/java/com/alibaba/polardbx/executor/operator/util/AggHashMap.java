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

package com.alibaba.polardbx.executor.operator.util;

import com.alibaba.polardbx.executor.chunk.Chunk;
import com.google.common.util.concurrent.ListenableFuture;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.util.List;

public interface AggHashMap extends GroupHashMap {

    void putChunk(Chunk keyChunk, Chunk inputChunk, IntArrayList groupIdResult);

    List<Chunk> getGroupChunkList();

    List<Chunk> getValueChunkList();

    AggResultIterator buildChunks();

    default ListenableFuture<?> startMemoryRevoke() {
        throw new UnsupportedOperationException(getClass().getName());
    }

    default void finishMemoryRevoke() {
        throw new UnsupportedOperationException(getClass().getName());
    }

    default void close() {
    }

    /**
     * To consume chunks, build hash table,
     * maintain the groupId, and accumulate agg-function.
     */
    interface GroupBy {
        /**
         * The key-chunk and input chunk will share the same blocks.
         *
         * @param keyChunk chunks of group-by blocks.
         * @param inputChunk chunks of total input blocks.
         * @param groupIdResult if not null, fill the group ids into it.
         */
        void putChunk(Chunk keyChunk, Chunk inputChunk, IntArrayList groupIdResult);

        /**
         * Get the precise fixed estimated size in bytes of this object.
         *
         * @return size in bytes
         */
        long fixedEstimatedSize();

        void close();
    }
}
