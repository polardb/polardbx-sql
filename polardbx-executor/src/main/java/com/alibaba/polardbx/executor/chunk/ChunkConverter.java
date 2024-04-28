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

package com.alibaba.polardbx.executor.chunk;

import com.google.common.base.Preconditions;

import java.util.function.Function;

/**
 * Map Chunk to Chunk according to each block converter
 *
 */
public class ChunkConverter implements Function<Chunk, Chunk> {

    private final int[] columnIndexes;
    private final BlockConverter[] blockConverters;

    public ChunkConverter(BlockConverter[] blockConverters, int[] columnIndexes) {
        Preconditions.checkArgument(blockConverters.length == columnIndexes.length);
        this.columnIndexes = columnIndexes;
        this.blockConverters = blockConverters;
    }

    @Override
    public Chunk apply(Chunk chunk) {
        Block[] blocks = new Block[columnIndexes.length];
        for (int i = 0; i < columnIndexes.length; i++) {
            blocks[i] = blockConverters[i].apply(chunk.getBlock(columnIndexes[i]));
        }
        return new Chunk(blocks);
    }

    public int columnWidth() {
        return columnIndexes.length;
    }
}
