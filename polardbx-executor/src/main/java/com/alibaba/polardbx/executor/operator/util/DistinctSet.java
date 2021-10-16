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

import com.alibaba.polardbx.optimizer.chunk.Block;
import com.alibaba.polardbx.optimizer.chunk.Chunk;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import it.unimi.dsi.fastutil.ints.IntList;

/**
 * DistinctSet answers whether a record is distinct for some distinct aggregator e.g. {@code COUNT(DISTINCT val)}
 *
 * @author Eric Fu
 */
public class DistinctSet {

    private final GroupOpenHashMap groupHashMap;

    private final int[] distinctIndexes;

    public DistinctSet(DataType[] aggInputType, int[] distinctIndexes, int expectedSize, int chunkSize,
                       ExecutionContext context) {
        this.distinctIndexes = distinctIndexes;
        DataType[] concatType = distinctAndConcat(DataTypes.IntegerType, aggInputType, distinctIndexes);
        this.groupHashMap = new GroupOpenHashMap(concatType, expectedSize, chunkSize, context);
    }

    public boolean[] checkDistinct(Block groupIdBlock, Chunk aggInputChunk) {
        Chunk chunk = distinctAndConcat(groupIdBlock, aggInputChunk, distinctIndexes);

        boolean[] isDistinct = new boolean[groupIdBlock.getPositionCount()];
        for (int i = 0; i < chunk.getPositionCount(); i++) {
            int currentSize = groupHashMap.getGroupCount();
            isDistinct[i] = groupHashMap.innerPut(chunk, i, -1) == currentSize;
        }
        return isDistinct;
    }

    public boolean[] checkDistinct(Block groupIdBlock, Chunk aggInputChunk, IntList positions) {
        Chunk chunk = distinctAndConcat(groupIdBlock, aggInputChunk, distinctIndexes);

        boolean[] isDistinct = new boolean[positions.size()];
        for (int i = 0; i < positions.size(); i++) {
            final int pos = positions.getInt(i);
            int currentSize = groupHashMap.getGroupCount();
            isDistinct[i] = groupHashMap.innerPut(chunk, pos, -1) == currentSize;
        }
        return isDistinct;
    }

    public boolean[] checkDistinct(Block groupIdBlock, Chunk aggInputChunk, int... positions) {
        Chunk chunk = distinctAndConcat(groupIdBlock, aggInputChunk, distinctIndexes);

        boolean[] isDistinct = new boolean[positions.length];
        for (int i = 0; i < positions.length; i++) {
            final int pos = positions[i];
            int currentSize = groupHashMap.getGroupCount();
            isDistinct[i] = groupHashMap.innerPut(chunk, pos, -1) == currentSize;
        }
        return isDistinct;
    }

    private static Chunk distinctAndConcat(Block b, Chunk c, int[] distinctIndexes) {
        Block[] blocks = new Block[1 + distinctIndexes.length];
        blocks[0] = b;
        for (int i = 0; i < distinctIndexes.length; i++) {
            blocks[i + 1] = c.getBlock(distinctIndexes[i]);
        }
        return new Chunk(blocks);
    }

    private static DataType[] distinctAndConcat(DataType t, DataType[] ts, int[] distinctIndexes) {
        DataType[] nt = new DataType[1 + distinctIndexes.length];
        nt[0] = t;
        for (int i = 0; i < distinctIndexes.length; i++) {
            nt[i + 1] = ts[distinctIndexes[i]];
        }
        return nt;
    }
}
