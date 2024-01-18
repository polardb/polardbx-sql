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

import com.alibaba.polardbx.executor.calc.Aggregator;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;

import java.util.ArrayList;
import java.util.List;

public class HashWindowOpenHashMap extends AggOpenHashMap {
    private List<Chunk> inputChunks = new ArrayList<>();
    private List<int[]> groupIds = new ArrayList<>();

    public HashWindowOpenHashMap(DataType[] groupKeyType, List<Aggregator> aggregators, DataType[] aggValueType,
                                 DataType[] inputType, int expectedSize, int chunkSize, ExecutionContext context) {
        super(groupKeyType, aggregators, aggValueType, inputType, expectedSize, DEFAULT_LOAD_FACTOR, chunkSize,
            context);
    }

    @Override
    public HashWindowAggResultIterator buildChunks() {
        super.valueChunks = buildValueChunks();
        return new HashWindowAggResultIterator(valueChunks, inputChunks, groupIds, valueBlockBuilders, chunkSize);
    }

    @Override
    public int[] putChunk(Chunk keyChunk, Chunk inputChunk) {
        inputChunks.add(inputChunk);
        int[] groupId = super.putChunk(keyChunk, inputChunk);
        groupIds.add(groupId);
        return groupId;
    }
}
