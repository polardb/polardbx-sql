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

import com.alibaba.polardbx.optimizer.chunk.Block;
import com.alibaba.polardbx.optimizer.chunk.Chunk;
import com.alibaba.polardbx.optimizer.chunk.ChunkBuilder;
import com.alibaba.polardbx.optimizer.chunk.ChunkConverter;
import com.alibaba.polardbx.optimizer.chunk.Converters;
import com.alibaba.polardbx.executor.mpp.execution.buffer.OutputBufferMemoryManager;
import com.alibaba.polardbx.executor.mpp.operator.PartitionedOutputCollector.HashBucketFunction;
import com.alibaba.polardbx.executor.operator.ConsumerExecutor;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class PartitioningExchanger extends LocalExchanger {
    private final HashBucketFunction partitionGenerator;
    private final List<Integer> partitionChannels;
    private List<DataType> types;
    private final List<AtomicBoolean> consumings;
    private ChunkConverter keyConverter;
    private ExecutionContext context;

    public PartitioningExchanger(OutputBufferMemoryManager bufferMemoryManager, List<ConsumerExecutor> executors,
                                 LocalExchangersStatus status,
                                 boolean asyncConsume,
                                 List<DataType> types,
                                 List<Integer> partitionChannels,
                                 List<DataType> keyTargetTypes,
                                 ExecutionContext context) {
        super(bufferMemoryManager, executors, status, asyncConsume);
        this.types = types;
        this.context = context;
        this.partitionGenerator = new HashBucketFunction(executors.size());
        this.partitionChannels = partitionChannels;
        this.consumings = status.getConsumings();
        if (keyTargetTypes.isEmpty()) {
            this.keyConverter = null;
        } else {
            int[] columnIndex = new int[partitionChannels.size()];
            for (int i = 0; i < partitionChannels.size(); i++) {
                columnIndex[i] = partitionChannels.get(i);
            }
            this.keyConverter = Converters.createChunkConverter(columnIndex, types, keyTargetTypes, context);
        }
    }

    @Override
    public void consumeChunk(Chunk chunk) {
        IntList[] partitionAssignments = new IntList[executors.size()];
        for (int i = 0; i < partitionAssignments.length; i++) {
            partitionAssignments[i] = new IntArrayList();
        }
        // assign each row to a partition
        Chunk keyChunk;
        if (keyConverter == null) {
            keyChunk = getPartitionFunctionArguments(chunk);
        } else {
            keyChunk = keyConverter.apply(chunk);
        }

        for (int position = 0; position < keyChunk.getPositionCount(); position++) {
            int partition = partitionGenerator.getPartition(keyChunk, position);
            partitionAssignments[partition].add(position);
        }

        // build a page for each partition
        Map<Integer, Chunk> partitionChunks = new HashMap<>();
        for (int partition = 0; partition < executors.size(); partition++) {
            List<Integer> positions = partitionAssignments[partition];
            if (!positions.isEmpty()) {
                ChunkBuilder builder = new ChunkBuilder(types, positions.size(), context);
                for (Integer pos : positions) {
                    builder.declarePosition();
                    for (int i = 0; i < chunk.getBlockCount(); i++) {
                        builder.appendTo(chunk.getBlock(i), i, pos);
                    }
                }
                partitionChunks.put(partition, builder.build());
            }
        }
        if (asyncConsume) {
            for (Map.Entry<Integer, Chunk> entry : partitionChunks.entrySet()) {
                int partition = entry.getKey();
                executors.get(partition).consumeChunk(entry.getValue());
            }
        } else {
            for (Map.Entry<Integer, Chunk> entry : partitionChunks.entrySet()) {
                int partition = entry.getKey();
                AtomicBoolean consuming = consumings.get(partition);
                while (true) {
                    if (consuming.compareAndSet(false, true)) {
                        try {
                            executors.get(partition).consumeChunk(entry.getValue());
                        } finally {
                            consuming.set(false);
                        }
                        break;
                    }
                }
            }
        }
    }

    private Chunk getPartitionFunctionArguments(Chunk page) {
        Block[] blocks = new Block[partitionChannels.size()];
        for (int i = 0; i < blocks.length; i++) {
            blocks[i] = page.getBlock(partitionChannels.get(i));
        }
        return new Chunk(page.getPositionCount(), blocks);
    }
}