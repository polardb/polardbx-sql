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

import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.ChunkBuilder;
import com.alibaba.polardbx.executor.mpp.operator.PartitionedOutputCollector;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

import java.util.ArrayList;
import java.util.List;

public class BucketMockExec extends MockExec {

    private final PartitionedOutputCollector.HashBucketFunction bucketGenerator;
    private final List<Integer> partitionChannels;
    private int totalBucketNum;
    private List<Chunk> bucketChunks = new ArrayList<>();

    public BucketMockExec(List<DataType> columnTypes,
                          List<Chunk> chunks,
                          int bucketNum,
                          List<Integer> partitionChannels) {
        super(columnTypes, chunks);

        this.partitionChannels = partitionChannels;
        this.totalBucketNum = bucketNum;
        this.bucketGenerator = new PartitionedOutputCollector.HashBucketFunction(totalBucketNum);
    }

    @Override
    public Chunk nextChunk() {
        if (bucketChunks.size() > 0) {
            return bucketChunks.remove(0);
        }

        Chunk ret = super.nextChunk();
        if (ret != null) {
            partitionChunk(ret);
            return bucketChunks.remove(0);
        }
        return null;
    }

    public void partitionChunk(Chunk chunk) {
        IntList[] partitionAssignments = new IntList[totalBucketNum];
        for (int i = 0; i < partitionAssignments.length; i++) {
            partitionAssignments[i] = new IntArrayList();
        }
        // assign each row to a partition
        Chunk keyChunk = getPartitionFunctionArguments(chunk);

        for (int position = 0; position < keyChunk.getPositionCount(); position++) {
            int bucket = bucketGenerator.getPartition(keyChunk, position);
            partitionAssignments[bucket].add(position);
        }

        // build a page for each partition

        for (int bucket = 0; bucket < totalBucketNum; bucket++) {
            List<Integer> positions = partitionAssignments[bucket];
            if (!positions.isEmpty()) {
                ChunkBuilder builder = new ChunkBuilder(columnTypes, positions.size(), new ExecutionContext());
                for (Integer pos : positions) {
                    builder.declarePosition();
                    for (int i = 0; i < chunk.getBlockCount(); i++) {
                        builder.appendTo(chunk.getBlock(i), i, pos);
                    }
                }
                bucketChunks.add(builder.build());
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
