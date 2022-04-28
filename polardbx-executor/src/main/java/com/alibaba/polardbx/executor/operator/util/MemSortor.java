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

import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;
import com.google.common.util.concurrent.ListenableFuture;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.executor.chunk.BlockBuilders;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.operator.ProducerExecutor;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.executor.utils.OrderByOption;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.memory.MemoryAllocatorCtx;
import it.unimi.dsi.fastutil.ints.AbstractIntComparator;
import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.fastutil.ints.IntComparator;

import java.util.Iterator;
import java.util.List;

public class MemSortor extends Sorter {

    private BlockBuilder[] blockBuilders;
    private ChunksIndex chunksIndex;
    private IntComparator comparator;
    private int resultPosition;
    private int[] index;
    private boolean revokeMem;
    private ExecutionContext context;

    public MemSortor(MemoryAllocatorCtx memoryAllocator, List<OrderByOption> orderBys,
                     List<DataType> columnMetas, int chunkLimit, boolean revokeMem, ExecutionContext context) {
        super(memoryAllocator, orderBys, columnMetas, chunkLimit);
        this.context = context;
        this.revokeMem = revokeMem;
        blockBuilders = new BlockBuilder[columnMetas.size()];
        for (int i = 0; i < columnMetas.size(); i++) {
            blockBuilders[i] = BlockBuilders.create(columnMetas.get(i), context);
        }
        this.chunksIndex = new ChunksIndex();
        this.resultPosition = 0;
        this.comparator = new AbstractIntComparator() {
            @Override
            public int compare(int position1, int position2) {
                for (int i = 0; i < orderBys.size(); i++) {
                    int index = orderBys.get(i).getIndex();
                    Object o1 = chunksIndex.getObjectForCmp(index, position1);
                    Object o2 = chunksIndex.getObjectForCmp(index, position2);
                    if (o1 == null && o2 == null) {
                        continue;
                    }

                    int n = ExecUtils.comp(o1, o2, columnMetas.get(index), orderBys.get(i).isAsc());
                    if (n != 0) {
                        return n;
                    }
                }
                return 0;
            }

        };
    }

    @Override
    public void addChunk(Chunk chunk) {
        if (revokeMem) {
            memoryAllocator.allocateRevocableMemory(chunk.estimateSize());
        } else {
            memoryAllocator.allocateReservedMemory(chunk.estimateSize());
        }
        chunksIndex.addChunk(chunk);
    }

    @Override
    public void sort() {
        // allocate memory for index array
        if (revokeMem) {
            memoryAllocator.allocateRevocableMemory(chunksIndex.getPositionCount() * Integer.BYTES);
        } else {
            memoryAllocator.allocateReservedMemory(chunksIndex.getPositionCount() * Integer.BYTES);
        }

        // init index
        index = new int[chunksIndex.getPositionCount()];
        for (int i = 0; i < index.length; i++) {
            index[i] = i;
        }

        // sort
        IntArrays.quickSort(index, comparator);
    }

    @Override
    public Chunk nextChunk() {
        if (resultPosition < chunksIndex.getPositionCount()) {
            while (resultPosition < chunksIndex.getPositionCount()
                && blockBuilders[0].getPositionCount() < chunkLimit) {
                for (int i = 0; i < chunksIndex.getChunk(0).getBlockCount(); i++) {
                    blockBuilders[i].writeObject(chunksIndex.getObject(i, index[resultPosition]));
                }
                resultPosition++;
            }
            return buildChunkAndReset();
        } else {
            return null;
        }
    }

    public Iterator<Chunk> getSortedChunks() {
        return new AbstractIterator<Chunk>() {

            private Chunk chunk = null;

            @Override
            public Chunk computeNext() {
                chunk = nextChunk();
                if (chunk == null) {
                    return endOfData();
                }
                return chunk;
            }
        };
    }

    private Chunk buildChunkAndReset() {
        Block[] blocks = new Block[blockBuilders.length];
        for (int i = 0; i < blockBuilders.length; i++) {
            blocks[i] = blockBuilders[i].build();
        }
        for (int i = 0; i < blockBuilders.length; i++) {
            blockBuilders[i] = blockBuilders[i].newBlockBuilder();
        }
        return new Chunk(blocks);
    }

    public ChunksIndex getChunksIndex() {
        return chunksIndex;
    }

    public void reset() {
        Preconditions.checkArgument(revokeMem, "Only Release Revocable Memory!");
        memoryAllocator.releaseRevocableMemory(memoryAllocator.getRevocableAllocated(), true);
        this.chunksIndex = new ChunksIndex();
        resultPosition = 0;
    }

    @Override
    public void close() {
        index = null;
        chunksIndex = null;
        resultPosition = 0;
    }

    public boolean isEmpty() {
        return chunksIndex.getChunkCount() == 0;
    }

    @Override
    public ListenableFuture<?> startMemoryRevoke() {
        return ProducerExecutor.NOT_BLOCKED;
    }

    @Override
    public void finishMemoryRevoke() {

    }
}
