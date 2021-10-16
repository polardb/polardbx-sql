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
import com.alibaba.polardbx.optimizer.chunk.IntegerBlock;
import com.alibaba.polardbx.optimizer.chunk.IntegerBlockBuilder;
import org.junit.Test;

import java.util.function.IntFunction;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class ChunkRowOpenHashMapTest {

    @Test
    public void test() {
        Chunk buildChunk1 = new Chunk(
            IntegerBlock.of(6, 5, 4, 3, 2, 1, 0), // key-0
            IntegerBlock.of(7, 8, 9, 4, 2, 6, 2)  // key-1
        );
        Chunk buildChunk2 = new Chunk(
            IntegerBlock.of(3, 2, 1, 0, 6, 5, 4), // key-0
            IntegerBlock.of(8, 9, 4, 2, 6, 2, 7)  // key-1
            //                       ^ duplicate!
        );

        Chunk probeChunk = new Chunk(
            IntegerBlock.of(6, 4, 0, 4, 4, 1, 0, 3), // key-0
            IntegerBlock.of(7, 9, 2, 9, 7, 4, 0, 8)  // key-1
        );

        ChunksIndex chunksIndex = new ChunksIndex();
        chunksIndex.addChunk(buildChunk1);
        chunksIndex.addChunk(buildChunk2);

        ChunkRowOpenHashMap hashMap = new ChunkRowOpenHashMap(chunksIndex);

        int[] putResult = new int[chunksIndex.getPositionCount()];
        for (int i = 0; i < chunksIndex.getPositionCount(); i++) {
            putResult[i] = hashMap.put(i, chunksIndex.hashCode(i));
        }

        int[] expectedPutResult = {-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 6, -1, -1, -1};
        assertArrayEquals(expectedPutResult, putResult);

        int[] getResult = new int[probeChunk.getPositionCount()];
        for (int i = 0; i < probeChunk.getPositionCount(); i++) {
            getResult[i] = hashMap.get(probeChunk, i);
        }

        int[] expectedGetResult = {0, 2, 10, 2, 13, 9, -1, 7};
        assertArrayEquals(expectedGetResult, getResult);
    }

    private final int CHUNK_COUNT = 100;
    private final int CHUNK_SIZE = 1000;
    private final int TOTAL_ROWS = CHUNK_SIZE * CHUNK_COUNT;

    private final int NUM_THREADS = 4;

    @Test
    public void testMultiThreads() throws InterruptedException {
        ChunksIndex chunksIndex = new ChunksIndex();
        for (int chunkId = 0; chunkId < CHUNK_COUNT; chunkId++) {
            IntegerBlockBuilder builder = new IntegerBlockBuilder(CHUNK_SIZE);
            for (int position = 0; position < CHUNK_SIZE; position++) {
                builder.writeInt(chunkId * CHUNK_SIZE + position);
            }
            Block block = builder.build();
            Chunk chunk = new Chunk(block);
            chunksIndex.addChunk(chunk);
        }

        ChunkRowOpenHashMap hashMap = new ChunkRowOpenHashMap(chunksIndex);

        IntFunction<Runnable> taskBuilder = startPosition -> () -> {
            for (int i = startPosition; i < TOTAL_ROWS; i += NUM_THREADS) {
                hashMap.put(i, chunksIndex.hashCode(i));
            }
        };

        Thread[] threads = new Thread[NUM_THREADS];
        for (int i = 0; i < NUM_THREADS; i++) {
            threads[i] = new Thread(taskBuilder.apply(i));
            threads[i].start();
        }

        for (int i = 0; i < NUM_THREADS; i++) {
            threads[i].join();
        }

        for (int value = 0; value < TOTAL_ROWS; value++) {
            assertEquals(value, hashMap.get(new Chunk(IntegerBlock.of(value)), 0));
        }
    }
}
