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

package com.alibaba.polardbx.optimizer.memory;

import com.alibaba.polardbx.common.exception.MemoryNotEnoughException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class BlockingMemoryPoolTest {

    private long globalMaxLimit = MemoryAllocatorCtx.BLOCK_SIZE * 1024;
    private long queryMaxLimit = MemoryAllocatorCtx.BLOCK_SIZE * 512;
    private double maxElasticThreshold = 0.8;

    private GlobalMemoryPool root;
    private MemoryPool memoryPool;

    @Before
    public void before() {
        MemorySetting.ENABLE_SPILL = true;
        root = new GlobalMemoryPool("root", globalMaxLimit);
        memoryPool = root.getOrCreatePool(MemoryType.OTHER);
        root.setMaxElasticMemory(maxElasticThreshold);
    }

    @After
    public void after() {
        MemorySetting.ENABLE_SPILL = false;
    }

    @Test
    public void testAllocateMemory() {
        QueryMemoryPool queryMemoryPool = (QueryMemoryPool) memoryPool.getOrCreatePool(
            "query", queryMaxLimit, MemoryType.QUERY);
        queryMemoryPool.setMaxElasticMemory(maxElasticThreshold);
        queryMemoryPool.allocateReserveMemory(queryMaxLimit - 1);
        Assert.assertTrue(queryMemoryPool.getBlockedFuture() == null);
        queryMemoryPool.freeReserveMemory(queryMaxLimit - 1);

        queryMemoryPool.allocateRevocableMemory(queryMaxLimit / 2);
        Assert.assertTrue(queryMemoryPool.getBlockedFuture() == null);
        queryMemoryPool.allocateReserveMemory(queryMaxLimit / 2 - 1);

        Assert.assertFalse(queryMemoryPool.getBlockedFuture().isDone());

        Assert.assertEquals(queryMaxLimit / 2 - 1, queryMemoryPool.getMinRequestMemory());
        queryMemoryPool.freeReserveMemory(1);
        Assert.assertFalse(queryMemoryPool.getBlockedFuture().isDone());
        queryMemoryPool.freeReserveMemory(queryMaxLimit / 2);
        Assert.assertFalse(queryMemoryPool.getBlockedFuture().isDone());
        queryMemoryPool.freeReserveMemory(queryMaxLimit / 4);
        Assert.assertFalse(queryMemoryPool.getBlockedFuture().isDone());
        queryMemoryPool.freeRevocableMemory(queryMaxLimit / 4);
        Assert.assertTrue(queryMemoryPool.getBlockedFuture().isDone());
    }

    @Test
    public void testNotBlockingMemory() {
        try {
            for (int i = 0; i < 1024; i++) {
                QueryMemoryPool queryMemoryPool = (QueryMemoryPool) memoryPool.getOrCreatePool(
                    "query" + i, queryMaxLimit, MemoryType.QUERY);
                queryMemoryPool.setMaxElasticMemory(maxElasticThreshold);
                queryMemoryPool.allocateReserveMemory(MemoryAllocatorCtx.BLOCK_SIZE * 4);
                if (queryMemoryPool.getBlockedFuture() != null && !queryMemoryPool.getBlockedFuture().isDone()) {
                    throw new AssertionError();
                }
            }
            throw new AssertionError();
        } catch (MemoryNotEnoughException t) {
            //ignore
        }
    }

    @Test
    public void testTryMemory1() {
        try {
            MemoryAllocateFuture allocateFuture = new MemoryAllocateFuture();
            for (int i = 0; i < 1024; i++) {
                QueryMemoryPool queryMemoryPool = (QueryMemoryPool) memoryPool.getOrCreatePool(
                    "query" + i, queryMaxLimit, MemoryType.QUERY);
                queryMemoryPool.setMaxElasticMemory(maxElasticThreshold);
                queryMemoryPool.tryAllocateReserveMemory(MemoryAllocatorCtx.BLOCK_SIZE * 4, allocateFuture);
            }
        } catch (MemoryNotEnoughException t) {
            //ignore
        }
    }

    @Test
    public void testTryMemory2() {
        MemoryAllocateFuture allocateFuture = new MemoryAllocateFuture();
        QueryMemoryPool queryMemoryPool = (QueryMemoryPool) memoryPool.getOrCreatePool(
            "query", queryMaxLimit, MemoryType.QUERY);
        queryMemoryPool.allocateRevocableMemory(MemoryAllocatorCtx.BLOCK_SIZE * 4);
        for (int i = 0; i < 1024; i++) {
            queryMemoryPool = (QueryMemoryPool) memoryPool.getOrCreatePool(
                "query" + i, queryMaxLimit, MemoryType.QUERY);
            queryMemoryPool.setMaxElasticMemory(maxElasticThreshold);
            boolean ret = queryMemoryPool.tryAllocateReserveMemory(MemoryAllocatorCtx.BLOCK_SIZE * 4, allocateFuture);
            if (!ret && !allocateFuture.getAllocateFuture().isDone()) {
                return;
            }
        }
        throw new AssertionError();
    }
}
