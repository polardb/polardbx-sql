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

import com.google.common.math.IntMath;
import com.alibaba.polardbx.common.exception.MemoryNotEnoughException;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class MemoryPoolTest {

    private static final Logger logger = LoggerFactory.getLogger(MemoryPoolTest.class);

    private long maxLimit = MemoryAllocatorCtx.BLOCK_SIZE * 1024;
    private Random randomAllocateSize = new Random();
    private int allocateCount = 16;

    @Test
    public void testAllocateTryReservedMemory() {
        MemoryPool root = new MemoryPool("root", 1024, MemoryType.OTHER);
        boolean ret = root.tryAllocateReserveMemory(1011, new MemoryAllocateFuture());
        Assert.assertTrue(ret);
        ret = root.tryAllocateReserveMemory(13, new MemoryAllocateFuture());
        Assert.assertTrue(ret);
        try {
            root.tryAllocateReserveMemory(13, new MemoryAllocateFuture());
            throw new AssertionError();
        } catch (MemoryNotEnoughException t) {
            //ignore
        }
    }

    @Test
    public void testAllocateRevocableMemory() {
        MemoryPool root = new MemoryPool("root", maxLimit, MemoryType.OTHER);
        long allocatedSize = 0;
        for (int i = 0; i < allocateCount; i++) {
            allocatedSize += testAllocateMemory(root, true);
            Assert.assertEquals(allocatedSize, root.getMemoryUsage());
        }
        root.destroy();
        Assert.assertEquals(0L, root.getMemoryUsage());
        Assert.assertEquals(allocatedSize, root.getMaxMemoryUsage());
        Assert.assertTrue(root.isDestoryed());
    }

    @Test
    public void testAllocateReservedMemory() {
        MemoryPool root = new MemoryPool("root", maxLimit, MemoryType.OTHER);
        long allocatedSize = 0;
        for (int i = 0; i < allocateCount; i++) {
            allocatedSize += testAllocateMemory(root, true);
            Assert.assertEquals(allocatedSize, root.getMemoryUsage());
        }
        root.destroy();
        Assert.assertEquals(0L, root.getMemoryUsage());
        Assert.assertEquals(allocatedSize, root.getMaxMemoryUsage());
        Assert.assertTrue(root.isDestoryed());
    }

    @Test
    public void testAllocateAllMemory() {
        MemoryPool root = new MemoryPool("root", maxLimit, MemoryType.OTHER);
        long allocatedSize = 0;
        for (int i = 0; i < allocateCount; i++) {
            allocatedSize += testAllocateMemory(root, false);
            allocatedSize += testAllocateMemory(root, true);
            Assert.assertEquals(allocatedSize, root.getMemoryUsage());
        }
        root.destroy();
        Assert.assertEquals(0L, root.getMemoryUsage());
        Assert.assertEquals(allocatedSize, root.getMaxMemoryUsage());
        Assert.assertTrue(root.isDestoryed());
    }

    @Test
    public void testAllocateExceedMemory() {
        MemoryPool root = new MemoryPool("root", 1024, MemoryType.OTHER);
        try {
            root.allocateReserveMemory(1025);
            throw new AssertionError();
        } catch (MemoryNotEnoughException t) {
            //ignore
        }

        try {
            root.allocateRevocableMemory(1025);
            throw new AssertionError();
        } catch (MemoryNotEnoughException t) {
            //ignore
        }
        root.allocateReserveMemory(1011);

        try {
            root.allocateRevocableMemory(1011);
            throw new AssertionError();
        } catch (MemoryNotEnoughException t) {
            //ignore
        }

        root.freeReserveMemory(1011);

        root.allocateRevocableMemory(1024);

        root.freeRevocableMemory(1024);

        Assert.assertEquals(0L, root.getMemoryUsage());
    }

    @Test
    public void testAllocateAndReleaseMemory() {
        MemoryPool root = new MemoryPool("root", maxLimit, MemoryType.OTHER);
        long allocatedSize = 0;
        long maxAllocatedSize = 0L;
        for (int i = 0; i < allocateCount; i++) {
            allocatedSize += testAllocateMemory(root, false);
            allocatedSize += testAllocateMemory(root, true);
            maxAllocatedSize = Math.max(maxAllocatedSize, allocatedSize);
            allocatedSize -= testReleaseMemory(root, false);
            allocatedSize -= testReleaseMemory(root, true);
            Assert.assertEquals(allocatedSize, root.getMemoryUsage());
        }
        Assert.assertEquals(allocatedSize, root.getMemoryUsage());
        root.destroy();
        Assert.assertEquals(0L, root.getMemoryUsage());
        Assert.assertEquals(maxAllocatedSize, root.getMaxMemoryUsage());
        Assert.assertTrue(root.isDestoryed());
    }

    public long testAllocateMemory(MemoryPool pool, boolean reserved) {
        long allocatedSize = 0L;
        if (reserved) {
            allocatedSize = pool.getReservedBytes();
        } else {
            allocatedSize = pool.getRevocableBytes();
        }

        long perAllocate = randomAllocateSize.nextInt((int) MemoryAllocatorCtx.BLOCK_SIZE * 16);
        allocatedSize += perAllocate;
        if (reserved) {
            pool.allocateReserveMemory(perAllocate);
        } else {
            pool.allocateRevocableMemory(perAllocate);
        }
        Assert.assertEquals(allocatedSize, reserved ? pool.getReservedBytes() : pool.getRevocableBytes());
        return perAllocate;
    }

    public long testReleaseMemory(MemoryPool pool, boolean reserved) {
        long allocatedSize = 0L;
        if (reserved) {
            allocatedSize = pool.getReservedBytes();
        } else {
            allocatedSize = pool.getRevocableBytes();
        }

        long perReleaseMem = randomAllocateSize.nextInt((int) MemoryAllocatorCtx.BLOCK_SIZE * 16);
        if (reserved) {
            pool.freeReserveMemory(perReleaseMem);
        } else {
            pool.freeRevocableMemory(perReleaseMem);
        }
        Assert.assertEquals(
            Math.max(0, allocatedSize - perReleaseMem), reserved ? pool.getReservedBytes() : pool.getRevocableBytes());
        return Math.min(allocatedSize, perReleaseMem);
    }

    @Test
    public void concurrentMemoryTest() throws InterruptedException {
        MemoryPool root = new MemoryPool("root", 102400, MemoryType.OTHER);

        List<MemoryPool> allPools = new ArrayList<>();
        allPools.add(root);

        List<MemoryPool> lastLayer = new ArrayList<>();
        lastLayer.add(root);

        for (int layer = 1; layer <= 5; layer++) {
            List<MemoryPool> subPools = new ArrayList<>();
            for (MemoryPool pool : lastLayer) {
                for (int i = 0; i < 4; i++) {
                    long limit = 10240 / IntMath.pow(4, layer);
                    subPools.add(pool.getOrCreatePool(Integer.toString(i), limit, MemoryType.OTHER));
                }
            }
            lastLayer = subPools;
            allPools.addAll(subPools);
        }

        final List<MemoryPool> pools = lastLayer;

        ExecutorService executors = Executors.newFixedThreadPool(10);

        List<TestJob> testJobs = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            TestJob testJob = new TestJob(pools, i % 3 == 0, i % 5 == 0);
            executors.submit(testJob);
            testJobs.add(testJob);
        }
        executors.shutdown();
        executors.awaitTermination(10L, TimeUnit.SECONDS);

        long allocatedReserved = 0L;
        long allocatedRevocable = 0L;
        for (TestJob testJob : testJobs) {
            if (testJob.isReserved()) {
                allocatedReserved += testJob.getAllocated();
            } else {
                allocatedRevocable += testJob.getAllocated();
            }
        }
        for (MemoryPool pool : pools) {
            Assert.assertEquals(pool.getMaxLimit(), pool.getMemoryUsage() + pool.getFreeBytes());
        }
        Assert.assertEquals(root.getReservedBytes(), allocatedReserved);
        Assert.assertEquals(root.getRevocableBytes(), allocatedRevocable);

        // Expect all memory blocks are released automatically when destroy()
        for (MemoryPool pool : pools) {
            pool.destroy();
        }

        // Assert all memory blocks are freed
        for (MemoryPool pool : allPools) {
            Assert.assertEquals(0L, pool.getMemoryUsage());
        }
    }

    @Test
    public void concurrentTest() throws InterruptedException {
        MemoryPool root = new MemoryPool("root", 10240, MemoryType.OTHER);

        List<MemoryPool> allPools = new ArrayList<>();
        allPools.add(root);

        List<MemoryPool> lastLayer = new ArrayList<>();
        lastLayer.add(root);

        for (int layer = 1; layer <= 5; layer++) {
            List<MemoryPool> subPools = new ArrayList<>();
            for (MemoryPool pool : lastLayer) {
                for (int i = 0; i < 4; i++) {
                    long limit = 10240 / IntMath.pow(4, layer);
                    subPools.add(pool.getOrCreatePool(Integer.toString(i), limit, MemoryType.OTHER));
                }
            }
            lastLayer = subPools;
            allPools.addAll(subPools);
        }

        final List<MemoryPool> pools = lastLayer;

        ExecutorService executors = Executors.newFixedThreadPool(10);

        for (int i = 0; i < 100; i++) {
            executors.submit(new TestJob(pools, true, true));
        }
        executors.shutdown();
        executors.awaitTermination(10L, TimeUnit.SECONDS);

        for (MemoryPool pool : pools) {
            Assert.assertEquals(pool.getMaxLimit(), pool.getMemoryUsage() + pool.getFreeBytes());
        }

        // Expect all memory blocks are released automatically when destroy()
        for (MemoryPool pool : pools) {
            pool.destroy();
        }

        // Assert all memory blocks are freed
        for (MemoryPool pool : allPools) {
            Assert.assertEquals(0L, pool.getMemoryUsage());
        }
    }

    private class TestJob implements Runnable {

        private final List<MemoryPool> pools;
        private final Random random = new Random();
        private boolean release;
        private boolean reserved;
        private long allocated = 0L;

        TestJob(List<MemoryPool> pools, boolean release, boolean reserved) {
            this.pools = pools;
            this.release = release;
            this.reserved = reserved;
        }

        @Override
        public void run() {
            for (int t = 0; t < 100; t++) {
                int index = random.nextInt(pools.size());
                MemoryPool pool = pools.get(index);
                try {
                    doTest(pool);
                } catch (Exception e) {
                    logger.debug("Failed to allocate from " + pool.getName() + ": " + e.getMessage());
                }
            }
        }

        private void doTest(MemoryPool pool) {
            int num = random.nextInt(10) + 1;
            logger.debug("allocating " + num + " blocks from pool " + pool.getName());

            List<Long> allocatedSizes = new ArrayList<>();
            try {
                while (num-- > 0) {
                    if (reserved) {
                        pool.allocateReserveMemory(1);
                    } else {
                        pool.allocateRevocableMemory(1);
                    }
                    allocated += 1;
                    allocatedSizes.add(1L);
                }
            } finally {
                if (release) {
                    for (Long mb : allocatedSizes) {
                        // Simulate that sometimes allocated memory blocks were not
                        // freed by user
                        if (random.nextDouble() < 0.9) {
                            if (reserved) {
                                pool.freeReserveMemory(mb);
                            } else {
                                pool.freeRevocableMemory(mb);
                            }
                            allocated -= mb;
                        }
                    }
                }
            }
        }

        public long getAllocated() {
            return allocated;
        }

        public boolean isReserved() {
            return reserved;
        }
    }
}
