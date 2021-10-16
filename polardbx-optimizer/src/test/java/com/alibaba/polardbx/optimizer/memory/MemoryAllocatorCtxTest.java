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

import org.junit.Assert;
import org.junit.Test;

public class MemoryAllocatorCtxTest {
    private long maxLimit = MemoryAllocatorCtx.BLOCK_SIZE * 1024;

    @Test
    public void testAllocateReservedCtx() {
        MemoryPool root = new MemoryPool("root", maxLimit, MemoryType.OTHER);
        MemoryAllocatorCtx defaultMemoryAllocatorCtx = root.getMemoryAllocatorCtx();
        defaultMemoryAllocatorCtx.allocateReservedMemory(1);
        Assert.assertEquals(MemoryAllocatorCtx.BLOCK_SIZE, defaultMemoryAllocatorCtx.getReservedAllocated());
        defaultMemoryAllocatorCtx.allocateReservedMemory(1011);
        Assert.assertEquals(MemoryAllocatorCtx.BLOCK_SIZE, defaultMemoryAllocatorCtx.getReservedAllocated());
        defaultMemoryAllocatorCtx.allocateReservedMemory(MemoryAllocatorCtx.BLOCK_SIZE - 512);
        Assert.assertEquals(MemoryAllocatorCtx.BLOCK_SIZE * 2, defaultMemoryAllocatorCtx.getReservedAllocated());
        defaultMemoryAllocatorCtx.allocateReservedMemory(MemoryAllocatorCtx.BLOCK_SIZE - 500);
        Assert.assertEquals(MemoryAllocatorCtx.BLOCK_SIZE * 2, defaultMemoryAllocatorCtx.getReservedAllocated());
        defaultMemoryAllocatorCtx.allocateReservedMemory(1);
        Assert.assertEquals(MemoryAllocatorCtx.BLOCK_SIZE * 3, defaultMemoryAllocatorCtx.getReservedAllocated());
        Assert.assertEquals(MemoryAllocatorCtx.BLOCK_SIZE * 3, root.getMemoryUsage());
    }

    @Test
    public void testFreeReservedCtx1() {
        MemoryPool root = new MemoryPool("root", maxLimit, MemoryType.OTHER);
        DefaultMemoryAllocatorCtx defaultMemoryAllocatorCtx = (DefaultMemoryAllocatorCtx) root.getMemoryAllocatorCtx();
        defaultMemoryAllocatorCtx.allocateReservedMemory(MemoryAllocatorCtx.BLOCK_SIZE / 2);
        Assert.assertEquals(MemoryAllocatorCtx.BLOCK_SIZE, defaultMemoryAllocatorCtx.getReservedAllocated());
        Assert.assertEquals(MemoryAllocatorCtx.BLOCK_SIZE / 2, defaultMemoryAllocatorCtx.getFree().get());
        defaultMemoryAllocatorCtx.releaseReservedMemory(1, true);
        Assert.assertEquals(MemoryAllocatorCtx.BLOCK_SIZE - 1, defaultMemoryAllocatorCtx.getReservedAllocated());
        Assert.assertEquals(MemoryAllocatorCtx.BLOCK_SIZE - 1, root.getMemoryUsage());
        Assert.assertEquals(MemoryAllocatorCtx.BLOCK_SIZE / 2, defaultMemoryAllocatorCtx.getFree().get());
        defaultMemoryAllocatorCtx.releaseReservedMemory(MemoryAllocatorCtx.BLOCK_SIZE / 2, true);
        Assert.assertEquals(0, root.getMemoryUsage());
        Assert.assertEquals(0, defaultMemoryAllocatorCtx.getFree().get());
        defaultMemoryAllocatorCtx.releaseReservedMemory(1, true);
        Assert.assertEquals(0, defaultMemoryAllocatorCtx.getReservedAllocated());
        Assert.assertEquals(0, root.getMemoryUsage());
        Assert.assertEquals(0, defaultMemoryAllocatorCtx.getFree().get());
        defaultMemoryAllocatorCtx.releaseReservedMemory(MemoryAllocatorCtx.BLOCK_SIZE, false);
        Assert.assertEquals(0, defaultMemoryAllocatorCtx.getReservedAllocated());
        Assert.assertEquals(0, root.getMemoryUsage());
        Assert.assertEquals(0, defaultMemoryAllocatorCtx.getFree().get());
    }

    @Test
    public void testFreeReservedCtx2() {
        MemoryPool root = new MemoryPool("root", maxLimit, MemoryType.OTHER);
        DefaultMemoryAllocatorCtx defaultMemoryAllocatorCtx = (DefaultMemoryAllocatorCtx) root.getMemoryAllocatorCtx();
        defaultMemoryAllocatorCtx.allocateReservedMemory(MemoryAllocatorCtx.BLOCK_SIZE / 2);
        defaultMemoryAllocatorCtx.releaseReservedMemory(maxLimit, false);
        Assert.assertEquals(0, defaultMemoryAllocatorCtx.getReservedAllocated());
        Assert.assertEquals(0, root.getMemoryUsage());
        Assert.assertEquals(0, defaultMemoryAllocatorCtx.getFree().get());
    }

    @Test
    public void testAllocatorMemoryException() {
        MemoryPool root = new MemoryPool("root", maxLimit, MemoryType.OTHER);
        DefaultMemoryAllocatorCtx defaultMemoryAllocatorCtx = (DefaultMemoryAllocatorCtx) root.getMemoryAllocatorCtx();

        try {
            defaultMemoryAllocatorCtx.allocateRevocableMemory(1);
            throw new AssertionError();
        } catch (UnsupportedOperationException t) {
            //ignore
        }

        try {
            defaultMemoryAllocatorCtx.releaseRevocableMemory(1, true);
            throw new AssertionError();
        } catch (UnsupportedOperationException t) {
            //ignore
        }

        defaultMemoryAllocatorCtx.releaseReservedMemory(1, true);
        defaultMemoryAllocatorCtx.releaseReservedMemory(1, false);
    }

    @Test
    public void testFreeRevocableCtx1() {
        MemoryPool root = new MemoryPool("root", maxLimit, MemoryType.OTHER);
        OperatorMemoryAllocatorCtx defaultMemoryAllocatorCtx = new OperatorMemoryAllocatorCtx(root, false);
        defaultMemoryAllocatorCtx.allocateReservedMemory(MemoryAllocatorCtx.BLOCK_SIZE / 2);
        defaultMemoryAllocatorCtx.releaseReservedMemory(maxLimit, false);
        Assert.assertEquals(0, defaultMemoryAllocatorCtx.getReservedAllocated());
        Assert.assertEquals(0, root.getMemoryUsage());
        Assert.assertEquals(0, defaultMemoryAllocatorCtx.getReservedFree().get());
    }

    @Test
    public void testFreeRevocableCtx2() {
        MemoryPool root = new MemoryPool("root", maxLimit, MemoryType.OTHER);
        OperatorMemoryAllocatorCtx defaultMemoryAllocatorCtx = new OperatorMemoryAllocatorCtx(root, false);
        defaultMemoryAllocatorCtx.allocateReservedMemory(MemoryAllocatorCtx.BLOCK_SIZE / 2);
        Assert.assertEquals(MemoryAllocatorCtx.BLOCK_SIZE, defaultMemoryAllocatorCtx.getReservedAllocated());
        Assert.assertEquals(MemoryAllocatorCtx.BLOCK_SIZE / 2, defaultMemoryAllocatorCtx.getReservedFree().get());
        defaultMemoryAllocatorCtx.releaseReservedMemory(1, true);
        Assert.assertEquals(MemoryAllocatorCtx.BLOCK_SIZE - 1, defaultMemoryAllocatorCtx.getReservedAllocated());
        Assert.assertEquals(MemoryAllocatorCtx.BLOCK_SIZE - 1, root.getMemoryUsage());
        Assert.assertEquals(MemoryAllocatorCtx.BLOCK_SIZE / 2, defaultMemoryAllocatorCtx.getReservedFree().get());
        defaultMemoryAllocatorCtx.releaseReservedMemory(MemoryAllocatorCtx.BLOCK_SIZE / 2, true);
        Assert.assertEquals(0, root.getMemoryUsage());
        Assert.assertEquals(0, defaultMemoryAllocatorCtx.getReservedFree().get());
        defaultMemoryAllocatorCtx.releaseReservedMemory(1, true);
        Assert.assertEquals(0, defaultMemoryAllocatorCtx.getReservedAllocated());
        Assert.assertEquals(0, root.getMemoryUsage());
        Assert.assertEquals(0, defaultMemoryAllocatorCtx.getReservedFree().get());
        defaultMemoryAllocatorCtx.releaseReservedMemory(MemoryAllocatorCtx.BLOCK_SIZE, false);
        Assert.assertEquals(0, defaultMemoryAllocatorCtx.getReservedAllocated());
        Assert.assertEquals(0, root.getMemoryUsage());
        Assert.assertEquals(0, defaultMemoryAllocatorCtx.getReservedFree().get());
    }

    @Test
    public void testFreeRevocableCtx3() {
        MemoryPool root = new MemoryPool("root", maxLimit, MemoryType.OTHER);
        OperatorMemoryAllocatorCtx defaultMemoryAllocatorCtx = new OperatorMemoryAllocatorCtx(root, true);
        defaultMemoryAllocatorCtx.allocateRevocableMemory(MemoryAllocatorCtx.BLOCK_SIZE / 2);
        defaultMemoryAllocatorCtx.releaseRevocableMemory(maxLimit, false);
        Assert.assertEquals(0, defaultMemoryAllocatorCtx.getRevocableAllocated());
        Assert.assertEquals(0, root.getMemoryUsage());
        Assert.assertEquals(0, defaultMemoryAllocatorCtx.getRevocableFree().get());
    }

    @Test
    public void testFreeRevocableCtx4() {
        MemoryPool root = new MemoryPool("root", maxLimit, MemoryType.OTHER);
        OperatorMemoryAllocatorCtx defaultMemoryAllocatorCtx = new OperatorMemoryAllocatorCtx(root, true);
        defaultMemoryAllocatorCtx.allocateRevocableMemory(MemoryAllocatorCtx.BLOCK_SIZE / 2);
        Assert.assertEquals(MemoryAllocatorCtx.BLOCK_SIZE, defaultMemoryAllocatorCtx.getRevocableAllocated());
        Assert.assertEquals(MemoryAllocatorCtx.BLOCK_SIZE / 2, defaultMemoryAllocatorCtx.getRevocableFree().get());
        defaultMemoryAllocatorCtx.releaseRevocableMemory(1, true);
        Assert.assertEquals(MemoryAllocatorCtx.BLOCK_SIZE - 1, defaultMemoryAllocatorCtx.getRevocableAllocated());
        Assert.assertEquals(MemoryAllocatorCtx.BLOCK_SIZE - 1, root.getMemoryUsage());
        Assert.assertEquals(MemoryAllocatorCtx.BLOCK_SIZE / 2, defaultMemoryAllocatorCtx.getRevocableFree().get());
        defaultMemoryAllocatorCtx.releaseRevocableMemory(MemoryAllocatorCtx.BLOCK_SIZE / 2, true);
        Assert.assertEquals(0, root.getMemoryUsage());
        Assert.assertEquals(0, defaultMemoryAllocatorCtx.getRevocableFree().get());
        defaultMemoryAllocatorCtx.releaseRevocableMemory(1, true);
        Assert.assertEquals(0, defaultMemoryAllocatorCtx.getRevocableAllocated());
        Assert.assertEquals(0, root.getMemoryUsage());
        Assert.assertEquals(0, defaultMemoryAllocatorCtx.getRevocableFree().get());
        defaultMemoryAllocatorCtx.releaseRevocableMemory(MemoryAllocatorCtx.BLOCK_SIZE, false);
        Assert.assertEquals(0, defaultMemoryAllocatorCtx.getRevocableAllocated());
        Assert.assertEquals(0, root.getMemoryUsage());
        Assert.assertEquals(0, defaultMemoryAllocatorCtx.getRevocableFree().get());
    }
}
