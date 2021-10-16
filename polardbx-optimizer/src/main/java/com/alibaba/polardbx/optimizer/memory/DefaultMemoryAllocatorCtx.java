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

import com.google.common.annotations.VisibleForTesting;
import com.alibaba.polardbx.common.exception.MemoryNotEnoughException;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Encapsulating the logic of applying memory which need align to the 1Mb.
 * This allocator only allocate the reserved memory.
 */
@NotThreadSafe
public class DefaultMemoryAllocatorCtx implements MemoryAllocatorCtx {

    private final MemoryPool memoryPool;
    private final AtomicLong free = new AtomicLong(0L);
    private final AtomicLong allocated = new AtomicLong(0L);

    public DefaultMemoryAllocatorCtx(MemoryPool memoryPool) {
        this.memoryPool = memoryPool;
    }

    @Override
    public void allocateReservedMemory(long bytes) {
        long left = free.addAndGet(-bytes);
        if (left < 0) {
            // Align to block size
            long amount = -Math.floorDiv(left, BLOCK_SIZE) * BLOCK_SIZE;
            try {
                memoryPool.allocateReserveMemory(amount);
            } catch (MemoryNotEnoughException t) {
                free.addAndGet(bytes);
                throw t;
            }
            free.addAndGet(amount);
            allocated.addAndGet(amount);
        }
    }

    @Override
    public void releaseReservedMemory(long bytes, boolean immediately) {
        if (immediately) {
            long alreadyAllocated = allocated.get();
            if (alreadyAllocated - bytes < free.get()) {
                memoryPool.freeReserveMemory(allocated.getAndSet(0));
                free.set(0);
            } else {
                long actualFreeSize = Math.min(alreadyAllocated, bytes);
                allocated.addAndGet(-actualFreeSize);
                memoryPool.freeReserveMemory(actualFreeSize);
            }
        } else if (allocated.get() > free.addAndGet(bytes)) {
            if (free.get() >= BLOCK_SIZE) {
                long freeSize = free.getAndSet(0);
                long actualFreeSize = Math.min(allocated.get(), freeSize);
                allocated.addAndGet(-actualFreeSize);
                memoryPool.freeReserveMemory(actualFreeSize);
            }
        } else {
            memoryPool.freeReserveMemory(allocated.getAndSet(0));
            free.set(0);
        }
    }

    @Override
    public long getReservedAllocated() {
        return allocated.get();
    }

    @Override
    public boolean tryAllocateReservedMemory(long bytes) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean tryAllocateRevocableMemory(long bytes) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void allocateRevocableMemory(long bytes) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void releaseRevocableMemory(long bytes, boolean immediately) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getRevocableAllocated() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getName() {
        return memoryPool.getName();
    }

    @Override
    public long getAllAllocated() {
        return allocated.get();
    }

    @VisibleForTesting
    public AtomicLong getFree() {
        return free;
    }
}


