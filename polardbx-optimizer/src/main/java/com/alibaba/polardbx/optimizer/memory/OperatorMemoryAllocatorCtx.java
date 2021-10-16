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
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.alibaba.polardbx.common.exception.MemoryNotEnoughException;
import com.alibaba.polardbx.common.properties.MppConfig;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;

/**
 * Encapsulating the logic of applying memory which need align to the 1Mb.
 * This allocator can allocate the reserved & revocable memory, and it need record the blocking flag.
 * It usually is used by operators.
 */
@NotThreadSafe
public class OperatorMemoryAllocatorCtx implements MemoryAllocatorCtx {

    private final MemoryPool memoryPool;
    private final AtomicLong reservedFree = new AtomicLong(0L);

    private final AtomicLong reservedAllocated = new AtomicLong(0L);

    private final AtomicLong revocableFree = new AtomicLong(0L);

    private final AtomicLong revocableAllocated = new AtomicLong(0L);

    private final AtomicReference<MemoryNotFuture> allocateBytesFuture;
    private final AtomicReference<MemoryNotFuture> tryAllocateBytesFuture;

    private final MemoryAllocateFuture allocateFuture = new MemoryAllocateFuture();

    private SettableFuture<?> memoryRevokingRequestedFuture;

    private final boolean revocable;

    public OperatorMemoryAllocatorCtx(MemoryPool memoryPool, boolean revocable) {
        this.memoryPool = memoryPool;
        this.allocateBytesFuture = new AtomicReference<>();
        this.allocateBytesFuture.set(MemoryNotFuture.create());
        this.allocateBytesFuture.get().set(null);

        this.tryAllocateBytesFuture = new AtomicReference<>();
        this.tryAllocateBytesFuture.set(MemoryNotFuture.create());
        this.tryAllocateBytesFuture.get().set(null);

        this.revocable = revocable;
        if (this.revocable) {
            memoryRevokingRequestedFuture = SettableFuture.create();
        }
    }

    @Override
    public void allocateReservedMemory(long bytes) {
        long left = reservedFree.addAndGet(-bytes);
        if (left < 0) {
            // Align to block size
            long amount = -Math.floorDiv(left, BLOCK_SIZE) * BLOCK_SIZE;
            try {
                updateMemoryFuture(memoryPool.allocateReserveMemory(amount), allocateBytesFuture, false);
            } catch (MemoryNotEnoughException t) {
                reservedFree.addAndGet(bytes);
                throw t;
            }
            reservedFree.addAndGet(amount);
            reservedAllocated.addAndGet(amount);
        }
    }

    @Override
    public boolean tryAllocateReservedMemory(long bytes) {
        Preconditions.checkState(revocable, "Don't  allocate memory in the un-spill mode!");
        long left = reservedFree.addAndGet(-bytes);
        allocateFuture.reset();
        if (left < 0) {
            long allocated = -left;
            if (this.memoryPool.tryAllocateReserveMemory(allocated, allocateFuture)) {
                reservedAllocated.addAndGet(allocated);
                reservedFree.addAndGet(allocated);
                return true;
            } else {
                reservedFree.addAndGet(bytes);
                ListenableFuture<?> driverFuture = allocateFuture.getAllocateFuture();
                updateMemoryFuture(driverFuture, tryAllocateBytesFuture, true);
                return false;
            }
        } else {
            return true;
        }
    }

    @Override
    public boolean tryAllocateRevocableMemory(long bytes) {
        Preconditions.checkState(revocable, "Don't  allocate memory in the un-spill mode!");
        long left = revocableFree.addAndGet(-bytes);
        allocateFuture.reset();
        if (left < 0) {
            long allocated = -left;
            if (this.memoryPool.tryAllocateRevocableMemory(allocated, allocateFuture)) {
                revocableAllocated.addAndGet(allocated);
                revocableFree.addAndGet(allocated);
                return true;
            } else {
                revocableFree.addAndGet(bytes);
                ListenableFuture<?> driverFuture = allocateFuture.getAllocateFuture();
                updateMemoryFuture(driverFuture, tryAllocateBytesFuture, true);
                return false;
            }
        } else {
            return true;
        }
    }

    @Override
    public long getReservedAllocated() {
        return reservedAllocated.get();
    }

    @Override
    public void allocateRevocableMemory(long bytes) {
        Preconditions.checkState(revocable, "Don't allocate memory in the un-spill mode!");
        long left = revocableFree.addAndGet(-bytes);
        if (left < 0) {
            // Align to block size
            long amount = -Math.floorDiv(left, BLOCK_SIZE) * BLOCK_SIZE;
            try {
                updateMemoryFuture(memoryPool.allocateRevocableMemory(amount), allocateBytesFuture, false);
            } catch (MemoryNotEnoughException t) {
                revocableFree.addAndGet(bytes);
                throw t;
            }
            revocableFree.addAndGet(amount);
            revocableAllocated.addAndGet(amount);
        }
    }

    @Override
    public void releaseReservedMemory(long bytes, boolean immediately) {
        if (immediately) {
            long alreadyAllocated = reservedAllocated.get();
            if (alreadyAllocated - bytes < reservedFree.get()) {
                memoryPool.freeReserveMemory(reservedAllocated.getAndSet(0));
                reservedFree.set(0);
            } else {
                long actualFreeSize = Math.min(alreadyAllocated, bytes);
                reservedAllocated.addAndGet(-actualFreeSize);
                memoryPool.freeReserveMemory(actualFreeSize);
            }
        } else if (reservedAllocated.get() > reservedFree.addAndGet(bytes)) {
            if (reservedFree.get() >= BLOCK_SIZE) {
                long freeSize = reservedFree.getAndSet(0);
                long actualFreeSize = Math.min(revocableAllocated.get(), freeSize);
                reservedAllocated.addAndGet(-actualFreeSize);
                memoryPool.freeReserveMemory(actualFreeSize);
            }
        } else {
            memoryPool.freeReserveMemory(reservedAllocated.getAndSet(0));
            reservedFree.set(0);
        }
    }

    @Override
    public void releaseRevocableMemory(long bytes, boolean immediately) {
        Preconditions.checkState(revocable, "Don't allocate memory in the reserved mode!");
        if (immediately) {
            long alreadyAllocated = revocableAllocated.get();
            if (alreadyAllocated - bytes <= revocableFree.get()) {
                memoryPool.freeRevocableMemory(revocableAllocated.getAndSet(0));
                revocableFree.set(0);
            } else {
                long actualFreeSize = Math.min(alreadyAllocated, bytes);
                revocableAllocated.addAndGet(-actualFreeSize);
                memoryPool.freeRevocableMemory(actualFreeSize);
            }
        } else if (revocableAllocated.get() > revocableFree.addAndGet(bytes)) {
            if (revocableFree.get() >= BLOCK_SIZE) {
                long alreadyAllocated = revocableAllocated.get();
                long freeSize = revocableFree.getAndSet(0);
                long actualFreeSize = Math.min(alreadyAllocated, freeSize);
                revocableAllocated.addAndGet(-actualFreeSize);
                memoryPool.freeRevocableMemory(actualFreeSize);
            }
        } else {
            memoryPool.freeRevocableMemory(revocableAllocated.getAndSet(0));
            revocableFree.set(0);
        }
    }

    @Override
    public long getRevocableAllocated() {
        return revocableAllocated.get();
    }

    public ListenableFuture<?> isWaitingForMemory() {
        return allocateBytesFuture.get();
    }

    public ListenableFuture<?> isWaitingForTryMemory() {
        return tryAllocateBytesFuture.get();
    }

    public synchronized SettableFuture<?> getMemoryRevokingRequestedFuture() {
        return memoryRevokingRequestedFuture;
    }

    public synchronized long requestMemoryRevokingOrReturnRevokingBytes() {
        checkState(revocable, "requestMemoryRevoking for unRevocable operator");
        boolean alreadyRequested = isMemoryRevokingRequested();
        if (!alreadyRequested && revocableAllocated.get() > 0) {
            memoryRevokingRequestedFuture.set(null);
            return revocableAllocated.get();
        }
        if (alreadyRequested) {
            return revocableAllocated.get();
        }
        return 0;
    }

    public synchronized boolean isMemoryRevokingRequested() {
        if (!revocable) {
            return false;
        }
        return memoryRevokingRequestedFuture.isDone();
    }

    public synchronized void resetMemoryRevokingRequested() {
        if (!revocable) {
            return;
        }
        SettableFuture<?> currentFuture = memoryRevokingRequestedFuture;
        if (!currentFuture.isDone()) {
            return;
        }
        memoryRevokingRequestedFuture = SettableFuture.create();
    }

    private void updateMemoryFuture(ListenableFuture<?> memoryPoolFuture,
                                    AtomicReference<MemoryNotFuture> targetFutureReference, boolean isTry) {
        if (memoryPoolFuture != null && !memoryPoolFuture.isDone()
            && (isTry || getAllAllocated() > MppConfig.getInstance().getLessRevokeBytes() / 8)) {
            //如果不是尝试性申请内存的话，则不阻塞 4MB 以下的算子
            MemoryNotFuture<?> currentMemoryFuture = targetFutureReference.get();
            if (currentMemoryFuture.isDone()) {
                MemoryNotFuture<?> settableFuture = MemoryNotFuture.create();
                targetFutureReference.set(settableFuture);
            }
            MemoryNotFuture<?> finalMemoryFuture = targetFutureReference.get();
            // Create a new future, so that this operator can un-block before the pool does, if it's moved to a new pool
            Futures.addCallback(memoryPoolFuture, new FutureCallback<Object>() {
                @Override
                public void onSuccess(Object result) {
                    finalMemoryFuture.set(null);
                }

                @Override
                public void onFailure(Throwable t) {
                    finalMemoryFuture.set(null);
                }
            }, directExecutor());
        }
    }

    @Override
    public String getName() {
        return memoryPool.getName();
    }

    public boolean isRevocable() {
        return revocable;
    }

    @Override
    public long getAllAllocated() {
        return reservedAllocated.get() + revocableAllocated.get();
    }

    @VisibleForTesting
    public AtomicLong getReservedFree() {
        return reservedFree;
    }

    @VisibleForTesting
    public AtomicLong getRevocableFree() {
        return revocableFree;
    }
}
