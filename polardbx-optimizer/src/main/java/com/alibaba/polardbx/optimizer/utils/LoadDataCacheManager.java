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

package com.alibaba.polardbx.optimizer.utils;

import com.alibaba.polardbx.optimizer.memory.MemoryAllocatorCtx;
import com.alibaba.polardbx.optimizer.memory.MemoryPool;
import com.google.common.util.concurrent.SettableFuture;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;

public class LoadDataCacheManager {

    protected static final Logger log = LoggerFactory.getLogger(LoadDataCacheManager.class);

    private final long loadDataMaxMemory;
    private final long notFullThreshold;
    private final Object lock = new Object();

    protected SettableFuture<?> notFull;

    /**
     * buffer是否已经关闭
     */
    private volatile boolean closed = false;

    private MemoryPool loadDataMemoryPool;
    private MemoryAllocatorCtx memoryAllocatorCtx;

    public LoadDataCacheManager(MemoryPool loadDataMemoryPool, long loadDataMaxMemory) {
        this.loadDataMaxMemory = loadDataMaxMemory;
        this.notFullThreshold = loadDataMaxMemory / 2;
        this.notFull = SettableFuture.create();
        this.notFull.set(null);
        this.loadDataMemoryPool = loadDataMemoryPool;
        this.memoryAllocatorCtx = loadDataMemoryPool.getMemoryAllocatorCtx();
    }

    public SettableFuture<?> settableFuture() {
        synchronized (lock) {
            if (isFull() && notFull.isDone()) {
                notFull = SettableFuture.create();
            }
        }
        return notFull;
    }

    private void setNotFull() {
        //只有当内存释放小于notFullThreshold的时候，我们才重置notFull，为了避免频繁重置
        if (!notFull.isDone() && memoryAllocatorCtx.getReservedAllocated() < notFullThreshold) {
            notFull.set(null);
        }
    }

    /**
     * 防止使用过多的内存
     */
    public boolean isFull() {
        synchronized (lock) {
            return memoryAllocatorCtx.getAllAllocated() > loadDataMaxMemory;
        }
    }

    public boolean isBlocked() {
        synchronized (lock) {
            return !this.notFull.isDone() && isFull();
        }
    }

    public boolean isClose() {
        return closed;
    }

    public void close() {
        synchronized (lock) {
            if (closed) {
                return;
            }
            closed = true;
            loadDataMemoryPool.destroy();
            if (!notFull.isDone()) {
                notFull.set(null);
            }
        }
    }

    public void allocateMemory(long length) {
        if (length > 0) {
            synchronized (lock) {
                memoryAllocatorCtx.allocateReservedMemory(length);
            }
        }
    }

    public void releaseMemory(long length) {
        if (length > 0) {
            synchronized (lock) {
                memoryAllocatorCtx.releaseReservedMemory(length, true);
                setNotFull();
            }
        }
    }

    public long getNotFullThreshold() {
        return notFullThreshold;
    }

    public Object getLock() {
        return lock;
    }

    public SettableFuture<?> getNotFull() {
        return notFull;
    }
}