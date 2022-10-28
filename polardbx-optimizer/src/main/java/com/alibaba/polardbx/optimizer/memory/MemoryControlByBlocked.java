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

/**
 * @author lijiu.lzw
 *
 * 以阻塞的方式控制内存池大小，申请空间时会阻塞，直到空闲空间足够，以控制流量
 */
public class MemoryControlByBlocked {
    private final MemoryPool memoryPool;
    private final MemoryAllocatorCtx memoryAllocatorCtx;
    private final long notifyMemorySize;
    private boolean closed = false;

    private final Object lock = new Object();


    public MemoryControlByBlocked(MemoryPool pool) {
        memoryPool = pool;
        memoryAllocatorCtx = memoryPool.getMemoryAllocatorCtx();
        notifyMemorySize = memoryPool.getMaxLimit() / 2;
    }
    public MemoryControlByBlocked(MemoryPool pool, MemoryAllocatorCtx ctx) {
        memoryPool = pool;
        memoryAllocatorCtx = ctx;
        notifyMemorySize = memoryPool.getMaxLimit() / 2;
    }

    public void allocate(long memorySize) {
        synchronized(lock) {
            while (!closed) {
                try {
                    memoryAllocatorCtx.allocateReservedMemory(memorySize);
                    break;
                } catch (MemoryNotEnoughException e) {
                    //无法申请该内存大小
                    if (memorySize > memoryPool.getMaxLimit()) {
                        throw e;
                    }
                    try {
                        lock.wait();
                    } catch (InterruptedException ie) {
                        throw new RuntimeException(ie);
                    }
                }
            }
        }
    }

    public void release(long memorySize) {
        synchronized(lock) {
            //积累到大于Block_size，才会释放
            memoryAllocatorCtx.releaseReservedMemory(memorySize, false);
            if (memoryAllocatorCtx.getAllAllocated() <= notifyMemorySize) {
                lock.notifyAll();
            }
        }

    }

    public void close() {
        synchronized(lock) {
            closed = true;
            lock.notifyAll();
        }
    }
}
