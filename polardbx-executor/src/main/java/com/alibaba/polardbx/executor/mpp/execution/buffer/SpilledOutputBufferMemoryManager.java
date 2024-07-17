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

package com.alibaba.polardbx.executor.mpp.execution.buffer;

import com.alibaba.polardbx.common.exception.MemoryNotEnoughException;
import com.alibaba.polardbx.executor.mpp.execution.SystemMemoryUsageListener;
import com.google.common.util.concurrent.SettableFuture;

import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;

public class SpilledOutputBufferMemoryManager extends OutputBufferMemoryManager {

    private final AtomicLong extBytes = new AtomicLong();

    public SpilledOutputBufferMemoryManager(long maxBufferedBytes,
                                            SystemMemoryUsageListener systemMemoryUsageListener,
                                            Executor notificationExecutor) {
        super(maxBufferedBytes, systemMemoryUsageListener, notificationExecutor);
    }

    @Override
    public synchronized void updateMemoryUsage(long bytesAdded) {
        try {
            bufferedBytes.addAndGet(bytesAdded);
            if (extBytes.get() > 0 && bytesAdded < 0) {
                long delta = extBytes.addAndGet(bytesAdded);
                if (delta < 0) {
                    extBytes.set(0);
                    systemMemoryUsageListener.updateSystemMemoryUsage(delta);
                }
            } else {
                systemMemoryUsageListener.updateSystemMemoryUsage(bytesAdded);
            }
        } catch (MemoryNotEnoughException e) {
            //占用extBytes空间后，会触发isFull，阻塞上游继续输入
            extBytes.addAndGet(bytesAdded);
        }
        if (!isFull() && !notFull.isDone()) {
            // Complete future in a new thread to avoid making a callback on the caller thread.
            // This make is easier for callers to use this class since they can update the memory
            // usage while holding locks.
            SettableFuture<?> future = this.notFull;
            notificationExecutor.execute(() -> future.set(null));
        }
    }

    @Override
    public boolean isFull() {
        return (bufferedBytes.get() > maxBufferedBytes || extBytes.get() > 0) && blockOnFull.get();
    }
}
