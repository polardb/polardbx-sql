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

package com.alibaba.polardbx.executor.pl;

import com.alibaba.polardbx.optimizer.memory.MemoryPool;
import com.alibaba.polardbx.optimizer.spill.QuerySpillSpaceMonitor;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class PlContext {
    long cursorMemoryLimit;

    QuerySpillSpaceMonitor spillMonitor;

    AtomicLong cursorId = new AtomicLong(0);

    private AtomicReference<ProcedureStatus> status = new AtomicReference(ProcedureStatus.RUNNING);

    private MemoryPool currentMemoryPool;

    public PlContext(long cursorMemoryLimit) {
        this.cursorMemoryLimit = cursorMemoryLimit;
    }

    public long getNextCursorId() {
        return cursorId.addAndGet(1);
    }

    public long getCursorMemoryLimit() {
        return cursorMemoryLimit;
    }

    public void setCursorMemoryLimit(long cursorMemoryLimit) {
        this.cursorMemoryLimit = cursorMemoryLimit;
    }

    public QuerySpillSpaceMonitor getSpillMonitor() {
        return spillMonitor;
    }

    public void setSpillMonitor(QuerySpillSpaceMonitor spillMonitor) {
        this.spillMonitor = spillMonitor;
    }

    public AtomicReference<ProcedureStatus> getStatus() {
        return status;
    }

    public boolean setStatus(ProcedureStatus expectStatus) {
        while (true) {
            ProcedureStatus currentStatus = status.get();
            if (currentStatus == expectStatus) {
                return false;
            }
            if (status.compareAndSet(currentStatus, expectStatus)) {
                return true;
            }
        }
    }

    public MemoryPool getCurrentMemoryPool() {
        return currentMemoryPool;
    }

    public void setCurrentMemoryPool(MemoryPool currentMemoryPool) {
        this.currentMemoryPool = currentMemoryPool;
    }
}
