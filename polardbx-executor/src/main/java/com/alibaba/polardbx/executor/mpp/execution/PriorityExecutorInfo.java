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

package com.alibaba.polardbx.executor.mpp.execution;

public class PriorityExecutorInfo {
    private String name;
    /**
     * number of created threads in the pool
     */
    private int poolSize;
    /**
     * number of active threads in the pool
     */
    private int activeCount;
    /**
     * number of splits that have been scheduled
     */
    private long runnerProcessCount;
    /**
     * number of completed task
     * Notice: one task can be scheduled for many times,
     * and each time the task running is called one split.
     */
    private long completedTaskCount;
    /**
     * number of pending splits(waiting for schedule)
     */
    private int pendingSplitsSize;
    /**
     * number of blocked splits
     */
    private int blockedSplitSize;

    public PriorityExecutorInfo(String name, int poolSize, int activeCount, long runnerProcessCount,
                                long completedTaskCount, int pendingSplitsSize, int blockedSplitSize) {
        this.name = name;
        this.poolSize = poolSize;
        this.activeCount = activeCount;
        this.runnerProcessCount = runnerProcessCount;
        this.completedTaskCount = completedTaskCount;
        this.pendingSplitsSize = pendingSplitsSize;
        this.blockedSplitSize = blockedSplitSize;
    }

    public String getName() {
        return name;
    }

    public int getPoolSize() {
        return poolSize;
    }

    public int getActiveCount() {
        return activeCount;
    }

    public long getRunnerProcessCount() {
        return runnerProcessCount;
    }

    public long getCompletedTaskCount() {
        return completedTaskCount;
    }

    public int getPendingSplitsSize() {
        return pendingSplitsSize;
    }

    public int getBlockedSplitSize() {
        return blockedSplitSize;
    }

    public long getTotalTask() {
        return pendingSplitsSize + activeCount + blockedSplitSize + completedTaskCount;
    }
}
