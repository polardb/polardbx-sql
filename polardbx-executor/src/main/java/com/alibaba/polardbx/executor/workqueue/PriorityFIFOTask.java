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

package com.alibaba.polardbx.executor.workqueue;

/**
 * @version 1.0
 */
public abstract class PriorityFIFOTask extends PriorityFIFOEntry implements Runnable {

    public enum TaskPriority {
        REALTIME_TASK(0),
        HIGH_PRIORITY_TASK(10),
        MEDIUM_PRIORITY_TASK(20),
        LOW_PRIORITY_TASK(30),
        GSI_BACKFILL_TASK(40),
        GSI_CHECK_TASK(50);

        private final int priority;

        TaskPriority(int priority) {
            this.priority = priority;
        }

        public int getPriority() {
            return priority;
        }
    }

    /**
     * @param priority smaller is more urgent.
     */
    public PriorityFIFOTask(TaskPriority priority) {
        super(priority.getPriority());
    }
}
