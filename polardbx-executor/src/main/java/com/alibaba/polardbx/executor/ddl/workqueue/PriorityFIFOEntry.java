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

package com.alibaba.polardbx.executor.ddl.workqueue;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @version 1.0
 */
public class PriorityFIFOEntry implements Comparable<PriorityFIFOEntry> {
    private static final AtomicLong seq = new AtomicLong(0);
    private final long seqNum;
    private final int priority;

    /**
     * @param priority smaller is more urgent.
     */
    public PriorityFIFOEntry(int priority) {
        seqNum = seq.getAndIncrement();
        this.priority = priority;
    }

    public int compareTo(PriorityFIFOEntry other) {
        if (priority != other.priority) {
            return priority < other.priority ? -1 : 1;
        } else if (seqNum != other.seqNum) {
            return seqNum < other.seqNum ? -1 : 1;
        }
        return 0;
    }
}
