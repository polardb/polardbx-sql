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

package com.alibaba.polardbx.optimizer.statis;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.openjdk.jol.info.ClassLayout;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Runtime Statistics of a single operator
 *
 */
public class OperatorStatistics {

    public static long INSTANCE_MEM_SIZE = ClassLayout.parseClass(OperatorStatistics.class).instanceSize();

    /**
     * Count of row produced by this operator
     */
    protected long rowCount = 0;

    protected long runtimeFilteredCount = 0;

    /**
     * Memory consumed by this operator (in bytes)
     */
    protected long memory = 0;

    /**
     * CPU or network time during opening in nanoseconds
     */
    protected long startupDuration = 0;

    /**
     * CPU or network time during execution in nanoseconds
     */
    protected long processDuration = 0;

    /**
     * CPU or network time during closing in nanoseconds
     */
    protected long closeDuration = 0;

    /**
     * CPU time consumed by worker threads run by this operator
     */
    protected final AtomicLong workerDuration = new AtomicLong(0);

    protected final AtomicInteger spillCnt = new AtomicInteger(0);

    public OperatorStatistics() {

    }

    @JsonCreator
    public OperatorStatistics(@JsonProperty("rowCount") long rowCount,
                              @JsonProperty("runtimeFilteredCount") long runtimeFilteredCount,
                              @JsonProperty("memory") long memory,
                              @JsonProperty("startupDuration") long startupDuration,
                              @JsonProperty("processDuration") long processDuration,
                              @JsonProperty("closeDuration") long closeDuration,
                              @JsonProperty("spillCnt") int spillCnt) {
        this.rowCount = rowCount;
        this.runtimeFilteredCount = runtimeFilteredCount;
        this.memory = memory;
        this.startupDuration = startupDuration;
        this.processDuration = processDuration;
        this.closeDuration = closeDuration;
        this.spillCnt.addAndGet(spillCnt);
    }

    @JsonProperty
    public long getRowCount() {
        return rowCount;
    }

    public void addRowCount(long rowCount) {
        this.rowCount += rowCount;
    }

    @JsonProperty
    public long getRuntimeFilteredCount() {
        return runtimeFilteredCount;
    }

    public void addRuntimeFilteredCount(long filteredCount) {
        this.runtimeFilteredCount += filteredCount;
    }

    @JsonProperty
    public long getStartupDuration() {
        return startupDuration;
    }

    public void addStartupDuration(long nanoseconds) {
        this.startupDuration += nanoseconds;
    }

    public void setStartupDuration(long nanoseconds) {
        this.startupDuration = nanoseconds;
    }

    @JsonProperty
    public long getProcessDuration() {
        return processDuration;
    }

    public void addProcessDuration(long nanoseconds) {
        this.processDuration += nanoseconds;
    }

    public void addCloseDuration(long nanoseconds) {
        this.closeDuration += nanoseconds;
    }

    @JsonProperty
    public long getMemory() {
        return memory;
    }

    public void addMemory(long bytes) {
        this.memory += bytes;
    }

    public void addWorkerDuration(long nanoseconds) {
        this.workerDuration.addAndGet(nanoseconds);
    }

    @JsonProperty
    public long getWorkerDuration() {
        return workerDuration.get();
    }

    @JsonProperty
    public long getCloseDuration() {
        return closeDuration;
    }

    public void setCloseDuration(long closeDuration) {
        this.closeDuration = closeDuration;
    }

    @JsonProperty
    public int getSpillCnt() {
        return spillCnt.get();
    }

    public void addSpillCnt(int cnt) {
        this.spillCnt.addAndGet(cnt);
    }
}
