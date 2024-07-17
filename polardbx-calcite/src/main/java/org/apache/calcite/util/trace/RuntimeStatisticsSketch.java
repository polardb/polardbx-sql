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

package org.apache.calcite.util.trace;

/**
 * Sketch of runtime statistics to display
 *
 */
public class RuntimeStatisticsSketch {

    /**
     * Total start-up duration in Seconds
     */
    protected final double startupDuration;

    /**
     * Total duration in Seconds
     */
    protected final double duration;

    /**
     * Total duration of worker threads run by this operator
     */
    protected final double workerDuration;

    /**
     * The duration of closing operator
     */
    //protected final double closeDuration;

    /**
     * Sum of produced rows;
     */
    protected final long rowCount;

    protected final long runtimeFilteredRowCount;

    /**
     * Sum of produced bytes;
     */
    protected final long outputBytes;

    /**
     * Sum of used memory in Bytes
     */
    protected final long memory;

    /**
     * Count the instance of operator
     */
    protected final int instances;

    /**
     * Count the spillCnt of operator
     */
    protected final int spillCnt;

    public RuntimeStatisticsSketch(double startupDuration, double duration, double workerDuration, long rowCount,
                                   long runtimeFilteredRowCount,
                                   long outputBytes, long memory, int instances, int spillCnt) {
        this.startupDuration = startupDuration;
        this.duration = duration;
        this.workerDuration = workerDuration;
        this.rowCount = rowCount;
        this.runtimeFilteredRowCount = runtimeFilteredRowCount;
        this.outputBytes = outputBytes;
        this.memory = memory;
        this.instances = instances;
        this.spillCnt = spillCnt;
    }

    public double getDuration() {
        return duration;
    }

    public double getStartupDuration() {
        return startupDuration;
    }

    public double getWorkerDuration() {
        return workerDuration;
    }

    public long getRowCount() {
        return rowCount;
    }

    public long getRuntimeFilteredRowCount() {
        return runtimeFilteredRowCount;
    }

    public long getMemory() {
        return memory;
    }

    public int getInstances() {
        return instances;
    }

    public long getOutputBytes() {
        return outputBytes;
    }

    public int getSpillCnt() {
        return spillCnt;
    }
}
