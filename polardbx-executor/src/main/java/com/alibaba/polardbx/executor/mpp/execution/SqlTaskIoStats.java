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

import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

public final class SqlTaskIoStats {
    private static final AtomicLongFieldUpdater<SqlTaskIoStats> inputDataSizeUpdater =
        AtomicLongFieldUpdater.newUpdater(
            SqlTaskIoStats.class, "inputDataSizeLong");
    private volatile long inputDataSizeLong = 0;

    private static final AtomicLongFieldUpdater<SqlTaskIoStats> inputPositionsUpdater =
        AtomicLongFieldUpdater.newUpdater(
            SqlTaskIoStats.class, "inputPositionsLong");
    private volatile long inputPositionsLong = 0;

    private static final AtomicLongFieldUpdater<SqlTaskIoStats> outputDataSizeUpdater =
        AtomicLongFieldUpdater.newUpdater(
            SqlTaskIoStats.class, "outputDataSizeLong");
    private volatile long outputDataSizeLong = 0;

    private static final AtomicLongFieldUpdater<SqlTaskIoStats> outputPositionsUpdater =
        AtomicLongFieldUpdater.newUpdater(
            SqlTaskIoStats.class, "outputPositionsLong");
    private volatile long outputPositionsLong = 0;

    public SqlTaskIoStats() {
        this(0, 0, 0, 0);
    }

    public SqlTaskIoStats(long inputDataSize, long inputPositions, long outputDataSize, long outputPositions) {
        inputDataSizeUpdater.set(this, inputDataSize);
        inputPositionsUpdater.set(this, inputPositions);
        outputDataSizeUpdater.set(this, outputDataSize);
        outputPositionsUpdater.set(this, outputPositions);
    }

    @Managed
    @Nested
    public long getInputDataSize() {
        return inputDataSizeUpdater.get(this);
    }

    @Managed
    @Nested
    public long getInputPositions() {
        return inputPositionsUpdater.get(this);
    }

    @Managed
    @Nested
    public long getOutputDataSize() {
        return outputDataSizeUpdater.get(this);
    }

    @Managed
    @Nested
    public long getOutputPositions() {
        return outputPositionsUpdater.get(this);
    }

    public void merge(SqlTaskIoStats ioStats) {
        inputDataSizeUpdater.getAndAdd(this, ioStats.getInputDataSize());
        inputPositionsUpdater.getAndAdd(this, ioStats.getInputPositions());
        outputDataSizeUpdater.getAndAdd(this, ioStats.getOutputDataSize());
        outputPositionsUpdater.getAndAdd(this, ioStats.getOutputPositions());
    }

    @SuppressWarnings("deprecation")
    public void resetTo(SqlTaskIoStats ioStats) {
        inputDataSizeUpdater.set(this, ioStats.getInputDataSize());
        inputPositionsUpdater.set(this, ioStats.getInputPositions());
        outputDataSizeUpdater.set(this, ioStats.getOutputDataSize());
        outputPositionsUpdater.set(this, ioStats.getOutputPositions());
    }
}