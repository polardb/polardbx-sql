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

/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.polardbx.executor.mpp.operator;

import com.google.common.collect.ImmutableSet;
import com.alibaba.polardbx.executor.mpp.deploy.ServiceProvider;
import com.alibaba.polardbx.executor.mpp.execution.PipelineContext;
import com.alibaba.polardbx.executor.mpp.execution.TaskId;
import com.alibaba.polardbx.executor.mpp.execution.TaskInfo;
import com.alibaba.polardbx.executor.mpp.execution.TaskState;
import com.alibaba.polardbx.executor.mpp.execution.TaskStatus;
import com.alibaba.polardbx.executor.mpp.execution.buffer.BufferState;
import com.alibaba.polardbx.executor.mpp.execution.buffer.OutputBufferInfo;
import com.alibaba.polardbx.executor.mpp.metadata.TaskLocation;
import com.alibaba.polardbx.executor.operator.SourceExec;
import org.joda.time.DateTime;
import org.joda.time.chrono.ISOChronology;
import org.weakref.jmx.internal.guava.collect.ImmutableList;

import javax.annotation.Nullable;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReference;

import static com.alibaba.polardbx.common.properties.MetricLevel.isSQLMetricEnabled;
import static java.util.Objects.requireNonNull;

/**
 * Only calling getDriverStats is ThreadSafe
 */
public class DriverContext {
    private static final ThreadMXBean THREAD_MX_BEAN = ManagementFactory.getThreadMXBean();

    // Atomic Updaters
    private static final AtomicLongFieldUpdater<DriverContext> startNanosUpdater =
        AtomicLongFieldUpdater.newUpdater(DriverContext.class, "startNanosLong");
    private static final AtomicLongFieldUpdater<DriverContext> endNanosUpdater =
        AtomicLongFieldUpdater.newUpdater(DriverContext.class, "endNanosLong");

    private static final AtomicLongFieldUpdater<DriverContext> intervalWallStartUpdater =
        AtomicLongFieldUpdater.newUpdater(DriverContext.class, "intervalWallStartLong");
    private static final AtomicLongFieldUpdater<DriverContext> intervalCpuStartUpdater =
        AtomicLongFieldUpdater.newUpdater(DriverContext.class, "intervalCpuStartLong");
    private static final AtomicLongFieldUpdater<DriverContext> intervalUserStartUpdater =
        AtomicLongFieldUpdater.newUpdater(DriverContext.class, "intervalUserStartLong");

    private static final AtomicLongFieldUpdater<DriverContext> processWallNanosUpdater =
        AtomicLongFieldUpdater.newUpdater(DriverContext.class, "processWallNanosLong");
    private static final AtomicLongFieldUpdater<DriverContext> processCpuNanosUpdater =
        AtomicLongFieldUpdater.newUpdater(DriverContext.class, "processCpuNanosLong");
    private static final AtomicLongFieldUpdater<DriverContext> processUserNanosUpdater =
        AtomicLongFieldUpdater.newUpdater(DriverContext.class, "processUserNanosLong");

    private static final AtomicLongFieldUpdater<DriverContext> blockedWallMillsUpdater =
        AtomicLongFieldUpdater.newUpdater(DriverContext.class, "blockedWallMillsLong");
    private static final AtomicLongFieldUpdater<DriverContext> blockedStartTimestampUpdater =
        AtomicLongFieldUpdater.newUpdater(DriverContext.class, "blockedStartTimestampLong");

    private final long createMillis = System.currentTimeMillis();

    private long driverOutputPosition = 0;

    // volatile members
    private volatile long startNanosLong = 0L;
    private volatile long endNanosLong = 0L;

    private volatile long intervalWallStartLong = 0L;
    private volatile long intervalCpuStartLong = 0L;
    private volatile long intervalUserStartLong = 0L;

    private volatile long processWallNanosLong = 0L;
    private volatile long processCpuNanosLong = 0L;
    private volatile long processUserNanosLong = 0L;

    private volatile long blockedWallMillsLong = 0L;
    private volatile long blockedStartTimestampLong = 0L;

    private final AtomicBoolean isBlocked = new AtomicBoolean(false);

    private final PipelineContext pipelineContext;

    private final AtomicBoolean finished = new AtomicBoolean();

    private final boolean partitioned;
    private final int metricLevel;
    private final int driverId;
    private String uniqueId;

    private final DriverYieldSignal yieldSignal;

    private final AtomicReference<DriverExec> driverExecRef = new AtomicReference<>();
    private final AtomicReference<DriverStats> driverStats = new AtomicReference<>();
    private List<Integer> driverInputs = new ArrayList<>();

    public DriverContext(PipelineContext pipelineContext, boolean partitioned, int driverId) {
        this.pipelineContext = requireNonNull(pipelineContext, "pipelineContext is null");
        this.partitioned = partitioned;
        this.driverId = driverId;
        this.metricLevel = pipelineContext.getMetricLevel();
        this.yieldSignal = new DriverYieldSignal(false);
    }

    public void setDriverExecRef(DriverExec driverExecRef) {
        this.driverExecRef.set(driverExecRef);
        this.driverInputs.addAll(driverExecRef.getAllInputIds());
    }

    public TaskId getTaskId() {
        return pipelineContext.getTaskId();
    }

    public PipelineContext getPipelineContext() {
        return pipelineContext;
    }

    public void startProcessTimer() {
        long now = System.currentTimeMillis();
        if (startNanosUpdater.compareAndSet(this, 0, now)) {
            pipelineContext.start();
        }
        intervalWallStartUpdater.set(this, now);
        if (isSQLMetricEnabled(metricLevel)) {
            intervalCpuStartUpdater.set(this, currentThreadCpuTime());
            intervalUserStartUpdater.set(this, currentThreadUserTime());
        }
    }

    public void recordProcessed() {
        processWallNanosUpdater.getAndAdd(this, System.currentTimeMillis() - intervalWallStartUpdater.get(this));
        if (isSQLMetricEnabled(metricLevel)) {
            processCpuNanosUpdater
                .getAndAdd(this, nanosBetween(intervalCpuStartUpdater.get(this), currentThreadCpuTime()));
            processUserNanosUpdater
                .getAndAdd(this, nanosBetween(intervalUserStartUpdater.get(this), currentThreadUserTime()));
        }
    }

    public long getTotalCpuTime() {
        return processCpuNanosUpdater.get(this);
    }

    public long getTotalScheduledTime() {
        return processWallNanosUpdater.get(this);
    }

    public long getTotalBlockedTime() {
        return blockedWallMillsUpdater.get(this);
    }

    public long getTotalUserTime() {
        return processUserNanosUpdater.get(this);
    }

    public void recordBlocked() {
        if (isSQLMetricEnabled(metricLevel)) {
            if (isBlocked.compareAndSet(false, true)) {
                blockedStartTimestampUpdater.set(this, System.currentTimeMillis());
            }
        }
    }

    public void recordBlockedFinished() {
        if (isSQLMetricEnabled(metricLevel)) {
            if (isBlocked.compareAndSet(true, false)) {
                long oldTime = blockedStartTimestampUpdater.getAndSet(this, 0);
                if (oldTime > 0) {
                    blockedWallMillsUpdater.getAndAdd(this, System.currentTimeMillis() - oldTime);
                }
            }
        }
    }

    public void close(boolean isException) {
        try {
            DriverExec driverExec = this.driverExecRef.get();
            if (driverExec != null) {
                if (isException) {
                    driverExec.closeOnException();
                } else {
                    driverExec.close();
                }
            }
        } catch (Exception e) {
            //ingore
        }
        try {
            this.finished();
        } catch (Exception e) {
            //ingore
        }
    }

    public void finished() {
        if (finished.compareAndSet(false, true)) {
            endNanosUpdater.set(this, System.currentTimeMillis());
            pipelineContext.driverFinished(this);
            this.driverStats.set(this.getDriverStats());
            this.driverExecRef.set(null);
        }
    }

    public void failed(Throwable cause) {
        pipelineContext.failed(cause);
        finished.set(true);
        this.driverStats.set(this.getDriverStats());
        this.driverExecRef.set(null);
    }

    public boolean isDone() {
        return finished.get() || pipelineContext.isDone();
    }

    public boolean isCpuTimerEnabled() {
        return pipelineContext.isCpuTimerEnabled();
    }

    private long getBlockedTime() {
        if (isBlocked.get() && blockedStartTimestampUpdater.get(this) > 0) {
            return System.currentTimeMillis() - blockedStartTimestampUpdater.get(this);
        }
        return 0;
    }

    public boolean isPartitioned() {
        return partitioned;
    }

    private long currentThreadUserTime() {
        if (!isCpuTimerEnabled()) {
            return 0;
        }
        return THREAD_MX_BEAN.getCurrentThreadUserTime();
    }

    private long currentThreadCpuTime() {
        if (!isCpuTimerEnabled()) {
            return 0;
        }
        return THREAD_MX_BEAN.getCurrentThreadCpuTime();
    }

    private static long nanosBetween(long start, long end) {
        return Math.abs(end - start);
    }

    public int getDriverId() {
        return driverId;
    }

    public String getUniqueId() {
        if (uniqueId == null) {
            StringBuilder sb = new StringBuilder();
            sb.append("tid:").append(pipelineContext.getTaskId())
                .append(" pid:").append(pipelineContext.getPipelineId())
                .append(" did:").append(driverId);
            this.uniqueId = sb.toString();
        }
        return uniqueId;
    }

    public DriverYieldSignal getYieldSignal() {
        return yieldSignal;
    }

    public AtomicBoolean getIsBlocked() {
        return isBlocked;
    }

    public boolean isStart() {
        return startNanosLong > 0;
    }

    @Nullable
    public DriverExec getDriverExec() {
        return driverExecRef.get();
    }

    public List<Integer> getDriverInputs() {
        return driverInputs;
    }

    public DriverStats getDriverStats() {
        DriverExec driverExec = this.driverExecRef.get();
        if (driverExec != null) {
            long inputDataSize = 0;
            long inputPositions = 0;
            long outputDataSize = 0;
            long outputPositions = 0;
            for (List<SourceExec> sources : driverExec.getSourceExecs().values()) {
                for (SourceExec source : sources) {
                    inputDataSize += source.getInputDataSize();
                    inputPositions += source.getInputPositions();
                }
            }
            if (driverExec.getConsumer() instanceof OutputCollector) {
                outputDataSize += ((OutputCollector) driverExec.getConsumer()).getOutputDataSize();
                outputPositions += ((OutputCollector) driverExec.getConsumer()).getOutputPositions();
            } else {
                outputPositions += driverOutputPosition;
            }
            return new DriverStats(inputDataSize, inputPositions, outputDataSize, outputPositions);
        } else if (driverStats.get() != null) {
            return driverStats.get();
        }
        return new DriverStats(0, 0, 0, 0);
    }

    private TaskStats getSecondTaskStats() {

        int queuedPipeExecs = 0;
        int runningPipeExecs = 0;
        int completePipeExecs = 0;

        long totalScheduledTime = getTotalScheduledTime();
        long totalCpuTime = getTotalCpuTime();
        long totalUserTime = getTotalUserTime();
        long totalBlockedTime = getTotalBlockedTime();

        DriverExec driverExec = this.driverExecRef.get();
        if (driverExec != null) {
            if (finished.get()) {
                completePipeExecs++;
            } else if (!driverExec.isOpened()) {
                queuedPipeExecs++;
            } else {
                if (getIsBlocked().get()) {
                    queuedPipeExecs++;
                } else {
                    runningPipeExecs++;
                }
            }
        } else {
            completePipeExecs++;
        }
        DriverStats driverStats = this.getDriverStats();

        long peakMemory = 0;
        long memoryReservation = 0;
        long cumulativeMemory = 0L;

        DateTime start = new DateTime(startNanosLong, ISOChronology.getInstance());
        long endTime = endNanosLong == 0 ? System.currentTimeMillis() : endNanosLong;
        DateTime end = new DateTime(endTime, ISOChronology.getInstance());
        return new TaskStats(start,
            start, end, endTime - startNanosLong, startNanosLong - createMillis, 0, 1,
            queuedPipeExecs, runningPipeExecs, completePipeExecs, cumulativeMemory, memoryReservation, peakMemory,
            totalScheduledTime, totalCpuTime, totalUserTime, totalBlockedTime, (runningPipeExecs > 0),
            ImmutableSet.of(), driverStats.getInputDataSize(), driverStats.getInputPositions(),
            driverStats.getOutputDataSize(), driverStats.getOutputPositions(), ImmutableList.of()
        );
    }

    public TaskInfo buildLocalModeTaskInfo(String queryId) {
        TaskId taskId = new TaskId(queryId, pipelineContext.getPipelineId(), driverId);
        TaskStats taskStats = getSecondTaskStats();

        OutputBufferInfo outputBufferInfo = new OutputBufferInfo(BufferState.OPEN, 0);

        TaskLocation taskLocation = new TaskLocation(
            ServiceProvider.getInstance().getServer().getLocalNode().getNodeServer(), taskId);

        TaskState state = getState();
        TaskStatus taskStatus = new TaskStatus(
            ServiceProvider.getInstance().getServer().getNodeId(),
            taskId,
            taskId.toString(),
            1,
            state,
            taskLocation,
            com.google.common.collect.ImmutableList.of(),
            com.google.common.collect.ImmutableList.of(),
            null,
            null,
            taskStats.getQueuedPipelineExecs(),
            taskStats.getRunningPipelineExecs(),
            taskStats.getMemoryReservation());

        return new TaskInfo(
            taskStatus,
            taskStats.getCreateTime(),
            outputBufferInfo,
            ImmutableSet.of(),
            taskStats,
            false,
            state.isDone(),
            taskStats.getCompletedPipelineExecs(),
            taskStats.getTotalPipelineExecs(),
            taskStats.getCumulativeMemory(),
            taskStats.getMemoryReservation(),
            taskStats.getElapsedTime(),
            0,
            taskStats.getElapsedTime(),
            taskStats.getTotalScheduledTime(),
            0,
            taskStats.getDeliveryTime()
        );
    }

    private TaskState getState() {
        if (startNanosLong > 0) {
            if (finished.get()) {
                return TaskState.FINISHED;
            } else {
                return TaskState.RUNNING;
            }
        } else {
            return TaskState.PLANNED;
        }
    }

    public void addOutputSize(long chunkSize) {
        this.driverOutputPosition += chunkSize;
    }
}
