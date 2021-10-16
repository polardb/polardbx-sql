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

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.mpp.operator.PipelineDepTree;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.memory.MemorySetting;
import com.alibaba.polardbx.optimizer.memory.QueryMemoryPool;
import org.joda.time.DateTime;

import javax.annotation.concurrent.ThreadSafe;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import static java.util.Objects.requireNonNull;

@ThreadSafe
public class TaskContext {

    private static final Logger log = LoggerFactory.getLogger(TaskContext.class);

    private static final AtomicLongFieldUpdater<TaskContext> startMillisUpdater =
        AtomicLongFieldUpdater.newUpdater(TaskContext.class, "startMillisLong");
    private static final AtomicLongFieldUpdater<TaskContext> endMillisUpdater =
        AtomicLongFieldUpdater.newUpdater(TaskContext.class, "endMillisLong");
    private static final AtomicReferenceFieldUpdater<TaskContext, DateTime> executionStartTimeUpdater =
        AtomicReferenceFieldUpdater.newUpdater(TaskContext.class, DateTime.class, "executionStartTimeReference");
    private static final AtomicReferenceFieldUpdater<TaskContext, DateTime> executionEndTimeUpdater =
        AtomicReferenceFieldUpdater.newUpdater(TaskContext.class, DateTime.class, "executionEndTimeReference");

    //------------------------- copy from driverContext or pipelineContext ---------------------------------------------

    private final List<PipelineContext> pipelineContexts = new CopyOnWriteArrayList<>();

    private final AtomicBoolean finished = new AtomicBoolean();

    private volatile long startMillisLong = 0L;
    private volatile long endMillisLong = 0L;

    private volatile DateTime executionStartTimeReference = null;
    private volatile DateTime executionEndTimeReference = null;

    private final StateMachineBase<?> taskStateMachine;
    private final ExecutionContext context;

    private final long createContextMillis = System.currentTimeMillis();

    private AtomicLong executeDriverTime = new AtomicLong();

    private final boolean isCpuTimerEnabled;

    private final int metricLevel;

    private ScheduledExecutorService yieldExecutor;

    private volatile PipelineDepTree pipelineDepTree;

    private final boolean isSpill;

    private final TaskId taskId;

    private final AtomicLong executeMillisLong = new AtomicLong();

    private final QueryMemoryPool queryMemoryPool;

    public TaskContext(ScheduledExecutorService yieldExecutor,
                       StateMachineBase<?> taskStateMachine, ExecutionContext context, TaskId taskId) {
        this.yieldExecutor = yieldExecutor;
        this.taskStateMachine = requireNonNull(taskStateMachine, "taskStateMachine is null");
        this.context = context;
        this.metricLevel = context.getParamManager().getInt(ConnectionParams.MPP_METRIC_LEVEL);
        //FIXME 非mpp模式就不要采集了，因为默认就是Operator Level，算子级别会采集的
        this.isCpuTimerEnabled = ExecUtils.isPipelineMetricEnabled(context) && ExecUtils.isMppMode(context);
        this.isSpill =
            MemorySetting.ENABLE_SPILL && context.getParamManager().getBoolean(ConnectionParams.ENABLE_SPILL);
        this.taskId = taskId;

        if (ExecUtils.isMppMode(context)) {
            queryMemoryPool = (QueryMemoryPool) context.getMemoryPool().getParent();
        } else {
            queryMemoryPool = (QueryMemoryPool) context.getMemoryPool();
        }
    }

    public int getMetricLevel() {
        return metricLevel;
    }

    public TaskId getTaskId() {
        return taskId;
    }

    public PipelineContext addPipelineContext(int pipelineId) {
        PipelineContext pipelineContext = new PipelineContext(pipelineId, this);
        pipelineContexts.add(pipelineContext);
        return pipelineContext;
    }

    public ExecutionContext getContext() {
        return context;
    }

    public long getStartMillis() {
        return startMillisUpdater.get(this);
    }

    public DateTime getStartTime() {
        return executionStartTimeUpdater.get(this);
    }

    public DateTime getEndTime() {
        return executionEndTimeUpdater.get(this);
    }

    public void start() {
        DateTime now = DateTime.now();
        executionStartTimeUpdater.compareAndSet(this, null, now);
        startMillisUpdater.compareAndSet(this, 0, System.currentTimeMillis());
    }

    public void failed(Throwable cause) {
        taskStateMachine.failed(cause);
        finished.set(true);
    }

    public boolean isSpillable() {
        return isSpill;
    }

    public ScheduledExecutorService getYieldExecutor() {
        return yieldExecutor;
    }

    public boolean isDone() {
        return finished.get() || taskStateMachine.isDone();
    }

    public void setPipelineDepTree(PipelineDepTree pipelineDepTree) {
        this.pipelineDepTree = pipelineDepTree;
    }

    public PipelineDepTree getPipelineDepTree() {
        return pipelineDepTree;
    }

    public void pipelineFinished(PipelineContext pipelineContext) {
        this.pipelineDepTree.pipelineFinish(pipelineContext.getPipelineId());
    }

    public long getInputDataSize() {
        long stat = 0L;
        return stat;
    }

    public long getInputPositions() {
        long stat = 0L;
        return stat;
    }

    public long getOutputDataSize() {
        long stat = 0L;
        return stat;
    }

    public long getOutputPositions() {
        long stat = 0L;
        return stat;
    }

    public void finished() {
        if (!finished.compareAndSet(false, true)) {
            // already finished
            return;
        }
    }

    public List<PipelineContext> getPipelineContexts() {
        return pipelineContexts;
    }

    public boolean isCpuTimerEnabled() {
        return isCpuTimerEnabled;
    }

    public long getCreateContextMillis() {
        return createContextMillis;
    }

    public AtomicLong getExecuteDriverTime() {
        return executeDriverTime;
    }

    public long getEndMillisLong() {
        return endMillisLong;
    }

    public void end() {
        if (executionEndTimeUpdater.compareAndSet(this, null, DateTime.now())) {
            endMillisUpdater.set(this, System.currentTimeMillis());
        }
    }

    public long getExecuteMillisLong(long delta) {
        return executeMillisLong.addAndGet(delta);
    }

    public synchronized Optional<StageInfo> buildStageInfo(String queryId, URI self) {
        if (pipelineContexts.size() > 0) {
            PipelineContext root = pipelineContexts.get(pipelineContexts.size() - 1);
            Map<StageId, StageInfo> stageInfoMap = new HashMap<>();
            try {
                StageInfo ret = root.buildLocalModeStageInfo(
                    queryId, self, stageInfoMap, taskStateMachine.isDone());
                return Optional.of(ret);
            } catch (Exception t) {
                log.error("queryId " + queryId, t);
            }
        }
        return Optional.empty();
    }

    public final QueryMemoryPool getQueryMemoryPool() {
        return queryMemoryPool;
    }
}
