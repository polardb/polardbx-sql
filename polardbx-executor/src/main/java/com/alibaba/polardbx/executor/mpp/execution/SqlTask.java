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

import com.alibaba.polardbx.common.TrxIdGenerator;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.MetricLevel;
import com.alibaba.polardbx.common.utils.bloomfilter.BloomFilterInfo;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.mpp.OutputBuffers;
import com.alibaba.polardbx.executor.mpp.Session;
import com.alibaba.polardbx.executor.mpp.execution.buffer.BufferResult;
import com.alibaba.polardbx.executor.mpp.execution.buffer.LazyOutputBuffer;
import com.alibaba.polardbx.executor.mpp.execution.buffer.OutputBuffer;
import com.alibaba.polardbx.executor.mpp.metadata.TaskLocation;
import com.alibaba.polardbx.executor.mpp.operator.DriverContext;
import com.alibaba.polardbx.executor.mpp.operator.DriverStats;
import com.alibaba.polardbx.executor.mpp.operator.OperatorStats;
import com.alibaba.polardbx.executor.mpp.operator.PipelineDepTree;
import com.alibaba.polardbx.executor.mpp.operator.TaskStats;
import com.alibaba.polardbx.executor.mpp.planner.PlanFragment;
import com.alibaba.polardbx.executor.mpp.util.Failures;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.memory.MemoryPool;
import com.alibaba.polardbx.optimizer.memory.TaskMemoryPool;
import com.alibaba.polardbx.optimizer.statis.SQLOperation;
import com.alibaba.polardbx.optimizer.statis.TaskMemoryStatisticsGroup;
import com.alibaba.polardbx.statistics.ExecuteSQLOperation;
import com.alibaba.polardbx.statistics.RuntimeStatHelper;
import com.alibaba.polardbx.statistics.RuntimeStatistics;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.trace.RuntimeStatisticsSketch;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.common.properties.MetricLevel.isSQLMetricEnabled;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.util.Objects.requireNonNull;

public class SqlTask {
    private static final Logger log = LoggerFactory.getLogger(SqlTask.class);

    private final TaskId taskId;
    private final String nodeId;
    private final TaskLocation location;
    private final TaskStateMachine taskStateMachine;
    private final OutputBuffer outputBuffer;

    // NOTE used before create sql task execution
    private ConcurrentMap<Integer, TaskSource> taskSources = new ConcurrentHashMap<>();

    private final SqlTaskExecutionFactory sqlTaskExecutionFactory;

    private final AtomicReference<DateTime> lastHeartbeat = new AtomicReference<>(DateTime.now());
    private final AtomicLong nextTaskInfoVersion = new AtomicLong(TaskStatus.STARTING_VERSION);

    private final AtomicReference<TaskHolder> taskHolderReference = new AtomicReference<>(new TaskHolder());
    private final AtomicBoolean needsPlan = new AtomicBoolean(true);

    public SqlTaskExecution taskExecution;
    private QueryContext queryContext;

    private final long createTime = System.currentTimeMillis();

    private boolean isMPPMetricEnabled = false;
    private long queryStart = -1;
    private Session session;
    private volatile long trxId = -1;
    private String schema;

    public SqlTask(
        String nodeId,
        TaskId taskId,
        TaskLocation location,
        QueryContext queryContext,
        SqlTaskExecutionFactory sqlTaskExecutionFactory,
        ExecutorService taskNotificationExecutor) {
        this.nodeId = requireNonNull(nodeId, "nodeId is null");
        this.taskId = requireNonNull(taskId, "taskId is null");
        this.location = requireNonNull(location, "location is null");
        this.queryContext = requireNonNull(queryContext, "queryContext is null");
        this.sqlTaskExecutionFactory = requireNonNull(sqlTaskExecutionFactory, "sqlTaskExecutionFactory is null");
        outputBuffer = new LazyOutputBuffer(taskId, taskId.toString(), taskNotificationExecutor);
        taskStateMachine = new TaskStateMachine(taskId, taskNotificationExecutor);
        taskStateMachine.addStateChangeListener(new StateMachine.StateChangeListener<TaskState>() {
            @Override
            public void stateChanged(TaskState newState) {
                if (!newState.isDone()) {
                    return;
                } else {
                    clean();
                }

                // store final task info
                while (true) {
                    TaskHolder taskHolder = taskHolderReference.get();
                    if (taskHolder.isFinished()) {
                        // another concurrent worker already set the final state
                        return;
                    }

                    if (taskHolderReference.compareAndSet(taskHolder,
                        new TaskHolder(createTaskInfoWithTaskStats(taskHolder), taskHolder.getIoStats()))) {
                        break;
                    }
                }

                // make sure buffers are cleaned up
                if (newState == TaskState.FAILED || newState == TaskState.ABORTED) {
                    // don't close buffers for a failed query
                    // closed buffers signal to upstream tasks that everything finished cleanly
                    // for system pool memory leak
                    outputBuffer.fail();
                } else {
                    outputBuffer.destroy();
                }
            }
        });
    }

    public SqlTaskIoStats getIoStats() {
        return taskHolderReference.get().getIoStats();
    }

    public TaskId getTaskId() {
        return taskStateMachine.getTaskId();
    }

    public List<TaskSource> removeTaskSources() {
        List<TaskSource> sources = new ArrayList<>(taskSources.values());
        taskSources.clear();
        return sources;
    }

    public void recordHeartbeat() {
        lastHeartbeat.set(DateTime.now());
    }

    @Deprecated
    public void setQueryStart(long queryStart) {
        this.queryStart = queryStart;
    }

    @Deprecated
    public long getQueryStart() {
        return queryStart;
    }

    public TaskInfo getInitialTaskInfo() {
        return createInitialTaskInfo(taskHolderReference.get());
    }

    public TaskInfo getTaskInfo() {
        return createTaskInfo(taskHolderReference.get());
    }

    public TaskStatus getTaskStatus() {
        return createTaskStatus(taskHolderReference.get(), Optional.of(getTaskStats(taskHolderReference.get())));
    }

    public QueryContext getQueryContext() {
        return queryContext;
    }

    public long getCreateTime() {
        return createTime;
    }

    public boolean isDone() {
        return taskStateMachine.getState().isDone();
    }

    private TaskStatus createInitialTaskStatus(Optional<TaskStats> stats, String nodeId) {
        // Always return a new TaskInfo with a larger version number;
        // otherwise a client will not accept the update
        long versionNumber = nextTaskInfoVersion.getAndIncrement();

        TaskState state = taskStateMachine.getState();
        List<ExecutionFailureInfo> failures = ImmutableList.of();
        if (state == TaskState.FAILED) {
            failures = Failures.toFailures(taskStateMachine.getFailureCauses());
        }

        if (stats.isPresent()) {
            //FIXME 现在taskstatus.queuedPartitionedDrivers 概念和QueuedPipelineExecs 一致，需要check下
            return new TaskStatus(nodeId, taskStateMachine.getTaskId(), taskId.toString(), versionNumber, state,
                location,
                failures, null, null, null, stats.get().getQueuedPipelineExecs(), stats.get().getRunningPipelineExecs(),
                stats.get().getMemoryReservation()
            );
        }

        return new TaskStatus(nodeId, taskStateMachine.getTaskId(), taskId.toString(), versionNumber, state, location,
            failures, null, null, null, 0, 0, 0);
    }

    private TaskStatus createTaskStatus(TaskHolder taskHolder, Optional<TaskStats> stats) {
        // Always return a new TaskInfo with a larger version number;
        // otherwise a client will not accept the update
        long versionNumber = nextTaskInfoVersion.getAndIncrement();

        TaskState state = taskStateMachine.getState();
        List<ExecutionFailureInfo> failures =
            state == TaskState.FAILED ? Failures.toFailures(taskStateMachine.getFailureCauses()) :
                ImmutableList.of();

        List<ExecuteSQLOperation> sqlTracer = null;
        Map<Integer, RuntimeStatistics.OperatorStatisticsGroup> runtimeStatistics = null;
        TaskMemoryStatisticsGroup memoryStatistics = null;
        if (state.isDone()) {
            if (taskHolder.getTaskExecution() != null) {
                ExecutionContext context =
                    taskHolder.getTaskExecution().getTaskContext().getContext();

                if (context.getTracer() != null) {
                    sqlTracer = new ArrayList<>();
                    List<SQLOperation> ops = context.getTracer().getOperations();
                    for (SQLOperation op : ops) {
                        if (op instanceof ExecuteSQLOperation) {
                            sqlTracer.add((ExecuteSQLOperation) op);
                        }
                    }
                }
                if (ExecUtils.isSQLMetricEnabled(context) && context.getRuntimeStatistics() != null) {
                    runtimeStatistics = ((RuntimeStatistics) context.getRuntimeStatistics()).getRelationToStatistics();
                    if (context.getMemoryPool() instanceof TaskMemoryPool) {
                        TaskMemoryPool taskMemoryPool = (TaskMemoryPool) context.getMemoryPool();

                        //TODO spill Statistics
                        memoryStatistics = new TaskMemoryStatisticsGroup(0, taskMemoryPool.getMaxMemoryUsage(), 0,
                            0, taskMemoryPool.getQueryMemoryPool().getMaxMemoryUsage(),
                            taskMemoryPool.getMemoryStatistics());
                    }
                }
            } else if (taskHolder.getFinalTaskInfo() != null) {
                sqlTracer = taskHolder.getFinalTaskInfo().getTaskStatus().getSqlTracer();
                runtimeStatistics = taskHolder.getFinalTaskInfo().getTaskStatus().getRuntimeStatistics();
                memoryStatistics = taskHolder.getFinalTaskInfo().getTaskStatus().getMemoryStatistics();
            }
        }

        if (stats.isPresent()) {
            //FIXME 现在taskstatus.queuedPartitionedDrivers 概念和QueuedPipelineExecs 一致，需要check下
            return new TaskStatus(
                nodeId,
                taskStateMachine.getTaskId(),
                taskId.toString(),
                versionNumber,
                state,
                location,
                failures,
                sqlTracer,
                runtimeStatistics,
                memoryStatistics,
                stats.get().getQueuedPipelineExecs(),
                stats.get().getRunningPipelineExecs(),
                stats.get().getMemoryReservation());
        }

        return new TaskStatus(
            nodeId,
            taskStateMachine.getTaskId(),
            taskId.toString(),
            versionNumber,
            state,
            location,
            failures,
            sqlTracer,
            runtimeStatistics,
            memoryStatistics,
            0,
            0,
            0);
    }

    public SqlTaskExecution getTaskExecution() {
        return this.taskExecution;
    }

    public MemoryPool getTaskMemoryPool() {
        return session != null ? session.getClientContext().getMemoryPool() : null;
    }

    private TaskStats getTaskStats(TaskHolder taskHolder) {
        TaskInfo finalTaskInfo = taskHolder.getFinalTaskInfo();
        if (finalTaskInfo != null) {
            if (finalTaskInfo.getTaskStats() != null) {
                return finalTaskInfo.getTaskStats();
            }
        }
        SqlTaskExecution taskExecution = taskHolder.getTaskExecution();
        if (taskExecution != null) {
            return getTaskStats(taskExecution.getTaskContext());
        }
        // if the task completed without creation, set end time
        DateTime endTime = taskStateMachine.getState().isDone() ? DateTime.now() : null;
        return new TaskStats(taskStateMachine.getCreatedTime(), endTime);
    }

    public long getEndTime() {
        return taskStateMachine.getEndTime();
    }

    public DateTime getTaskEndTime() {
        TaskHolder taskHolder = taskHolderReference.get();
        if (taskHolder != null) {
            TaskInfo finalTaskInfo = taskHolder.getFinalTaskInfo();
            if (finalTaskInfo != null) {
                return finalTaskInfo.getTaskStats().getEndTime();
            }
            SqlTaskExecution taskExecution = taskHolder.getTaskExecution();
            if (taskExecution != null) {
                return taskExecution.getTaskContext().getEndTime();
            }
        }
        return taskStateMachine.getState().isDone() ? DateTime.now() : null;
    }

    private static Set<Integer> getNoMoreSplits(TaskHolder taskHolder) {
        TaskInfo finalTaskInfo = taskHolder.getFinalTaskInfo();
        if (finalTaskInfo != null) {
            return finalTaskInfo.getNoMoreSplits();
        }
        SqlTaskExecution taskExecution = taskHolder.getTaskExecution();
        if (taskExecution != null) {
            return taskExecution.getNoMoreSplits();
        }
        return ImmutableSet.of();
    }

    private TaskInfo createInitialTaskInfo(TaskHolder taskHolder) {
        TaskStats taskStats = getTaskStats(taskHolder);

        if (isMPPMetricEnabled) {
            TaskStatus taskStatus = createInitialTaskStatus(Optional.of(taskStats), nodeId);

            return new TaskInfo(
                taskStatus,
                lastHeartbeat.get(),
                outputBuffer.getInfo(),
                ImmutableSet.of(),
                taskStats,
                needsPlan.get(),
                taskStatus.getState().isDone(),
                taskStats.getCompletedPipelineExecs(),
                taskStats.getTotalPipelineExecs(),
                taskStats.getCumulativeMemory(),
                taskStats.getMemoryReservation(),
                System.currentTimeMillis() - createTime,
                0,
                taskStats.getElapsedTimeMillis(),
                taskStats.getTotalScheduledTimeNanos(),
                taskStateMachine.getPullDataTime(),
                taskStats.getDeliveryTimeMillis());
        } else {
            TaskStatus taskStatus = createInitialTaskStatus(Optional.empty(), nodeId);

            return new TaskInfo(
                taskStatus,
                lastHeartbeat.get(),
                outputBuffer.getInfo(),
                ImmutableSet.of(),
                null,
                needsPlan.get(),
                taskStatus.getState().isDone(),
                taskStats.getCompletedPipelineExecs(),
                taskStats.getTotalPipelineExecs(),
                taskStats.getCumulativeMemory(),
                taskStats.getMemoryReservation(),
                System.currentTimeMillis() - createTime,
                taskStateMachine.getDriverEndTime(),
                taskStats.getElapsedTimeMillis(),
                taskStats.getTotalScheduledTimeNanos(),
                taskStateMachine.getPullDataTime(),
                taskStats.getDeliveryTimeMillis());
        }
    }

    private TaskInfo createTaskInfo(TaskHolder taskHolder) {
        TaskStats taskStats = getTaskStats(taskHolder);
        Set<Integer> noMoreSplits = getNoMoreSplits(taskHolder);
        TaskStatus taskStatus =
            createTaskStatus(taskHolder, isMPPMetricEnabled ? Optional.of(taskStats) : Optional.empty());
        if (isMPPMetricEnabled) {
            return new TaskInfo(
                taskStatus,
                lastHeartbeat.get(),
                outputBuffer.getInfo(),
                noMoreSplits,
                taskStats,
                needsPlan.get(),
                taskStatus.getState().isDone(),
                taskStats.getCompletedPipelineExecs(),
                taskStats.getTotalPipelineExecs(),
                taskStats.getCumulativeMemory(),
                taskStats.getMemoryReservation(),
                System.currentTimeMillis() - createTime,
                0,
                taskStats.getElapsedTimeMillis(),
                taskStats.getTotalScheduledTimeNanos(),
                taskStateMachine.getPullDataTime(),
                taskStats.getDeliveryTimeMillis());
        } else {
            return new TaskInfo(
                taskStatus,
                lastHeartbeat.get(),
                outputBuffer.getInfo(),
                noMoreSplits,
                taskStats,
                needsPlan.get(),
                taskStatus.getState().isDone(),
                taskStats.getCompletedPipelineExecs(),
                taskStats.getTotalPipelineExecs(),
                taskStats.getCumulativeMemory(),
                taskStats.getMemoryReservation(),
                System.currentTimeMillis() - createTime,
                taskStateMachine.getDriverEndTime(),
                taskStats.getElapsedTimeMillis(),
                taskStats.getTotalScheduledTimeNanos(),
                taskStateMachine.getPullDataTime(),
                taskStats.getDeliveryTimeMillis());
        }
    }

    private TaskInfo createTaskInfoWithTaskStats(TaskHolder taskHolder) {
        TaskStats taskStats = getTaskStats(taskHolder);
        Set<Integer> noMoreSplits = getNoMoreSplits(taskHolder);
        TaskStatus taskStatus = createTaskStatus(taskHolder, Optional.empty());
        TaskInfo taskInfo = new TaskInfo(
            taskStatus,
            lastHeartbeat.get(),
            outputBuffer.getInfo(),
            noMoreSplits,
            taskStats,
            needsPlan.get(),
            taskStatus.getState().isDone(),
            taskStats.getCompletedPipelineExecs(),
            taskStats.getTotalPipelineExecs(),
            taskStats.getCumulativeMemory(),
            taskStats.getMemoryReservation(),
            System.currentTimeMillis() - createTime,
            taskStateMachine.getDriverEndTime(),
            taskStats.getElapsedTimeMillis(),
            taskStats.getTotalScheduledTimeNanos(),
            taskStateMachine.getPullDataTime(),
            taskStats.getDeliveryTimeMillis());
        return taskInfo;
    }

    public ListenableFuture<TaskStatus> getTaskStatus(TaskState callersCurrentState) {
        requireNonNull(callersCurrentState, "callersCurrentState is null");

        if (callersCurrentState.isDone()) {
            return immediateFuture(getTaskStatus());
        }

        ListenableFuture<TaskState> futureTaskState = taskStateMachine.getStateChange(callersCurrentState);
        return Futures.transform(futureTaskState, input -> getTaskStatus(), directExecutor());
    }

    public ListenableFuture<TaskInfo> getTaskInfo(TaskState callersCurrentState) {
        requireNonNull(callersCurrentState, "callersCurrentState is null");

        // If the caller's current state is already done, just return the current
        // state of this task as it will either be done or possibly still running
        // (due to a bug in the caller), since we can not transition from a done
        // state.
        if (callersCurrentState.isDone()) {
            return immediateFuture(getTaskInfo());
        }

        ListenableFuture<TaskState> futureTaskState = taskStateMachine.getStateChange(callersCurrentState);
        return Futures.transform(futureTaskState, input -> getTaskInfo(), directExecutor());
    }

    public TaskInfo updateTask(SessionRepresentation sessionRepresentation,
                               Optional<PlanFragment> fragment, List<TaskSource> sources,
                               OutputBuffers outputBuffers, Optional<List<BloomFilterInfo>> bloomFilterInfos,
                               URI uri) {
        try {
            synchronized (this) {
                // assure the task execution is only created once
                TaskHolder taskHolder = taskHolderReference.get();
                if (taskHolder.isFinished()) {
                    return taskHolder.getFinalTaskInfo();
                }

                if (session == null) {
                    schema = sessionRepresentation.getSchema();
                    trxId = TrxIdGenerator.getInstance().nextId();
                    session = sessionRepresentation.toSession(taskId, queryContext, trxId);
                    queryContext.registerTaskMemoryPool(session.getClientContext().getMemoryPool());

                    isMPPMetricEnabled = isSQLMetricEnabled(
                        session.getClientContext().getParamManager().getInt(ConnectionParams.MPP_METRIC_LEVEL));
                }

                // The LazyOutput buffer does not support write methods, so the actual
                // output buffer must be established before drivers are created (e.g.
                // a VALUES query).
                outputBuffer.setOutputBuffers(outputBuffers, session.getClientContext());

                if (taskHolder.getTaskExecution() == null) {
                    //FIXME 2019-08-22 xiaojian.fxj
                    bootstrapTask(fragment, sources, uri);
                }
            }
            if (taskExecution != null) {
                taskExecution.addSources(sources);
            }

            taskExecution.addBloomFilter(bloomFilterInfos);
        } catch (Error e) {
            failed(e);
            throw e;
        } catch (RuntimeException e) {
            failed(e);
        }
        return getInitialTaskInfo();
    }

    // NOTE should only use it after check taskHolderReference and try/catch any throwable
    private SqlTaskExecution bootstrapTask(Optional<PlanFragment> fragment, List<TaskSource> sources,
                                           URI uri) {
        checkState(fragment.isPresent(), "fragment must be present");

        if (MetricLevel.isSQLMetricEnabled(
            session.getClientContext().getParamManager().getInt(ConnectionParams.MPP_METRIC_LEVEL))) {
            session.getClientContext()
                .setRuntimeStatistics(RuntimeStatHelper.buildRuntimeStat(session.getClientContext()));
        }

        taskExecution = sqlTaskExecutionFactory.create(session, queryContext, taskStateMachine,
            outputBuffer, fragment.get(), sources, uri);
        taskHolderReference.compareAndSet(taskHolderReference.get(), new TaskHolder(taskExecution));
        needsPlan.set(false);
        return taskExecution;
    }

    public ListenableFuture<BufferResult> getTaskResults(OutputBuffers.OutputBufferId bufferId, boolean preferLocal,
                                                         long startingSequenceId, DataSize maxSize) {
        requireNonNull(bufferId, "bufferId is null");
        checkArgument(maxSize.toBytes() > 0, "maxSize must be at least 1 byte");

        return outputBuffer.get(bufferId, preferLocal, startingSequenceId, maxSize);
    }

    public void checkFlushComplete() {
        outputBuffer.checkFlushComplete();
    }

    public void abortTaskResults(OutputBuffers.OutputBufferId bufferId) {
        requireNonNull(bufferId, "bufferId is null");

        if (log.isDebugEnabled()) {
            log.debug(String.format("Aborting task %s output %s", taskId, bufferId));
        }
        outputBuffer.abort(bufferId);
    }

    public void failed(Throwable cause) {
        requireNonNull(cause, "cause is null");

        taskStateMachine.failed(cause);
    }

    public TaskInfo cancel() {
        taskStateMachine.cancel();
        return getTaskInfo();
    }

    public TaskInfo abort() {
        taskStateMachine.abort();
        return getTaskInfo();
    }

    public DateTime getLastHeartbeat() {
        return lastHeartbeat.get();
    }

    @Override
    public String toString() {
        return taskId.toString();
    }

    private static final class TaskHolder {
        private final SqlTaskExecution taskExecution;
        private final TaskInfo finalTaskInfo;
        private final SqlTaskIoStats finalIoStats;

        private TaskHolder() {
            this.taskExecution = null;
            this.finalTaskInfo = null;
            this.finalIoStats = null;
        }

        private TaskHolder(SqlTaskExecution taskExecution) {
            this.taskExecution = requireNonNull(taskExecution, "taskExecution is null");
            this.finalTaskInfo = null;
            this.finalIoStats = null;
        }

        private TaskHolder(TaskInfo finalTaskInfo, SqlTaskIoStats finalIoStats) {
            this.taskExecution = null;
            this.finalTaskInfo = requireNonNull(finalTaskInfo, "finalTaskInfo is null");
            this.finalIoStats = requireNonNull(finalIoStats, "finalIoStats is null");
        }

        public boolean isFinished() {
            return finalTaskInfo != null;
        }

        @Nullable
        public SqlTaskExecution getTaskExecution() {
            return taskExecution;
        }

        @Nullable
        public TaskInfo getFinalTaskInfo() {
            return finalTaskInfo;
        }

        public SqlTaskIoStats getIoStats() {
            // if we are finished, return the final IoStats
            if (finalIoStats != null) {
                return finalIoStats;
            }
            // if we haven't started yet, return an empty IoStats
            if (taskExecution == null) {
                return new SqlTaskIoStats();
            }
            // get IoStats from the current task execution
            TaskContext taskContext = taskExecution.getTaskContext();
            return new SqlTaskIoStats(taskContext.getInputDataSize(), taskContext.getInputPositions(),
                taskContext.getOutputDataSize(), taskContext.getOutputPositions());
        }
    }

    public void addStateChangeListener(StateMachine.StateChangeListener<TaskState> stateChangeListener) {
        taskStateMachine.addStateChangeListener(stateChangeListener);
    }

    public void releaseTaskMemoryInAdvance() {
        queryContext = null;
        removeTaskSources();
        taskExecution = null;
        // make sure buffers are cleaned up
        if (taskStateMachine.getState() == TaskState.FAILED || taskStateMachine.getState() == TaskState.ABORTED) {
            // don't close buffers for a failed query
            // closed buffers signal to upstream tasks that everything finished cleanly
            // for system pool memory leak
            outputBuffer.fail();
        } else {
            outputBuffer.destroy();
        }
    }

    private TaskStats getTaskStats(TaskContext context) {
        // check for end state to avoid callback ordering problems

        long cumulativeMemory = 0L;
        List<OperatorStats> operatorStatsList = new ArrayList<>();

        List<DriverContext> driverContexts = new ArrayList<>();

        Map<Integer, String> idToName = new HashMap<>();
        Map<Integer, RuntimeStatisticsSketch> idToStatisticsSketch = new HashMap<>();

        if (context.getContext().getRuntimeStatistics() != null) {
            Map<RelNode, RuntimeStatisticsSketch> statisticsSketchMap =
                ((RuntimeStatistics) context.getContext().getRuntimeStatistics()).toSketch();

            for (Map.Entry<RelNode, RuntimeStatisticsSketch> entry : statisticsSketchMap.entrySet()) {
                idToStatisticsSketch.put(entry.getKey().getRelatedId(), entry.getValue());
                idToName.put(entry.getKey().getRelatedId(), entry.getKey().getClass().getSimpleName());
            }
            for (RuntimeStatisticsSketch statistics : idToStatisticsSketch.values()) {
                cumulativeMemory += statistics.getMemory();
            }

        }
        for (PipelineContext pipelineContext : context.getPipelineContexts()) {
            for (DriverContext driverContext : pipelineContext.getDriverContexts()) {
                driverContexts.add(driverContext);
                for (Integer operatorId : driverContext.getDriverInputs()) {
                    RuntimeStatisticsSketch ret = idToStatisticsSketch.get(operatorId);
                    if (ret != null) {
                        OperatorStats operatorStats =
                            new OperatorStats(Optional.empty(), driverContext.getPipelineContext().getPipelineId(),
                                Optional.of(idToName.get(operatorId)), operatorId, ret.getRowCount(),
                                ret.getRuntimeFilteredRowCount(),
                                ret.getOutputBytes(), ret.getStartupDuration(),
                                ret.getDuration(), ret.getMemory(), ret.getInstances(), ret.getSpillCnt());
                        operatorStatsList.add(operatorStats);
                    }
                }
            }
        }

        if (taskStateMachine.getState().isDone()) {
            context.end();
            if (context.getContext().getRuntimeStatistics() != null) {
                Set<Integer> finishedStatics = new HashSet<>();

                for (DriverContext driverContext : driverContexts) {
                    for (Integer operatorId : driverContext.getDriverInputs()) {
                        RuntimeStatisticsSketch ret = idToStatisticsSketch.get(operatorId);
                        if (ret != null && !finishedStatics.contains(operatorId)) {
                            OperatorStats operatorStats =
                                new OperatorStats(Optional.empty(), driverContext.getPipelineContext().getPipelineId(),
                                    Optional.of(idToName.get(operatorId)), operatorId, ret.getRowCount(),
                                    ret.getRuntimeFilteredRowCount(),
                                    ret.getOutputBytes(), ret.getStartupDuration(),
                                    ret.getDuration(), ret.getMemory(), ret.getInstances(), ret.getSpillCnt());
                            operatorStatsList.add(operatorStats);
                            finishedStatics.add(operatorId);
                        }
                    }
                }
            }
        }

        int totalPipeExecs = driverContexts.size();

        int queuedPipeExecs = 0;
        int runningPipeExecs = 0;
        int completePipeExecs = 0;

        long processedInputDataSize = 0;
        long processedInputPositions = 0;

        long outputDataSize = 0;
        long outputPositions = 0;

        long totalScheduledTime = 0;
        long totalCpuTime = 0;
        long totalUserTime = 0;
        long totalBlockedTime = 0;

        if (isSQLMetricEnabled(context.getMetricLevel())) {
            for (PipelineContext pipelineContext : context.getPipelineContexts()) {
                totalScheduledTime += pipelineContext.getTotalScheduledTime().get();
                totalCpuTime += pipelineContext.getTotalCpuTime().get();
                totalUserTime += pipelineContext.getTotalUserTime().get();
                totalBlockedTime += pipelineContext.getTotalBlockedTime().get();
            }
        }

        List<DriverStats> driverStatsList = new ArrayList<>(driverContexts.size());

        for (int i = 0; i < driverContexts.size(); i++) {

            DriverContext driverContext = driverContexts.get(i);
            if (driverContext.isDone()) {
                completePipeExecs++;
            } else if (!driverContext.isStart()) {
                queuedPipeExecs++;
            } else {
                if (driverContext.getIsBlocked().get()) {
                    queuedPipeExecs++;
                } else {
                    runningPipeExecs++;
                }
            }
            DriverStats driverStats = driverContext.getDriverStats();
            processedInputDataSize += driverStats.getInputDataSize();
            processedInputPositions += driverStats.getInputPositions();
            outputDataSize += driverStats.getOutputDataSize();
            outputPositions += driverStats.getOutputPositions();
            driverStatsList.add(driverStats);
        }

        long startMillis = context.getStartMillis();
        if (startMillis == 0) {
            startMillis = System.currentTimeMillis();
        }
        //启动到初始化时间准备运行时间，既worker端排队等待调度运行时间
        long queuedTime = startMillis - context.getCreateContextMillis();

        long elapsedTime;
        long endMillis = context.getEndMillisLong();
        if (endMillis >= startMillis) {
            //整体耗时
            elapsedTime = endMillis - context.getCreateContextMillis();
        } else {
            elapsedTime = 0;
        }

        long deliveryTime;
        if (context.getCreateContextMillis() >= taskStateMachine.getCreatedTime().getMillis()) {
            deliveryTime = context.getCreateContextMillis() - taskStateMachine.getCreatedTime().getMillis();
        } else {
            deliveryTime = 0;
        }

        MemoryPool taskMemoryPool = context.getContext().getMemoryPool();
        long peakMemory = taskMemoryPool.getMaxMemoryUsage();

        long memoryReservation = taskMemoryPool.getMemoryUsage();
        Map<Integer, List<Integer>> pipelineDeps = buildPipelineDeps(context.getPipelineDepTree());

        return new TaskStats(taskStateMachine.getCreatedTime(), context.getStartTime(),
            context.getEndTime(), elapsedTime, queuedTime, deliveryTime, totalPipeExecs,
            queuedPipeExecs, runningPipeExecs, completePipeExecs, cumulativeMemory, memoryReservation, peakMemory,
            totalScheduledTime, totalCpuTime, totalUserTime, totalBlockedTime, (runningPipeExecs > 0),
            ImmutableSet.of(), processedInputDataSize, processedInputPositions,
            outputDataSize, outputPositions, operatorStatsList,
            driverStatsList, pipelineDeps);
    }

    private Map<Integer, List<Integer>> buildPipelineDeps(PipelineDepTree pipelineDepTree) {
        if (pipelineDepTree == null) {
            return null;
        }
        Map<Integer, List<Integer>> deps = new HashMap<>(pipelineDepTree.size());
        for (int i = 0; i < pipelineDepTree.size(); i++) {
            PipelineDepTree.TreeNode node = pipelineDepTree.getNode(i);
            List<Integer> depIds = node.getDependChildren().stream()
                .map(PipelineDepTree.TreeNode::getId).collect(Collectors.toList());
            deps.put(node.getId(), depIds);
        }
        return deps;
    }

    public void clean() {
        if (trxId != -1) {
            ExecutorContext.getContext(schema).getTransactionManager().unregister(trxId);
            trxId = -1;
        }
    }
}
