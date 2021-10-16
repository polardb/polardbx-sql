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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.alibaba.polardbx.common.DefaultSchema;
import com.alibaba.polardbx.executor.mpp.Session;
import com.alibaba.polardbx.executor.mpp.Threads;
import com.alibaba.polardbx.executor.mpp.execution.buffer.OutputBuffer;
import com.alibaba.polardbx.executor.mpp.operator.Driver;
import com.alibaba.polardbx.executor.mpp.operator.DriverContext;
import com.alibaba.polardbx.executor.mpp.operator.DriverExec;
import com.alibaba.polardbx.executor.mpp.operator.LocalExecutionPlanner;
import com.alibaba.polardbx.executor.mpp.operator.PipelineDepTree;
import com.alibaba.polardbx.executor.mpp.operator.factory.PipelineFactory;
import com.alibaba.polardbx.executor.mpp.planner.PlanFragment;
import com.alibaba.polardbx.executor.operator.util.bloomfilter.BloomFilterExpression;
import com.alibaba.polardbx.util.bloomfilter.BloomFilterInfo;
import io.airlift.concurrent.SetThreadName;

import javax.annotation.concurrent.GuardedBy;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class SqlTaskExecution {
    private final TaskId taskId;
    private final TaskStateMachine taskStateMachine;
    private final TaskContext taskContext;
    private final OutputBuffer outputBuffer;

    private final TaskExecutor.TaskHandle taskHandle;
    private final TaskExecutor taskExecutor;

    private final Executor notificationExecutor;

    private final List<WeakReference<Driver>> drivers = new CopyOnWriteArrayList<>();
    private final List<WeakReference<Driver>> partitionDrivers = new CopyOnWriteArrayList<>();

    /**
     * Number of drivers that have been sent to the TaskExecutor that have not finished.
     */
    private final AtomicInteger remainingDrivers = new AtomicInteger();

    // guarded for update only
    @GuardedBy("this")
    private final ConcurrentMap<Integer, TaskSource> unpartitionedSources = new ConcurrentHashMap<>();
    // guarded for update only
    @GuardedBy("this")
    private final ConcurrentMap<Integer, TaskSource> partitionedSources = new ConcurrentHashMap<>();

    @GuardedBy("this")
    private long maxAcknowledgedSplit = Long.MIN_VALUE;

    private final Map<List<Integer>, PipelineFactory> partitionedDriverFactories;

    private final List<PipelineFactory> unpartitionedDriverFactories;

    private final Map<Integer, BloomFilterExpression> bloomFilterExpression;

    public static SqlTaskExecution createSqlTaskExecution(
        TaskStateMachine taskStateMachine,
        TaskContext taskContext,
        OutputBuffer outputBuffer,
        PlanFragment fragment,
        Session session,
        LocalExecutionPlanner planner,
        List<TaskSource> sources,
        TaskExecutor taskExecutor,
        Executor notificationExecutor,
        Map<Integer, BloomFilterExpression> bloomFilterExpressionMap) {
        SqlTaskExecution task = new SqlTaskExecution(
            taskStateMachine,
            taskContext,
            session,
            outputBuffer,
            fragment,
            planner,
            sources,
            taskExecutor,
            notificationExecutor,
            bloomFilterExpressionMap);
        return task;
    }

    private SqlTaskExecution(
        TaskStateMachine taskStateMachine,
        TaskContext taskContext,
        Session session,
        OutputBuffer outputBuffer,
        PlanFragment fragment,
        LocalExecutionPlanner planner,
        List<TaskSource> sources,
        TaskExecutor taskExecutor,
        Executor notificationExecutor,
        Map<Integer, BloomFilterExpression> bloomFilterExpressionMap) {
        this.taskStateMachine = requireNonNull(taskStateMachine, "taskStateMachine is null");
        this.taskId = taskStateMachine.getTaskId();
        this.taskContext = requireNonNull(taskContext, "taskContext is null");
        this.outputBuffer = requireNonNull(outputBuffer, "outputBuffer is null");

        this.taskExecutor = requireNonNull(taskExecutor, "driverExecutor is null");
        this.notificationExecutor = requireNonNull(notificationExecutor, "notificationExecutor is null");
        this.bloomFilterExpression = bloomFilterExpressionMap;

        List<PipelineFactory> pipelineFactories = planner.plan(
            fragment, outputBuffer, session);
        taskContext.setPipelineDepTree(new PipelineDepTree(pipelineFactories));

        // index driver factories
        ImmutableMap.Builder<List<Integer>, PipelineFactory> partitionedDriverFactories = ImmutableMap.builder();
        ImmutableList.Builder<PipelineFactory> unpartitionedDriverFactories = ImmutableList.builder();
        for (int i = 0; i < pipelineFactories.size(); i++) {
            PipelineFactory driverFactory = pipelineFactories.get(i);
            int sourceSize = driverFactory.getLogicalView().size();
            if (sourceSize > 0 && fragment.isPartitionedSources(driverFactory.getLogicalView().get(0).getRelatedId())) {
                partitionedDriverFactories.put(driverFactory.getLogicalView().stream().map(
                    t -> t.getRelatedId()).collect(Collectors.toList()), driverFactory);
            } else {
                unpartitionedDriverFactories.add(driverFactory);
            }
        }
        this.partitionedDriverFactories = partitionedDriverFactories.build();
        this.unpartitionedDriverFactories = unpartitionedDriverFactories.build();

        // don't register the task if it is already completed (most likely failed during planning above)
        if (!taskStateMachine.getState().isDone()) {
            taskHandle = taskExecutor.addTask(taskId);
            taskStateMachine.addStateChangeListener(state -> {
                if (state.isDone()) {
                    taskExecutor.removeTask(taskHandle);
                    taskContext.finished();
                    //这里是cancel和abort操作有关系
                    for (PipelineContext pipelineContext : taskContext.getPipelineContexts()) {
                        for (DriverContext driverContext : pipelineContext.getDriverContexts()) {
                            driverContext.close(state.isException());
                        }
                    }
                    //send bloomfilter
                    cleanBloomFilterMemory();
                }
            });
        } else {
            taskHandle = null;
        }
        outputBuffer.addStateChangeListener(new CheckTaskCompletionOnBufferFinish(SqlTaskExecution.this));
        List<DriverSplitRunner> runners = start(session);
        //add the sources firstly before enqueue the drivers.
        addSources(sources);
        enqueueDrivers(runners);
    }

    private void cleanBloomFilterMemory() {
        if (bloomFilterExpression != null) {
            bloomFilterExpression.clear();
        }
    }

    //
    // This code starts registers a callback with access to this class, and this
    // call back is access from another thread, so this code can not be placed in the constructor
    private List<DriverSplitRunner> start(Session session) {
        DefaultSchema.setSchemaName(session.getSchema());
        // start unpartitioned drivers
        List<DriverSplitRunner> runners = new ArrayList<>();
        for (PipelineFactory pipelineFactory : unpartitionedDriverFactories) {
            PipelineContext pipelineContext = taskContext.addPipelineContext(pipelineFactory.getPipelineId());
            for (int i = 0; i < pipelineFactory.getParallelism(); i++) {
                DriverContext driverContext = pipelineContext.addDriverContext(false);
                DriverExec driverExec =
                    pipelineFactory.createDriverExec(taskContext.getContext(), driverContext, i);
                runners.add(
                    new DriverSplitRunner(createDriver(driverExec, driverContext)));
            }
        }

        for (PipelineFactory pipelineFactory : partitionedDriverFactories.values()) {
            PipelineContext pipelineContext = taskContext.addPipelineContext(pipelineFactory.getPipelineId());
            for (int i = 0; i < pipelineFactory.getParallelism(); i++) {
                DriverContext driverContext = pipelineContext.addDriverContext(true);
                DriverExec driverExec =
                    pipelineFactory.createDriverExec(taskContext.getContext(), driverContext, i);
                runners.add(new DriverSplitRunner(createDriver(driverExec, driverContext)));
            }
        }
        return runners;
    }

    public TaskId getTaskId() {
        return taskId;
    }

    public TaskContext getTaskContext() {
        return taskContext;
    }

    public void addSources(List<TaskSource> sources) {
        requireNonNull(sources, "sources is null");
        if (!Threads.ENABLE_WISP) {
            checkState(!Thread.holdsLock(this), "Can not add sources while holding a lock on the %s",
                getClass().getSimpleName());
        }

        //try (SetThreadName ignored = new SetThreadName("Task-%s", taskId)) {
        // update our record of sources and schedule drivers for new partitioned splits
        Map<Integer, TaskSource> updatedUnpartitionedSources = updateSources(sources);

        // tell existing drivers about the new splits; it is safe to update drivers
        // multiple times and out of order because sources contain full record of
        // the unpartitioned splits
        for (TaskSource source : updatedUnpartitionedSources.values()) {
            // tell all the existing drivers this source is finished
            for (WeakReference<Driver> driverReference : drivers) {
                Driver driver = driverReference.get();
                // the driver can be GCed due to a failure or a limit
                if (driver != null) {
                    driver.updateSource(source);
                } else {
                    // remove the weak reference from the list to avoid a memory leak
                    // NOTE: this is a concurrent safe operation on a CopyOnWriteArrayList
                    drivers.remove(driverReference);
                }
            }
        }

        // we may have transitioned to no more splits, so check for completion
//		checkTaskCompletion();
        //}
    }

    private synchronized Map<Integer, TaskSource> updateSources(List<TaskSource> sources) {
        Map<Integer, TaskSource> updatedUnpartitionedSources = new HashMap<>();

        // first remove any split that was already acknowledged
        long currentMaxAcknowledgedSplit = this.maxAcknowledgedSplit;

        List<TaskSource> newSources = new ArrayList<>(sources.size());
        for (int i = 0; i < sources.size(); i++) {
            TaskSource source = sources.get(i);
            LinkedHashSet<ScheduledSplit> splits = Sets.newLinkedHashSet();
            for (ScheduledSplit scheduledSplit : source.getSplits()) {
                if (scheduledSplit.getSequenceId() > currentMaxAcknowledgedSplit) {
                    splits.add(scheduledSplit);
                }
            }
            TaskSource newSource = new TaskSource(
                source.getPlanNodeId(),
                splits,
                source.isNoMoreSplits(),
                source.isExpand());
            newSources.add(newSource);
        }

        // update task with new sources
        for (int i = 0; i < newSources.size(); i++) {
            TaskSource source = newSources.get(i);
            if (partitionedDriverFactories.keySet().stream().anyMatch(keys -> keys.contains(source.getPlanNodeId()))) {
                schedulePartitionedSource(source);
            } else {
                scheduleUnpartitionedSource(source, updatedUnpartitionedSources);
            }
        }

        // update maxAcknowledgedSplit
        long maxAck = maxAcknowledgedSplit;
        for (int i = 0; i < newSources.size(); i++) {
            TaskSource source = newSources.get(i);
            for (ScheduledSplit split : source.getSplits()) {
                if (split.getSequenceId() > maxAck) {
                    maxAck = split.getSequenceId();
                }
            }
        }
        maxAcknowledgedSplit = maxAck;
        return updatedUnpartitionedSources;
    }

    private void schedulePartitionedSource(TaskSource source) {
        //TODO 这里splits 有分配不均匀问题，如何考虑呢？
        // create new source
        Set<ScheduledSplit> newSplits = new HashSet<>();
        TaskSource currentSource = partitionedSources.get(source.getIntegerId());
        if (currentSource == null) {
            partitionedSources.put(source.getIntegerId(), source);
            newSplits = source.getSplits();
        } else {
            TaskSource newSource = currentSource.update(source);
            // only record new source if something changed
            if (newSource != currentSource) {
                partitionedSources.put(source.getIntegerId(), newSource);
                // find the new splits to add
                newSplits = Sets.difference(newSource.getSplits(), currentSource.getSplits());
            }
        }

        if (newSplits.size() > 0) {

            List<Driver> matchDrivers = new ArrayList<>();
            List<TaskSource> updateTaskSources = new ArrayList<>();
            for (WeakReference<Driver> driverReference : partitionDrivers) {
                Driver driver = driverReference.get();
                // the driver can be GCed due to a failure or a limit
                if (driver != null) {
                    if (driver.matchSource(source.getIntegerId())) {
                        matchDrivers.add(driver);
                        updateTaskSources
                            .add(new TaskSource(source.getIntegerId(), new LinkedHashSet<>(), source.isNoMoreSplits(),
                                source.isExpand()));
                    }
                } else {
                    // remove the weak reference from the list to avoid a memory leak
                    // NOTE: this is a concurrent safe operation on a CopyOnWriteArrayList
                    partitionDrivers.remove(driverReference);
                }
            }
            if (source.isExpand()) {
                int start = ThreadLocalRandom.current().nextInt(updateTaskSources.size());
                List<ScheduledSplit> shuffleLists = zigzagSplitsByMysqlInst(new ArrayList<>(newSplits));
                for (int i = 0; i < updateTaskSources.size(); i++) {
                    int index = i + start;
                    for (int j = 0; j < shuffleLists.size(); j++) {
                        if (index >= shuffleLists.size()) {
                            index = 0;
                        }
                        updateTaskSources.get(i).addSplit(shuffleLists.get(index++));
                    }
                }
            } else {
                int start = ThreadLocalRandom.current().nextInt(updateTaskSources.size());
                //排序的目的是为了保证同一个实例的splits被打散
                List<ScheduledSplit> shuffleLists = zigzagSplitsByMysqlInst(new ArrayList<>(newSplits));
                for (ScheduledSplit scheduledSplit : shuffleLists) {
                    updateTaskSources.get(start++).addSplit(scheduledSplit);
                    if (start >= updateTaskSources.size()) {
                        start = 0;
                    }
                }
            }

            for (int i = 0; i < updateTaskSources.size(); i++) {
                matchDrivers.get(i).updateSource(updateTaskSources.get(i));
            }
        }
    }

    private List<ScheduledSplit> zigzagSplitsByMysqlInst(List<ScheduledSplit> splits) {
        List<ScheduledSplit> newSplits = new ArrayList<ScheduledSplit>(splits.size());
        int instCount = 0;
        List<List<ScheduledSplit>> instSplitsArrList = new ArrayList<List<ScheduledSplit>>();
        Map<String, Integer> instIndexMap = new HashMap<String, Integer>();

        int maxSplitsIndexOfOneInst = 0;
        for (int i = 0; i < splits.size(); i++) {
            String instAddr = (splits.get(i).getSplit().getConnectorSplit()).getHostAddress();
            Integer instIndex = instIndexMap.get(instAddr);

            List<ScheduledSplit> splitListOfInst = null;
            if (instIndex == null) {
                ++instCount;
                instIndex = instCount - 1;
                instIndexMap.put(instAddr, instIndex);
                splitListOfInst = new ArrayList<>();
                instSplitsArrList.add(splitListOfInst);
            }
            splitListOfInst = instSplitsArrList.get(instIndex);
            splitListOfInst.add(splits.get(i));
            if (splitListOfInst.size() > maxSplitsIndexOfOneInst) {
                maxSplitsIndexOfOneInst = splitListOfInst.size();
            }
        }
        for (int relIdx = 0; relIdx < maxSplitsIndexOfOneInst; ++relIdx) {
            for (int instIdx = 0; instIdx < instSplitsArrList.size(); ++instIdx) {
                List<ScheduledSplit> phyRelArr = instSplitsArrList.get(instIdx);
                if (relIdx < phyRelArr.size()) {
                    newSplits.add(phyRelArr.get(relIdx));
                }
            }
        }
        return newSplits;
    }

    private void scheduleUnpartitionedSource(TaskSource source, Map<Integer, TaskSource> updatedUnpartitionedSources) {
        // create new source
        TaskSource newSource;
        TaskSource currentSource = unpartitionedSources.get(source.getIntegerId());
        if (currentSource == null) {
            newSource = source;
        } else {
            newSource = currentSource.update(source);
        }

        // only record new source if something changed
        if (newSource != currentSource) {
            unpartitionedSources.put(source.getIntegerId(), newSource);
            updatedUnpartitionedSources.put(source.getIntegerId(), newSource);
        }
    }

    private synchronized void enqueueDrivers(List<DriverSplitRunner> runners) {
        // schedule driver to be executed
        List<ListenableFuture<?>> finishedFutures = taskExecutor.enqueueSplits(taskHandle, false, runners);
        checkState(finishedFutures.size() == runners.size(), "Expected %s futures but got %s", runners.size(),
            finishedFutures.size());

        // record new driver
        remainingDrivers.addAndGet(finishedFutures.size());

        // when driver completes, update state and fire events
        for (int i = 0; i < finishedFutures.size(); i++) {
            ListenableFuture<?> finishedFuture = finishedFutures.get(i);
            Futures.addCallback(finishedFuture, new FutureCallback<Object>() {
                @Override
                public void onSuccess(Object result) {
                    // record driver is finished
                    remainingDrivers.decrementAndGet();
                    checkTaskCompletion();
                }

                @Override
                public void onFailure(Throwable cause) {
                    taskStateMachine.failed(cause);
                    // record driver is finished
                    remainingDrivers.decrementAndGet();
                }
            }, notificationExecutor);
        }
    }

    public Set<Integer> getNoMoreSplits() {
        ImmutableSet.Builder<Integer> noMoreSplits = ImmutableSet.builder();
        for (TaskSource taskSource : partitionedSources.values()) {
            if (taskSource.isNoMoreSplits()) {
                noMoreSplits.add(taskSource.getIntegerId());
            }
        }
        for (TaskSource taskSource : unpartitionedSources.values()) {
            if (taskSource.isNoMoreSplits()) {
                noMoreSplits.add(taskSource.getIntegerId());
            }
        }
        return noMoreSplits.build();
    }

    public void addBloomFilter(Optional<List<BloomFilterInfo>> bloomFilterInfos) {
        if (bloomFilterInfos.isPresent()) {
            for (BloomFilterExpression bloomFilterExpression : bloomFilterExpression.values()) {
                bloomFilterExpression.addFilter(bloomFilterInfos.get());
            }
        }
    }

    public synchronized void checkTaskCompletion() {
        if (taskStateMachine.getState().isDone()) {
            return;
        }

        // do we still have running tasks?
        if (remainingDrivers.get() != 0) {
            return;
        }

        taskStateMachine.recordDriverEndTime(taskContext.getStartMillis());
        taskStateMachine.recordDataFushTime();
        // no more output will be created
        outputBuffer.setNoMorePages();

        // are there still pages in the output buffer
        if (!outputBuffer.isFinished()) {
            taskStateMachine.flushing();
            return;
        }

        taskStateMachine.recordDataFinishTime();
        // Cool! All done!
        taskStateMachine.finished();
    }

    public void cancel() {
        // todo this should finish all input sources and let the task finish naturally
        try (SetThreadName ignored = new SetThreadName("Task-%s", taskId)) {
            taskStateMachine.cancel();
        }
    }

    public void fail(Throwable cause) {
        try (SetThreadName ignored = new SetThreadName("Task-%s", taskId)) {
            taskStateMachine.failed(cause);
        }
    }

    @Override
    public String toString() {
        return toStringHelper(this)
            .add("taskId", taskId)
            .add("remainingDrivers", remainingDrivers)
            .add("unpartitionedSources", unpartitionedSources)
            .toString();
    }

    private Driver createDriver(DriverExec executor, DriverContext context) {
        Driver driver = new Driver(context, executor);
        // record driver so other threads add unpartitioned sources can see the driver
        // NOTE: this MUST be done before reading unpartitionedSources, so we see a consistent view of the unpartitioned sources
        drivers.add(new WeakReference<>(driver));
        if (driver.getSources().size() > 0) {
            partitionDrivers.add(new WeakReference<>(driver));
        }
        // add unpartitioned sources
        for (TaskSource source : unpartitionedSources.values()) {
            driver.updateSource(source);
        }
        return driver;
    }
}
