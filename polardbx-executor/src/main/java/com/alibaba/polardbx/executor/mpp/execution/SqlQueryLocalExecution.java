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

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.utils.bloomfilter.BloomFilterInfo;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.mpp.Session;
import com.alibaba.polardbx.executor.mpp.execution.buffer.OutputBufferMemoryManager;
import com.alibaba.polardbx.executor.mpp.metadata.Split;
import com.alibaba.polardbx.executor.mpp.operator.Driver;
import com.alibaba.polardbx.executor.mpp.operator.DriverContext;
import com.alibaba.polardbx.executor.mpp.operator.DriverExec;
import com.alibaba.polardbx.executor.mpp.operator.LocalBufferExec;
import com.alibaba.polardbx.executor.mpp.operator.LocalExecutionPlanner;
import com.alibaba.polardbx.executor.mpp.operator.PipelineDepTree;
import com.alibaba.polardbx.executor.mpp.operator.factory.LocalBufferExecutorFactory;
import com.alibaba.polardbx.executor.mpp.operator.factory.PipelineFactory;
import com.alibaba.polardbx.executor.mpp.split.SplitInfo;
import com.alibaba.polardbx.executor.mpp.split.SplitManagerImpl;
import com.alibaba.polardbx.executor.operator.spill.SpillerFactory;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.memory.MemoryEstimator;
import com.alibaba.polardbx.optimizer.utils.CalciteUtils;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import io.airlift.units.Duration;
import org.apache.calcite.rel.RelNode;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.common.properties.ConnectionParams.INSERT_SELECT_LIMIT;
import static com.alibaba.polardbx.common.properties.ConnectionParams.UPDATE_DELETE_SELECT_LIMIT;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

public class SqlQueryLocalExecution extends QueryExecution {

    private static final Logger logger = LoggerFactory.getLogger(SqlQueryLocalExecution.class);

    private final QueryManager queryManager;

    private final TaskExecutor taskExecutor;

    private final Executor notificationExecutor;
    private final ExecutionContext context;

    private TaskExecutor.TaskHandle taskHandle;
    private final TaskId taskId;
    private final TaskContext taskContext;
    private final RelNode physicalPlan;

    private final boolean needReserved;
    /**
     * Number of drivers that have been sent to the TaskExecutor that have not finished.
     */
    private final AtomicInteger remainingDrivers = new AtomicInteger();

    private AtomicReference<LocalBufferExec> resultBufferExec = new AtomicReference<LocalBufferExec>();

    private final LocationFactory locationFactory;
    private final SpillerFactory spillerFactory;

    private URI querySelf;

    private boolean useServerThread;

    private List<DriverSplitRunner> driverSplitRunners = new ArrayList<>();

    private List<ColumnMeta> returnColumns;

    private SqlQueryLocalExecution(
        QueryManager queryManager,
        TaskId taskId,
        String query,
        RelNode physicalPlan,
        LocationFactory locationFactory,
        SpillerFactory spillerFactory,
        Session session,
        TaskExecutor taskExecutor,
        ExecutorService notificationExecutor,
        ScheduledExecutorService driverYieldExecutor) {
        this.queryManager = queryManager;
        this.taskId = taskId;
        this.context = session.getClientContext();
        this.physicalPlan = physicalPlan;
        this.locationFactory = locationFactory;
        this.spillerFactory = spillerFactory;
        ParamManager pm = session.getClientContext().getParamManager();
        this.needReserved =
            pm.getBoolean(ConnectionParams.MPP_QUERY_NEED_RESERVE);

        this.stateMachine =
            QueryStateMachine.begin(query, session, notificationExecutor, this.needReserved);
        this.taskExecutor = requireNonNull(taskExecutor, "taskExecutor is null");
        this.notificationExecutor = requireNonNull(notificationExecutor, "notificationExecutor is null");
        this.taskContext = new TaskContext(driverYieldExecutor, stateMachine, context, taskId);
    }

    @Override
    public void start() {
        try {
            if (!stateMachine.transitionToStarting()) {
                // query already started or finished
                return;
            }
            int parallelism = ExecUtils.getParallelismForLocal(context);
            LocalExecutionPlanner planner =
                new LocalExecutionPlanner(context, null, parallelism, parallelism, 1,
                    context.getParamManager().getInt(ConnectionParams.PREFETCH_SHARDS), notificationExecutor,
                    taskContext.isSpillable() ? spillerFactory : null, null, null, false,
                    -1, -1, ImmutableMap.of(), new SplitManagerImpl());
            returnColumns = CalciteUtils.buildColumnMeta(physicalPlan, "Last");

            boolean syncMode = stateMachine.getSession().isLocalResultIsSync();

            OutputBufferMemoryManager localBufferManager;
            LocalBufferExecutorFactory factory;

            if (getSession().isCacheOutput()) {
                String memoryName = "OutputBuffer@" + System.identityHashCode(this);
                long estimateRowSize = MemoryEstimator.estimateRowSize(physicalPlan.getRowType(), null);
                long bufferSize =
                    context.getParamManager().getLong(ConnectionParams.SPILL_OUTPUT_MAX_BUFFER_SIZE);
                long totalEstimate = Math.max(context.getParamManager().getLong(INSERT_SELECT_LIMIT),
                    context.getParamManager().getLong(UPDATE_DELETE_SELECT_LIMIT)) * estimateRowSize;
                localBufferManager = planner.createCacheAllBufferMemoryManager(memoryName,
                    Math.max(bufferSize, totalEstimate));
                factory =
                    new LocalBufferExecutorFactory(
                        localBufferManager,
                        returnColumns.stream().map(t -> t.getDataType()).collect(Collectors.toList()), 1, syncMode,
                        true);

            } else {
                localBufferManager = planner.createLocalMemoryManager();
                factory =
                    new LocalBufferExecutorFactory(
                        localBufferManager,
                        returnColumns.stream().map(t -> t.getDataType()).collect(Collectors.toList()), 1, syncMode,
                        false);
            }

            List<PipelineFactory> pipelineFactories = planner.plan(physicalPlan, factory, localBufferManager,
                this.stateMachine.getQueryId());

            taskContext.setPipelineDepTree(new PipelineDepTree(pipelineFactories));

            // start drivers
            for (PipelineFactory pipelineFactory : pipelineFactories) {
                PipelineContext pipelineContext = taskContext.addPipelineContext(pipelineFactory.getPipelineId());
                pipelineContext.setPipelineProperties(pipelineFactory.getProperties());

                List<Driver> drivers = new ArrayList<>();
                for (int i = 0; i < pipelineFactory.getParallelism(); i++) {
                    DriverContext driverContext = pipelineContext.addDriverContext(false);
                    DriverExec driverExec = pipelineFactory.createDriverExec(context, driverContext, i);
                    Driver driver = new Driver(driverContext, driverExec);
                    drivers.add(driver);
                    driverSplitRunners.add(new DriverSplitRunner(driver));
                }

                if (!pipelineFactory.getLogicalView().isEmpty()) {
                    for (LogicalView logicalView : pipelineFactory.getLogicalView()) {
                        SplitInfo splitInfo = pipelineFactory.getSource(logicalView.getRelatedId());
                        if (splitInfo != null) {
                            schedulePartitionedSource(
                                splitInfo, drivers, logicalView.getRelatedId(), logicalView.isExpandView());
                        }
                    }
                }
            }
            stateMachine.transitionToRunning();
            if (this.driverSplitRunners.size() == 1 && !getSession().isCacheOutput()) {
                // 当只有一个Driver时，直接使用当前Server线程来处理
                this.useServerThread = true;
                return;
            }
            this.resultBufferExec.set((LocalBufferExec) factory.createExecutor(context, 0));
            if (!stateMachine.isDone()) {
                taskHandle = taskExecutor.addTask(taskId);
                enqueueDrivers(driverSplitRunners, ExecUtils.isTpMode(context));

                stateMachine.addStateChangeListener(state -> {
                    if (state.isDone()) {
                        taskExecutor.removeTask(taskHandle);

                        taskContext.end();
                        taskContext.finished();
                        //这里是cancel和abort操作有关系
                        for (PipelineContext pipelineContext : taskContext.getPipelineContexts()) {
                            for (DriverContext driverContext : pipelineContext.getDriverContexts()) {
                                driverContext.close(state == QueryState.FAILED);
                            }
                        }
                        if (state == QueryState.FAILED && this.resultBufferExec.get() != null) {
                            this.resultBufferExec.get().close();
                        }
                    }
                });
            } else {
                taskHandle = null;
            }
        } catch (Throwable e) {
            fail(e);
            this.queryManager.removeLocalQuery(stateMachine.getQueryId(), true);
            throw new TddlNestableRuntimeException(e);
        }
    }

    private void schedulePartitionedSource(SplitInfo splitInfo, List<Driver> drivers, Integer sourceId,
                                           boolean expand) {
        List<Driver> filterDrivers =
            drivers.stream().filter(driver -> driver.matchSource(sourceId)).collect(Collectors.toList());
        Preconditions.checkArgument(filterDrivers.size() > 0, "filterDrivers is null!");

        if (filterDrivers.size() == 1) {
            //并发度==1
            if (splitInfo.getSplits().size() > 1) {
                List<Split> splitList = new ArrayList<>();
                //开启了事务,单个分库都会merge成一个split,同样的split顺序已经被打散到各个rds实例上
                for (List<Split> list : splitInfo.getSplits()) {
                    for (Split split : list) {
                        splitList.add(split);
                    }
                }
                filterDrivers.get(0).processNewSources(sourceId, splitList, expand, true);
            } else {
                //split顺序已经被打散到各个rds实例上
                filterDrivers.get(0).processNewSources(sourceId, splitInfo.getSplits().iterator().next(), expand, true);
            }
        } else {
            //并发度 > 1
            if (splitInfo.getSplits().size() > 1) {

                if (expand) {
                    //其实这里将无法保证事务，除非并发度设置为1,所以这里显示异常
                    throw new TddlNestableRuntimeException("Don't Support expand logicalview!");
                }

                //开启事务, 不支持worker内部各个scan动态消费split
                List<List<Split>> taskLocalSplits = new ArrayList<>();
                filterDrivers.stream().forEach(driver -> taskLocalSplits.add(new ArrayList<>()));

                int index = 0;
                for (List<Split> list : splitInfo.getSplits()) {
                    index++;
                    if (index >= taskLocalSplits.size()) {
                        index = 0;
                    }
                    for (Split split : list) {
                        taskLocalSplits.get(index).add(split);
                    }
                }

                for (int i = 0; i < taskLocalSplits.size(); i++) {
                    filterDrivers.get(i).processNewSources(sourceId, taskLocalSplits.get(i), expand, true);
                }
            } else {
                if (expand) {
                    //expand, 不支持worker内部各个scan动态消费split
                    List<List<Split>> taskLocalSplits = new ArrayList<>();
                    filterDrivers.stream().forEach(driver -> taskLocalSplits.add(new ArrayList<>()));
                    List<Split> splits = splitInfo.getSplits().iterator().next();
                    int start = ThreadLocalRandom.current().nextInt(splits.size());
                    for (int i = 0; i < taskLocalSplits.size(); i++) {
                        int index = i + start;
                        for (int j = 0; j < splits.size(); j++) {
                            if (index >= splits.size()) {
                                index = 0;
                            }
                            taskLocalSplits.get(i).add(splits.get(index));
                            index++;
                        }
                    }
                    for (int i = 0; i < taskLocalSplits.size(); i++) {
                        filterDrivers.get(i).processNewSources(sourceId, taskLocalSplits.get(i), expand, true);
                    }
                } else {
                    //split顺序已经被打散到各个rds实例上
                    List<Split> splits = splitInfo.getSplits().iterator().next();
                    int index = 0;
                    for (Split split : splits) {
                        if (index >= filterDrivers.size()) {
                            index = 0;
                        }
                        filterDrivers.get(index++).processNewSources(sourceId, ImmutableList.of(split), expand, false);
                    }

                    for (int i = 0; i < filterDrivers.size(); i++) {
                        filterDrivers.get(i).processNewSources(sourceId, ImmutableList.of(), expand, true);
                    }
                }
            }
        }
    }

    private void enqueueDrivers(List<DriverSplitRunner> runners, boolean highPriority) {

        List<ListenableFuture<?>> finishedFutures = taskExecutor.enqueueSplits(taskHandle, highPriority, runners);
        checkState(finishedFutures.size() == runners.size(), "Expected %s futures but got %s", runners.size(),
            finishedFutures.size());

        remainingDrivers.addAndGet(finishedFutures.size());

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
                    stateMachine.failed(cause);
                    // record driver is finished
                    remainingDrivers.decrementAndGet();
                }
            }, notificationExecutor);
        }
    }

    @Override
    public void mergeBloomFilter(List<BloomFilterInfo> filterInfos) {
        throw new RuntimeException("Don't support BloomFilter now!");
    }

    private synchronized void checkTaskCompletion() {
        if (stateMachine.isDone()) {
            return;
        }
        // do we still have running tasks?
        if (remainingDrivers.get() != 0) {
            return;
        }
        // Cool! All done!
        stateMachine.transitionToFinishing();
    }

    @Override
    public TaskContext getTaskContext() {
        return taskContext;
    }

    public static class SqlQueryLocalExecutionFactory
        implements QueryExecutionFactory<SqlQueryLocalExecution> {

        private final ExecutorService notificationExecutor;
        private final TaskExecutor taskExecutor;
        private final SpillerFactory spillerFactory;
        private final ScheduledExecutorService driverYieldExecutor;
        private final LocationFactory locationFactory;

        @Inject
        public SqlQueryLocalExecutionFactory(
            LocationFactory locationFactory, SpillerFactory spillerFactory,
            @ForTaskNotificationExecutor ExecutorService notificationExecutor,
            TaskExecutor taskExecutor,
            @ForDriverYieldExecutor ScheduledExecutorService driverYieldExecutor) {
            this.locationFactory = locationFactory;
            this.spillerFactory = spillerFactory;
            this.notificationExecutor = notificationExecutor;
            this.taskExecutor = taskExecutor;
            this.driverYieldExecutor = driverYieldExecutor;
        }

        @Override
        public SqlQueryLocalExecution createQueryExecution(QueryManager queryManager, String query,
                                                           RelNode physicalPlan,
                                                           Session session) {
            TaskId taskId = new TaskId(session.getQueryId(), 0, 0);
            return new SqlQueryLocalExecution(queryManager, taskId, query, physicalPlan, locationFactory,
                spillerFactory, session, taskExecutor, notificationExecutor, driverYieldExecutor);
        }

        @Override
        public void setThreadPoolExecutor(int poolSize) {

        }
    }

    @Override
    public QueryInfo getQueryInfo() {
        Optional<QueryInfo> finalQueryInfo = stateMachine.getFinalQueryInfo();
        if (finalQueryInfo.isPresent()) {
            return finalQueryInfo.get();
        }

        return buildQueryInfo();
    }

    private QueryInfo buildQueryInfo() {
        if (querySelf == null) {
            querySelf = locationFactory.createQueryLocation(getQueryId());
        }
        Optional<StageInfo> stageInfo = taskContext.buildStageInfo(getQueryId(), querySelf);
        QueryInfo queryInfo = stateMachine.updateQueryInfo(stageInfo, querySelf);
        return queryInfo;
    }

    @Override
    public long getTotalMemoryReservation() {
        Optional<QueryInfo> finalQueryInfo = stateMachine.getFinalQueryInfo();
        if (finalQueryInfo.isPresent()) {
            return finalQueryInfo.get().getQueryStats().getTotalMemoryReservation().toBytes();
        } else {
            return 0;
        }
    }

    @Override
    public Duration getTotalCpuTime() {
        Optional<QueryInfo> finalQueryInfo = stateMachine.getFinalQueryInfo();
        if (finalQueryInfo.isPresent()) {
            return finalQueryInfo.get().getQueryStats().getTotalCpuTime();
        } else {
            return new Duration(0, SECONDS);
        }
    }

    @Override
    public void cancelStage(StageId stageId) {
        throw new IllegalStateException("Local mode don't support cancel Stage!");
    }

    @Override
    public void close(Throwable throwable) {
        this.taskContext.end();
        this.taskContext.finished();
        if (throwable != null) {
            this.stateMachine.transitionToFailed(throwable);
            this.queryManager.removeLocalQuery(stateMachine.getQueryId(), true);
        } else {
            this.stateMachine.transitionToFinishing();
            this.queryManager.removeLocalQuery(stateMachine.getQueryId(), this.needReserved);
        }
        this.driverSplitRunners.clear();
    }

    public boolean isUseServerThread() {
        return useServerThread;
    }

    public List<DriverSplitRunner> getDriverSplitRunners() {
        return driverSplitRunners;
    }

    public List<ColumnMeta> getReturnColumns() {
        return returnColumns;
    }

    @Override
    public boolean isNeedReserveAfterExpired() {
        return needReserved;
    }

    public LocalBufferExec getResultBufferExec() {
        return resultBufferExec.get();
    }
}
