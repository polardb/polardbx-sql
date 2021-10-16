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

package com.alibaba.polardbx.executor.mpp.execution;

import com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.mpp.OutputBuffers;
import com.alibaba.polardbx.executor.mpp.Session;
import com.alibaba.polardbx.executor.mpp.execution.scheduler.NodeScheduler;
import com.alibaba.polardbx.executor.mpp.execution.scheduler.NodeSelector;
import com.alibaba.polardbx.executor.mpp.execution.scheduler.SqlQueryScheduler;
import com.alibaba.polardbx.executor.mpp.planner.NodePartitioningManager;
import com.alibaba.polardbx.executor.mpp.planner.PlanFragment;
import com.alibaba.polardbx.executor.mpp.planner.PlanFragmenter;
import com.alibaba.polardbx.executor.mpp.planner.StageExecutionPlan;
import com.alibaba.polardbx.executor.mpp.planner.SubPlan;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.util.bloomfilter.BloomFilterInfo;
import io.airlift.units.Duration;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeField;

import javax.annotation.concurrent.ThreadSafe;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicReference;

import static com.alibaba.polardbx.executor.utils.ExecUtils.existMppOnlyInstanceNode;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

@ThreadSafe
public class SqlQueryExecution extends QueryExecution {
    private static final Logger logger = LoggerFactory.getLogger(SqlQueryExecution.class);
    private static final OutputBuffers.OutputBufferId OUTPUT_BUFFER_ID = new OutputBuffers.OutputBufferId(0);

    private final RelNode physicalPlan;
    private final Session session;

    private final LocationFactory locationFactory;
    private final RemoteTaskFactory remoteTaskFactory;
    private final ExecutorService queryExecutor;

    private final AtomicReference<SqlQueryScheduler> queryScheduler = new AtomicReference<>();
    private final NodeTaskMap nodeTaskMap;
    private final NodeScheduler nodeScheduler;
    private final NodePartitioningManager nodePartitioningManager;
    private final boolean needReserved;
    private final URI querySelf;

    public SqlQueryExecution(String query, RelNode physicalPlan, Session session, URI self,
                             ExecutorService queryExecutor, LocationFactory locationFactory,
                             RemoteTaskFactory remoteTaskFactory,
                             NodeTaskMap nodeTaskMap, NodeScheduler nodeScheduler,
                             NodePartitioningManager nodePartitioningManager) {
        this.physicalPlan = physicalPlan;
        this.queryExecutor = queryExecutor;
        this.session = session;
        this.locationFactory = locationFactory;
        this.nodeTaskMap = nodeTaskMap;
        this.nodeScheduler = nodeScheduler;
        this.querySelf = self;
        this.stateMachine = QueryStateMachine.begin(query, session, queryExecutor, true);

        // when the query finishes cache the final query info, and clear the reference to the output stage
        stateMachine.addStateChangeListener(state -> {
            if (state.isDone()) {
                // query is now done, so abort any work that is still running
                SqlQueryScheduler scheduler = queryScheduler.get();
                if (scheduler != null) {
                    scheduler.abort();
                }
            }
        });

        //MemoryTracking现在并没有实际用途
        this.remoteTaskFactory = remoteTaskFactory;
        //this.remoteTaskFactory = new MemoryTrackingRemoteTaskFactory(remoteTaskFactory, stateMachine);
        this.nodePartitioningManager = nodePartitioningManager;

        ParamManager pm = session.getClientContext().getParamManager();
        this.needReserved = pm.getBoolean(ConnectionParams.MPP_QUERY_NEED_RESERVE);
    }

    @Override
    public void start() {
        try {
            // transition to planning
            if (!stateMachine.transitionToPlanning()) {
                // query already started or finished
                return;
            }

            long distributedPlanningStart = System.currentTimeMillis();
            // plan distribution of query
            Pair<SubPlan, Integer> subPlan = PlanFragmenter.buildRootFragment(physicalPlan, session);
            int polarXParallelism = ExecUtils.getPolarDBXCores(
                session.getClientContext().getParamManager(), !existMppOnlyInstanceNode());
            int limitNode = subPlan.getValue() % polarXParallelism > 0 ? subPlan.getValue() / polarXParallelism + 1 :
                subPlan.getValue() / polarXParallelism;
            NodeSelector nodeSelector = nodeScheduler.createNodeSelector(session, limitNode);
            planDistribution(subPlan.getKey(), nodeSelector);
            stateMachine.recordDistributedPlanningTime(distributedPlanningStart);
            // transition to starting
            if (!stateMachine.transitionToStarting()) {
                // query already started or finished
                return;
            }

            // if query is not finished, start the scheduler, otherwise cancel it
            SqlQueryScheduler scheduler = queryScheduler.get();

            if (!stateMachine.isDone()) {
                session.generateTsoInfo();
                scheduler.start();
            }
        } catch (Throwable e) {
            fail(e);
            Throwables.propagateIfInstanceOf(e, Error.class);
        }
    }

    public StageExecutionPlan getStagePlan(SubPlan plan, List<PlanFragment> planFragmentList) {
        List<StageExecutionPlan> subStages = new ArrayList<>();
        planFragmentList.add(plan.getFragment());
        for (SubPlan subPlan : plan.getChildren()) {
            subStages.add(getStagePlan(subPlan, planFragmentList));
        }
        return new StageExecutionPlan(plan.getFragment(), plan.getLogicalViewInfo(), plan.getExpandSplitInfos(),
            subStages);
    }

    private void planDistribution(SubPlan plan, NodeSelector nodeSelector) {
        // plan the execution on the active nodes
        List<PlanFragment> planFragmentList = new ArrayList<>();
        StageExecutionPlan outputStageExecutionPlan = getStagePlan(plan, planFragmentList);

        // if query was canceled, skip creating scheduler
        if (stateMachine.isDone()) {
            return;
        }

        List<RelDataTypeField> relDataTypeList = physicalPlan.getRowType().getFieldList();
        List<String> fieldNames = new ArrayList<>(relDataTypeList.size());
        for (int i = 0; i < relDataTypeList.size(); i++) {
            fieldNames.add(relDataTypeList.get(i).getName());
        }
        // build the stage execution objects (this doesn't schedule execution)
        OutputBuffers rootOutputBuffers =
            OutputBuffers.createInitialEmptyOutputBuffers(OutputBuffers.BufferType.PARTITIONED)
                .withBuffer(OUTPUT_BUFFER_ID, OutputBuffers.BROADCAST_PARTITION_ID)
                .withNoMoreBufferIds();
        SqlQueryScheduler scheduler = new SqlQueryScheduler(stateMachine,
            locationFactory,
            outputStageExecutionPlan,
            nodeSelector,
            remoteTaskFactory,
            session,
            false,
            queryExecutor,
            rootOutputBuffers,
            nodeTaskMap,
            nodePartitioningManager,
            new QueryBloomFilter(planFragmentList));
        queryScheduler.set(scheduler);

        // if query was canceled during scheduler creation, abort the scheduler
        // directly since the callback may have already fired
        if (stateMachine.isDone()) {
            scheduler.abort();
            queryScheduler.set(null);
        }
    }

    @Override
    public QueryInfo getQueryInfo() {
        SqlQueryScheduler scheduler = queryScheduler.get();

        Optional<QueryInfo> finalQueryInfo = stateMachine.getFinalQueryInfo();
        if (finalQueryInfo.isPresent()) {
            return finalQueryInfo.get();
        }

        return buildQueryInfo(scheduler);
    }

    private QueryInfo buildQueryInfo(SqlQueryScheduler scheduler) {
        Optional<StageInfo> stageInfo = Optional.empty();
        if (scheduler != null) {
            stageInfo = Optional.ofNullable(scheduler.getStageInfo());
        }

        QueryInfo queryInfo = stateMachine.updateQueryInfo(stageInfo, querySelf);
        if (queryInfo.isFinalQueryInfo()) {
            // capture the final query state and drop reference to the scheduler
            queryScheduler.set(null);
        }

        return queryInfo;
    }

    @Override
    public long getTotalMemoryReservation() {
        SqlQueryScheduler scheduler = queryScheduler.get();
        Optional<QueryInfo> finalQueryInfo = stateMachine.getFinalQueryInfo();
        if (finalQueryInfo.isPresent()) {
            return finalQueryInfo.get().getQueryStats().getTotalMemoryReservation().toBytes();
        }
        if (scheduler == null) {
            return 0;
        }
        return scheduler.getTotalMemoryReservation();
    }

    @Override
    public Duration getTotalCpuTime() {
        SqlQueryScheduler scheduler = queryScheduler.get();
        Optional<QueryInfo> finalQueryInfo = stateMachine.getFinalQueryInfo();
        if (finalQueryInfo.isPresent()) {
            return finalQueryInfo.get().getQueryStats().getTotalCpuTime();
        }
        if (scheduler == null) {
            return new Duration(0, SECONDS);
        }
        return scheduler.getTotalCpuTime();
    }

    @Override
    public void cancelStage(StageId stageId) {
        requireNonNull(stageId, "stageId is null");

        //try (SetThreadName ignored = new SetThreadName("Query-%s", stateMachine.getQueryId())) {
        SqlQueryScheduler scheduler = queryScheduler.get();
        if (scheduler != null) {
            scheduler.cancelStage(stageId);
        }
    }

    @Override
    public void close(Throwable throwable) {

    }

    @Override
    public void mergeBloomFilter(List<BloomFilterInfo> filterInfos) {
        if (filterInfos != null && filterInfos.size() > 0 && queryScheduler.get() != null) {
            queryScheduler.get().getBloomFilterManager().mergeBuildBloomFilter(filterInfos);
        }
    }

    @Override
    public boolean isNeedReserveAfterExpired() {
        return needReserved;
    }

    @Override
    public TaskContext getTaskContext() {
        return null;
    }

    public static class SqlQueryExecutionFactory implements QueryExecutionFactory<SqlQueryExecution> {

        private LocationFactory locationFactory;
        private ExecutorService executor;
        private RemoteTaskFactory remoteTaskFactory;
        private NodeTaskMap nodeTaskMap;
        private NodeScheduler nodeScheduler;
        private NodePartitioningManager nodePartitioningManager;

        protected SqlQueryExecutionFactory() {

        }

        @Inject
        public SqlQueryExecutionFactory(LocationFactory locationFactory,
                                        @ForQueryExecution ExecutorService executor,
                                        RemoteTaskFactory remoteTaskFactory,
                                        NodeTaskMap nodeTaskMap,
                                        NodeScheduler nodeScheduler,
                                        NodePartitioningManager nodePartitioningManager) {
            this.locationFactory = locationFactory;
            this.executor = executor;
            this.remoteTaskFactory = remoteTaskFactory;
            this.nodeTaskMap = nodeTaskMap;
            this.nodeScheduler = nodeScheduler;
            this.nodePartitioningManager = nodePartitioningManager;
        }

        @Override
        public SqlQueryExecution createQueryExecution(QueryManager queryManager, String query, RelNode physicalPlan,
                                                      Session session) {
            return new SqlQueryExecution(query, physicalPlan, session,
                locationFactory.createQueryLocation(session.getQueryId()), executor, locationFactory,
                remoteTaskFactory, this.nodeTaskMap, nodeScheduler, nodePartitioningManager);
        }

        @Override
        public void setThreadPoolExecutor(int poolSize) {
            if (executor instanceof ThreadPoolExecutor) {
                ThreadPoolExecutor threadPoolExecutor = (ThreadPoolExecutor) executor;
                if (poolSize > 0 && threadPoolExecutor.getMaximumPoolSize() != poolSize) {
                    threadPoolExecutor.setCorePoolSize(poolSize);
                    threadPoolExecutor.setMaximumPoolSize(poolSize);
                }
            }
        }
    }

    public static class NullExecutionFactory extends SqlQueryExecutionFactory {

        @Override
        public SqlQueryExecution createQueryExecution(QueryManager queryManager, String query, RelNode physicalPlan,
                                                      Session session) {
            throw new IllegalStateException("Local mode don't support submit cluster query!");
        }

        @Override
        public void setThreadPoolExecutor(int poolSize) {
            throw new IllegalStateException();
        }
    }
}
