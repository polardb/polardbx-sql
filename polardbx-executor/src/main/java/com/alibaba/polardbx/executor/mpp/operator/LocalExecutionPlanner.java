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

package com.alibaba.polardbx.executor.mpp.operator;

import com.alibaba.polardbx.common.DefaultSchema;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.mpp.Session;
import com.alibaba.polardbx.executor.mpp.execution.EmptyMemSystemListener;
import com.alibaba.polardbx.executor.mpp.execution.RecordMemSystemListener;
import com.alibaba.polardbx.executor.mpp.execution.buffer.OutputBuffer;
import com.alibaba.polardbx.executor.mpp.execution.buffer.OutputBufferMemoryManager;
import com.alibaba.polardbx.executor.mpp.execution.buffer.PagesSerdeFactory;
import com.alibaba.polardbx.executor.mpp.execution.buffer.SpilledOutputBufferMemoryManager;
import com.alibaba.polardbx.executor.mpp.operator.factory.CacheExecFactory;
import com.alibaba.polardbx.executor.mpp.operator.factory.DynamicValuesExecutorFactory;
import com.alibaba.polardbx.executor.mpp.operator.factory.EmptyExecutorFactory;
import com.alibaba.polardbx.executor.mpp.operator.factory.ExchangeExecFactory;
import com.alibaba.polardbx.executor.mpp.operator.factory.ExecutorFactory;
import com.alibaba.polardbx.executor.mpp.operator.factory.ExpandExecFactory;
import com.alibaba.polardbx.executor.mpp.operator.factory.FilterExecFactory;
import com.alibaba.polardbx.executor.mpp.operator.factory.HashAggExecutorFactory;
import com.alibaba.polardbx.executor.mpp.operator.factory.HashGroupJoinExecutorFactory;
import com.alibaba.polardbx.executor.mpp.operator.factory.HybridHashJoinExecutorFactory;
import com.alibaba.polardbx.executor.mpp.operator.factory.LimitExecFactory;
import com.alibaba.polardbx.executor.mpp.operator.factory.LocalBufferConsumerFactory;
import com.alibaba.polardbx.executor.mpp.operator.factory.LocalBufferExecutorFactory;
import com.alibaba.polardbx.executor.mpp.operator.factory.LocalExchangeConsumerFactory;
import com.alibaba.polardbx.executor.mpp.operator.factory.LocalMergeSortExecutorFactory;
import com.alibaba.polardbx.executor.mpp.operator.factory.LogicalCorrelateFactory;
import com.alibaba.polardbx.executor.mpp.operator.factory.LogicalViewExecutorFactory;
import com.alibaba.polardbx.executor.mpp.operator.factory.LookupJoinExecFactory;
import com.alibaba.polardbx.executor.mpp.operator.factory.LoopJoinExecutorFactory;
import com.alibaba.polardbx.executor.mpp.operator.factory.MaterializedJoinExecFactory;
import com.alibaba.polardbx.executor.mpp.operator.factory.NonBlockGeneralExecFactory;
import com.alibaba.polardbx.executor.mpp.operator.factory.OutputExecutorFactory;
import com.alibaba.polardbx.executor.mpp.operator.factory.OverWindowFramesExecFactory;
import com.alibaba.polardbx.executor.mpp.operator.factory.ParallelHashJoinExecutorFactory;
import com.alibaba.polardbx.executor.mpp.operator.factory.PipelineFactory;
import com.alibaba.polardbx.executor.mpp.operator.factory.ProjectExecFactory;
import com.alibaba.polardbx.executor.mpp.operator.factory.RuntimeFilterBuilderExecFactory;
import com.alibaba.polardbx.executor.mpp.operator.factory.SortAggExecFactory;
import com.alibaba.polardbx.executor.mpp.operator.factory.SortExecutorFactory;
import com.alibaba.polardbx.executor.mpp.operator.factory.SortMergeJoinFactory;
import com.alibaba.polardbx.executor.mpp.operator.factory.SubPipelineFactory;
import com.alibaba.polardbx.executor.mpp.operator.factory.TopNExecutorFactory;
import com.alibaba.polardbx.executor.mpp.operator.factory.UnionExecFactory;
import com.alibaba.polardbx.executor.mpp.operator.factory.ValueExecutorFactory;
import com.alibaba.polardbx.executor.mpp.planner.LocalExchange;
import com.alibaba.polardbx.executor.mpp.planner.PipelineFragment;
import com.alibaba.polardbx.executor.mpp.planner.PlanFragment;
import com.alibaba.polardbx.executor.mpp.planner.RemoteSourceNode;
import com.alibaba.polardbx.executor.mpp.planner.WrapPipelineFragment;
import com.alibaba.polardbx.executor.mpp.split.SplitInfo;
import com.alibaba.polardbx.executor.mpp.split.SplitManager;
import com.alibaba.polardbx.executor.operator.spill.SpillerFactory;
import com.alibaba.polardbx.executor.operator.util.bloomfilter.BloomFilterExpression;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.join.EquiJoinKey;
import com.alibaba.polardbx.optimizer.core.join.EquiJoinUtils;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.core.rel.BKAJoin;
import com.alibaba.polardbx.optimizer.core.rel.BaseTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.Gather;
import com.alibaba.polardbx.optimizer.core.rel.HashAgg;
import com.alibaba.polardbx.optimizer.core.rel.HashGroupJoin;
import com.alibaba.polardbx.optimizer.core.rel.HashJoin;
import com.alibaba.polardbx.optimizer.core.rel.Limit;
import com.alibaba.polardbx.optimizer.core.rel.LocalBufferNode;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.MaterializedSemiJoin;
import com.alibaba.polardbx.optimizer.core.rel.MemSort;
import com.alibaba.polardbx.optimizer.core.rel.MergeSort;
import com.alibaba.polardbx.optimizer.core.rel.NLJoin;
import com.alibaba.polardbx.optimizer.core.rel.PhysicalFilter;
import com.alibaba.polardbx.optimizer.core.rel.PhysicalProject;
import com.alibaba.polardbx.optimizer.core.rel.SemiBKAJoin;
import com.alibaba.polardbx.optimizer.core.rel.SemiHashJoin;
import com.alibaba.polardbx.optimizer.core.rel.SemiNLJoin;
import com.alibaba.polardbx.optimizer.core.rel.SemiSortMergeJoin;
import com.alibaba.polardbx.optimizer.core.rel.SortAgg;
import com.alibaba.polardbx.optimizer.core.rel.SortMergeJoin;
import com.alibaba.polardbx.optimizer.core.rel.SortWindow;
import com.alibaba.polardbx.optimizer.core.rel.TopN;
import com.alibaba.polardbx.optimizer.core.rel.mpp.MppExchange;
import com.alibaba.polardbx.optimizer.memory.MemoryPool;
import com.alibaba.polardbx.optimizer.memory.MemoryType;
import com.alibaba.polardbx.optimizer.utils.CalciteUtils;
import com.alibaba.polardbx.optimizer.utils.QueryConcurrencyPolicy;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.optimizer.utils.RexUtils;
import com.alibaba.polardbx.statistics.RuntimeStatistics;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import io.airlift.http.client.HttpClient;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.DynamicValues;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalExpand;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalOutFile;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.logical.RuntimeFilterBuilder;
import org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

public class LocalExecutionPlanner {

    private static final Logger log = LoggerFactory.getLogger(LocalExecutionPlanner.class);

    private static final Set<Class<? extends RelNode>> SUPPORT_NODES = ImmutableSet.of(
        LogicalView.class, HashJoin.class, SemiHashJoin.class, SortMergeJoin.class, SemiSortMergeJoin.class,
        BKAJoin.class, SemiBKAJoin.class, NLJoin.class, SemiNLJoin.class, MaterializedSemiJoin.class, HashAgg.class,
        SortAgg.class, LogicalProject.class, LogicalExpand.class, LogicalFilter.class, MemSort.class, Limit.class,
        TopN.class, LogicalSort.class, LogicalValues.class, RemoteSourceNode.class, Gather.class,
        MergeSort.class, LogicalUnion.class, Exchange.class, HashGroupJoin.class, LogicalExpand.class,
        DynamicValues.class, SortWindow.class, RuntimeFilterBuilder.class, LogicalCorrelate.class,
        LogicalOutFile.class, PhysicalProject.class, PhysicalFilter.class);

    public static final Set<Class<? extends RelNode>> SUPPORT_ONE_SIDE_CACHE_NODES = ImmutableSet.of(
        HashJoin.class, SemiHashJoin.class, NLJoin.class, SemiNLJoin.class, HashGroupJoin.class);

    public static final Set<Class<? extends RelNode>> SUPPORT_ALL_CACHE_NODES = ImmutableSet.of(
        HashAgg.class, MemSort.class, TopN.class, LogicalSort.class);

    public static final boolean isAssignableFrom(Class<?> ret) {
        return isAssignableFrom(ret, SUPPORT_NODES);
    }

    public static final boolean isAssignableFrom(Class<?> ret, Set<Class<? extends RelNode>> relNodes) {
        for (Class<? extends RelNode> classNode : relNodes) {
            if (classNode.isAssignableFrom(ret)) {
                return true;
            }
        }
        return false;
    }

    private final ExchangeClientSupplier exchangeClientSupplier;
    private final PagesSerdeFactory pagesSerdeFactory;
    private ExecutionContext context;
    private int taskNumber;
    private int pipelineIdGen;
    private List<PipelineFactory> pipelineFactorys;
    private final boolean supportLocalBuffer;
    private final long localBufferSize;
    private final Executor notificationExecutor;
    private final SpillerFactory spillerFactory;

    private final boolean isCluster;

    private int defaultParallelism;

    //FIXME MPP模式该参数已经失效
    private int bkaJoinParallelism;

    //在Final Sort后pipeline并发度都需要保持为1
    private boolean holdCollation;

    private HttpClient httpClient;
    private URI runtimeFilterUpdateUri;
    private final Map<Integer, BloomFilterExpression> bloomFilterExpressionMap = new HashMap<>();
    // Used to skip runtime filter in AP_LOCAL executor mode.
    private final boolean enableRuntimeFilter;

    //only be used for logicalView
    private int totalPrefetch;

    private boolean forbidMultipleReadConn;

    private boolean expandView;

    public LocalExecutionPlanner(ExecutionContext context, ExchangeClientSupplier exchangeClientSupplier,
                                 int defaultParallelism, int bkaJoinParallelism, int taskNumber, int prefetch,
                                 Executor notificationExecutor, SpillerFactory spillerFactory,
                                 HttpClient httpClient,
                                 URI runtimeFilterUpdateUri,
                                 boolean enableRuntimeFilter) {
        this.exchangeClientSupplier = exchangeClientSupplier;
        this.pagesSerdeFactory = new PagesSerdeFactory(false);
        this.context = context;
        this.defaultParallelism = defaultParallelism;
        this.bkaJoinParallelism = bkaJoinParallelism;
        this.taskNumber = taskNumber;
        this.totalPrefetch = prefetch;
        this.pipelineIdGen = 0;
        this.pipelineFactorys = new ArrayList<>();
        this.supportLocalBuffer =
            context.getParamManager().getBoolean(ConnectionParams.MPP_TASK_LOCAL_BUFFER_ENABLED);
        this.isCluster = ExecUtils.isMppMode(context);
        this.localBufferSize = context.getParamManager().getLong(ConnectionParams.MPP_TASK_LOCAL_MAX_BUFFER_SIZE);
        this.notificationExecutor = notificationExecutor;
        this.spillerFactory = spillerFactory;
        this.httpClient = httpClient;
        this.runtimeFilterUpdateUri = runtimeFilterUpdateUri;
        this.enableRuntimeFilter = enableRuntimeFilter;
    }

    public OutputBufferMemoryManager createLocalMemoryManager() {
        return new OutputBufferMemoryManager(localBufferSize, new EmptyMemSystemListener(), notificationExecutor);
    }

    public OutputBufferMemoryManager createCacheAllBufferMemoryManager(String memoryName, long bufferSize) {
        MemoryPool memoryPool = context.getMemoryPool().getOrCreatePool(
            memoryName, bufferSize,
            MemoryType.TASK);
        return new SpilledOutputBufferMemoryManager(
            bufferSize, new RecordMemSystemListener(memoryPool.getMemoryAllocatorCtx()), notificationExecutor);
    }

    public Map<Integer, BloomFilterExpression> getBloomFilterExpressionMap() {
        return bloomFilterExpressionMap;
    }

    public List<PipelineFactory> plan(PlanFragment fragment, OutputBuffer outputBuffer, Session session) {
        try {
            DefaultSchema.setSchemaName(session.getSchema());
            RelNode relNode = fragment.getSerRootNode(
                context.getSchemaName(), PlannerContext.fromExecutionContext(context));
            //PipelineFactory 创建executor的构造函数中，可能会去引用RelMetadataProvider，所以这里需要保证一定是
            //DrdsRelMetadataProvider
            RelMetadataQuery.THREAD_PROVIDERS.set(JaninoRelMetadataProvider.of(
                relNode.getCluster().getMetadataProvider()));
            PipelineFragment pipelineFragment = new PipelineFragment(defaultParallelism, relNode);
            ExecutorFactory factory = visit(null, relNode, pipelineFragment);
            if (pipelineFragment.getParallelism() > 1
                && fragment.getPartitioningScheme().getShuffleHandle().isMergeSort()) {
                holdCollation = true;
                MergeSort sort = MergeSort
                    .create(relNode, RelCollations.of(fragment.getPartitioningScheme().getOrderByOptions().stream()
                        .map(t -> t.createRelFieldCollation()).collect(
                            Collectors.toList())), null, null);
                factory = new LocalMergeSortExecutorFactory(sort, factory, pipelineFragment.getParallelism());
                pipelineFragment.holdSingleTonParallelism();
            }

            PipelineFactory pipelineFactory = new PipelineFactory(factory,
                new OutputExecutorFactory(fragment, outputBuffer, pagesSerdeFactory),
                pipelineFragment.setPipelineId(pipelineIdGen++));
            pipelineFactorys.add(pipelineFactory);
            if (log.isDebugEnabled()) {
                for (PipelineFactory pipeline : pipelineFactorys) {
                    StringBuilder stringBuilder = new StringBuilder();
                    pipeline.getProduceFactory().explain(stringBuilder);
                    log.debug("queryId=" + session.getQueryId() + ", stageId=" + session.getStageId() +
                        ", pipelineId=" + pipeline.getFragment().getPipelineId() + ", produce=" + stringBuilder
                        .toString() +
                        ", consume=" + pipeline.getConsumeFactory().getClass().getSimpleName() +
                        ", pipelineFragment=" + pipeline.getFragment());
                }
            }
            return pipelineFactorys;
        } catch (Throwable t) {
            log.error("Don't support some relNode in mpp mode!", t);
            String error = t.getMessage() != null ? t.getMessage() : "Don't support some relNode in mpp mode!";
            throw new TddlRuntimeException(ErrorCode.ERR_GENERATE_PLAN, t, error);
        }
    }

    public List<PipelineFactory> plan(RelNode relNode, LocalBufferExecutorFactory resultBuffer,
                                      OutputBufferMemoryManager manager, String queryId) {
        try {
            DefaultSchema.setSchemaName(context.getSchemaName());
            //PipelineFactory 创建executor的构造函数中，可能会去引用RelMetadataProvider，所以这里需要保证一定是
            //DrdsRelMetadataProvider
            RelMetadataQuery.THREAD_PROVIDERS.set(JaninoRelMetadataProvider.of(
                relNode.getCluster().getMetadataProvider()));
            PipelineFragment pipelineFragment = new PipelineFragment(defaultParallelism, relNode);
            ExecutorFactory factory = visit(null, relNode, pipelineFragment);

            if (factory instanceof NonBlockGeneralExecFactory) {
                throw new RuntimeException("Don't support one level NonBlockGeneralExecFactory!");
            }
            LocalExchange localExchange = new LocalExchange(
                CalciteUtils.getTypes(relNode.getRowType()), ImmutableList.of(), LocalExchange.LocalExchangeMode.SINGLE,
                true);
            LocalExchangeConsumerFactory consumerFactory =
                new LocalExchangeConsumerFactory(resultBuffer, manager, localExchange);

            PipelineFactory pipelineFactory = new PipelineFactory(
                factory, consumerFactory, pipelineFragment.setPipelineId(pipelineIdGen++));
            pipelineFactorys.add(pipelineFactory);
            if (log.isDebugEnabled()) {
                for (PipelineFactory pipeline : pipelineFactorys) {
                    StringBuilder stringBuilder = new StringBuilder();
                    pipeline.getProduceFactory().explain(stringBuilder);
                    log.debug("queryId=" + queryId + ", pipelineId=" + pipeline.getPipelineId() +
                        ", produce=" + stringBuilder.toString() +
                        ", consume=" + pipeline.getConsumeFactory().getClass().getSimpleName() +
                        ", pipelineFragment=" + pipeline.getFragment());
                }
            }

            if (forbidMultipleReadConn) {
                List<PipelineFactory> pipelineFactoryList = new ArrayList<>();
                for (PipelineFactory pipelineFactory1 : pipelineFactorys) {
                    if (pipelineFactory1.getFragment() instanceof WrapPipelineFragment) {
                        int start = 0;
                        for (PipelineFragment subFragment :
                            ((WrapPipelineFragment) pipelineFactory1.getFragment()).getSubPipelineFragment()) {
                            pipelineFactoryList.add(new SubPipelineFactory(pipelineFactory1, subFragment, start));
                            start += subFragment.getParallelism();
                        }
                    } else {
                        pipelineFactoryList.add(pipelineFactory1);
                    }
                }
                return pipelineFactoryList;
            } else {
                return pipelineFactorys;
            }
        } catch (Throwable t) {
            log.error("can't split the sub plan in worker!", t);
            String error = t.getMessage() != null ? t.getMessage() : "can't split the sub plan in worker!";
            throw new TddlRuntimeException(ErrorCode.ERR_GENERATE_PLAN, t, error);
        }
    }

    private ExecutorFactory visit(RelNode parent, RelNode current, PipelineFragment pipelineFragment) {
        Integer relatedId = current.getRelatedId();
        boolean supportCache = context.getCacheRelNodeIds().contains(current.getRelatedId());
        if (context.getCacheRefs().containsKey(relatedId)) {
            Pair<CacheExecFactory, PipelineFragment> pair =
                (Pair<CacheExecFactory, PipelineFragment>) context.getCacheRefs().get(relatedId);
            CacheExecFactory cacheExecFactory = pair.left;
            if (cacheExecFactory.isAllReady()) {
                pipelineFragment.setParallelism(pair.right.getParallelism());
                return new CacheExecFactory(
                    pipelineFragment.getParallelism(), cacheExecFactory.getAllExecutors(context));
            }
        }

        if (supportCache && !context.getCacheRefs().containsKey(relatedId)) {
            PipelineFragment cacheFragment = new PipelineFragment(pipelineFragment.getParallelism(), current);
            ExecutorFactory executorFactory = visitRelNode(parent, current, cacheFragment);
            if (holdCollation) {
                //排序属性之后修正并发度
                cacheFragment.holdSingleTonParallelism();
                pipelineFragment.holdSingleTonParallelism();
            }
            OutputBufferMemoryManager localBufferManager = createLocalMemoryManager();
            List<DataType> innerColumns = CalciteUtils.getTypes(current.getRowType());
            CacheExecFactory cacheExecFactory = new CacheExecFactory(innerColumns, cacheFragment.getParallelism());
            //generate child's pipelineFactory
            pipelineFragment.setParallelism(cacheFragment.getParallelism());
            LocalExchange localExchange =
                new LocalExchange(CalciteUtils.getTypes(current.getRowType()), ImmutableList.of(),
                    LocalExchange.LocalExchangeMode.DIRECT, true);
            LocalExchangeConsumerFactory consumerFactory =
                new LocalExchangeConsumerFactory(cacheExecFactory, localBufferManager, localExchange);
            PipelineFactory cachePipelineFactory =
                new PipelineFactory(executorFactory, consumerFactory, cacheFragment.setPipelineId(pipelineIdGen++));
            pipelineFragment.addDependency(cachePipelineFactory.getPipelineId());
            pipelineFragment.addChild(cacheFragment);
            pipelineFactorys.add(cachePipelineFactory);
            Pair<CacheExecFactory, PipelineFragment> pair = new Pair(cacheExecFactory, cacheFragment);
            context.getCacheRefs().put(current.getRelatedId(), pair);
            return cacheExecFactory;
        } else {
            ExecutorFactory ret = visitRelNode(parent, current, pipelineFragment);
            if (holdCollation) {
                //排序属性之后修正并发度
                pipelineFragment.holdSingleTonParallelism();
            }
            return ret;
        }
    }

    //visit -> visitRelNode -> PhysicalVisit
    private ExecutorFactory visitRelNode(RelNode parent, RelNode current, PipelineFragment pipelineFragment) {
        if (current instanceof LogicalView) {
            return visitView(parent, (LogicalView) current, pipelineFragment);
        } else if (current instanceof HashJoin) {
            return visitHashJoin((Join) current, pipelineFragment, ((HashJoin) current).getOtherCondition(),
                ((HashJoin) current).getEqualCondition(), false, null
            );
        } else if (current instanceof SemiHashJoin) {
            return visitSemiJoin((SemiHashJoin) current, pipelineFragment);
        } else if (current instanceof SortMergeJoin) {
            SortMergeJoin mergeJoin = (SortMergeJoin) current;
            List<Boolean> columnIsAscending = mergeJoin.getCollation().getFieldCollations().stream().map(t ->
                (t.getDirection() == RelFieldCollation.Direction.ASCENDING
                    || t.getDirection() == RelFieldCollation.Direction.STRICTLY_ASCENDING))
                .collect(Collectors.toList());

            return visitSortMergeJoin(mergeJoin, mergeJoin.getLeftColumns(), mergeJoin.getRightColumns(),
                columnIsAscending,
                mergeJoin.getOtherCondition(), false, null, pipelineFragment
            );
        } else if (current instanceof SemiSortMergeJoin) {
            SemiSortMergeJoin mergeJoin = (SemiSortMergeJoin) current;
            boolean maxOneRow =
                mergeJoin.getJoinType() == JoinRelType.LEFT || mergeJoin.getJoinType() == JoinRelType.INNER;
            List<Boolean> columnIsAscending = mergeJoin.getCollation().getFieldCollations().stream().map(t ->
                (t.getDirection() == RelFieldCollation.Direction.ASCENDING
                    || t.getDirection() == RelFieldCollation.Direction.STRICTLY_ASCENDING))
                .collect(Collectors.toList());
            return visitSortMergeJoin(mergeJoin, mergeJoin.getLeftColumns(), mergeJoin.getRightColumns(),
                columnIsAscending,
                mergeJoin.getOtherCondition(), maxOneRow, mergeJoin.getOperands(), pipelineFragment
            );
        } else if (current instanceof HashGroupJoin) {
            return visitGroupJoin(parent, (HashGroupJoin) current, pipelineFragment);
        } else if (current instanceof BKAJoin) {
            return visitBKAJoin((Join) current, pipelineFragment);
        } else if (current instanceof SemiBKAJoin) {
            return visitBKAJoin((Join) current, pipelineFragment);
        } else if (current instanceof NLJoin) {
            return visitNLJoin((Join) current, pipelineFragment, ((NLJoin) current).getCondition(), false, null, null);
        } else if (current instanceof SemiNLJoin) {
            SemiNLJoin nlJoin = (SemiNLJoin) current;
            boolean maxOneRow = nlJoin.getJoinType() == JoinRelType.LEFT || nlJoin.getJoinType() == JoinRelType.INNER;
            return visitNLJoin(nlJoin, pipelineFragment, nlJoin.getCondition(), maxOneRow, nlJoin.getOperands(),
                buildAntiCondition(nlJoin));
        } else if (current instanceof MaterializedSemiJoin) {
            return visitMaterializedSemiJoin((MaterializedSemiJoin) current, pipelineFragment);
        } else if (current instanceof HashAgg) {
            return visitHashAgg((HashAgg) current, pipelineFragment);
        } else if (current instanceof SortAgg) {
            return visitSortAgg((SortAgg) current, pipelineFragment);
        } else if (current instanceof Project) {
            return visitProject((Project) current, pipelineFragment);
        } else if (current instanceof Filter) {
            return visitFilter((Filter) current, pipelineFragment);
        } else if (current instanceof RuntimeFilterBuilder) {
            if (enableRuntimeFilter) {
                return visitRuntimeFilterBuild((RuntimeFilterBuilder) current, pipelineFragment);
            } else {
                return visit(parent, ((RuntimeFilterBuilder) current).getInput(), pipelineFragment);
            }
        } else if (current instanceof MemSort) {
            return visitMemSort(parent, (MemSort) current, pipelineFragment);
        } else if (current instanceof Limit) {
            return visitLimit((Limit) current, pipelineFragment);
        } else if (current instanceof TopN) {
            return visitTopN(parent, (TopN) current, pipelineFragment);
        } else if (current instanceof LogicalValues) {
            return visitValue((LogicalValues) current, pipelineFragment);
        } else if (current instanceof DynamicValues) {
            return visitDynamicValues((DynamicValues) current, pipelineFragment);
        } else if (current instanceof RemoteSourceNode) {
            return visitRemoteNode((RemoteSourceNode) current, pipelineFragment);
        } else if (current instanceof Gather) {
            //对Gather && MppExchange特殊处理，Gather现在并没有统计CPU耗时，
            // 计算Gather && MppExchange的cpu耗时时，不再减去input算子的耗时，否则结果会负数
            RuntimeStatistics statistics = (RuntimeStatistics) context.getRuntimeStatistics();
            if (statistics != null) {
                RuntimeStatistics.OperatorStatisticsGroup og =
                    statistics.getRelationToStatistics().get(current.getRelatedId());
                if (og != null) {
                    og.hasInputOperator = false;
                }
            }
            return visit(parent, ((Gather) current).getInput(), pipelineFragment);
        } else if (current instanceof MppExchange) {
            return visitMppExchange(parent, (MppExchange) current, pipelineFragment);
        } else if (current instanceof MergeSort) {
            return visitMergeSort(parent, (MergeSort) current, pipelineFragment);
        } else if (current instanceof LogicalUnion) {
            return visitUnion(parent, (LogicalUnion) current, pipelineFragment);
        } else if (current instanceof BaseTableOperation) {
            return visitBaseTable(parent, (BaseTableOperation) current, pipelineFragment);
        } else if (current instanceof LogicalExpand) {
            return visitExpand((LogicalExpand) current, pipelineFragment);
        } else if (current instanceof SortWindow) {
            return visitOverWindow((SortWindow) current, pipelineFragment);
        } else if (current instanceof LogicalCorrelate) {
            return visitLogicalCorrelate((LogicalCorrelate) current, pipelineFragment);
        } else {
            return visitGeneralExec(current, pipelineFragment);
        }
    }

    private RexNode buildAntiCondition(SemiNLJoin nlJoin) {
        List<RexNode> operands = nlJoin.getOperands();
        if (operands == null || operands.size() == 0) {
            return null;
        }
        int startIndex = nlJoin.getLeft().getRowType().getFieldCount();
        SqlOperator so = RexUtils.buildSemiOperator(nlJoin.getOperator());
        RexBuilder builder = nlJoin.getCluster().getRexBuilder();

        List<RelDataType> rTypes =
            nlJoin.getRight().getRowType().getFieldList().stream().map(t -> t.getType()).collect(Collectors.toList());
        if (operands.size() == 1) {
            return builder.makeCall(so, operands.get(0), builder.makeInputRef(rTypes.get(0), startIndex));
        }
        List<RexNode> toAnd = Lists.newArrayList();
        for (int i = 0; i < nlJoin.getOperands().size(); i++) {
            toAnd.add(builder.makeCall(so, operands.get(i), builder.makeInputRef(rTypes.get(i), startIndex + i)));
        }
        return builder.makeCall(SqlStdOperatorTable.AND, toAnd);
    }

    private ExecutorFactory visitLogicalCorrelate(LogicalCorrelate current, PipelineFragment pipelineFragment) {
        if (!ExecUtils.allowMultipleReadConns(context, null)) {
            PipelineFragment producerFragment = new PipelineFragment(
                pipelineFragment.getParallelism(), current.getLeft());

            Pair<PipelineFactory, LocalBufferExecutorFactory> pair = createAllBufferPipelineFragment(
                current, producerFragment, "CacheForCorrelate@");

            pipelineFragment.setParallelism(producerFragment.getParallelism());
            pipelineFactorys.add(pair.left);
            pipelineFragment.addDependency(pair.left.getPipelineId());
            pipelineFragment.addChild(producerFragment);
            return new LogicalCorrelateFactory(current, pair.right);
        } else {
            ExecutorFactory inner = visit(current, current.getLeft(), pipelineFragment);
            return new LogicalCorrelateFactory(current, inner);
        }
    }

    private ExecutorFactory visitOverWindow(SortWindow overWindow, PipelineFragment pipelineFragment) {
        ExecutorFactory overWindowFactory = createOverWindowFactory(overWindow, pipelineFragment);
        return overWindowFactory;
    }

    private ExecutorFactory createOverWindowFactory(SortWindow overWindow, PipelineFragment pipelineFragment) {
        RelNode input = overWindow.getInput();
        ExecutorFactory childExecutorFactory = visit(overWindow, input, pipelineFragment);
        return new OverWindowFramesExecFactory(overWindow, childExecutorFactory);
    }

    private ExecutorFactory visitExpand(LogicalExpand expand, PipelineFragment pipelineFragment) {
        ExecutorFactory childExecutorFactory = visit(expand, expand.getInput(), pipelineFragment);
        return new ExpandExecFactory(expand, childExecutorFactory);
    }

    private ExecutorFactory visitBaseTable(RelNode parent, BaseTableOperation other,
                                           PipelineFragment pipelineFragment) {
        if (isCluster) {
            throw new RuntimeException("Don't support SingleTableOperation for mpp!");
        }

        LogicalView logicalView = ExecUtils.convertToLogicalView(other);
        logicalView.setRelatedId(other.getRelatedId());
        pipelineFragment.addLogicalView(logicalView);
        pipelineFragment.holdSingleTonParallelism();
        SplitInfo splitInfo = new SplitManager().getSingleSplit(logicalView, context);
        pipelineFragment.putSource(logicalView.getRelatedId(), splitInfo);
        return createViewFactory(parent, logicalView, pipelineFragment, spillerFactory, 1);
    }

    private ExecutorFactory visitUnion(RelNode parent, LogicalUnion union, PipelineFragment pipelineFragment) {
        if (!union.all) {
            HashAgg agg =
                HashAgg.create(union.getTraitSet(), union,
                    ImmutableBitSet.range(union.getRowType().getFieldCount()), null, ImmutableList.of());
            Integer rowCount = context.getRecordRowCnt().get(union.getRelatedId());
            context.getRecordRowCnt().put(agg.getRelatedId(), rowCount);
            return visit(parent, agg, pipelineFragment);
        } else {

            List<ExecutorFactory> inputs = new ArrayList<>();
            List<PipelineFragment> childFragments = new ArrayList<>();
            List<Integer> parallelisms = new ArrayList<>();
            int parallelism = 0;

            for (int i = 0; i < union.getInputs().size(); i++) {
                PipelineFragment child = new PipelineFragment(defaultParallelism, union.getInput(i));
                ExecutorFactory factory = visit(union, union.getInput(i), child);
                child.setPipelineId(pipelineIdGen++);

                childFragments.add(child);
                inputs.add(factory);
                parallelisms.add(child.getParallelism());
                parallelism += child.getParallelism();
            }

            if (forbidMultipleReadConn && inputs.size() > 1) {
                PipelineFragment preSubPipeline = null;
                for (PipelineFragment childFragment : childFragments) {
                    if (preSubPipeline != null) {
                        childFragment.getProperties().addDependencyForChildren(preSubPipeline.getPipelineId());
                        childFragment.addDependency(preSubPipeline.getPipelineId());
                    }
                    preSubPipeline = childFragment;
                    pipelineFragment.addChild(childFragment);
                }

                PipelineFragment wrapPipelineFragment = new WrapPipelineFragment(childFragments);
                UnionExecFactory unionExecFactory = new UnionExecFactory(union, inputs, parallelisms);

                List<DataType> columns = CalciteUtils.getTypes(union.getRowType());
                OutputBufferMemoryManager localBufferManager = createLocalMemoryManager();
                LocalBufferExecutorFactory localBufferExecutorFactory = new LocalBufferExecutorFactory(
                    localBufferManager, columns, pipelineFragment.getParallelism());

                LocalExchange localExchange =
                    new LocalExchange(CalciteUtils.getTypes(union.getRowType()), ImmutableList.of(),
                        LocalExchange.LocalExchangeMode.RANDOM, true);
                LocalExchangeConsumerFactory localConsumerFactory =
                    new LocalExchangeConsumerFactory(localBufferExecutorFactory, localBufferManager, localExchange);
                PipelineFactory producerPipelineFactory = new PipelineFactory(
                    unionExecFactory, localConsumerFactory, wrapPipelineFragment);

                pipelineFactorys.add(producerPipelineFactory);
                if (holdCollation) {
                    //union 算子后会打乱排序属性，所以并发度不强制做约定
                    this.holdCollation = false;
                }
                return localBufferExecutorFactory;
            } else {
                for (PipelineFragment childFragment : childFragments) {
                    pipelineFragment.inherit(childFragment);
                }
                pipelineFragment.setParallelism(parallelism);
                if (holdCollation) {
                    //union 算子后会打乱排序属性，所以并发度不强制做约定
                    this.holdCollation = false;
                }
                return new UnionExecFactory(union, inputs, parallelisms);
            }
        }
    }

    private ExecutorFactory visitSortMergeJoin(Join join, List<Integer> leftColumns,
                                               List<Integer> rightColumns, List<Boolean> columnIsAscending,
                                               RexNode otherCond, boolean maxOneRow, List<RexNode> oprands,
                                               PipelineFragment fragment) {
        ExecutorFactory outer = visit(join, join.getOuter(), fragment);
        ExecutorFactory inner = visit(join, join.getInner(), fragment);

        return new SortMergeJoinFactory(join, leftColumns, rightColumns, columnIsAscending, otherCond, oprands,
            maxOneRow, inner, outer);
    }

    private ExecutorFactory visitSemiJoin(SemiHashJoin current, PipelineFragment fragment) {
        boolean maxOneRow = current.getJoinType() == JoinRelType.LEFT || current.getJoinType() == JoinRelType.INNER;
        return visitHashJoin(current, fragment, current.getOtherCondition(), current.getEqualCondition(),
            maxOneRow, current.getOperands()
        );
    }

    private ExecutorFactory visitNLJoin(Join current, PipelineFragment fragment,
                                        RexNode otherCond, boolean maxOneRow, List<RexNode> oprands,
                                        RexNode antiCondition) {
        OutputBufferMemoryManager localBufferManager = createLocalMemoryManager();
        //build
        List<DataType> columns = CalciteUtils.getTypes(current.getInner().getRowType());
        ExecutorFactory emptybuildFactory = new EmptyExecutorFactory(columns);
        PipelineFragment buildFragment = new PipelineFragment(defaultParallelism, current.getInner());
        ExecutorFactory buildFactory = visit(current, current.getInner(), buildFragment);

        int buildPipelineId = pipelineIdGen++;
        //probe
        ExecutorFactory outerExecutorFactory = visit(current, current.getOuter(), fragment);
        LoopJoinExecutorFactory joinExecutorFactory =
            new LoopJoinExecutorFactory(current, otherCond, maxOneRow, oprands, antiCondition, emptybuildFactory,
                outerExecutorFactory, fragment.getParallelism());
        //generate child's pipelineFactory
        LocalExchange buildExchange =
            new LocalExchange(CalciteUtils.getTypes(current.getInner().getRowType()), ImmutableList.of(),
                LocalExchange.LocalExchangeMode.RANDOM, true);
        LocalExchangeConsumerFactory consumerFactory =
            new LocalExchangeConsumerFactory(joinExecutorFactory, localBufferManager, buildExchange);
        PipelineFactory buildPipelineFactory =
            new PipelineFactory(buildFactory, consumerFactory, buildFragment.setPipelineId(buildPipelineId));
        buildFragment.setBuildDepOnAllConsumers(true);
        pipelineFactorys.add(buildPipelineFactory);
        fragment.addDependency(buildPipelineFactory.getPipelineId());
        if (forbidMultipleReadConn) {
            fragment.getProperties().addDependencyForChildren(buildPipelineFactory.getPipelineId());
        }
        fragment.addChild(buildFragment);

        if (fragment.isContainLimit() && outerExecutorFactory instanceof LogicalViewExecutorFactory &&
            context.getParamManager().getBoolean(ConnectionParams.ENABLE_DRIVING_STREAM_SCAN)) {
            ((LogicalViewExecutorFactory) outerExecutorFactory).enableDrivingResumeSource();
            joinExecutorFactory.enableStreamJoin(true);
        }

        return joinExecutorFactory;
    }

    private int getExchangeParallelism(int parallelism) {
        int n = parallelism - 1;
        n |= n >>> 1;
        n |= n >>> 2;
        n |= n >>> 4;
        n |= n >>> 8;
        n |= n >>> 16;
        return (n < 0) ? 1 : n + 1;
    }

    private ExecutorFactory visitGroupJoin(RelNode parent, HashGroupJoin current, PipelineFragment fragment) {
        RexNode otherCond = current.getOtherCondition();
        RexNode equalCond = current.getEqualCondition();
        boolean maxOneRow = false;
        if (defaultParallelism == 1) {
            //the build child side
            List<DataType> outerColumes = CalciteUtils.getTypes(current.getOuter().getRowType());
            ExecutorFactory emptybuildFactory = new EmptyExecutorFactory(outerColumes);
            PipelineFragment buildFramgent = new PipelineFragment(defaultParallelism, current.getOuter());
            ExecutorFactory childFactory = visit(current, current.getOuter(), buildFramgent);
            int pipelineId = pipelineIdGen++;
            ExecutorFactory probeExecutorFactory = visit(current, current.getInner(), fragment);
            HashGroupJoinExecutorFactory joinExecutorFactory =
                new HashGroupJoinExecutorFactory(current, fragment.getParallelism(), taskNumber,
                    otherCond, equalCond, false, getRelNodeRowCount(current));
            joinExecutorFactory.getInputs().addAll(
                Lists.newArrayList(emptybuildFactory, probeExecutorFactory));

            //generate child's pipelineFactory
            LocalExchange localExchange =
                new LocalExchange(CalciteUtils.getTypes(current.getOuter().getRowType()), ImmutableList.of(),
                    LocalExchange.LocalExchangeMode.DIRECT, true);
            OutputBufferMemoryManager localBufferManager = createLocalMemoryManager();
            LocalExchangeConsumerFactory consumerFactory =
                new LocalExchangeConsumerFactory(joinExecutorFactory, localBufferManager, localExchange);

            PipelineFactory buildPipelineFactory =
                new PipelineFactory(childFactory, consumerFactory, buildFramgent.setPipelineId(pipelineId));
            pipelineFactorys.add(buildPipelineFactory);
            buildFramgent.setBuildDepOnAllConsumers(true);
            fragment.addDependency(buildPipelineFactory.getPipelineId());
            if (forbidMultipleReadConn) {
                fragment.getProperties().addDependencyForChildren(buildPipelineFactory.getPipelineId());
            }
            fragment.addChild(buildFramgent);
            return joinExecutorFactory;
        } else {
            //join build
            RelNode outerInput = current.getOuter();
            List<DataType> buildColumns = CalciteUtils.getTypes(outerInput.getRowType());
            List<EquiJoinKey> joinKeys = EquiJoinUtils
                .buildEquiJoinKeys(current, outerInput, current.getInner(), (RexCall) equalCond, current.getJoinType());
            List<DataType> keyTypes = joinKeys.stream().map(t -> t.getUnifiedType()).collect(Collectors.toList());
            List<Integer> keyInnerIndexes = joinKeys.stream().map(t -> t.getInnerIndex()).collect(Collectors.toList());
            List<Integer> keyOuterIndexes = joinKeys.stream().map(t -> t.getOuterIndex()).collect(Collectors.toList());
            ExecutorFactory emptyBuildFactory = new EmptyExecutorFactory(buildColumns);

            //join build
            PipelineFragment joinBuildFragment = new PipelineFragment(defaultParallelism, outerInput);
            ExecutorFactory joinBuildFactory = visit(current, outerInput, joinBuildFragment);

            //join probe
            List<DataType> joinProbeColumns = CalciteUtils.getTypes(current.getInner().getRowType());
            PipelineFragment joinProbeFragment = new PipelineFragment(defaultParallelism, current.getInner());
            ExecutorFactory probeFactory = visit(current, current.getInner(), joinProbeFragment);

            //HashGroupJoinExecutorFactory

            int parallelism = Math.max(joinProbeFragment.getParallelism(), joinBuildFragment.getParallelism());
            fragment.setParallelism(parallelism);
            HashGroupJoinExecutorFactory joinExecutorFactory =
                new HashGroupJoinExecutorFactory(current, fragment.getParallelism(), taskNumber,
                    otherCond, equalCond, maxOneRow, getRelNodeRowCount(current));

            //generate build pipelineFactory
            LocalExchange joinBuildExchange =
                new LocalExchange(CalciteUtils.getTypes(outerInput.getRowType()), keyOuterIndexes,
                    keyTypes, LocalExchange.LocalExchangeMode.PARTITION, true);

            PipelineFactory buildPipelineFactory = createConsumeSideExchangeFactory(
                joinExecutorFactory, fragment, joinBuildExchange, joinBuildFactory,
                joinBuildFragment,
                buildColumns, supportLocalBuffer & joinBuildFragment.getParallelism() > 1);

            //generate probe pipelineFactory ----
            joinProbeFragment.addDependency(buildPipelineFactory.getPipelineId());
            if (forbidMultipleReadConn) {
                joinProbeFragment.getProperties().addDependencyForChildren(buildPipelineFactory.getPipelineId());
            }
            LocalExchange joinProbeExchange =
                new LocalExchange(CalciteUtils.getTypes(current.getInner().getRowType()), keyInnerIndexes,
                    keyTypes, LocalExchange.LocalExchangeMode.PARTITION, true);

            OutputBufferMemoryManager probeBufferManager = createLocalMemoryManager();
            LocalBufferExecutorFactory probeBufferExecutorFactory = new LocalBufferExecutorFactory(
                probeBufferManager, joinProbeColumns, fragment.getParallelism());

            LocalExchangeConsumerFactory
                probeConsumerFactory = new LocalExchangeConsumerFactory(
                probeBufferExecutorFactory, probeBufferManager, joinProbeExchange);
            PipelineFactory probePipeFactory =
                new PipelineFactory(probeFactory, probeConsumerFactory,
                    joinProbeFragment);

            pipelineFactorys.add(probePipeFactory);

            LocalBufferNode probeBufferNode = LocalBufferNode.create(current.getInner());
            fragment.inheritDependency(joinProbeFragment);
            fragment.addBufferNodeChild(
                probeBufferNode.getInput().getRelatedId(), joinProbeFragment.setPipelineId(pipelineIdGen++));

            //FIXME add buildInput&&probeInPut
            joinExecutorFactory.getInputs().addAll(
                Lists.newArrayList(emptyBuildFactory, probeBufferExecutorFactory));
            return joinExecutorFactory;
        }
    }

    private ExecutorFactory visitHashJoin(Join current, PipelineFragment fragment,
                                          RexNode otherCond, RexNode equalCond, boolean maxOneRow,
                                          List<RexNode> oprands) {
        boolean hybrid = false;

        if (spillerFactory != null && context.getParamManager().getInt(
            ConnectionParams.HYBRID_HASH_JOIN_BUCKET_NUM) > 1) {
            hybrid = true;
        }
        boolean driverBuilder = false;
        RelNode probeNode = current.getOuter();
        RelNode buildNode = current.getInner();
        if (current instanceof HashJoin) {
            driverBuilder = ((HashJoin) current).isOuterBuild();
            if (driverBuilder) {
                //make sure that it is not hybrid
                if (hybrid) {
                    hybrid = false;
                    log.warn("Hybrid-Hash-Join don't support driverBuilder mode!");
                }
                probeNode = current.getInner();
                buildNode = current.getOuter();
            }
        }
        //the build child side
        List<DataType> builderColumns = CalciteUtils.getTypes(buildNode.getRowType());
        ExecutorFactory emptybuildFactory = new EmptyExecutorFactory(builderColumns);
        PipelineFragment buildFramgent = new PipelineFragment(defaultParallelism, buildNode);
        ExecutorFactory childFactory = visit(current, buildNode, buildFramgent);

        if (hybrid) {

            List<EquiJoinKey> joinKeys = EquiJoinUtils
                .buildEquiJoinKeys(current, current.getOuter(), current.getInner(), (RexCall) equalCond,
                    current.getJoinType());
            List<DataType> keyTypes = joinKeys.stream().map(t -> t.getUnifiedType()).collect(Collectors.toList());
            List<Integer> keyInnerIndexes = joinKeys.stream().map(t -> t.getInnerIndex()).collect(Collectors.toList());
            List<Integer> keyOuterIndexes = joinKeys.stream().map(t -> t.getOuterIndex()).collect(Collectors.toList());

            PipelineFragment probeChildFragment = new PipelineFragment(
                getExchangeParallelism(defaultParallelism), current.getOuter());
            ExecutorFactory probeChildExecutorFactory = visit(current, current.getOuter(), probeChildFragment);

            //计算并发度
            int bucketNum = holdCollation ? 1 : context.getParamManager().getInt(
                ConnectionParams.HYBRID_HASH_JOIN_BUCKET_NUM);
            int fragmentParallelism = holdCollation ? 1 : fragment.getParallelism();
            fragment.setParallelism(fragmentParallelism);

            //probe 一定要有local buffer
            OutputBufferMemoryManager localProbeBufferManager = createLocalMemoryManager();
            LocalExchange localProbeExchange =
                new LocalExchange(CalciteUtils.getTypes(current.getOuter().getRowType()), keyOuterIndexes,
                    keyTypes, LocalExchange.LocalExchangeMode.PARTITION, true, bucketNum);
            List<DataType> outerColumns = CalciteUtils.getTypes(current.getOuter().getRowType());
            LocalBufferExecutorFactory localBufferExecutorFactory = new LocalBufferExecutorFactory(
                localProbeBufferManager, outerColumns, fragmentParallelism);

            //generate local-buffer consume pipelineFactory
            LocalExchangeConsumerFactory consumerFactory = new LocalExchangeConsumerFactory(
                localBufferExecutorFactory, localProbeBufferManager, localProbeExchange);
            PipelineFactory consumerPipeFactory = new PipelineFactory(probeChildExecutorFactory, consumerFactory,
                probeChildFragment.setPipelineId(pipelineIdGen++));
            pipelineFactorys.add(consumerPipeFactory);
            fragment.addChild(probeChildFragment);

            HybridHashJoinExecutorFactory joinExecutorFactory =
                new HybridHashJoinExecutorFactory(current, otherCond, equalCond, maxOneRow, oprands, emptybuildFactory,
                    localBufferExecutorFactory, fragmentParallelism, bucketNum,
                    spillerFactory.getStreamSpillerFactory());

            boolean supportBuffer = supportLocalBuffer && buildFramgent.getParallelism() > 1;
            //build exchange
            LocalExchange localBuildExchange =
                new LocalExchange(CalciteUtils.getTypes(current.getInner().getRowType()), keyInnerIndexes,
                    keyTypes, LocalExchange.LocalExchangeMode.PARTITION, supportBuffer, bucketNum);
            //generate child's local exchange pipelineFactory
            PipelineFactory buildPipelineFactory = createConsumeSideExchangeFactory(
                joinExecutorFactory, fragment, localBuildExchange, childFactory,
                buildFramgent,
                builderColumns, supportBuffer);
            //build端消费完后，我们再启动消费probe端
            probeChildFragment.addDependency(buildPipelineFactory.getPipelineId());
            if (forbidMultipleReadConn) {
                probeChildFragment.getProperties().addDependencyForChildren(buildPipelineFactory.getPipelineId());
            }
            return joinExecutorFactory;
        } else {
            OutputBufferMemoryManager localBufferManager = createLocalMemoryManager();
            ExecutorFactory probeExecutorFactory = visit(current, probeNode, fragment);
            //generate current executorFactory
            int numPartitions = buildFramgent.getParallelism() > fragment.getParallelism() ?
                fragment.getParallelism() : buildFramgent.getParallelism();
            ParallelHashJoinExecutorFactory joinExecutorFactory =
                new ParallelHashJoinExecutorFactory(current, otherCond, equalCond, maxOneRow, oprands,
                    emptybuildFactory,
                    probeExecutorFactory, fragment.getParallelism(), numPartitions, driverBuilder);

            //generate child's pipelineFactory
            LocalExchange localExchange =
                new LocalExchange(CalciteUtils.getTypes(buildNode.getRowType()), ImmutableList.of(),
                    LocalExchange.LocalExchangeMode.RANDOM, true);
            LocalExchangeConsumerFactory consumerFactory =
                new LocalExchangeConsumerFactory(joinExecutorFactory, localBufferManager, localExchange);

            PipelineFactory buildPipelineFactory =
                new PipelineFactory(childFactory, consumerFactory, buildFramgent.setPipelineId(pipelineIdGen++));
            buildFramgent.setBuildDepOnAllConsumers(true);
            pipelineFactorys.add(buildPipelineFactory);
            fragment.addDependency(buildPipelineFactory.getPipelineId());
            if (forbidMultipleReadConn) {
                fragment.getProperties().addDependencyForChildren(buildPipelineFactory.getPipelineId());
            }
            fragment.addChild(buildFramgent);

            if (fragment.isContainLimit() && probeExecutorFactory instanceof LogicalViewExecutorFactory &&
                context.getParamManager().getBoolean(ConnectionParams.ENABLE_DRIVING_STREAM_SCAN)) {
                ((LogicalViewExecutorFactory) probeExecutorFactory).enableDrivingResumeSource();
                joinExecutorFactory.enableStreamJoin(true);
            }

            return joinExecutorFactory;
        }
    }

    private ExecutorFactory visitBKAJoin(Join current, PipelineFragment pipelineFragment) {

        if (defaultParallelism != 1 && ExecUtils.useExplicitTransaction(context)) {
            //FIXME 本来这里应该根据forbidMultipleReadConn来设置并发度的，这里投机取巧下知识使用useTransaction
            //事务下并发度必须为1,并且当前并发度需要被保持
            pipelineFragment.holdSingleTonParallelism();
        }

        ExecutorFactory outerExecutorFactory = visit(current, current.getOuter(), pipelineFragment);
        boolean oldExpandView = expandView;
        ExecutorFactory innerExecutorFactory;

        try {
            expandView = true;
            innerExecutorFactory = visit(current, current.getInner(), pipelineFragment);
            if (forbidMultipleReadConn) {
                Preconditions.checkArgument(pipelineFragment.getParallelism() == 1,
                    "The LookupJoinExec's parallelism must be 1!");
            }
        } finally {
            expandView = oldExpandView;
        }

        if (pipelineFragment.isContainLimit() && outerExecutorFactory instanceof LogicalViewExecutorFactory) {
            if (((LogicalViewExecutorFactory) outerExecutorFactory).getLogicalView().getJoin() == null) {
                ((LogicalViewExecutorFactory) outerExecutorFactory).enablePassiveResumeSource();
            }
        }
        return new LookupJoinExecFactory(current, outerExecutorFactory, innerExecutorFactory);
    }

    private ExecutorFactory visitMaterializedSemiJoin(MaterializedSemiJoin current, PipelineFragment pipelineFragment) {
        List<DataType> columns = CalciteUtils.getTypes(current.getInner().getRowType());
        ExecutorFactory emptyFactory = new EmptyExecutorFactory(columns);

        PipelineFragment innerFragment = new PipelineFragment(pipelineFragment.getParallelism(), current.getInner());
        ExecutorFactory innerExecutorFactory = visit(current, current.getInner(), innerFragment);

        int buildPipelineId = pipelineIdGen++;

        ExecutorFactory outerExecutorFactory = visit(current, current.getOuter(), pipelineFragment);

        LocalExchange localExchange =
            new LocalExchange(CalciteUtils.getTypes(current.getOuter().getRowType()), ImmutableList.of(),
                LocalExchange.LocalExchangeMode.BORADCAST, innerFragment.getParallelism() == 1);
        MaterializedJoinExecFactory joinExecutorFactory = new MaterializedJoinExecFactory(
            current, emptyFactory, outerExecutorFactory, pipelineFragment.getParallelism());
        //generate child's pipelineFactory
        OutputBufferMemoryManager localBufferManager = createLocalMemoryManager();
        LocalExchangeConsumerFactory consumerFactory =
            new LocalExchangeConsumerFactory(joinExecutorFactory, localBufferManager, localExchange);
        PipelineFactory innerPipelineFactory =
            new PipelineFactory(innerExecutorFactory, consumerFactory, innerFragment.setPipelineId(buildPipelineId));
        pipelineFactorys.add(innerPipelineFactory);
        innerFragment.setBuildDepOnAllConsumers(true);
        pipelineFragment.addDependency(innerPipelineFactory.getPipelineId());
        if (forbidMultipleReadConn) {
            pipelineFragment.getProperties().addDependencyForChildren(innerPipelineFactory.getPipelineId());
        }
        pipelineFragment.addChild(innerFragment);
        return joinExecutorFactory;
    }

    private ExecutorFactory visitValue(LogicalValues current, PipelineFragment pipelineFragment) {
        pipelineFragment.holdSingleTonParallelism();
        return new ValueExecutorFactory(current);
    }

    private ExecutorFactory visitDynamicValues(DynamicValues current, PipelineFragment pipelineFragment) {
        pipelineFragment.holdSingleTonParallelism();
        return new DynamicValuesExecutorFactory(current);
    }

    private ExecutorFactory visitGeneralExec(RelNode current, PipelineFragment pipelineFragment) {
        pipelineFragment.holdSingleTonParallelism();
        return new NonBlockGeneralExecFactory(current);
    }

    private ExecutorFactory visitRemoteNode(RemoteSourceNode current, PipelineFragment pipelineFragment) {
        RelCollation collation = current.getRelCollation();
        if (collation != null && (collation.getFieldCollations() != null
            && collation.getFieldCollations().size() > 0)) {
            pipelineFragment.holdSingleTonParallelism();
            holdCollation = true;
        }
        return new ExchangeExecFactory(pagesSerdeFactory, current, exchangeClientSupplier, holdCollation);
    }

    private ExecutorFactory visitLimit(Limit limit, PipelineFragment pipelineFragment) {

        PipelineFragment childFragment =
            new PipelineFragment(defaultParallelism, limit.getInput());
        childFragment.setContainLimit(true);
        ExecutorFactory childExecutorFactory = visit(limit, limit.getInput(), childFragment);
        pipelineFragment.holdSingleTonParallelism();

        if (childFragment.getParallelism() != 1) {
            List<DataType> outputColumns = CalciteUtils.getTypes(limit.getRowType());
            LocalBufferNode localBufferNode = LocalBufferNode.create(childFragment.getRoot());

            OutputBufferMemoryManager localBufferManager = createLocalMemoryManager();
            LocalBufferExecutorFactory localBufferExecutorFactory = new LocalBufferExecutorFactory(
                localBufferManager, outputColumns, pipelineFragment.getParallelism());

            LocalExchange localBufferExchange = new LocalExchange(
                CalciteUtils.getTypes(
                    limit.getRowType()), ImmutableList.of(), LocalExchange.LocalExchangeMode.SINGLE, true);

            LocalExchangeConsumerFactory consumerFactory = new LocalExchangeConsumerFactory(
                localBufferExecutorFactory, localBufferManager, localBufferExchange);

            PipelineFactory bufferPipelineFactory =
                new PipelineFactory(childExecutorFactory, consumerFactory, childFragment);

            pipelineFactorys.add(bufferPipelineFactory);

            pipelineFragment.inheritDependency(childFragment);
            pipelineFragment.addBufferNodeChild(localBufferNode.getInput().getRelatedId(),
                childFragment.setPipelineId(pipelineIdGen++));
            holdCollation = true;
            LimitExecFactory limitExecFactory = new LimitExecFactory(
                limit, localBufferExecutorFactory, 1);
            return limitExecFactory;
        } else {
            pipelineFragment.inherit(childFragment);
            holdCollation = true;
            return new LimitExecFactory(limit, childExecutorFactory, 1);
        }
    }

    private ExecutorFactory visitMergeSort(RelNode parent, MergeSort mergeSort, PipelineFragment pipelineFragment) {
        PipelineFragment childFragment =
            new PipelineFragment(defaultParallelism, mergeSort.getInput());
        if (mergeSort.getInput() instanceof LogicalView) {
            ((LogicalView) mergeSort.getInput()).setIsUnderMergeSort(true);
        }
        ExecutorFactory childExecutorFactory = visit(mergeSort, mergeSort.getInput(), childFragment);
        if (childFragment.getParallelism() == 1) {
            //ignore current mergeSort
            pipelineFragment.inherit(childFragment);
            holdCollation = true;
            if (!((childExecutorFactory instanceof LogicalViewExecutorFactory) &&
                ((LogicalViewExecutorFactory) childExecutorFactory).isPushDownSort())) {
                if (mergeSort.fetch != null) {
                    RexNode offset = null;
                    if (mergeSort.offset != null) {
                        offset = mergeSort.offset;
                    }
                    Limit limit =
                        Limit.create(mergeSort.getTraitSet(), mergeSort, offset, mergeSort.fetch);
                    limit.setRelatedId(mergeSort.getRelatedId());
                    return new LimitExecFactory(limit, childExecutorFactory, 1);
                }
            }
            return childExecutorFactory;
        } else {
            pipelineFragment.inherit(childFragment);
            pipelineFragment.holdSingleTonParallelism();
            holdCollation = true;
            return new LocalMergeSortExecutorFactory(mergeSort, childExecutorFactory, childFragment.getParallelism());
        }
    }

    private ExecutorFactory visitMppExchange(
        RelNode parent, MppExchange exchange, PipelineFragment pipelineFragment) {
        if (exchange.isMergeSortExchange()) {
            MergeSort mergeSort = MergeSort.create(exchange.getInput(), exchange.getCollation(), null, null);
            mergeSort.setRelatedId(exchange.getRelatedId());
            return visitMergeSort(parent, mergeSort, pipelineFragment);
        } else {
            //对Gather && MppExchange特殊处理，Gather现在并没有统计CPU耗时，
            // 计算Gather && MppExchange的cpu耗时时，不再减去input算子的耗时，否则结果会负数
            RuntimeStatistics statistics = (RuntimeStatistics) context.getRuntimeStatistics();
            if (statistics != null) {
                RuntimeStatistics.OperatorStatisticsGroup og =
                    statistics.getRelationToStatistics().get(exchange.getRelatedId());
                if (og != null) {
                    og.hasInputOperator = false;
                }
            }

            return visit(parent, exchange.getInput(), pipelineFragment);
        }
    }

    private ExecutorFactory visitMemSort(RelNode parent, MemSort sort, PipelineFragment pipelineFragment) {
        LocalExchange localExchange = null;

        //generate child's executor factory
        List<DataType> columns = CalciteUtils.getTypes(sort.getInput().getRowType());
        PipelineFragment childFragment = new PipelineFragment(defaultParallelism, sort.getInput());
        ExecutorFactory childFactory = visit(sort, sort.getInput(), childFragment);
        holdCollation = true;
        pipelineFragment.holdSingleTonParallelism();

        if (pipelineFragment.getParallelism() != childFragment.getParallelism()) {
            localExchange =
                new LocalExchange(CalciteUtils.getTypes(sort.getInput().getRowType()), ImmutableList.of(),
                    LocalExchange.LocalExchangeMode.SINGLE, false);
        } else {
            localExchange =
                new LocalExchange(CalciteUtils.getTypes(sort.getInput().getRowType()), ImmutableList.of(),
                    LocalExchange.LocalExchangeMode.DIRECT, true);
        }

        OutputBufferMemoryManager localBufferManager = createLocalMemoryManager();

        int memSortParallelism = pipelineFragment.getParallelism();
        //generate localSort executorFactory
        SortExecutorFactory sortExecutorFactory =
            new SortExecutorFactory(sort, memSortParallelism, columns, spillerFactory);
        //generate child's pipelineFactory
        LocalExchangeConsumerFactory consumerFactory =
            new LocalExchangeConsumerFactory(sortExecutorFactory, localBufferManager, localExchange);
        PipelineFactory childPipelineFactory = new PipelineFactory(
            childFactory, consumerFactory, childFragment.setPipelineId(pipelineIdGen++));
        pipelineFactorys.add(childPipelineFactory);
        pipelineFragment.addChild(childFragment);
        //generate child's build pipelineFactory
        if (pipelineFragment.getParallelism() != childFragment.getParallelism()) {
            childFragment.setBuildDepOnAllConsumers(true);
        }

        pipelineFragment.addDependency(childPipelineFactory.getPipelineId());
        return sortExecutorFactory;
    }

    private ExecutorFactory visitTopN(RelNode parent, TopN topN, PipelineFragment pipelineFragment) {
        //generate child's executor factory
        List<DataType> columns = CalciteUtils.getTypes(topN.getInput().getRowType());
        PipelineFragment childFragment = new PipelineFragment(defaultParallelism, topN.getInput());
        ExecutorFactory childFactory = visit(topN, topN.getInput(), childFragment);
        //TopN在二层调度上就没必要多并发
        holdCollation = true;
        pipelineFragment.holdSingleTonParallelism();

        boolean supportBuffer = supportLocalBuffer && childFragment.getParallelism() > 1;
        boolean asyncConsume = supportBuffer || childFragment.getParallelism() == 1;

        LocalExchange localExchange =
            new LocalExchange(CalciteUtils.getTypes(topN.getInput().getRowType()), ImmutableList.of(),
                LocalExchange.LocalExchangeMode.SINGLE, asyncConsume);

        OutputBufferMemoryManager localBufferManager = createLocalMemoryManager();

        ExecutorFactory topNExecutorFactory = null;
        if (supportBuffer) {
            //generate current executorFactory
            topNExecutorFactory =
                new TopNExecutorFactory(topN, pipelineFragment.getParallelism(), columns, spillerFactory);

            LocalBufferExecutorFactory localBufferExecutorFactory = new LocalBufferExecutorFactory(
                localBufferManager, columns, pipelineFragment.getParallelism());
            LocalBufferConsumerFactory localConsumerFactory = new LocalBufferConsumerFactory(topNExecutorFactory);

            //generate local-buffer consume pipelineFactory
            LocalExchangeConsumerFactory consumerFactory = new LocalExchangeConsumerFactory(
                localBufferExecutorFactory, localBufferManager, localExchange);
            PipelineFactory consumerPipeFactory =
                new PipelineFactory(childFactory, consumerFactory, childFragment.setPipelineId(pipelineIdGen++));
            pipelineFactorys.add(consumerPipeFactory);

            //generate local-buffer produce pipelineFactory
            LocalBufferNode localBufferNode = LocalBufferNode.create(childFragment.getRoot());
            PipelineFragment produceFragment = new PipelineFragment(
                pipelineFragment.getParallelism(), localBufferNode, childFragment.getDependency());
            PipelineFactory producePipelineFactory =
                new PipelineFactory(localBufferExecutorFactory, localConsumerFactory,
                    produceFragment.setPipelineId(pipelineIdGen++));
            pipelineFactorys.add(producePipelineFactory);
            produceFragment.addChild(childFragment);
            pipelineFragment.addDependency(consumerPipeFactory.getPipelineId());
            pipelineFragment.addDependency(producePipelineFactory.getPipelineId());
            pipelineFragment.addBufferNodeChild(localBufferNode.getInput().getRelatedId(), produceFragment);
        } else {
            //generate current executorFactory
            topNExecutorFactory =
                new TopNExecutorFactory(topN, pipelineFragment.getParallelism(), columns, spillerFactory);

            //generate child's pipelineFactory
            LocalExchangeConsumerFactory consumerFactory =
                new LocalExchangeConsumerFactory(topNExecutorFactory, localBufferManager, localExchange);
            PipelineFactory childPipelineFactory =
                new PipelineFactory(childFactory, consumerFactory, childFragment.setPipelineId(pipelineIdGen++));
            pipelineFactorys.add(childPipelineFactory);
            childFragment.setBuildDepOnAllConsumers(true);
            //generate buildConsume pipelineFactory
            pipelineFragment.addDependency(childPipelineFactory.getPipelineId());
            pipelineFragment.addChild(childFragment);
        }

        if (topN.fetch != null && topN.offset != null) {
            long skip = CBOUtil.getRexParam(topN.offset, context.getParams().getCurrentParameter());
            if (skip > 0) {
                Limit limit = Limit.create(topN.getTraitSet(), topN, topN.offset, topN.fetch);
                return new LimitExecFactory(limit, topNExecutorFactory, 1);
            }
        }

        return topNExecutorFactory;
    }

    private ExecutorFactory visitView(RelNode parent, LogicalView logicalView, PipelineFragment pipelineFragment) {
        pipelineFragment.addLogicalView(logicalView);
        if (expandView) {
            logicalView.setExpandView(true);
        }
        int prefetch = totalPrefetch;
        if (isCluster) {
            if (logicalView.isUnderMergeSort() ||
                (logicalView.pushedRelNodeIsSort() && logicalView.isSingleGroupSingleTable())) {
                holdCollation = true;
                pipelineFragment.holdSingleTonParallelism();
            }
            if (prefetch <= 0) {
                prefetch = pipelineFragment.getParallelism();
            }
        } else {
            this.forbidMultipleReadConn =
                this.forbidMultipleReadConn || !ExecUtils.allowMultipleReadConns(context, logicalView);
            SplitInfo splitInfo;
            if (logicalView.fromTableOperation() != null) {
                splitInfo = new SplitManager().getSingleSplit(logicalView, context);
            } else {
                splitInfo = new SplitManager().getSplits(logicalView, context, false);
            }

            if (logicalView.isUnderMergeSort() ||
                (logicalView.pushedRelNodeIsSort() && logicalView.isSingleGroupSingleTable())) {
                holdCollation = true;
            }

            if (splitInfo.getConcurrencyPolicy() == QueryConcurrencyPolicy.SEQUENTIAL) {
                pipelineFragment.holdSingleTonParallelism();
                prefetch = 1;
            } else {
                if (prefetch < 0) {
                    prefetch = ExecUtils.getPrefetchNumForLogicalView(splitInfo.getDbCount());
                }
                if (expandView) {
                    //The parallelism of the ExpandView must keep constant.
                } else {
                    if (defaultParallelism > 1 && !holdCollation) {
                        if (!pipelineFragment.isHoldParallelism()) {
                            int parallelism =
                                logicalView.isUnderMergeSort() ? 1 :
                                    context.getParamManager().getInt(ConnectionParams.PARALLELISM);
                            if (parallelism < 0) {
                                int shards;
                                switch (splitInfo.getConcurrencyPolicy()) {
                                case GROUP_CONCURRENT_BLOCK:
                                    shards = splitInfo.getDbCount();
                                    break;
                                case CONCURRENT:
                                    shards = splitInfo.getSplitCount();
                                    break;
                                default:
                                    // disable parallel query
                                    shards = 1;
                                    break;
                                }
                                parallelism = ExecUtils.getParallelismForLogicalView(shards);
                            }

                            if (parallelism >= splitInfo.getSplitCount()) {
                                parallelism = splitInfo.getSplitCount();
                                prefetch = 1 * parallelism;
                            }

                            if (parallelism == 0) {
                                // Parallel query is disabled but we have a parallel plan... Very strange...
                                parallelism = 1;
                            }

                            pipelineFragment.setParallelism(parallelism);
                        }
                    } else {
                        pipelineFragment.holdSingleTonParallelism();
                    }
                }
            }

            pipelineFragment.putSource(logicalView.getRelatedId(), splitInfo);

            if (log.isDebugEnabled()) {
                log.debug(
                    logicalView.getRelatedId() + " pipelineFragment.getParallelism=" + pipelineFragment
                        .getParallelism()
                        + ",totalprefetch=" + prefetch
                        + ",isUnderSort=" + logicalView.isUnderMergeSort() + ",pushedRelNodeIsSort=" + logicalView
                        .pushedRelNodeIsSort() + ",SplitCount=" + splitInfo.getSplitCount());
            }
        }

        return createViewFactory(parent, logicalView, pipelineFragment, spillerFactory, prefetch);
    }

    private LogicalViewExecutorFactory createViewFactory(
        RelNode parent, LogicalView logicalView, PipelineFragment pipelineFragment, SpillerFactory spillerFactory,
        int prefetch) {
        long fetched = Long.MAX_VALUE;
        long offset = 0;
        boolean bSort = false;

        Map<Integer, ParameterContext> params = context.getParams().getCurrentParameter();
        Sort sort = null;
        if (parent instanceof Sort && logicalView.pushedRelNodeIsSort() && pipelineFragment.getParallelism() == 1) {
            sort = (Sort) parent;
            if (sort.fetch != null) {
                fetched = CBOUtil.getRexParam(sort.fetch, params);
                if (sort.offset != null) {
                    offset = CBOUtil.getRexParam(sort.offset, params);
                }
            }

        } else if (logicalView.pushedRelNodeIsSort()) {
            sort = (Sort) logicalView.getOptimizedPushedRelNodeForMetaQuery();
        }

        if (sort != null) {
            bSort = true;

        }
        long maxRowCount = ExecUtils.getMaxRowCount(sort, context);
        return new LogicalViewExecutorFactory(logicalView, prefetch, pipelineFragment.getParallelism(),
            maxRowCount, bSort, fetched, offset, spillerFactory, bloomFilterExpressionMap, enableRuntimeFilter);
    }

    private ExecutorFactory visitProject(Project project, PipelineFragment pipelineFragment) {

        boolean canConvertToVectorizedExpression =
            RexUtils.canConvertToVectorizedExpression(context, project.getProjects());
        List<RexNode> projectExprs = project.getProjects();
        ExecutorFactory childExecutorFactory = visit(project, project.getInput(), pipelineFragment);
        return new ProjectExecFactory(
            project, projectExprs, childExecutorFactory, canConvertToVectorizedExpression);
    }

    private Pair<PipelineFactory, LocalBufferExecutorFactory> createAllBufferPipelineFragment(
        RelNode relNode, PipelineFragment producerFragment, String name) {
        List<DataType> columns = CalciteUtils.getTypes(relNode.getInput(0).getRowType());
        ExecutorFactory producerExecutorFactory = visit(relNode, relNode.getInput(0), producerFragment);
        long bufferSize =
            context.getParamManager().getLong(ConnectionParams.SPILL_OUTPUT_MAX_BUFFER_SIZE);
        String memoryName = name + System.identityHashCode(producerFragment);
        OutputBufferMemoryManager localBufferManager = createCacheAllBufferMemoryManager(memoryName, bufferSize);
        LocalBufferExecutorFactory localBufferExecutorFactory = new LocalBufferExecutorFactory(
            localBufferManager, columns, producerFragment.getParallelism(), false, true);

        //在事务场景下子查询的Filter和Project并发度必须是1否则，否则存在多个并发同时去apply subquery
        LocalExchange localExchange =
            new LocalExchange(CalciteUtils.getTypes(relNode.getInput(0).getRowType()), ImmutableList.of(),
                LocalExchange.LocalExchangeMode.SINGLE, true);
        LocalExchangeConsumerFactory localConsumerFactory =
            new LocalExchangeConsumerFactory(localBufferExecutorFactory, localBufferManager, localExchange);
        PipelineFactory producerPipelineFactory = new PipelineFactory(
            producerExecutorFactory, localConsumerFactory, producerFragment.setPipelineId(pipelineIdGen++));
        return new Pair<>(producerPipelineFactory, localBufferExecutorFactory);
    }

    private ExecutorFactory visitSortAgg(SortAgg agg, PipelineFragment pipelineFragment) {
        PipelineFragment childFragment = new PipelineFragment(defaultParallelism, agg.getInput());
        childFragment.setContainLimit(pipelineFragment.isContainLimit());
        ExecutorFactory childExecutorFactory = visit(agg, agg.getInput(), childFragment);
        if (childFragment.getParallelism() > 1 && agg.getGroupSet().cardinality() == 0) {
            pipelineFragment.holdSingleTonParallelism();
            List<DataType> columns = CalciteUtils.getTypes(agg.getInput().getRowType());
            LocalBufferNode localBufferNode = LocalBufferNode.create(childFragment.getRoot());
            OutputBufferMemoryManager localBufferManager = createLocalMemoryManager();
            LocalBufferExecutorFactory localBufferExecutorFactory = new LocalBufferExecutorFactory(
                localBufferManager, columns, pipelineFragment.getParallelism());
            LocalExchange localBufferExchange = new LocalExchange(
                CalciteUtils.getTypes(
                    agg.getInput().getRowType()), ImmutableList.of(), LocalExchange.LocalExchangeMode.SINGLE, true);

            LocalExchangeConsumerFactory consumerFactory = new LocalExchangeConsumerFactory(
                localBufferExecutorFactory, localBufferManager, localBufferExchange);

            PipelineFactory bufferPipelineFactory =
                new PipelineFactory(childExecutorFactory, consumerFactory,
                    childFragment.setPipelineId(pipelineIdGen++));

            pipelineFactorys.add(bufferPipelineFactory);

            pipelineFragment.getDependency().addAll(childFragment.getDependency());
            pipelineFragment.addBufferNodeChild(localBufferNode.getInput().getRelatedId(), childFragment);

            return new SortAggExecFactory(agg, localBufferExecutorFactory, pipelineFragment.getParallelism());
        } else {
            pipelineFragment.inherit(childFragment);
            return new SortAggExecFactory(agg, childExecutorFactory, pipelineFragment.getParallelism());
        }
    }

    private ExecutorFactory visitFilter(Filter filter, PipelineFragment pipelineFragment) {
        boolean canConvertToVectorizedExpression = RexUtils
            .canConvertToVectorizedExpression(context, filter.getCondition());
        ExecutorFactory childExecutorFactory = visit(filter, filter.getInput(), pipelineFragment);
        return new FilterExecFactory(filter, filter.getCondition(), childExecutorFactory,
            canConvertToVectorizedExpression,
            enableRuntimeFilter, bloomFilterExpressionMap);
    }

    private ExecutorFactory visitRuntimeFilterBuild(RuntimeFilterBuilder filterBuilder,
                                                    PipelineFragment pipelineFragment) {
        ExecutorFactory childExecutorFactory = visit(filterBuilder, filterBuilder.getInput(), pipelineFragment);
        return new RuntimeFilterBuilderExecFactory(filterBuilder, childExecutorFactory, httpClient,
            runtimeFilterUpdateUri);
    }

    private ExecutorFactory visitHashAgg(HashAgg agg, PipelineFragment pipelineFragment) {
        //generate child's executor factory
        List<DataType> columns = CalciteUtils.getTypes(agg.getInput().getRowType());
        PipelineFragment childFragment = new PipelineFragment(defaultParallelism, agg.getInput());
        ExecutorFactory childFactory = visit(agg, agg.getInput(), childFragment);
        if (holdCollation) {
            pipelineFragment.holdSingleTonParallelism();
        }

        boolean supportBuffer = supportLocalBuffer && childFragment.getParallelism() > 1;
        boolean asyncConsume = supportBuffer || childFragment.getParallelism() == 1;
        LocalExchange localExchange = null;
        if (agg.isPartial()) {
            if (supportBuffer) {
                LocalExchange.LocalExchangeMode mode =
                    pipelineFragment.getParallelism() == 1 ? LocalExchange.LocalExchangeMode.SINGLE :
                        LocalExchange.LocalExchangeMode.PARTITION;
                localExchange = new LocalExchange(
                    CalciteUtils.getTypes(agg.getInput().getRowType()), ImmutableList.of(), mode, true);
            } else {
                LocalExchange.LocalExchangeMode mode =
                    pipelineFragment.getParallelism() == 1 ? LocalExchange.LocalExchangeMode.SINGLE :
                        LocalExchange.LocalExchangeMode.RANDOM;
                localExchange =
                    new LocalExchange(CalciteUtils.getTypes(agg.getInput().getRowType()), ImmutableList.of(),
                        mode, childFragment.getParallelism() == 1);
            }
        } else {
            if (agg.getGroupSet().cardinality() == 0) {
                localExchange =
                    new LocalExchange(CalciteUtils.getTypes(agg.getInput().getRowType()), ImmutableList.of(),
                        LocalExchange.LocalExchangeMode.SINGLE, asyncConsume);
                pipelineFragment.holdSingleTonParallelism();
            } else {
                LocalExchange.LocalExchangeMode mode =
                    pipelineFragment.getParallelism() == 1 ? LocalExchange.LocalExchangeMode.SINGLE :
                        LocalExchange.LocalExchangeMode.PARTITION;
                localExchange = new LocalExchange(CalciteUtils.getTypes(agg.getInput().getRowType()),
                    agg.getGroupSet().toList(), mode, asyncConsume);
            }
        }

        //generate current executorFactory
        ExecutorFactory aggFactory = new HashAggExecutorFactory(agg, pipelineFragment.getParallelism(),
            taskNumber, spillerFactory, getRelNodeRowCount(agg), columns);
        createConsumeSideExchangeFactory(aggFactory, pipelineFragment, localExchange, childFactory, childFragment,
            columns, supportBuffer);
        return aggFactory;
    }

    private Integer getRelNodeRowCount(RelNode relNode) {
        Integer rowCount = context.getRecordRowCnt().get(relNode.getRelatedId());
        if (rowCount == null) {
            rowCount = RelUtils.getRowCount(relNode).intValue();
            context.getRecordRowCnt().put(relNode.getRelatedId(), rowCount);
        }
        return rowCount;
    }

    private PipelineFactory createConsumeSideExchangeFactory(
        ExecutorFactory parentFactory,
        PipelineFragment pipelineFragment,
        LocalExchange localExchange,
        ExecutorFactory childFactory,
        PipelineFragment childFragment,
        List<DataType> columnMetaList,
        boolean isBuffer) {
        OutputBufferMemoryManager localBufferManager = createLocalMemoryManager();
        if (isBuffer) {
            LocalBufferExecutorFactory localBufferExecutorFactory = new LocalBufferExecutorFactory(
                localBufferManager, columnMetaList, pipelineFragment.getParallelism());
            LocalBufferConsumerFactory localConsumerFactory = new LocalBufferConsumerFactory(parentFactory);

            //generate local-buffer consume pipelineFactory
            LocalExchangeConsumerFactory
                consumerFactory = new LocalExchangeConsumerFactory(
                localBufferExecutorFactory, localBufferManager, localExchange);
            PipelineFactory consumerPipeFactory =
                new PipelineFactory(childFactory, consumerFactory, childFragment.setPipelineId(pipelineIdGen++));
            pipelineFactorys.add(consumerPipeFactory);

            //generate local-buffer produce pipelineFactory
            LocalBufferNode localBufferNode = LocalBufferNode.create(childFragment.getRoot());
            PipelineFragment produceFragment = new PipelineFragment(
                pipelineFragment.getParallelism(), localBufferNode, childFragment.getDependency());
            produceFragment.addChild(childFragment);
            PipelineFactory producePipelineFactory =
                new PipelineFactory(
                    localBufferExecutorFactory, localConsumerFactory, produceFragment.setPipelineId(pipelineIdGen++));

            pipelineFactorys.add(producePipelineFactory);
            pipelineFragment.addDependency(consumerPipeFactory.getPipelineId());
            pipelineFragment.addDependency(producePipelineFactory.getPipelineId());
            pipelineFragment.addBufferNodeChild(localBufferNode.getInput().getRelatedId(), produceFragment);
            return producePipelineFactory;
        } else {
            //generate child's pipelineFactory
            LocalExchangeConsumerFactory consumerFactory = new LocalExchangeConsumerFactory(
                parentFactory, localBufferManager, localExchange);
            PipelineFactory childPipelineFactory =
                new PipelineFactory(childFactory, consumerFactory, childFragment.setPipelineId(pipelineIdGen++));
            pipelineFactorys.add(childPipelineFactory);
            childFragment.setBuildDepOnAllConsumers(true);
            pipelineFragment.addDependency(childPipelineFactory.getPipelineId());
            pipelineFragment.addChild(childFragment);
            return childPipelineFactory;
        }
    }

}
