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
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.utils.ExecutorMode;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.mpp.Session;
import com.alibaba.polardbx.executor.mpp.execution.EmptyMemSystemListener;
import com.alibaba.polardbx.executor.mpp.execution.RecordMemSystemListener;
import com.alibaba.polardbx.executor.mpp.execution.buffer.OutputBuffer;
import com.alibaba.polardbx.executor.mpp.execution.buffer.OutputBufferMemoryManager;
import com.alibaba.polardbx.executor.mpp.execution.buffer.PagesSerdeFactory;
import com.alibaba.polardbx.executor.mpp.execution.buffer.SpilledOutputBufferMemoryManager;
import com.alibaba.polardbx.executor.mpp.metadata.Split;
import com.alibaba.polardbx.executor.mpp.operator.factory.CacheExecFactory;
import com.alibaba.polardbx.executor.mpp.operator.factory.CteAnchorExecFactory;
import com.alibaba.polardbx.executor.mpp.operator.factory.CteExecFactory;
import com.alibaba.polardbx.executor.mpp.operator.factory.DynamicValuesExecutorFactory;
import com.alibaba.polardbx.executor.mpp.operator.factory.EmptyExecutorFactory;
import com.alibaba.polardbx.executor.mpp.operator.factory.ExchangeExecFactory;
import com.alibaba.polardbx.executor.mpp.operator.factory.ExecutorFactory;
import com.alibaba.polardbx.executor.mpp.operator.factory.ExpandExecFactory;
import com.alibaba.polardbx.executor.mpp.operator.factory.FilterExecFactory;
import com.alibaba.polardbx.executor.mpp.operator.factory.HashAggExecutorFactory;
import com.alibaba.polardbx.executor.mpp.operator.factory.HashGroupJoinExecutorFactory;
import com.alibaba.polardbx.executor.mpp.operator.factory.HashWindowExecFactory;
import com.alibaba.polardbx.executor.mpp.operator.factory.HybridHashJoinExecutorFactory;
import com.alibaba.polardbx.executor.mpp.operator.factory.InsertSelectExecFactory;
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
import com.alibaba.polardbx.executor.mpp.operator.factory.ParallelHashJoinExecutorFactory;
import com.alibaba.polardbx.executor.mpp.operator.factory.PipelineFactory;
import com.alibaba.polardbx.executor.mpp.operator.factory.ProjectExecFactory;
import com.alibaba.polardbx.executor.mpp.operator.factory.RuntimeFilterBuilderExecFactory;
import com.alibaba.polardbx.executor.mpp.operator.factory.SortAggExecFactory;
import com.alibaba.polardbx.executor.mpp.operator.factory.SortExecutorFactory;
import com.alibaba.polardbx.executor.mpp.operator.factory.SortMergeJoinFactory;
import com.alibaba.polardbx.executor.mpp.operator.factory.SortWindowFramesExecFactory;
import com.alibaba.polardbx.executor.mpp.operator.factory.SubPipelineFactory;
import com.alibaba.polardbx.executor.mpp.operator.factory.TopNExecutorFactory;
import com.alibaba.polardbx.executor.mpp.operator.factory.UnionExecFactory;
import com.alibaba.polardbx.executor.mpp.operator.factory.ValueExecutorFactory;
import com.alibaba.polardbx.executor.mpp.planner.FragmentRFItem;
import com.alibaba.polardbx.executor.mpp.planner.FragmentRFItemImpl;
import com.alibaba.polardbx.executor.mpp.planner.FragmentRFItemKey;
import com.alibaba.polardbx.executor.mpp.planner.FragmentRFManager;
import com.alibaba.polardbx.executor.mpp.planner.LocalExchange;
import com.alibaba.polardbx.executor.mpp.planner.PipelineFragment;
import com.alibaba.polardbx.executor.mpp.planner.PlanFragment;
import com.alibaba.polardbx.executor.mpp.planner.RangeScanUtils;
import com.alibaba.polardbx.executor.mpp.planner.RemoteSourceNode;
import com.alibaba.polardbx.executor.mpp.planner.SimpleFragmentRFManager;
import com.alibaba.polardbx.executor.mpp.planner.WrapPipelineFragment;
import com.alibaba.polardbx.executor.mpp.split.SplitInfo;
import com.alibaba.polardbx.executor.mpp.split.SplitManager;
import com.alibaba.polardbx.executor.operator.spill.SpillerFactory;
import com.alibaba.polardbx.executor.operator.util.bloomfilter.BloomFilterExpression;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.executor.utils.OrderByOption;
import com.alibaba.polardbx.executor.vectorized.build.VectorizedExpressionBuilder;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.meta.CostModelWeight;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
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
import com.alibaba.polardbx.optimizer.core.rel.HashWindow;
import com.alibaba.polardbx.optimizer.core.rel.Limit;
import com.alibaba.polardbx.optimizer.core.rel.LocalBufferNode;
import com.alibaba.polardbx.optimizer.core.rel.LogicalInsert;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.MaterializedSemiJoin;
import com.alibaba.polardbx.optimizer.core.rel.MemSort;
import com.alibaba.polardbx.optimizer.core.rel.MergeSort;
import com.alibaba.polardbx.optimizer.core.rel.NLJoin;
import com.alibaba.polardbx.optimizer.core.rel.OSSTableScan;
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
import com.alibaba.polardbx.optimizer.core.rel.mpp.ColumnarExchange;
import com.alibaba.polardbx.optimizer.core.rel.mpp.MppExchange;
import com.alibaba.polardbx.optimizer.core.rel.util.TargetTableInfo;
import com.alibaba.polardbx.optimizer.core.rel.util.TargetTableInfoOneTable;
import com.alibaba.polardbx.optimizer.memory.MemoryPool;
import com.alibaba.polardbx.optimizer.memory.MemoryType;
import com.alibaba.polardbx.optimizer.partition.PartitionByDefinition;
import com.alibaba.polardbx.optimizer.partition.common.PartitionStrategy;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.optimizer.utils.CalciteUtils;
import com.alibaba.polardbx.optimizer.utils.PartitionUtils;
import com.alibaba.polardbx.optimizer.utils.PhyTableOperationUtil;
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
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.DynamicValues;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RecursiveCTE;
import org.apache.calcite.rel.core.RecursiveCTEAnchor;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.core.Window;
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
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Pair;

import java.net.URI;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

public class LocalExecutionPlanner {

    public static final Set<Class<? extends RelNode>> SUPPORT_ONE_SIDE_CACHE_NODES = ImmutableSet.of(
        HashJoin.class, SemiHashJoin.class, NLJoin.class, SemiNLJoin.class, HashGroupJoin.class);
    public static final Set<Class<? extends RelNode>> SUPPORT_ALL_CACHE_NODES = ImmutableSet.of(
        HashAgg.class, MemSort.class, TopN.class, LogicalSort.class, HashWindow.class);
    private static final Logger log = LoggerFactory.getLogger(LocalExecutionPlanner.class);

    // logger for new runtime filter.
    private static final Logger LOGGER_FRAGMENT_MANAGER = LoggerFactory.getLogger(FragmentRFManager.class);

    private static final Set<Class<? extends RelNode>> SUPPORT_NODES = ImmutableSet.of(
        LogicalView.class, HashJoin.class, SemiHashJoin.class, SortMergeJoin.class, SemiSortMergeJoin.class,
        BKAJoin.class, SemiBKAJoin.class, NLJoin.class, SemiNLJoin.class, MaterializedSemiJoin.class, HashAgg.class,
        SortAgg.class, LogicalProject.class, LogicalExpand.class, LogicalFilter.class, MemSort.class, Limit.class,
        TopN.class, LogicalSort.class, LogicalValues.class, RemoteSourceNode.class, Gather.class,
        MergeSort.class, LogicalUnion.class, Exchange.class, HashGroupJoin.class, LogicalExpand.class,
        DynamicValues.class, SortWindow.class, HashWindow.class, RuntimeFilterBuilder.class, LogicalCorrelate.class,
        LogicalOutFile.class, PhysicalProject.class, PhysicalFilter.class, LogicalInsert.class, RecursiveCTE.class,
        RecursiveCTEAnchor.class
    );
    private final ExchangeClientSupplier exchangeClientSupplier;
    private final PagesSerdeFactory pagesSerdeFactory;
    private final boolean supportLocalBuffer;
    private final long localBufferSize;
    private final Executor notificationExecutor;
    private final SpillerFactory spillerFactory;
    private final boolean isCluster;
    private final Map<Integer, BloomFilterExpression> bloomFilterExpressionMap = new HashMap<>();
    // Used to skip runtime filter in AP_LOCAL executor mode.
    private final boolean enableRuntimeFilter;
    private final int localPartitionCount;
    private final int totalPartitionCount;
    private ExecutionContext context;
    private int taskNumber;
    private int pipelineIdGen;
    private List<PipelineFactory> pipelineFactorys;
    private int defaultParallelism;
    //FIXME MPP模式该参数已经失效
    private int bkaJoinParallelism;
    //在Final Sort后pipeline并发度都需要保持为1
    private boolean holdCollation;
    private HttpClient httpClient;
    private URI runtimeFilterUpdateUri;
    //only be used for logicalView
    private int totalPrefetch;
    private boolean forbidMultipleReadConn;
    private boolean expandView;
    private boolean localBloomFilter = false;

    // logical table name and its split count
    private Map<String, Integer> splitCountMap;

    private SplitManager splitManager;

    private final boolean isSimpleMergeSort;

    public LocalExecutionPlanner(ExecutionContext context, ExchangeClientSupplier exchangeClientSupplier,
                                 int defaultParallelism, int bkaJoinParallelism, int taskNumber, int prefetch,
                                 Executor notificationExecutor, SpillerFactory spillerFactory,
                                 HttpClient httpClient,
                                 URI runtimeFilterUpdateUri,
                                 boolean enableRuntimeFilter,
                                 int localPartitionCount, int totalPartitionCount,
                                 Map<String, Integer> splitCountMap, SplitManager splitManager) {
        this(context, exchangeClientSupplier, defaultParallelism, bkaJoinParallelism, taskNumber, prefetch,
            notificationExecutor, spillerFactory, httpClient, runtimeFilterUpdateUri, enableRuntimeFilter,
            localPartitionCount, totalPartitionCount, splitCountMap, splitManager, false);
    }

    public LocalExecutionPlanner(ExecutionContext context, ExchangeClientSupplier exchangeClientSupplier,
                                 int defaultParallelism, int bkaJoinParallelism, int taskNumber, int prefetch,
                                 Executor notificationExecutor, SpillerFactory spillerFactory,
                                 HttpClient httpClient,
                                 URI runtimeFilterUpdateUri,
                                 boolean enableRuntimeFilter,
                                 int localPartitionCount, int totalPartitionCount,
                                 Map<String, Integer> splitCountMap, SplitManager splitManager,
                                 boolean isSimpleMergeSort) {
        this.exchangeClientSupplier = exchangeClientSupplier;
        this.totalPartitionCount = totalPartitionCount;
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
        this.localPartitionCount = localPartitionCount;
        this.splitCountMap = splitCountMap;
        this.splitManager = splitManager;
        this.isSimpleMergeSort = isSimpleMergeSort;
    }

    public void setForbidMultipleReadConn(boolean forbidMultipleReadConn) {
        this.forbidMultipleReadConn = forbidMultipleReadConn;
    }

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
            localBloomFilter = fragment.isLocalBloomFilter();
            PipelineFragment pipelineFragment = new PipelineFragment(defaultParallelism, relNode);
            ExecutorFactory factory = visit(null, relNode, pipelineFragment);
            if (pipelineFragment.getParallelism() > 1
                && fragment.getPartitioningScheme().getShuffleHandle().isMergeSort()
                && !pipelineFragment.isDirectMerge()) {
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
                ((HashJoin) current).getEqualCondition(), false, null, ((HashJoin) current).isKeepPartition()
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
            ExecutorFactory ret = visit(parent, ((Gather) current).getInput(), pipelineFragment);
            ret.setExchange((Gather) current);
            return ret;
        } else if (current instanceof MppExchange) {
            return visitMppExchange(parent, (MppExchange) current, pipelineFragment);
        } else if (current instanceof ColumnarExchange) {
            return visitColumnarExchange(parent, (ColumnarExchange) current, pipelineFragment);
        } else if (current instanceof MergeSort) {
            return visitMergeSort(parent, (MergeSort) current, pipelineFragment);
        } else if (current instanceof LogicalUnion) {
            return visitUnion(parent, (LogicalUnion) current, pipelineFragment);
        } else if (current instanceof BaseTableOperation) {
            return visitBaseTable(parent, (BaseTableOperation) current, pipelineFragment);
        } else if (current instanceof LogicalExpand) {
            return visitExpand((LogicalExpand) current, pipelineFragment);
        } else if (current instanceof SortWindow) {
            return visitSortWindow((SortWindow) current, pipelineFragment);
        } else if (current instanceof HashWindow) {
            return visitHashWindow((HashWindow) current, pipelineFragment);
        } else if (current instanceof LogicalCorrelate) {
            return visitLogicalCorrelate((LogicalCorrelate) current, pipelineFragment);
        } else if (current instanceof LogicalInsert) {
            return visitLogicalInsertExec((LogicalInsert) current, pipelineFragment);
        } else if (current instanceof RecursiveCTE) {
            return visitCTE((RecursiveCTE) current, pipelineFragment);
        } else if (current instanceof RecursiveCTEAnchor) {
            return visitCTEAnchor((RecursiveCTEAnchor) current, pipelineFragment);
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

            pipelineFragment.setParallelism(1);
            pipelineFactorys.add(pair.left);
            pipelineFragment.addDependency(pair.left.getPipelineId());
            pipelineFragment.addChild(producerFragment);
            return new LogicalCorrelateFactory(current, pair.right);
        } else {
            ExecutorFactory inner = visit(current, current.getLeft(), pipelineFragment);
            return new LogicalCorrelateFactory(current, inner);
        }
    }

    private ExecutorFactory visitSortWindow(SortWindow sortWindow, PipelineFragment pipelineFragment) {
        RelNode child = sortWindow.getInput();

        boolean useParallelSortWindow =
            // 1. ENABLE_PARALLEL_SORT_WINDOW=true
            context.getParamManager().getBoolean(ConnectionParams.ENABLE_PARALLEL_SORT_WINDOW)

                // 2. only support in mpp mode.
                && context.getExecuteMode() == ExecutorMode.MPP

                // 3. parent is sort-window and child is mem-sort
                && child instanceof MemSort

                // 4. window has only one partition group.
                && (sortWindow).groups.size() == 1

                // 5. window has group keys.
                && sortWindow.groups.get(0).keys.size() > 0;

        if (useParallelSortWindow) {
            MemSort sort = (MemSort) child;

            // 6. check bound: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            Window.Group group = sortWindow.groups.get(0);
//            useParallelSortWindow &= group.lowerBound.isPreceding()
//                && group.lowerBound.isUnbounded()
//                && group.upperBound.isCurrentRow();

            // 7. check group keys are same as order by keys in sorter.
            List<OrderByOption> orderByOptionsInSort = ExecUtils.convertFrom(sort.collation.getFieldCollations());
            ImmutableBitSet keys = group.keys;

            List<Integer> partitionChannel = new ArrayList<>();
            boolean isPrefixColumn = true;
            int i = 0;
            for (int index = keys.nextSetBit(0); index >= 0; index = keys.nextSetBit(index + 1)) {
                if (i >= orderByOptionsInSort.size() || orderByOptionsInSort.get(i).index != index) {
                    isPrefixColumn = false;
                    break;
                }
                partitionChannel.add(orderByOptionsInSort.get(i).index);
                i++;
            }
            useParallelSortWindow &= isPrefixColumn;
            if (useParallelSortWindow) {
                return visitParallelSortWindow(sortWindow, pipelineFragment, partitionChannel);
            }
        }

        PipelineFragment childFragment = new PipelineFragment(defaultParallelism, sortWindow.getInput());
        childFragment.setContainLimit(pipelineFragment.isContainLimit());
        ExecutorFactory childExecutorFactory = visit(sortWindow, sortWindow.getInput(), childFragment);
        boolean noGroup = sortWindow.groups.stream().anyMatch(g -> g.keys.size() == 0);
        if (childFragment.getParallelism() > 1 && noGroup) {
            pipelineFragment.holdSingleTonParallelism();
            List<DataType> columns = CalciteUtils.getTypes(sortWindow.getInput().getRowType());
            LocalBufferNode localBufferNode = LocalBufferNode.create(childFragment.getRoot());
            OutputBufferMemoryManager localBufferManager = createLocalMemoryManager();
            LocalBufferExecutorFactory localBufferExecutorFactory = new LocalBufferExecutorFactory(
                localBufferManager, columns, pipelineFragment.getParallelism());
            LocalExchange localBufferExchange = new LocalExchange(
                CalciteUtils.getTypes(
                    sortWindow.getInput().getRowType()), ImmutableList.of(), LocalExchange.LocalExchangeMode.SINGLE,
                true);

            LocalExchangeConsumerFactory consumerFactory = new LocalExchangeConsumerFactory(
                localBufferExecutorFactory, localBufferManager, localBufferExchange);

            PipelineFactory bufferPipelineFactory =
                new PipelineFactory(childExecutorFactory, consumerFactory,
                    childFragment.setPipelineId(pipelineIdGen++));

            pipelineFactorys.add(bufferPipelineFactory);

            pipelineFragment.getDependency().addAll(childFragment.getDependency());
            pipelineFragment.addBufferNodeChild(localBufferNode.getInput().getRelatedId(), childFragment);

            return new SortWindowFramesExecFactory(sortWindow, localBufferExecutorFactory,
                pipelineFragment.getParallelism());
        } else {
            pipelineFragment.inherit(childFragment);
            return new SortWindowFramesExecFactory(sortWindow, childExecutorFactory, pipelineFragment.getParallelism());
        }
    }

    private ExecutorFactory visitParallelSortWindow(SortWindow sortWindow, PipelineFragment pipelineFragment,
                                                    List<Integer> partitionChannel) {
        // producer: sort -> overWindowFrame
        // consumer: localExchange(DIRECT) -> localBuffer

        PipelineFragment inputFragment = new PipelineFragment(defaultParallelism, sortWindow.getInput());
        inputFragment.setContainLimit(pipelineFragment.isContainLimit());
        ExecutorFactory inputExecutorFactory = visitMemSortWithParallelSortWindow(
            sortWindow,
            (MemSort) sortWindow.getInput(), // asserted.
            inputFragment,
            partitionChannel
        ); // sort exec

        SortWindowFramesExecFactory producerExecFactory =
            new SortWindowFramesExecFactory(sortWindow, inputExecutorFactory,
                pipelineFragment.getParallelism()); // producer: sort -> overWindowFrame

        List<DataType> columns = CalciteUtils.getTypes(sortWindow.getRowType());
        LocalBufferNode localBufferNode = LocalBufferNode.create(inputFragment.getRoot());
        OutputBufferMemoryManager localBufferManager = createLocalMemoryManager();
        LocalBufferExecutorFactory localBufferExecutorFactory =
            new LocalBufferExecutorFactory(localBufferManager, columns, pipelineFragment.getParallelism());
        LocalExchange localBufferExchange =
            new LocalExchange(columns, ImmutableList.of(), LocalExchange.LocalExchangeMode.DIRECT, true);

        LocalExchangeConsumerFactory consumerFactory = new LocalExchangeConsumerFactory(
            localBufferExecutorFactory, localBufferManager,
            localBufferExchange);  // consumer: localExchange(DIRECT) -> localBuffer

        PipelineFactory bufferPipelineFactory =
            new PipelineFactory(producerExecFactory, consumerFactory,
                inputFragment.setPipelineId(pipelineIdGen++));

        pipelineFactorys.add(bufferPipelineFactory);

        pipelineFragment.getDependency().addAll(inputFragment.getDependency());
        pipelineFragment.addBufferNodeChild(localBufferNode.getInput().getRelatedId(), inputFragment);
        pipelineFragment.setDirectMerge(true);

        return localBufferExecutorFactory;
    }

    private ExecutorFactory visitHashWindow(HashWindow overWindow, PipelineFragment pipelineFragment) {
        List<DataType> columnTypes = CalciteUtils.getTypes(overWindow.getInput().getRowType());
        PipelineFragment childFragment = new PipelineFragment(defaultParallelism, overWindow.getInput());
        ExecutorFactory childExecutorFactory = visit(overWindow, overWindow.getInput(), childFragment);
        if (holdCollation) {
            pipelineFragment.holdSingleTonParallelism();
        }

        boolean supportBuffer = supportLocalBuffer && childFragment.getParallelism() > 1;
        boolean asyncConsume = supportBuffer || childFragment.getParallelism() == 1;
        LocalExchange localExchange = null;
        if (overWindow.groups.get(0).keys.cardinality() == 0) {
            localExchange = new LocalExchange(columnTypes, ImmutableList.of(),
                LocalExchange.LocalExchangeMode.SINGLE, asyncConsume);
            pipelineFragment.holdSingleTonParallelism();
        } else {
            LocalExchange.LocalExchangeMode mode = pipelineFragment.getParallelism() == 1 ?
                LocalExchange.LocalExchangeMode.SINGLE : LocalExchange.LocalExchangeMode.PARTITION;
            localExchange = new LocalExchange(columnTypes, overWindow.groups.get(0).keys.toList(), mode, asyncConsume);
        }
        ExecutorFactory windowFactory = new HashWindowExecFactory(overWindow, pipelineFragment.getParallelism(),
            taskNumber, spillerFactory, getRelNodeDistinctKeyCount(overWindow, overWindow.groups.get(0).keys),
            columnTypes);
        createConsumeSideExchangeFactory(windowFactory, pipelineFragment, localExchange, childExecutorFactory,
            childFragment, columnTypes, supportBuffer);
        return windowFactory;
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

        LogicalView logicalView = ExecUtils.convertToLogicalView(other, context);
        logicalView.setRelatedId(other.getRelatedId());
        pipelineFragment.addLogicalView(logicalView);
        pipelineFragment.holdSingleTonParallelism();
        SplitInfo splitInfo = splitManager.getSingleSplit(logicalView, context);
        pipelineFragment.putSource(logicalView.getRelatedId(), splitInfo);
        return createViewFactory(
            parent, logicalView, pipelineFragment, spillerFactory, 1, false, null);
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
            maxOneRow, current.getOperands(), current.isKeepPartition()
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
                .buildEquiJoinKeys(
                    current, outerInput, current.getInner(), (RexCall) equalCond, current.getJoinType());
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
                                          List<RexNode> oprands, boolean keepPartition) {
        Join join = current;
        RelNode buildNode = join.getInner();
        RelNode probeNode = join.getOuter();

        // Parse the item keys from the RelNode of the join.
        List<FragmentRFItemKey> rfItemKeys = FragmentRFItemKey.buildItemKeys(join);

        if (LOGGER_FRAGMENT_MANAGER.isDebugEnabled()) {
            boolean isOuterBuild = (current instanceof HashJoin && ((HashJoin) current).isOuterBuild())
                || (current instanceof SemiHashJoin && ((SemiHashJoin) current).isOuterBuild());
            for (FragmentRFItemKey itemKey : rfItemKeys) {
                LOGGER_FRAGMENT_MANAGER.debug("itemKey = " + itemKey + " isReversed = " + isOuterBuild);
            }
        }

        // During the stage of configuring columnar parameters for the PlanExecutor, it's impossible to know the actual
        // execution mode (MPP or AP_LOCAL). Therefore, when the ENABLE_NEW_RF parameter is actually invoked,
        // the execution mode should be checked again. If it's AP_LOCAL mode, the runtime filter should be disabled.
        boolean useRF = context.getParamManager().getBoolean(ConnectionParams.ENABLE_NEW_RF)
            && context.getExecuteMode() == ExecutorMode.MPP;
        final boolean useXXHashRFinBuild =
            context.getParamManager().getBoolean(ConnectionParams.ENABLE_XXHASH_RF_IN_BUILD);
        final boolean useXXHashRFinFilter =
            context.getParamManager().getBoolean(ConnectionParams.ENABLE_XXHASH_RF_IN_FILTER);
        final float defaultFloatFpp = context.getParamManager().getFloat(ConnectionParams.RUNTIME_FILTER_FPP);
        final long rowUpperBound = context.getParamManager().getLong(ConnectionParams.GLOBAL_RF_ROWS_UPPER_BOUND);
        final long rowLowerBound = context.getParamManager().getLong(ConnectionParams.GLOBAL_RF_ROWS_LOWER_BOUND);
        final int rfSampleCount = context.getParamManager().getInt(ConnectionParams.NEW_RF_SAMPLE_COUNT);
        final float floatFilterRatio =
            context.getParamManager().getFloat(ConnectionParams.NEW_RF_FILTER_RATIO_THRESHOLD);

        final double defaultDoubleFpp = Double.parseDouble(Float.toString(defaultFloatFpp));
        final double doubleFilterRatio = Double.parseDouble(Float.toString(floatFilterRatio));

        // pre-check the local partition count and total partition count
        if (Integer.signum(totalPartitionCount) != Integer.signum(localPartitionCount)) {
            useRF = false;
            LOGGER_FRAGMENT_MANAGER.error(MessageFormat.format(
                "The values of totalPartitionCount {0} and localPartitionCount {1} are incorrect.",
                totalPartitionCount, localPartitionCount
            ));
        }

        // Initialize the runtime filter manager in this fragment.
        if (fragment.getFragmentRFManager() == null && useRF) {
            FragmentRFManager fragmentRFManager = new SimpleFragmentRFManager(
                totalPartitionCount, localPartitionCount,
                defaultDoubleFpp, rowUpperBound, rowLowerBound,
                doubleFilterRatio, rfSampleCount
            );

            fragment.setFragmentRFManager(fragmentRFManager);
        }

        // Build runtime filter item.
        if (fragment.getFragmentRFManager() != null && useRF) {
            for (int itemIndex = 0; itemIndex < rfItemKeys.size(); itemIndex++) {
                FragmentRFItemKey itemKey = rfItemKeys.get(itemIndex);

                String buildColumnName = itemKey.getBuildColumnName();
                String probeColumnName = itemKey.getProbeColumnName();

                FragmentRFManager manager = fragment.getFragmentRFManager();

                boolean localPairWise = localPartitionCount > 0 && context.getParamManager()
                    .getBoolean(ConnectionParams.ENABLE_LOCAL_PARTITION_WISE_JOIN)
                    && current.getTraitSet().getPartitionWise().isLocalPartition();

                // Use local mode for local partition wise because the hash table does not shared among all partitions.
                // When using local mode the totalPartitionCount must be greater than 0.
                FragmentRFManager.RFType rfType = localPairWise && totalPartitionCount > 0
                    ? FragmentRFManager.RFType.LOCAL : FragmentRFManager.RFType.BROADCAST;

                // localPartitionCount * taskNumber;

                // To build a fragment RF item
                FragmentRFItem rfItem = new FragmentRFItemImpl(
                    manager, buildColumnName, probeColumnName,
                    useXXHashRFinBuild, useXXHashRFinFilter, rfType
                );

                // add rf item into manager
                manager.addItem(itemKey, rfItem);
            }
        }

        boolean hybrid = false;

        if (spillerFactory != null && context.getParamManager().getInt(
            ConnectionParams.HYBRID_HASH_JOIN_BUCKET_NUM) > 1) {
            hybrid = true;
        }
        boolean driverBuilder = false;

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
            boolean localPairWise =
                context.getParamManager().getBoolean(ConnectionParams.ENABLE_LOCAL_PARTITION_WISE_JOIN)
                    && current.getTraitSet().getPartitionWise().isLocalPartition();
            if (localPairWise) {
                //make sure that it is not hybrid
                if (hybrid) {
                    hybrid = false;
                    log.warn("Hybrid-Hash-Join don't support local pairwise mode!");
                }
            }
        } else if (current instanceof SemiHashJoin) {
            driverBuilder = ((SemiHashJoin) current).isOuterBuild();
            JoinRelType type = current.getJoinType();
            // TODO support null safe under reverse anti join
            if (type == JoinRelType.ANTI) {
                driverBuilder &= !ParallelHashJoinExecutorFactory.containAntiJoinOperands(oprands, current);
            }
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
            boolean localPairWise = localPartitionCount > 0 && context.getParamManager()
                .getBoolean(ConnectionParams.ENABLE_LOCAL_PARTITION_WISE_JOIN)
                && current.getTraitSet().getPartitionWise().isLocalPartition();
            boolean separateProbe =
                context.getParamManager().getBoolean(ConnectionParams.LOCAL_PAIRWISE_PROBE_SEPARATE);
            if (localPairWise && separateProbe) {
                PipelineFragment probeChildFragment = new PipelineFragment(
                    defaultParallelism, probeNode);

                if (fragment.getFragmentRFManager() != null) {
                    // Set fragment manager for children pipeline when using partition exchanger.
                    probeChildFragment.setFragmentRFManager(fragment.getFragmentRFManager());
                }

                ExecutorFactory probeChildExecutorFactory = visit(current, probeNode, probeChildFragment);

                OutputBufferMemoryManager localProbeBufferManager = createLocalMemoryManager();
                List<DataType> outerColumns = CalciteUtils.getTypes(probeNode.getRowType());
                List<EquiJoinKey> joinKeys = EquiJoinUtils
                    .buildEquiJoinKeys(current, current.getOuter(), current.getInner(), (RexCall) equalCond,
                        current.getJoinType());
                List<DataType> keyTypes =
                    joinKeys.stream().map(EquiJoinKey::getUnifiedType).collect(Collectors.toList());
                List<Integer> keyIndexes =
                    joinKeys.stream().map(EquiJoinKey::getOuterIndex).collect(Collectors.toList());
                LocalExchange localProbeExchange =
                    new LocalExchange(CalciteUtils.getTypes(probeNode.getRowType()), keyIndexes,
                        keyTypes, LocalExchange.LocalExchangeMode.CHUNK_PARTITION, true);

                LocalBufferExecutorFactory localBufferExecutorFactory = new LocalBufferExecutorFactory(
                    localProbeBufferManager, outerColumns, probeChildFragment.getParallelism());

                //generate local-buffer consume pipelineFactory
                LocalExchangeConsumerFactory consumerFactory = new LocalExchangeConsumerFactory(
                    localBufferExecutorFactory, localProbeBufferManager, localProbeExchange);
                PipelineFactory consumerPipeFactory = new PipelineFactory(probeChildExecutorFactory, consumerFactory,
                    probeChildFragment.setPipelineId(pipelineIdGen++));
                pipelineFactorys.add(consumerPipeFactory);
                fragment.addChild(probeChildFragment);

                //generate current executorFactory
                int numPartitions = Math.min(buildFramgent.getParallelism(), fragment.getParallelism());
                ParallelHashJoinExecutorFactory joinExecutorFactory =
                    new ParallelHashJoinExecutorFactory(fragment, current, otherCond,
                        equalCond, maxOneRow, oprands,
                        emptybuildFactory,
                        localBufferExecutorFactory, fragment.getParallelism(), numPartitions, driverBuilder,
                        localPartitionCount, keepPartition);

                //generate child's pipelineFactory
                LocalExchange localExchange = null;
                // create partition exchange under partition wise join
                keyTypes = joinKeys.stream().map(EquiJoinKey::getUnifiedType).collect(Collectors.toList());
                List<Integer> keyInnerIndexes =
                    joinKeys.stream().map(EquiJoinKey::getInnerIndex).collect(Collectors.toList());

                //generate build pipelineFactory
                localExchange =
                    new LocalExchange(CalciteUtils.getTypes(buildNode.getRowType()), keyInnerIndexes,
                        keyTypes, LocalExchange.LocalExchangeMode.CHUNK_PARTITION, true);

                OutputBufferMemoryManager localBufferManager = createLocalMemoryManager();
                LocalExchangeConsumerFactory buildConsumerFactory =
                    new LocalExchangeConsumerFactory(joinExecutorFactory, localBufferManager, localExchange);

                PipelineFactory buildPipelineFactory =
                    new PipelineFactory(childFactory, buildConsumerFactory,
                        buildFramgent.setPipelineId(pipelineIdGen++));
                buildFramgent.setBuildDepOnAllConsumers(true);
                pipelineFactorys.add(buildPipelineFactory);
                fragment.addDependency(buildPipelineFactory.getPipelineId());
                if (forbidMultipleReadConn) {
                    log.error("forbid multiple read connection is true, check this");
                    fragment.getProperties().addDependencyForChildren(buildPipelineFactory.getPipelineId());
                    probeChildFragment.getProperties().addDependencyForChildren(buildPipelineFactory.getPipelineId());
                }
                fragment.addChild(buildFramgent);

                return joinExecutorFactory;
            } else {
                OutputBufferMemoryManager localBufferManager = createLocalMemoryManager();
                ExecutorFactory probeExecutorFactory = visit(current, probeNode, fragment);
                //generate current executorFactory
                int numPartitions = buildFramgent.getParallelism() > fragment.getParallelism() ?
                    fragment.getParallelism() : buildFramgent.getParallelism();
                ParallelHashJoinExecutorFactory joinExecutorFactory =
                    new ParallelHashJoinExecutorFactory(fragment, current, otherCond,
                        equalCond, maxOneRow, oprands,
                        emptybuildFactory,
                        probeExecutorFactory, fragment.getParallelism(), numPartitions, driverBuilder,
                        localPairWise ? localPartitionCount : -1, keepPartition);

                //generate child's pipelineFactory
                LocalExchange localExchange = null;
                if (localPairWise) {
                    // create partition exchange under partition wise join
                    List<EquiJoinKey> joinKeys = EquiJoinUtils
                        .buildEquiJoinKeys(current, current.getOuter(), current.getInner(), (RexCall) equalCond,
                            current.getJoinType());
                    List<DataType> keyTypes =
                        joinKeys.stream().map(t -> t.getUnifiedType()).collect(Collectors.toList());
                    List<Integer> keyInnerIndexes =
                        joinKeys.stream().map(t -> t.getInnerIndex()).collect(Collectors.toList());

                    //generate build pipelineFactory
                    localExchange =
                        new LocalExchange(CalciteUtils.getTypes(current.getInner().getRowType()), keyInnerIndexes,
                            keyTypes, LocalExchange.LocalExchangeMode.CHUNK_PARTITION, true);
                } else {
                    // create random exchange under normal hash join
                    localExchange = new LocalExchange(CalciteUtils.getTypes(buildNode.getRowType()), ImmutableList.of(),
                        LocalExchange.LocalExchangeMode.RANDOM, true);
                }

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
    }

    private ExecutorFactory visitBKAJoin(Join current, PipelineFragment pipelineFragment) {
        new RelVisitor() {
            @Override
            public void visit(RelNode node, int ordinal, RelNode parent) {
                if (node instanceof LogicalView) {
                    forbidMultipleReadConn = forbidMultipleReadConn ||
                        !ExecUtils.allowMultipleReadConns(context, (LogicalView) node);
                }
                if (node instanceof Filter ||
                    node instanceof Project ||
                    node instanceof Exchange ||
                    node instanceof Union ||
                    node instanceof BKAJoin) {
                    super.visit(node, ordinal, parent);
                }
            }
        }.go(current);

        boolean forbidMultipleConn = this.forbidMultipleReadConn;
        if (defaultParallelism != 1 && forbidMultipleConn) {
            //事务下并发度必须为1,并且当前并发度需要被保持
            pipelineFragment.holdSingleTonParallelism();
        }

        ExecutorFactory outerExecutorFactory = visit(current, current.getOuter(), pipelineFragment);
        boolean oldExpandView = expandView;
        ExecutorFactory innerExecutorFactory;

        try {
            expandView = true;
            innerExecutorFactory = visit(current, current.getInner(), pipelineFragment);
            if (forbidMultipleConn) {
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

    private ExecutorFactory visitLogicalInsertExec(LogicalInsert current, PipelineFragment pipelineFragment) {
        ExecutorFactory childExecutorFactory = visit(current, current.getInput(), pipelineFragment);
        return new InsertSelectExecFactory(current, childExecutorFactory);
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

    private ExecutorFactory visitColumnarExchange(
        RelNode parent, ColumnarExchange exchange, PipelineFragment pipelineFragment) {
        if (exchange.isMergeSortExchange()) {
            MergeSort mergeSort = MergeSort.create(exchange.getInput(), exchange.getCollation(), null, null);
            mergeSort.setRelatedId(exchange.getRelatedId());
            ExecutorFactory ret = visitMergeSort(parent, mergeSort, pipelineFragment);
            ret.setExchange(exchange);
            return ret;
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

            ExecutorFactory ret = visit(parent, exchange.getInput(), pipelineFragment);
            ret.setExchange(exchange);
            return ret;
        }
    }

    private ExecutorFactory visitMppExchange(
        RelNode parent, MppExchange exchange, PipelineFragment pipelineFragment) {
        if (exchange.isMergeSortExchange()) {
            MergeSort mergeSort = MergeSort.create(exchange.getInput(), exchange.getCollation(), null, null);
            mergeSort.setRelatedId(exchange.getRelatedId());
            ExecutorFactory ret = visitMergeSort(parent, mergeSort, pipelineFragment);
            ret.setExchange(exchange);
            return ret;
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

            ExecutorFactory ret = visit(parent, exchange.getInput(), pipelineFragment);
            ret.setExchange(exchange);
            return ret;
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

    // For structure:
    // sortWindow(partition by group_col1 desc|asc, group_col2  desc|asc ... , order by order_col desc|asc)
    //      sort(group_col1 desc|asc, group_col2  desc|asc ..., order_col desc|asc)
    //          any input
    //
    // We should build these pipelines:
    // 1. ChildrenFragment: (InputExec) -> Driver -> (LocalExchanger -> LocalBufferExec)
    // 2. MiddleFragment: (LocalBufferExec) -> Driver -> (SortExec)
    private ExecutorFactory visitMemSortWithParallelSortWindow(RelNode parent, MemSort sort,
                                                               PipelineFragment pipelineFragment,
                                                               List<Integer> partitionChannel) {
        Preconditions.checkArgument(parent instanceof SortWindow);
        LocalExchange localExchange = null;

        // generate child's executor factory
        List<DataType> columns = CalciteUtils.getTypes(sort.getInput().getRowType());
        PipelineFragment childFragment = new PipelineFragment(defaultParallelism, sort.getInput());
        ExecutorFactory childFactory = visit(sort, sort.getInput(), childFragment);

        // Local exchange by first column in sorter under sort-window because it's the partition column.
        // Therefore, partitioning chunk by LocalExchanger are equivalent to dividing into buckets.
        localExchange =
            new LocalExchange(CalciteUtils.getTypes(sort.getInput().getRowType()), partitionChannel,
                LocalExchange.LocalExchangeMode.PARTITION, true);

        // ChildrenFragment: Input -> Driver -> (LocalExchanger -> LocalBufferExec)
        OutputBufferMemoryManager localBufferManager = createLocalMemoryManager();

        // Create local buffer exec.
        LocalBufferNode localBufferNode = LocalBufferNode.create(sort.getInput());
        LocalBufferExecutorFactory bufferExecFactory =
            new LocalBufferExecutorFactory(localBufferManager, columns, pipelineFragment.getParallelism());

        //generate child's pipelineFactory
        LocalExchangeConsumerFactory consumerFactory =
            new LocalExchangeConsumerFactory(bufferExecFactory, localBufferManager, localExchange);
        PipelineFactory childPipelineFactory = new PipelineFactory(
            childFactory, consumerFactory, childFragment.setPipelineId(pipelineIdGen++));
        pipelineFactorys.add(childPipelineFactory);

        // MiddleFragment: LocalBufferExec -> Driver -> SortExec
        int memSortParallelism = pipelineFragment.getParallelism();
        PipelineFragment middleFragment = new PipelineFragment(memSortParallelism, localBufferNode);

        middleFragment.addChild(childFragment);
        middleFragment.addDependency(childPipelineFactory.getPipelineId());

        //generate localSort executorFactory
        SortExecutorFactory sortExecutorFactory =
            new SortExecutorFactory(sort, memSortParallelism, columns, spillerFactory);

        LocalBufferConsumerFactory consumerOfSingleTopNFactory =
            new LocalBufferConsumerFactory(sortExecutorFactory);

        // generate middle's pipelineFactory
        PipelineFactory middlePipelineFactory =
            new PipelineFactory(bufferExecFactory, consumerOfSingleTopNFactory,
                middleFragment.setPipelineId(pipelineIdGen++));

        pipelineFactorys.add(middlePipelineFactory);

        // Handle dependency between pipelines
        pipelineFragment.addChild(middleFragment);
        middleFragment.setBuildDepOnAllConsumers(false);
        pipelineFragment.addDependency(middlePipelineFactory.getPipelineId());

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

    private ExecutorFactory visitView(
        RelNode parent, LogicalView logicalView, PipelineFragment pipelineFragment) {

        boolean isUnderMergeSort = parent instanceof MergeSort;
        pipelineFragment.addLogicalView(logicalView);
        if (expandView) {
            logicalView.setExpandView(true);
        }
        int prefetch = totalPrefetch;
        RangeScanMode rangeScanMode = null;
        if (isCluster) {
            if (isUnderMergeSort || logicalView.pushedRelNodeIsSort()) {
                rangeScanMode = RangeScanUtils.useRangeScan(logicalView, context);
                holdCollation = true;
                pipelineFragment.holdSingleTonParallelism();
            }
            if (prefetch <= 0) {
                prefetch = pipelineFragment.getParallelism();
            }
        } else {
            this.forbidMultipleReadConn =
                this.forbidMultipleReadConn || !ExecUtils.allowMultipleReadConns(context, logicalView);

            PhyTableOperationUtil.enableIntraGroupParallelism(logicalView.getSchemaName(), context);

            SplitInfo splitInfo;
            if (logicalView.fromTableOperation() != null) {
                splitInfo = splitManager.getSingleSplit(logicalView, context);
            } else {
                if (isUnderMergeSort && logicalView.pushedRelNodeIsSort()) {
                    rangeScanMode = RangeScanUtils.useRangeScan(logicalView, context, isSimpleMergeSort);
                }

                int mergeUnionSize = context.getParamManager().getInt(ConnectionParams.MERGE_UNION_SIZE);

                // range scan can not union physical sql
                if (rangeScanMode != null) {
                    context.putIntoHintCmds(ConnectionProperties.MERGE_UNION_SIZE, 1);
                }

                splitInfo = splitManager.getSplits(logicalView, context, false);

                if (rangeScanMode != null) {
                    // reset merge union size
                    context.putIntoHintCmds(ConnectionProperties.MERGE_UNION_SIZE, mergeUnionSize);
                }

                rangeScanMode = RangeScanUtils.checkSplitInfo(splitInfo, rangeScanMode);
            }

            if (logicalView.pushedRelNodeIsSort()) {
                holdCollation = true;
            }

            if (splitInfo.getConcurrencyPolicy() == QueryConcurrencyPolicy.SEQUENTIAL) {
                pipelineFragment.holdSingleTonParallelism();
                prefetch = 1;
            } else {
                if (prefetch < 0) {
                    prefetch = ExecUtils.getPrefetchNumForLogicalView(splitInfo.getSplitParallelism());
                }
                if (expandView) {
                    //The parallelism of the ExpandView must keep constant.
                } else {
                    if (defaultParallelism > 1 && !holdCollation) {
                        if (!pipelineFragment.isHoldParallelism()) {
                            boolean columnarIndex =
                                logicalView instanceof OSSTableScan && ((OSSTableScan) logicalView).isColumnarIndex();
                            int parallelism =
                                !columnarIndex ? getParallelismForInnodbScan(isUnderMergeSort, splitInfo) :
                                    getParallelismForColumnarScan(isUnderMergeSort, defaultParallelism, parent == null,
                                        splitInfo.getSplitCount());
                            if (!columnarIndex && parallelism >= splitInfo.getSplitCount()) {
                                prefetch = parallelism;
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
                        + ",isUnderSort=" + isUnderMergeSort + ",pushedRelNodeIsSort=" + logicalView
                        .pushedRelNodeIsSort() + ",SplitCount=" + splitInfo.getSplitCount());
            }
        }

        LogicalViewExecutorFactory logicalViewExecutorFactory =
            createViewFactory(parent, logicalView, pipelineFragment, spillerFactory, prefetch, isUnderMergeSort,
                rangeScanMode);

        boolean isProbeSideOfJoin =
            parent != null && parent instanceof HashJoin && ((HashJoin) parent).getOuter() == logicalView;
        boolean useFileConcurrency =
            ExecUtils.getQueryConcurrencyPolicy(context, logicalView) == QueryConcurrencyPolicy.FILE_CONCURRENT;
        boolean enableScanRandomShuffle =
            context.getParamManager().getBoolean(ConnectionParams.ENABLE_SCAN_RANDOM_SHUFFLE);

        boolean localPairWise =
            parent != null && parent instanceof HashJoin
                && parent.getTraitSet().getPartitionWise().isLocalPartition()
                && context.getParamManager().getBoolean(ConnectionParams.ENABLE_LOCAL_PARTITION_WISE_JOIN);

        boolean joinKeepPartition =
            parent != null && parent instanceof HashJoin && ((HashJoin) parent).isKeepPartition();

        // Don't use scan random shuffle when using fragment RF.
        if (isProbeSideOfJoin && useFileConcurrency && enableScanRandomShuffle
            && splitCountMap.containsKey(logicalView.getLogicalTableName())
            && !localPairWise && pipelineFragment.getFragmentRFManager() == null && !joinKeepPartition) {

            // NOTE: the task number equal to the count of workers that executing this query.
            final int totalFileCount = splitCountMap.get(logicalView.getLogicalTableName());
            final int targetParallelism = pipelineFragment.getParallelism();

            int shuffleThreshold = context.getParamManager().getInt(ConnectionParams.SCAN_RANDOM_SHUFFLE_THRESHOLD);

            if (totalFileCount >= targetParallelism * taskNumber * shuffleThreshold) {
                return logicalViewExecutorFactory;
            }

            final List<DataType> scanOutputTypes = CalciteUtils.getTypes(logicalView.getRowType());

            // Create local buffer exec.
            LocalBufferExecutorFactory bufferExecFactory =
                new LocalBufferExecutorFactory(createLocalMemoryManager(), scanOutputTypes, targetParallelism);

            // Create local exchanger exec.
            LocalExchange localExchange = new LocalExchange(scanOutputTypes, ImmutableList.of(),
                LocalExchange.LocalExchangeMode.RANDOM, true);

            LocalExchangeConsumerFactory consumerFactory =
                new LocalExchangeConsumerFactory(bufferExecFactory, createLocalMemoryManager(), localExchange);

            // Create a pipeline with structure of (producer: SCAN) -> (consumer: EXCHANGER -> CACHE)
            PipelineFragment newPipelineFragment = new PipelineFragment(targetParallelism, logicalView);
            PipelineFactory newPipelineFactory = new PipelineFactory(
                logicalViewExecutorFactory, consumerFactory, newPipelineFragment.setPipelineId(pipelineIdGen++));

            // Handle dependency between pipelines
            pipelineFragment.addDependency(newPipelineFactory.getPipelineId());
            pipelineFragment.addChild(newPipelineFragment);
            pipelineFactorys.add(newPipelineFactory);

            return bufferExecFactory;
        }

        return logicalViewExecutorFactory;
    }

    private int getParallelismForInnodbScan(boolean isUnderMergeSort, SplitInfo splitInfo) {
        int parallelism = isUnderMergeSort ? 1 :
            context.getParamManager().getInt(ConnectionParams.PARALLELISM);
        if (parallelism < 0) {
            int shards;
            switch (splitInfo.getConcurrencyPolicy()) {
            case GROUP_CONCURRENT_BLOCK:
                shards = splitInfo.getSplitParallelism();
                break;
            case RELAXED_GROUP_CONCURRENT:
                shards = splitInfo.getSplitParallelism();
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
        }

        if (parallelism == 0) {
            // Parallel query is disabled but we have a parallel plan... Very strange...
            parallelism = 1;
        }

        return parallelism;
    }

    private int getParallelismForColumnarScan(boolean isUnderMergeSort, int defaultParallelism, boolean noParent,
                                              int fileCount) {
        int parallelism = isUnderMergeSort ? 1 : context.getParamManager().getInt(ConnectionParams.PARALLELISM);
        if (parallelism < 0) {
            parallelism = defaultParallelism > 0 ? defaultParallelism : ExecUtils.getParallelismForLocal(context);
        }
        if (noParent && fileCount > 0) {
            parallelism = Math.min(parallelism, fileCount);
        }
        return parallelism;
    }

    private LogicalViewExecutorFactory createViewFactory(
        RelNode parent, LogicalView logicalView, PipelineFragment pipelineFragment, SpillerFactory spillerFactory,
        int prefetch, boolean isUnderMergeSort, RangeScanMode rangeScanMode) {
        long fetched = Long.MAX_VALUE;
        long offset = 0;
        boolean bSort = logicalView.pushedRelNodeIsSort();

        Map<Integer, ParameterContext> params = context.getParams().getCurrentParameter();
        Sort sort = null;
        if (bSort) {
            sort = (Sort) logicalView.getOptimizedPushedRelNodeForMetaQuery();
            if (isUnderMergeSort) {
                sort = (Sort) parent;
                if (sort.fetch != null) {
                    fetched = CBOUtil.getRexParam(sort.fetch, params);
                    if (sort.offset != null) {
                        offset = CBOUtil.getRexParam(sort.offset, params);
                    }
                }
            }
        }

        long maxRowCount = ExecUtils.getMaxRowCount(sort, context);

        int policy = -1;
        boolean randomSplits = false;
        if (!bSort && isUnderMergeSort) {
            policy = context.getParamManager().getInt(ConnectionParams.PREFETCH_EXECUTE_POLICY);
            switch (policy) {
            case 1:
                prefetch = 1;
                break;
            case 2:
                prefetch = 1;
                randomSplits = true;
                break;
            default:
                break;
            }
        }

        // if number rows to fetch is too large, should not use serialize mode
        if (rangeScanMode != null && !rangeScanMode.isNormalMode() && fetched > context.getParamManager()
            .getInt(ConnectionParams.RANGE_SCAN_SERIALIZE_LIMIT)) {
            rangeScanMode = RangeScanMode.NORMAL;
        }

        if (rangeScanMode == RangeScanMode.SERIALIZE) {
            prefetch = 1;
        }

        if (rangeScanMode != null) {
            randomSplits = false;
        }

        return new LogicalViewExecutorFactory(pipelineFragment, logicalView, prefetch,
            pipelineFragment.getParallelism(),
            maxRowCount, bSort, sort, fetched, offset, spillerFactory, bloomFilterExpressionMap, enableRuntimeFilter,
            randomSplits, rangeScanMode);
    }

    private ExecutorFactory visitProject(Project project, PipelineFragment pipelineFragment) {

        boolean canConvertToVectorizedExpression =
            VectorizedExpressionBuilder.canConvertToVectorizedExpression(context, project.getProjects());
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
        boolean canConvertToVectorizedExpression = VectorizedExpressionBuilder
            .canConvertToVectorizedExpression(context, filter.getCondition());
        ExecutorFactory childExecutorFactory = visit(filter, filter.getInput(), pipelineFragment);
        return new FilterExecFactory(filter, filter.getCondition(), childExecutorFactory,
            canConvertToVectorizedExpression,
            enableRuntimeFilter, bloomFilterExpressionMap);
    }

    private ExecutorFactory visitCTE(RecursiveCTE cte, PipelineFragment pipelineFragment) {
        ExecutorFactory anchorExecutorFactory = visit(cte, cte.getLeft(), pipelineFragment);
        ExecutorFactory recursiveExecutorFactory = visit(cte, cte.getRight(), pipelineFragment);
        return new CteExecFactory(cte, anchorExecutorFactory, recursiveExecutorFactory);
    }

    private ExecutorFactory visitCTEAnchor(RecursiveCTEAnchor cteAnchor, PipelineFragment pipelineFragment) {
        List<DataType> dataTypes = CalciteUtils.getTypes(cteAnchor.getRowType());
        return new CteAnchorExecFactory(cteAnchor, dataTypes);
    }

    private ExecutorFactory visitRuntimeFilterBuild(RuntimeFilterBuilder filterBuilder,
                                                    PipelineFragment pipelineFragment) {
        ExecutorFactory childExecutorFactory = visit(filterBuilder, filterBuilder.getInput(), pipelineFragment);
        return new RuntimeFilterBuilderExecFactory(filterBuilder, childExecutorFactory, httpClient,
            runtimeFilterUpdateUri, localBloomFilter);
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
                    childFragment.getParallelism() == pipelineFragment.getParallelism() ?
                        LocalExchange.LocalExchangeMode.DIRECT : LocalExchange.LocalExchangeMode.RANDOM;
                localExchange = new LocalExchange(columns, ImmutableList.of(), mode, true);
            } else {
                LocalExchange.LocalExchangeMode mode = pipelineFragment.getParallelism() == 1 ?
                    LocalExchange.LocalExchangeMode.SINGLE : LocalExchange.LocalExchangeMode.RANDOM;
                localExchange =
                    new LocalExchange(columns, ImmutableList.of(), mode, childFragment.getParallelism() == 1);
            }
        } else {
            if (agg.getGroupSet().cardinality() == 0) {
                localExchange = new LocalExchange(columns, ImmutableList.of(), LocalExchange.LocalExchangeMode.SINGLE,
                    asyncConsume);
                pipelineFragment.holdSingleTonParallelism();
            } else {
                int parallelism = pipelineFragment.getParallelism();

                boolean localPairWise = localPartitionCount > 0 && context.getParamManager()
                    .getBoolean(ConnectionParams.ENABLE_LOCAL_PARTITION_WISE_JOIN)
                    && agg.getTraitSet().getPartitionWise().isLocalPartition();

                LocalExchange.LocalExchangeMode mode;
                if (localPairWise && localPartitionCount == parallelism) {
                    // if parts_per_worker = parallelism * N, N = 1
                    mode = LocalExchange.LocalExchangeMode.DIRECT;
                } else if (parallelism == 1) {
                    mode = LocalExchange.LocalExchangeMode.SINGLE;
                } else {
                    mode = LocalExchange.LocalExchangeMode.PARTITION;
                }

                localExchange = new LocalExchange(columns, agg.getGroupSet().toList(), mode, asyncConsume);
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

    private Integer getRelNodeDistinctKeyCount(RelNode relNode, ImmutableBitSet groupKey) {
        Integer rowCount = context.getDistinctKeyCnt().get(relNode.getRelatedId());
        if (rowCount == null) {
            rowCount =
                Optional.ofNullable(RelUtils.getDistinctKeyCount(relNode, groupKey, null)).map(Double::intValue).orElse(
                    CostModelWeight.GUESS_AGG_OUTPUT_NUM);
            context.getDistinctKeyCnt().put(relNode.getRelatedId(), rowCount);
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

            pipelineFactorys.add(producePipelineFactory); //
            pipelineFragment.addDependency(consumerPipeFactory.getPipelineId());
            pipelineFragment.addDependency(producePipelineFactory.getPipelineId()); //
            pipelineFragment.addBufferNodeChild(localBufferNode.getInput().getRelatedId(), produceFragment); //
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
