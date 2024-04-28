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

package com.alibaba.polardbx.executor.mpp.operator.factory;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.executor.chunk.MutableChunk;
import com.alibaba.polardbx.executor.operator.AbstractOSSTableScanExec;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpressionUtils;
import com.alibaba.polardbx.executor.vectorized.build.InputRefTypeChecker;
import com.alibaba.polardbx.executor.vectorized.build.Rex2VectorizedExpressionVisitor;
import com.alibaba.polardbx.executor.vectorized.build.VectorizedExpressionBuilder;
import com.alibaba.polardbx.optimizer.core.rel.OSSTableScan;
import com.alibaba.polardbx.optimizer.core.rel.OrcTableScan;
import com.google.common.base.Preconditions;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.chunk.MutableChunk;
import com.alibaba.polardbx.executor.mpp.operator.LocalExecutionPlanner;
import com.alibaba.polardbx.executor.mpp.planner.FragmentRFItem;
import com.alibaba.polardbx.executor.mpp.planner.FragmentRFItemKey;
import com.alibaba.polardbx.executor.mpp.planner.FragmentRFManager;
import com.alibaba.polardbx.executor.mpp.planner.PipelineFragment;
import com.alibaba.polardbx.executor.operator.AbstractOSSTableScanExec;
import com.alibaba.polardbx.executor.operator.ColumnarDeletedScanExec;
import com.alibaba.polardbx.executor.operator.ColumnarScanExec;
import com.alibaba.polardbx.executor.operator.DrivingStreamTableScanExec;
import com.alibaba.polardbx.executor.operator.DrivingStreamTableScanSortExec;
import com.alibaba.polardbx.executor.operator.Executor;
import com.alibaba.polardbx.executor.operator.LookupTableScanExec;
import com.alibaba.polardbx.executor.operator.MergeSortTableScanClient;
import com.alibaba.polardbx.executor.operator.MergeSortWithBufferTableScanClient;
import com.alibaba.polardbx.executor.operator.ResumeTableScanExec;
import com.alibaba.polardbx.executor.operator.ResumeTableScanSortExec;
import com.alibaba.polardbx.executor.operator.TableScanClient;
import com.alibaba.polardbx.executor.operator.TableScanExec;
import com.alibaba.polardbx.executor.operator.TableScanSortExec;
import com.alibaba.polardbx.executor.operator.lookup.LookupConditionBuilder;
import com.alibaba.polardbx.executor.operator.spill.SpillerFactory;
import com.alibaba.polardbx.executor.operator.util.bloomfilter.BloomFilterConsume;
import com.alibaba.polardbx.executor.operator.util.bloomfilter.BloomFilterExpression;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpressionUtils;
import com.alibaba.polardbx.executor.vectorized.build.InputRefTypeChecker;
import com.alibaba.polardbx.executor.vectorized.build.Rex2VectorizedExpressionVisitor;
import com.alibaba.polardbx.executor.vectorized.build.VectorizedExpressionBuilder;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.CursorMeta;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.join.EquiJoinUtils;
import com.alibaba.polardbx.optimizer.core.join.LookupEquiJoinKey;
import com.alibaba.polardbx.optimizer.core.join.LookupPredicate;
import com.alibaba.polardbx.optimizer.core.join.LookupPredicateBuilder;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.OSSTableScan;
import com.alibaba.polardbx.optimizer.core.rel.OrcTableScan;
import com.alibaba.polardbx.optimizer.utils.CalciteUtils;
import com.alibaba.polardbx.statistics.RuntimeStatHelper;
import com.google.common.base.Preconditions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class LogicalViewExecutorFactory extends ExecutorFactory {
    private static final Logger MPP_LOGGER = LoggerFactory.getLogger(LocalExecutionPlanner.class);
    private final PipelineFragment fragment;

    private final int totalPrefetch;
    private final CursorMeta meta;
    private final AtomicInteger counter = new AtomicInteger(0);
    private final int parallelism;
    private final long maxRowCount;
    private final LogicalView logicalView;
    private TableScanClient scanClient;
    private final SpillerFactory spillerFactory;

    private final boolean bSort;
    private final Sort sort;
    private final long fetch;
    private final long skip;

    private BloomFilterExpression filterExpression;

    private boolean enablePassiveResume;

    private boolean enableDrivingResume;

    private List<LookupEquiJoinKey> allJoinKeys; // including null-safe equal (`<=>`)
    private LookupPredicate predicates;
    private List<DataType> dataTypeList;
    private boolean randomSplits;

    public LogicalViewExecutorFactory(
        PipelineFragment fragment, LogicalView logicalView,
        int totalPrefetch, int parallelism, long maxRowCount, boolean bSort, Sort sort,
        long fetch, long skip, SpillerFactory spillerFactory, Map<Integer, BloomFilterExpression> bloomFilters,
        boolean enableRuntimeFilter, boolean randomSplits) {
        this.fragment = fragment;
        this.logicalView = logicalView;
        this.totalPrefetch = totalPrefetch;
        this.meta = CursorMeta.build(CalciteUtils.buildColumnMeta(logicalView, "TableScanColumns"));
        this.parallelism = parallelism;
        this.maxRowCount = maxRowCount;
        this.bSort = bSort;
        this.sort = sort;
        this.fetch = fetch;
        this.skip = skip;
        this.spillerFactory = spillerFactory;
        this.randomSplits = randomSplits;

        if (logicalView.getJoin() != null) {
            Join join = logicalView.getJoin();
            this.allJoinKeys = EquiJoinUtils.buildLookupEquiJoinKeys(join, join.getOuter(), join.getInner(),
                (RexCall) join.getCondition(), join.getJoinType());
            List<String> columnOrigins = logicalView.getColumnOrigins();
            this.predicates = new LookupPredicateBuilder(join, columnOrigins).build(allJoinKeys);
        }

        if (enableRuntimeFilter) {
            List<Integer> bloomFilterIds = logicalView.getBloomFilters();
            if (bloomFilterIds.size() > 0) {
                List<BloomFilterConsume> consumes = new ArrayList<>();
                for (Integer bloomId : bloomFilterIds) {
                    consumes.add(new BloomFilterConsume(null, bloomId));
                }
                this.filterExpression = new BloomFilterExpression(consumes, true);
                bloomFilters.put(logicalView.getRelatedId(), filterExpression);
            }
        }
        this.dataTypeList = CalciteUtils.getTypes(logicalView.getRowType());
    }

    @Override
    public Executor createExecutor(ExecutionContext context, int index) {
        if (logicalView instanceof OSSTableScan) {
            return buildOSSTableScanExec(context);
        } else {
            return buildTableScanExec(context);
        }
    }

    @NotNull
    private Executor buildTableScanExec(ExecutionContext context) {
        TableScanExec scanExec;
        Join join = logicalView.getJoin();
        if (join != null) {
            boolean canShard = false;
            if (context.getParamManager().getBoolean(ConnectionParams.ENABLE_BKA_PRUNING)) {
                LogicalView lv = this.getLogicalView();
                if (lv.getTableNames().size() == 1) {
                    canShard = new LookupConditionBuilder(allJoinKeys, predicates, lv, context).canShard();
                }
            }
            scanExec = createLookupScanExec(context, canShard, predicates, allJoinKeys);
        } else {
            boolean useTransactionConnection = ExecUtils.useExplicitTransaction(context);

            if (bSort) {
                long limit = context.getParamManager().getLong(ConnectionParams.MERGE_SORT_BUFFER_SIZE);
                if (limit > 0 && logicalView.pushedRelNodeIsSort()) {
                    this.scanClient = new MergeSortWithBufferTableScanClient(
                        context, meta, useTransactionConnection, totalPrefetch);
                } else {
                    this.scanClient = new MergeSortTableScanClient(
                        context, meta, useTransactionConnection, totalPrefetch);
                }
            } else if (useTransactionConnection || enablePassiveResume || enableDrivingResume) {
                int prefetch = calculatePrefetchNum(counter.incrementAndGet(), parallelism);
                this.scanClient = new TableScanClient(context, meta, useTransactionConnection, prefetch);
            } else {
                synchronized (this) {
                    if (scanClient == null) {
                        this.scanClient =
                            new TableScanClient(context, meta, false, Math.max(totalPrefetch, parallelism));
                    }
                }
            }

            if (filterExpression != null) {
                scanClient.initWaitFuture(filterExpression.getWaitBloomFuture());
            }

            scanExec = buildTableScanExec(scanClient, context);

            if (randomSplits) {
                scanExec.setRandomSplits(randomSplits);
            }
        }
        registerRuntimeStat(scanExec, logicalView, context);

        return scanExec;
    }

    @Override
    protected void registerRuntimeStat(Executor scanExec, RelNode relNode, ExecutionContext context) {
        super.registerRuntimeStat(scanExec, relNode, context);
        if (context.getRuntimeStatistics() != null) {
            if (bSort && scanExec instanceof TableScanSortExec) {
                RuntimeStatHelper.registerStatForExec(sort, scanExec, context);
            }
        }
    }

    private Executor buildOSSTableScanExec(ExecutionContext context) {
        OSSTableScan ossTableScan = (OSSTableScan) logicalView;

        if (context.isEnableOrcDeletedScan()) {
            // Special path for check cci consistency.
            // Normal oss read should not get here.
            Executor exec = new ColumnarDeletedScanExec(ossTableScan, context, dataTypeList);
            registerRuntimeStat(exec, logicalView, context);
            return exec;
        }

        // Use columnar table scan exec.
        if (context.getParamManager().getBoolean(ConnectionParams.ENABLE_COLUMNAR_SCAN_EXEC)) {
            ColumnarScanExec exec = new ColumnarScanExec(ossTableScan, context, dataTypeList);
            registerRuntimeStat(exec, logicalView, context);

            if (fragment.getFragmentRFManager() != null) {
                FragmentRFManager fragmentRFManager = fragment.getFragmentRFManager();

                Map<FragmentRFItemKey, FragmentRFItem> allItems = fragmentRFManager.getAllItems();

                for (FragmentRFItemKey itemKey : allItems.keySet()) {
                    FragmentRFItem item = allItems.get(itemKey);

                    String probeColumnName = item.getProbeColumnName();

                    // inspect the filter channel according to registered RF columns.
                    List<String> fieldNames = logicalView.getRowType().getFieldNames();
                    final int outProjectIndex = fieldNames.indexOf(probeColumnName);

                    if (outProjectIndex == -1) {
                        if (MPP_LOGGER.isDebugEnabled()) {
                            MPP_LOGGER.debug(
                                "Cannot find the filter channel according to registered RF columns "
                                    + ", fragmentRFItemKey = " + itemKey
                                    + ", for scan: " + logicalView);
                        }

                    } else {
                        // Mapping to input index in file.
                        final int inProjectIndex = ossTableScan.getOrcNode().getInProjects().get(outProjectIndex);
                        item.setSourceRefInFile(inProjectIndex);
                        item.setSourceFilterChannel(outProjectIndex);

                        // register column scan exec in all threads into fragment RF manager.
                        item.registerSource(exec);
                    }
                }

            }

            return exec;
        }

        AbstractOSSTableScanExec exec = AbstractOSSTableScanExec.create(ossTableScan, context, dataTypeList);

        OrcTableScan orcTableScan = ossTableScan.getOrcNode();
        if (!orcTableScan.getFilters().isEmpty()) {
            RexNode filterCondition = orcTableScan.getFilters().get(0);

            List<DataType<?>> inputTypes = orcTableScan.getInProjectsDataType();

            // binding vec expression
            RexNode root = VectorizedExpressionBuilder.rewriteRoot(filterCondition, true);
            InputRefTypeChecker inputRefTypeChecker = new InputRefTypeChecker(inputTypes);
            root = root.accept(inputRefTypeChecker);
            Rex2VectorizedExpressionVisitor converter =
                new Rex2VectorizedExpressionVisitor(context, inputTypes.size());
            VectorizedExpression vectorizedExpression = root.accept(converter);
            List<DataType<?>> filterOutputTypes = converter.getOutputDataTypes();
            MutableChunk preAllocatedChunk = MutableChunk.newBuilder(context.getExecutorChunkLimit())
                .addEmptySlots(inputTypes)
                .addEmptySlots(filterOutputTypes)
                .build();

            // prepare filter bitmap
            List<Integer> inputIndex = VectorizedExpressionUtils.getInputIndex(vectorizedExpression);
            int[] filterBitmap = new int[inputTypes.size() + filterOutputTypes.size()];
            for (int i : inputIndex) {
                filterBitmap[i] = 1;
            }

            exec.setPreAllocatedChunk(preAllocatedChunk);
            exec.setFilterInputTypes(inputTypes);
            exec.setFilterOutputTypes(filterOutputTypes);
            exec.setCondition(vectorizedExpression);
            exec.setFilterBitmap(filterBitmap);
            int[] outProject = new int[orcTableScan.getOutProjects().size()];
            for (int i = 0; i < orcTableScan.getOutProjects().size(); i++) {
                outProject[i] = orcTableScan.getOutProjects().get(i);
            }
            exec.setOutProject(outProject);
        }
        registerRuntimeStat(exec, logicalView, context);

        if (filterExpression != null) {
            exec.initWaitFuture(filterExpression.getWaitBloomFuture());
        }

        return exec;
    }

    private TableScanExec buildTableScanExec(TableScanClient scanClient, ExecutionContext context) {
        int stepSize = context.getParamManager().getInt(ConnectionParams.RESUME_SCAN_STEP_SIZE);
        if (enablePassiveResume && !context.isShareReadView()) {
            if (bSort) {
                return new ResumeTableScanSortExec(
                    logicalView, context, scanClient.incrementSourceExec(), maxRowCount, skip, fetch, spillerFactory,
                    stepSize, dataTypeList);
            } else {
                return new ResumeTableScanExec(logicalView, context, scanClient.incrementSourceExec(),
                    spillerFactory, stepSize, dataTypeList);

            }
        } else if (enableDrivingResume) {
            if (bSort) {
                return new DrivingStreamTableScanSortExec(
                    logicalView, context, scanClient.incrementSourceExec(), maxRowCount, skip, fetch, spillerFactory,
                    stepSize, dataTypeList);
            } else {
                return new DrivingStreamTableScanExec(logicalView, context, scanClient.incrementSourceExec(),
                    spillerFactory, stepSize, dataTypeList);

            }
        } else {
            if (bSort) {
                return new TableScanSortExec(
                    logicalView, context, scanClient.incrementSourceExec(), maxRowCount, skip, fetch,
                    spillerFactory, dataTypeList);
            } else {
                return new TableScanExec(logicalView, context, scanClient.incrementSourceExec(),
                    maxRowCount,
                    spillerFactory, dataTypeList);
            }
        }
    }

    public void enablePassiveResumeSource() {
        this.enablePassiveResume = true;
        Preconditions.checkArgument(
            !(enablePassiveResume && enableDrivingResume), "Don't support stream scan in different mode");
    }

    public void enableDrivingResumeSource() {
        this.enableDrivingResume = true;
        Preconditions.checkArgument(
            !(enablePassiveResume && enableDrivingResume), "Don't support stream scan in different mode");
    }

    public TableScanExec createLookupScanExec(ExecutionContext context, boolean canShard,
                                              LookupPredicate predicate, List<LookupEquiJoinKey> allJoinKeys) {
        boolean allowMultipleReadConn = ExecUtils.allowMultipleReadConns(context, logicalView);
        boolean useTransaction = ExecUtils.useExplicitTransaction(context);

        int prefetch = 1;
        if (allowMultipleReadConn) {
            prefetch = calculatePrefetchNum(counter.incrementAndGet(), parallelism);
            if (parallelism > 1 && totalPrefetch > 1) {
                //由于bkaJoin有动态裁剪能力，会导致部分scan的分配split被裁剪为0，浪费prefetch的分配名额
                prefetch = prefetch == 1 ? 2 : prefetch;
            }
        }

        TableScanClient scanClient = new TableScanClient(context, meta, useTransaction, prefetch);
        TableScanExec scanExec =
            new LookupTableScanExec(logicalView, context, scanClient.incrementSourceExec(), canShard, spillerFactory,
                predicate, allJoinKeys, dataTypeList);
        registerRuntimeStat(scanExec, logicalView, context);
        return scanExec;
    }

    private int calculatePrefetchNum(int index, int parallelism) {
        Preconditions.checkArgument(index <= parallelism, "index must less than " + parallelism);
        if (parallelism >= totalPrefetch) {
            return 1;
        } else {
            if (index <= totalPrefetch % parallelism) {
                return totalPrefetch / parallelism + 1;
            } else {
                return totalPrefetch / parallelism;
            }
        }
    }

    public boolean isPushDownSort() {
        return bSort;
    }

    public LogicalView getLogicalView() {
        return logicalView;
    }

    public int getParallelism() {
        return this.parallelism;
    }
}
