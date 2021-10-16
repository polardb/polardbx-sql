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

package com.alibaba.polardbx.executor.mpp.planner;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.mpp.Session;
import com.alibaba.polardbx.executor.mpp.operator.LocalExecutionPlanner;
import com.alibaba.polardbx.executor.mpp.split.SplitInfo;
import com.alibaba.polardbx.executor.mpp.split.SplitManager;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.executor.utils.OrderByOption;
import com.alibaba.polardbx.optimizer.core.rel.BKAJoin;
import com.alibaba.polardbx.optimizer.core.rel.BaseTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.Limit;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.MergeSort;
import com.alibaba.polardbx.optimizer.core.rel.SemiBKAJoin;
import com.alibaba.polardbx.optimizer.core.rel.mpp.MppExchange;
import com.alibaba.polardbx.optimizer.workload.WorkloadUtil;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.DynamicValues;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.logical.RuntimeFilterBuilder;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlRuntimeFilterBuildFunction;
import org.apache.calcite.sql.fun.SqlRuntimeFilterFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.alibaba.polardbx.executor.mpp.operator.LocalExecutionPlanner.SUPPORT_ALL_CACHE_NODES;
import static com.alibaba.polardbx.executor.mpp.operator.LocalExecutionPlanner.SUPPORT_ONE_SIDE_CACHE_NODES;
import static com.alibaba.polardbx.executor.mpp.planner.PartitionHandle.SINGLETON_SOURCE;
import static com.alibaba.polardbx.executor.utils.ExecUtils.convertBuildSide;
import static com.alibaba.polardbx.executor.utils.ExecUtils.existMppOnlyInstanceNode;

public class PlanFragmenter {

    private static final Logger log = LoggerFactory.getLogger(PlanFragmenter.class);

    public static Pair<SubPlan, Integer> buildRootFragment(RelNode root, Session session) {
        FragmentProperties properties = new FragmentProperties();
        Fragmenter fragmenter = new Fragmenter(session, root);
        root = fragmenter.visit(null, root, properties);
        SubPlan result = fragmenter.buildRootFragment(root, properties);
        return new Pair<>(result, fragmenter.getMaxConcurrentParallelism());
    }

    private static class Fragmenter {
        private int nextFragmentId = 0;
        private Session session;

        private int maxConcurrentParallelism = -1;
        private int currentConcurrentParallelism = 0;
        private boolean onlyUseReadInstance;
        private boolean lowConcurrencyQuery = false;

        public Fragmenter(Session session, RelNode root) {
            this.session = session;
            if (!WorkloadUtil.isApWorkload(session.getClientContext().getWorkloadType())) {
                lowConcurrencyQuery = true;
            } else {
                session.getClientContext().getExtraCmds().put(ConnectionProperties.MERGE_UNION, false);
            }

            this.onlyUseReadInstance = existMppOnlyInstanceNode();
        }

        public int getMaxConcurrentParallelism() {
            return Math.max(maxConcurrentParallelism, currentConcurrentParallelism);
        }

        public SubPlan buildRootFragment(RelNode root, FragmentProperties currentProperties) {
            PartitioningScheme partitioningScheme = null;
            if (isMergeSortSourceNode(root)) {
                List<SubPlan> children = currentProperties.getChildren();
                if (children.size() == 1 && children.get(0).getFragment().getPartitioning().getPartitionCount() == 1) {
                    //exchange 的 input 是分区有序，且其并发度已经为1了，则可以省去添加当前fragment
                    return currentProperties.getChildren().get(0);
                }
                PartitionShuffleHandle shuffleHandle = new PartitionShuffleHandle(
                    PartitionShuffleHandle.PartitionShuffleMode.SINGLE, true);
                RemoteSourceNode remoteSourceNode = (RemoteSourceNode) root;
                RelCollation ret = remoteSourceNode.getRelCollation();
                List<RelFieldCollation> sortList = ret.getFieldCollations();
                List<OrderByOption> orderByOptions = ExecUtils.convertFrom(sortList);
                partitioningScheme =
                    new PartitioningScheme(ImmutableList.of(), orderByOptions, shuffleHandle);
            } else {
                PartitionShuffleHandle shuffleHandle = new PartitionShuffleHandle(
                    PartitionShuffleHandle.PartitionShuffleMode.SINGLE, false);
                partitioningScheme =
                    new PartitioningScheme(ImmutableList.of(), ImmutableList.of(), shuffleHandle);
            }
            List<Integer> filterIds = new ArrayList<>();
            List<Integer> produceFilterIds = new ArrayList<>();
            if (!lowConcurrencyQuery) {
                RuntimeFilterIdCollector runtimeFilterIdCollector = new RuntimeFilterIdCollector();
                runtimeFilterIdCollector.collect(root, session);
                filterIds = runtimeFilterIdCollector.filterIds;
                produceFilterIds = runtimeFilterIdCollector.produceFilterIds;
            }

            return generateSubPlan(
                root, currentProperties, SerializeDataType.convertToSerilizeType(root.getRowType().getFieldList()),
                partitioningScheme, filterIds, produceFilterIds);
        }

        protected RelNode visitChild(RelNode parent, int i, RelNode relNode, FragmentProperties parentProperties) {
            RelNode child = visit(parent, relNode, parentProperties);
            if (child != relNode) {
                final List<RelNode> newInputs = new ArrayList<>(parent.getInputs());
                newInputs.set(i, child);
                RelNode newParent = parent.copy(parent.getTraitSet(), newInputs).setHints(parent.getHints());
                newParent.setRelatedId(parent.getRelatedId());
                return newParent;
            }
            return parent;
        }

        protected RelNode visitChildren(RelNode rel, FragmentProperties properties) {

            if (rel instanceof BKAJoin || rel instanceof SemiBKAJoin) {
                //对于BKAJoin，我们先遍历outer端，直接忽略遍历inner端
                Join join = (Join) rel;
                int outerIndex = join.getOuter() == join.getInput(0) ? 0 : 1;
                rel = visitChild(rel, outerIndex, join.getOuter(), properties);
                RelNode innerRelNode = join.getInner();

                new RelVisitor() {
                    @Override
                    public void visit(RelNode node, int ordinal, RelNode parent) {
                        if (node instanceof LogicalView) {
                            properties.addExpandView(((LogicalView) node).setExpandView(true));
                        }
                        super.visit(node, ordinal, parent);
                    }
                }.go(innerRelNode);

            } else if (rel instanceof LogicalCorrelate) {
                //对于LogicalCorrelate，直接忽略遍历right端
                rel = visitChild(rel, 0, ((LogicalCorrelate) rel).getLeft(), properties);
            } else if (rel.getInputs().size() == 2) {

                Join join = (Join) rel;
                int innerIndex = join.getInner() == join.getInput(0) ? 0 : 1;
                int outerIndex = join.getOuter() == join.getInput(0) ? 0 : 1;
                boolean convertBuildSide = convertBuildSide(join);
                if (convertBuildSide) {
                    //先切分outer端，再切分inner端
                    rel = visitChild(rel, outerIndex, join.getOuter(), properties);
                    if (LocalExecutionPlanner.isAssignableFrom(rel.getClass(), SUPPORT_ONE_SIDE_CACHE_NODES)) {
                        properties.setInnerChildParallelism(currentConcurrentParallelism);
                    }
                    rel = visitChild(rel, innerIndex, join.getInner(), properties);
                } else {
                    //先切分inner端，再切分outer端
                    rel = visitChild(rel, innerIndex, join.getInner(), properties);
                    if (LocalExecutionPlanner.isAssignableFrom(rel.getClass(), SUPPORT_ONE_SIDE_CACHE_NODES)) {
                        properties.setInnerChildParallelism(currentConcurrentParallelism);
                    }
                    rel = visitChild(rel, outerIndex, join.getOuter(), properties);
                }
            } else if (rel.getInputs().size() == 1) {
                rel = visitChild(rel, 0, rel.getInput(0), properties);
                if (LocalExecutionPlanner.isAssignableFrom(rel.getClass(), SUPPORT_ALL_CACHE_NODES)) {
                    properties.setSingleChildParallelism(currentConcurrentParallelism);
                }
            } else {
                throw new RuntimeException("Don't support multi inputs whose quality is more than 2!");
            }
            return rel;
        }

        public RelNode visit(RelNode parent, RelNode other, FragmentProperties parentProperties) {
            if (other instanceof Exchange) {
                return visitExchange(parent, (Exchange) other, parentProperties);
            } else if (other instanceof MergeSort) {
                return visitMergeSort(parent, (MergeSort) other, parentProperties);
            } else if (other instanceof LogicalUnion) {
                return visit(parent, (LogicalUnion) other, parentProperties);
            } else if (other instanceof LogicalView) {
                parentProperties.setSourceNode();
                parentProperties.updateRootCount(ExecUtils.calcRowCount((LogicalView) other));
                parentProperties.updateRootIo(ExecUtils.calcIo((LogicalView) other));
                parentProperties.setLogicalView((LogicalView) other);
                return other;
            } else if (other instanceof LogicalValues || other instanceof DynamicValues) {
                parentProperties.setSingleTonNode();
                return other;
            } else if (other instanceof BaseTableOperation) {
                LogicalView logicalView = ExecUtils.convertToLogicalView((BaseTableOperation) other);
                logicalView.setRelatedId(other.getRelatedId());
                RelNode newParent = parent;
                if (parent != null) {
                    newParent = parent.copy(
                        parent.getTraitSet(), Lists.newArrayList(logicalView)).setHints(parent.getHints());
                }
                return visit(newParent, logicalView, parentProperties);
            } else if (!LocalExecutionPlanner.isAssignableFrom(other.getClass())) {
                //push 执行器不支持
                parentProperties.setSingleTonNode();
                return other;
            } else {
                return visitChildren(other, parentProperties);
            }
        }

        private RelNode visitMergeSort(RelNode parent, MergeSort mergeSort, FragmentProperties parentProperties) {
            PartitionShuffleHandle shuffleHandle =
                new PartitionShuffleHandle(PartitionShuffleHandle.PartitionShuffleMode.SINGLE, true);
            RelCollation ret = mergeSort.getCollation();
            List<RelFieldCollation> sortList = ret.getFieldCollations();
            List<OrderByOption> orderBys = ExecUtils.convertFrom(sortList);
            parentProperties.setNode(PartitionHandle.SINGLETON);
            RelNode remoteSourceNode = null;
            if (mergeSort.getInput() instanceof LogicalUnion) {
                LogicalUnion union = (LogicalUnion) mergeSort.getInput();
                remoteSourceNode = visitUnion(union, shuffleHandle, parentProperties);
            } else {
                SubPlan subPlan =
                    generateFragment(mergeSort, 0, shuffleHandle, ImmutableList.of(), orderBys);
                parentProperties.addChildren(subPlan);
                remoteSourceNode = new RemoteSourceNode(
                    mergeSort.getCluster(), mergeSort.getTraitSet(),
                    ImmutableList.of(subPlan.getFragment().getId()), mergeSort.getRowType(),
                    estimateRowCount(mergeSort).intValue());
            }
            RelNode relNode = remoteSourceNode;
            if (mergeSort.offset != null || mergeSort.fetch != null) {
                relNode = Limit.create(mergeSort.getTraitSet(), remoteSourceNode, mergeSort.offset, mergeSort.fetch);
            }
            relNode.setRelatedId(mergeSort.getRelatedId());
            return relNode;
        }

        private RelNode visitExchange(RelNode parent, Exchange exchange, FragmentProperties parentProperties) {
            PartitionShuffleHandle shuffleHandle;
            boolean mergeSort =
                exchange instanceof MppExchange ? ((MppExchange) exchange).isMergeSortExchange() : false;
            if (exchange.getDistribution() == RelDistributions.SINGLETON) {
                if (mergeSort) {
                    shuffleHandle =
                        new PartitionShuffleHandle(PartitionShuffleHandle.PartitionShuffleMode.SINGLE, true);
                } else {
                    shuffleHandle = new PartitionShuffleHandle(
                        PartitionShuffleHandle.PartitionShuffleMode.SINGLE, false);
                }
                parentProperties.setNode(PartitionHandle.SINGLETON);
            } else if (exchange.getDistribution() == RelDistributions.BROADCAST_DISTRIBUTED) {
                parentProperties.updateRootCount(estimateRowCount(exchange));
                parentProperties.setNode(new PartitionHandle(PartitionHandle.PartitionMode.DEFAULT));
                if (parentProperties.getOptionalHandle().isPresent()) {
                    shuffleHandle = new PartitionShuffleHandle(
                        PartitionShuffleHandle.PartitionShuffleMode.BROADCAST, mergeSort);
                } else {
                    throw new IllegalStateException("the parent subPlan must be set partitionCount before");
                }
            } else {
                parentProperties.updateRootCount(estimateRowCount(exchange));
                parentProperties.setNode(new PartitionHandle(PartitionHandle.PartitionMode.DEFAULT));
                shuffleHandle = new PartitionShuffleHandle(
                    PartitionShuffleHandle.PartitionShuffleMode.FIXED,
                    mergeSort);
            }
            List<OrderByOption> orderBys = new ArrayList<>();
            if (mergeSort) {
                RelCollation ret = ((MppExchange) exchange).getCollation();
                List<RelFieldCollation> sortList = ret.getFieldCollations();
                orderBys = ExecUtils.convertFrom(sortList);
            }
            if (exchange.getInput() instanceof LogicalUnion) {
                LogicalUnion union = (LogicalUnion) exchange.getInput();
                RelNode remoteSourceNode = visitUnion(union, shuffleHandle, parentProperties);
                remoteSourceNode.setRelatedId(exchange.getRelatedId());
                return remoteSourceNode;
            } else {
                SubPlan subPlan =
                    generateFragment(exchange, 0, shuffleHandle, exchange.distribution.getKeys(), orderBys);
                parentProperties.addChildren(subPlan);
                RemoteSourceNode remoteSourceNode = new RemoteSourceNode(exchange.getCluster(), exchange.getTraitSet(),
                    ImmutableList.of(subPlan.getFragment().getId()), exchange.getRowType(),
                    estimateRowCount(exchange).intValue());
                remoteSourceNode.setRelatedId(exchange.getRelatedId());
                return remoteSourceNode;
            }
        }

        private RelNode visit(RelNode parent, LogicalUnion union, FragmentProperties parentProperties) {
            //如果是union + exchange, 则union 和 exchange chain在一起，并发度由exchange决定;
            //如果是unionall单独存在，则unionall可以多并发,其中shuffle的模式将采样random方式
            parentProperties.updateRootCount(estimateRowCount(union));
            parentProperties.setNode(new PartitionHandle(PartitionHandle.PartitionMode.DEFAULT));
            PartitionShuffleHandle shuffleHandle = new PartitionShuffleHandle(
                PartitionShuffleHandle.PartitionShuffleMode.FIXED,
                false);
            return visitUnion(union, shuffleHandle, parentProperties);
        }

        private RelNode visitUnion(
            LogicalUnion union, PartitionShuffleHandle shuffleHandle, FragmentProperties parentProperties) {
            List<Integer> sourceFragmentIds = new ArrayList<>();
            for (int i = 0; i < union.getInputs().size(); i++) {
                SubPlan subPlan = generateFragment(union, i, shuffleHandle, ImmutableList.of(), ImmutableList.of());
                parentProperties.addChildren(subPlan);
                sourceFragmentIds.add(subPlan.getFragment().getId());
            }
            RemoteSourceNode remoteSourceNode = new RemoteSourceNode(
                union.getCluster(), union.getTraitSet(),
                sourceFragmentIds,
                union.getRowType(), estimateRowCount(union).intValue());
            remoteSourceNode.setRelatedId(union.getRelatedId());
            return remoteSourceNode;
        }

        private SubPlan generateFragment(
            RelNode parent, int i, PartitionShuffleHandle partitioningHandle, List<Integer> keys,
            List<OrderByOption> orderByOptions) {
            FragmentProperties currentProperties = new FragmentProperties();
            RelNode newParent = visitChild(parent, i, parent.getInput(i), currentProperties);
            RelNode root = newParent.getInput(i);
            PartitioningScheme partitioningScheme =
                new PartitioningScheme(keys, orderByOptions, partitioningHandle);
            RuntimeFilterIdCollector runtimeFilterIdCollector = new RuntimeFilterIdCollector();
            runtimeFilterIdCollector.collect(root, session);
            List<SerializeDataType> outputTypes = SerializeDataType.convertToSerilizeType(
                parent.getRowType().getFieldList());
            return generateSubPlan(root, currentProperties, outputTypes, partitioningScheme,
                runtimeFilterIdCollector.filterIds,
                runtimeFilterIdCollector.produceFilterIds);
        }

        private SubPlan generateSubPlan(
            RelNode root, FragmentProperties currentProperties, List<SerializeDataType> outputTypes,
            PartitioningScheme partitioningScheme, List<Integer> filterIds, List<Integer> produceFilterIds) {
            Pair<List<Integer>, List<Integer>> pairs = getPartitionSourceIds(currentProperties);

            SplitInfo splitInfo = null;
            if (currentProperties.getCurrentLogicalView() != null) {
                LogicalView logicalView = currentProperties.getCurrentLogicalView();
                if (logicalView.fromTableOperation() != null) {
                    splitInfo = new SplitManager().getSingleSplit(logicalView, session.getClientContext());
                } else {
                    splitInfo = new SplitManager().getSplits(
                        logicalView, session.getClientContext(), !lowConcurrencyQuery);
                }
                session.getGroups().putAll(splitInfo.getGroups());
            }

            PlanFragment planFragment = null;
            final int outerParallelism;
            if (currentProperties.getPartitionHandle().isSingleTon()) {
                outerParallelism = 1;
            } else if (splitInfo != null) {
                outerParallelism = calcScanParallelism(currentProperties.rootIo, splitInfo);
            } else {
                outerParallelism = calcParallelism(currentProperties);
            }

            List<SplitInfo> expandSplitInfos = new ArrayList<>();
            Integer bkaJoinParallelism = -1;
            if (currentProperties.getExpandView().size() > 0) {
                for (LogicalView logicalView : currentProperties.getExpandView()) {
                    SplitInfo info;
                    if (logicalView.fromTableOperation() != null) {
                        info = new SplitManager().getSingleSplit(logicalView, session.getClientContext());
                    } else {
                        info = new SplitManager().getSplits(
                            logicalView, session.getClientContext(), !lowConcurrencyQuery);
                    }

                    if (splitInfo != null) {
                        session.getGroups().putAll(splitInfo.getGroups());
                    }
                    session.getGroups().putAll(info.getGroups());
                    expandSplitInfos.add(info);

                    ParamManager paramManager = session.getClientContext().getParamManager();
                    int lookupJoinParallelismFactor =
                        paramManager.getInt(ConnectionParams.LOOKUP_JOIN_PARALLELISM_FACTOR);
                    bkaJoinParallelism = Math.max(outerParallelism * lookupJoinParallelismFactor, bkaJoinParallelism);
                    bkaJoinParallelism = Math.max(Math.min(bkaJoinParallelism,
                        ExecUtils
                            .getMppMaxParallelism(session.getClientContext().getParamManager(), !onlyUseReadInstance)),
                        ExecUtils.getMppMinParallelism(session.getClientContext().getParamManager()));
                }
            }

            planFragment = new PlanFragment(nextFragmentId++, root, outputTypes,
                currentProperties.getPartitionHandle().setPartitionCount(outerParallelism), pairs.getKey(),
                pairs.getValue(), partitioningScheme, bkaJoinParallelism, filterIds, produceFilterIds);

            int currentParallelism = currentProperties.getPartitionHandle().getPartitionCount();
            int singleChildParallelism = currentProperties.getSingleChildParallelism();
            int innerChildParallelism = currentProperties.getInnerChildParallelism();

            if (singleChildParallelism > 0) {
                currentConcurrentParallelism += currentParallelism;
                maxConcurrentParallelism =
                    Math.max(maxConcurrentParallelism, currentConcurrentParallelism);
                currentConcurrentParallelism = currentParallelism;
            } else if (innerChildParallelism > 0) {
                currentConcurrentParallelism += currentParallelism;
                maxConcurrentParallelism =
                    Math.max(maxConcurrentParallelism, currentConcurrentParallelism);
                currentConcurrentParallelism = currentConcurrentParallelism - innerChildParallelism;
            } else {
                currentConcurrentParallelism += currentParallelism;
            }
            return new SubPlan(planFragment, splitInfo, expandSplitInfos, currentProperties.getChildren());
        }

        public int calcParallelism(FragmentProperties properties) {

            if (lowConcurrencyQuery) {
                return 1;
            }

            ParamManager paramManager = session.getClientContext().getParamManager();

            int parallelism = -1;
            if (paramManager.getBoolean(ConnectionParams.MPP_PARALLELISM_AUTO_ENABLE)) {
                parallelism = (int) (properties.rootRowCnt / paramManager
                    .getInt(ConnectionParams.MPP_QUERY_ROWS_PER_PARTITION));
                parallelism =
                    Math.max(Math.min(parallelism, ExecUtils.getMppMaxParallelism(paramManager, !onlyUseReadInstance)),
                        ExecUtils.getMppMinParallelism(paramManager));
            } else {
                parallelism = paramManager.getInt(ConnectionParams.MPP_PARALLELISM);
                if (parallelism == 0) {
                    // Parallel query is disabled but we have a parallel plan... Very strange...
                    parallelism = 1;
                } else if (parallelism < 0) {
                    for (SubPlan subPlan : properties.getChildren()) {
                        parallelism = Math.max(
                            parallelism, subPlan.getFragment().getPartitioning().getPartitionCount());
                        parallelism = Math.max(
                            parallelism, subPlan.getFragment().getBkaJoinParallelism());
                    }
                    parallelism = Math.max(
                        Math.min(parallelism, ExecUtils.getMppMaxParallelism(paramManager, !onlyUseReadInstance)),
                        ExecUtils.getMppMinParallelism(paramManager));
                }
            }
            return parallelism;
        }

        public int calcScanParallelism(double io, SplitInfo splitInfo) {

            if (lowConcurrencyQuery) {
                return 1;
            }
            ParamManager paramManager = session.getClientContext().getParamManager();

            int parallelsim = -1;
            if (paramManager.getBoolean(ConnectionParams.MPP_PARALLELISM_AUTO_ENABLE)) {
                int maxParallelism = ExecUtils.getMppMaxParallelism(paramManager, !onlyUseReadInstance);
                int minParallelism = ExecUtils.getMppMinParallelism(paramManager);
                parallelsim = (int) (io / paramManager
                    .getInt(ConnectionParams.MPP_QUERY_IO_PER_PARTITION)) + 1;
                parallelsim = Math.max(Math.min(parallelsim, maxParallelism), minParallelism);
            } else {
                parallelsim = paramManager.getInt(ConnectionParams.MPP_PARALLELISM);
                if (parallelsim == 0) {
                    // Parallel query is disabled but we have a parallel plan... Very strange...
                    parallelsim = 1;
                } else if (parallelsim < 0) {
                    parallelsim = (int) (io / paramManager
                        .getInt(ConnectionParams.MPP_QUERY_IO_PER_PARTITION)) + 1;
                    parallelsim = Math.max(
                        Math.min(parallelsim, ExecUtils.getMppMaxParallelism(paramManager, !onlyUseReadInstance)),
                        ExecUtils.getMppMinParallelism(paramManager));
                }
            }
            int dbParallelism = ExecUtils.getPolarDbCores(paramManager, !onlyUseReadInstance);
            parallelsim = Math.max(Math.min(Math.min(
                splitInfo.getSplitCount(), dbParallelism * splitInfo.getInsCount()), parallelsim), 1);
            return parallelsim;
        }

        public boolean isMergeSortSourceNode(RelNode node) {
            if (node instanceof RemoteSourceNode
                && node.getTraitSet().getTrait(RelCollationTraitDef.INSTANCE).getFieldCollations().size() > 0) {
                return true;
            }
            return false;
        }

        public <T extends RelNode> Double estimateRowCount(T relNode) {
            double rowCount = -1;
            if (session.getClientContext().getParamManager().getBoolean(
                ConnectionParams.MPP_PARALLELISM_AUTO_ENABLE)) {
                RelMetadataQuery relMetadataQuery = relNode.getCluster().getMetadataQuery();
                synchronized (relMetadataQuery) {
                    rowCount = relMetadataQuery.getRowCount(relNode);
                }
                if (rowCount <= 0) {
                    log.warn("RowCount is less than 1, maybe lack of the statistics for " + relNode);
                }
            }
            return rowCount;
        }

        public Pair<List<Integer>, List<Integer>> getPartitionSourceIds(FragmentProperties properties) {
            List<Integer> partitionSources = new ArrayList<>();
            List<Integer> expandSources = new ArrayList<>();
            if (properties.getCurrentLogicalView() != null) {
                partitionSources.add(properties.getCurrentLogicalView().getRelatedId());
            }
            if (properties.getExpandView().size() > 0) {
                for (LogicalView logicalView : properties.getExpandView()) {
                    partitionSources.add(logicalView.getRelatedId());
                    expandSources.add(logicalView.getRelatedId());
                }
            }
            return new Pair<>(partitionSources, expandSources);
        }
    }

    private static class FragmentProperties {
        private final List<SubPlan> children = new ArrayList<>();
        private LogicalView currentLogicalView;
        private List<LogicalView> expandViews = new ArrayList<>();
        private double rootRowCnt;
        private double rootIo;
        //节点属性
        private Optional<PartitionHandle> partitionHandle = Optional.empty();

        private int innerChildParallelism = 0;

        private int singleChildParallelism = 0;

        public int getInnerChildParallelism() {
            return innerChildParallelism;
        }

        public void setInnerChildParallelism(int innerChildParallism) {
            this.innerChildParallelism = innerChildParallism;
        }

        public int getSingleChildParallelism() {
            return singleChildParallelism;
        }

        public void setSingleChildParallelism(int singleChildParallelism) {
            this.singleChildParallelism = singleChildParallelism;
        }

        public LogicalView getCurrentLogicalView() {
            return currentLogicalView;
        }

        public void setLogicalView(LogicalView logicalView) {
            Preconditions.checkState(this.currentLogicalView == null, "currentLogicalView is already exist!");
            this.currentLogicalView = logicalView;
        }

        public List<LogicalView> getExpandView() {
            return expandViews;
        }

        public void addExpandView(LogicalView expandView) {
            Preconditions.checkState(!this.expandViews.contains(expandView), "expandView is already exist!");
            this.expandViews.add(expandView);
        }

        public void updateRootCount(double rowCnt) {
            this.rootRowCnt = Math.max(rowCnt, rootRowCnt);
        }

        public void updateRootIo(double io) {
            this.rootIo = Math.max(io, rootIo);
        }

        public List<SubPlan> getChildren() {
            return children;
        }

        public PartitionHandle getPartitionHandle() {
            return partitionHandle.get();
        }

        public Optional<PartitionHandle> getOptionalHandle() {
            return partitionHandle;
        }

        public FragmentProperties addChildren(SubPlan children) {
            this.children.add(children);
            return this;
        }

        //------------------------------- node property -------------------------------
        public FragmentProperties setSourceNode() {
            if (partitionHandle.isPresent()) {
                if (partitionHandle.get().equals(PartitionHandle.SINGLETON)) {
                    //当source和singleton冲突的时候，以singleton为准
                    partitionHandle =
                        Optional.of(SINGLETON_SOURCE);
                    return this;
                }
            }
            //否则设置为SOURCE
            partitionHandle =
                Optional.of(new PartitionHandle(PartitionHandle.PartitionMode.SOURCE));
            return this;
        }

        public FragmentProperties setSingleTonNode() {

            if (partitionHandle.isPresent() && (partitionHandle.get().getPartitionMode().equals(
                PartitionHandle.PartitionMode.SOURCE) || partitionHandle.get().isSingletonSourceNode())) {
                //当singleton和source冲突时，设置为FIXED_SOURCE
                partitionHandle = Optional.of(SINGLETON_SOURCE);
            } else {
                partitionHandle = Optional.of(PartitionHandle.SINGLETON);
            }
            return this;
        }

        public FragmentProperties setNode(PartitionHandle handle) {

            if (partitionHandle.isPresent()) {
                if (partitionHandle.get().equals(PartitionHandle.SINGLETON)) {
                    if (handle.getPartitionMode().equals(PartitionHandle.PartitionMode.SOURCE) || handle
                        .isSingletonSourceNode()) {
                        log.warn(String.format("Cannot overwrite partitioning with %s (currently set to %s)",
                            partitionHandle, handle));
                        //冲突后，设置为FIXED_SOURCE
                        partitionHandle =
                            Optional.of(SINGLETON_SOURCE);
                    } else {
                        //以SINGLETON为准
                    }
                } else if (partitionHandle.get().getPartitionMode().equals(PartitionHandle.PartitionMode.SOURCE) ||
                    partitionHandle.get().isSingletonSourceNode()) {
                    if (handle.equals(PartitionHandle.SINGLETON)) {
                        log.warn(String.format("Cannot overwrite partitioning with %s (currently set to %s)",
                            partitionHandle, handle));
                        //冲突后，设置为FIXED_SOURCE
                        partitionHandle =
                            Optional.of(SINGLETON_SOURCE);
                    } else {
                        //以SOURCE为准
                    }
                } else if (partitionHandle.get().isDefaultNode()) {
                    //后来居上
                    partitionHandle = Optional.of(handle);
                }
                return this;
            } else {
                partitionHandle = Optional.of(handle);
                return this;
            }
        }
    }

    private static class RuntimeFilterIdCollector {
        private final List<Integer> filterIds = new ArrayList<>();
        private final List<Integer> produceFilterIds = new ArrayList<>();

        public void collect(RelNode rel, Session session) {
            if (!session.getClientContext().getParamManager().getBoolean(ConnectionParams.ENABLE_RUNTIME_FILTER)) {
                return;
            }
            visit(rel);
            verify();
        }

        private void verify() {
            for (int id : filterIds) {
                if (produceFilterIds.contains(id)) {
                    throw new IllegalStateException(
                        "Runtime filter producer and consumer: " + id + " should not exists in same stage!");
                }
            }
        }

        private void visit(RelNode rel) {
            if (rel instanceof LogicalView) {
                List<Integer> bloomFilterIds = ((LogicalView) rel).getBloomFilters();
                filterIds.addAll(bloomFilterIds);
            } else if (rel instanceof Filter) {
                List<RexNode> conditions = RelOptUtil.conjunctions(((Filter) rel).getCondition());
                for (RexNode rexNode : conditions) {
                    if (rexNode instanceof RexCall
                        && ((RexCall) rexNode).getOperator() instanceof SqlRuntimeFilterFunction) {
                        filterIds.add(((SqlRuntimeFilterFunction) ((RexCall) rexNode).getOperator()).getId());
                    }
                }
                visit(rel.getInput(0));
            } else if (rel instanceof RuntimeFilterBuilder) {
                List<RexNode> conditions = RelOptUtil.conjunctions(((RuntimeFilterBuilder) rel).getCondition());
                for (RexNode rexNode : conditions) {
                    if (rexNode instanceof RexCall
                        && ((RexCall) rexNode).getOperator() instanceof SqlRuntimeFilterBuildFunction) {
                        produceFilterIds
                            .addAll(((SqlRuntimeFilterBuildFunction) ((RexCall) rexNode).getOperator())
                                .getRuntimeFilterIds());
                    }
                }
                visit(rel.getInput(0));
            } else if (rel instanceof Join) {
                if (rel instanceof BKAJoin || rel instanceof SemiBKAJoin) {
                    // Schedule outer source firstly, then inner source
                    RelNode outer = ((Join) rel).getOuter();
                    visit(outer);
                } else {
                    // Schedule Inner source firstly, then Outer source
                    RelNode inner = ((Join) rel).getInner();
                    RelNode outer = ((Join) rel).getOuter();
                    visit(inner);
                    visit(outer);
                }
            } else {
                for (RelNode input : rel.getInputs()) {
                    visit(input);
                }
            }
        }
    }
}
