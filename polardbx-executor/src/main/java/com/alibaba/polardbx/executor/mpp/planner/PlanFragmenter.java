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

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.mpp.Session;
import com.alibaba.polardbx.executor.mpp.deploy.ServiceProvider;
import com.alibaba.polardbx.executor.mpp.operator.LocalExecutionPlanner;
import com.alibaba.polardbx.executor.mpp.operator.RangeScanMode;
import com.alibaba.polardbx.executor.mpp.split.OssSplit;
import com.alibaba.polardbx.executor.mpp.split.SplitInfo;
import com.alibaba.polardbx.executor.mpp.split.SplitManager;
import com.alibaba.polardbx.executor.mpp.split.SplitManagerImpl;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.executor.utils.OrderByOption;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.gms.node.MppScope;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.core.rel.BKAJoin;
import com.alibaba.polardbx.optimizer.core.rel.BaseTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.Limit;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.MaterializedSemiJoin;
import com.alibaba.polardbx.optimizer.core.rel.MergeSort;
import com.alibaba.polardbx.optimizer.core.rel.OSSTableScan;
import com.alibaba.polardbx.optimizer.core.rel.SemiBKAJoin;
import com.alibaba.polardbx.optimizer.core.rel.mpp.ColumnarExchange;
import com.alibaba.polardbx.optimizer.core.rel.mpp.MppExchange;
import com.alibaba.polardbx.optimizer.workload.WorkloadUtil;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
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
import org.apache.calcite.rel.core.JoinRelType;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.executor.mpp.operator.LocalExecutionPlanner.SUPPORT_ALL_CACHE_NODES;
import static com.alibaba.polardbx.executor.mpp.operator.LocalExecutionPlanner.SUPPORT_ONE_SIDE_CACHE_NODES;
import static com.alibaba.polardbx.executor.mpp.planner.PartitionHandle.SINGLETON_SOURCE;
import static com.alibaba.polardbx.executor.utils.ExecUtils.convertBuildSide;

public class PlanFragmenter {

    private static final Logger log = LoggerFactory.getLogger(PlanFragmenter.class);

    public static boolean needExchange(RelNode node) {
        if (!(node instanceof Exchange)
            && node.getTraitSet().getTrait(RelCollationTraitDef.INSTANCE).getFieldCollations().size() > 0) {
            return true;
        }
        return false;
    }

    public static Pair<SubPlan, Integer> buildRootFragment(RelNode root, Session session) {
        FragmentProperties properties = new FragmentProperties();
        if (needExchange(root)) {
            RelTraitSet relTraits = root.getTraitSet();
            RelCollation toCollation = relTraits.getTrait(RelCollationTraitDef.INSTANCE);
            root = MppExchange.create(root, toCollation, RelDistributions.SINGLETON);
        }
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
        private boolean lowConcurrencyQuery = false;

        private SplitManager splitManager;
        private boolean isColumnar = false;

        public Fragmenter(Session session, RelNode root) {
            this.session = session;
            if (!WorkloadUtil.isApWorkload(session.getClientContext().getWorkloadType())) {
                lowConcurrencyQuery = true;
            } else {
                session.getClientContext().getExtraCmds().put(ConnectionProperties.MERGE_UNION, false);
            }
            this.splitManager = new SplitManagerImpl();
            this.isColumnar = CBOUtil.isColumnarOptimizer(root);

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
                // TODO consider this when partition wise support bka join
                properties.setPruneExchangePartition(false);
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
                if ((join.getJoinType() != JoinRelType.INNER) && (join.getJoinType() != JoinRelType.SEMI)) {
                    properties.setPruneExchangePartition(false);
                }
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
            } else if (other instanceof MergeSort && !(parent instanceof MaterializedSemiJoin)) {
                return visitMergeSort(parent, (MergeSort) other, parentProperties);
            } else if (other instanceof LogicalUnion) {
                return visit(parent, (LogicalUnion) other, parentProperties);
            } else if (other instanceof LogicalView) {
                parentProperties.setSourceNode();
                parentProperties.updateRootCount(ExecUtils.calcRowCount((LogicalView) other));
                parentProperties.updateRootIo(ExecUtils.calcIo((LogicalView) other));
                parentProperties.addLogicalView((LogicalView) other);
                if (other instanceof OSSTableScan) {
                    parentProperties.setSimpleOssScan(parent instanceof ColumnarExchange || parent == null);
                    parentProperties.updatePairWiseInfo((OSSTableScan) other, session.getClientContext().
                        getParamManager().getBoolean(ConnectionParams.ENABLE_LOCAL_PARTITION_WISE_JOIN));
                    // handle case like this:
                    //     JOIN
                    //    |    |
                    //    Ex   T1
                    // when we reached exchange, we can not know this Exchange is under pairwise
                    // but when we reached table t1, we can go back to set all the children
                    if (parentProperties.isRemotePairWise()) {
                        parentProperties.getChildren().forEach(subPlan -> subPlan.getFragment().
                            getPartitioningScheme().getShuffleHandle().setRemotePairWise(true));
                    }
                }
                return other;
            } else if (other instanceof LogicalValues || other instanceof DynamicValues) {
                parentProperties.setSingleTonNode();
                return other;
            } else if (other instanceof BaseTableOperation) {
                LogicalView logicalView = ExecUtils.convertToLogicalView((BaseTableOperation) other,
                    session.getClientContext());
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
                remoteSourceNode = visitUnion(union, shuffleHandle, parentProperties, ImmutableList.of(), orderBys);
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
                exchange instanceof MppExchange ? ((MppExchange) exchange).isMergeSortExchange() :
                    (exchange instanceof ColumnarExchange && ((ColumnarExchange) exchange).isMergeSortExchange());
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
                shuffleHandle.setFullPartCount(exchange.getDistribution().getShardCnt());
            }
            List<OrderByOption> orderBys = new ArrayList<>();
            if (mergeSort) {
                RelCollation ret = exchange instanceof MppExchange ? ((MppExchange) exchange).getCollation() :
                    ((ColumnarExchange) exchange).getCollation();
                List<RelFieldCollation> sortList = ret.getFieldCollations();
                orderBys = ExecUtils.convertFrom(sortList);
            }

            // handle case like this:
            //     JOIN
            //    |    |
            //    T1   Ex
            // when we reached exchange, we have known this Exchange is under pairwise
            if (parentProperties.isRemotePairWise()) {
                shuffleHandle.setRemotePairWise(true);
            }

            // TODO yuehan check union under partition wise join
            if (exchange.getInput() instanceof LogicalUnion) {
                LogicalUnion union = (LogicalUnion) exchange.getInput();
                RelNode remoteSourceNode = visitUnion(
                    union, shuffleHandle, parentProperties, exchange.distribution.getKeys(), orderBys);
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
            return visitUnion(union, shuffleHandle, parentProperties, ImmutableList.of(), ImmutableList.of());
        }

        private RelNode visitUnion(
            LogicalUnion union, PartitionShuffleHandle shuffleHandle, FragmentProperties parentProperties,
            List<Integer> keys, List<OrderByOption> orderByOptions) {
            List<Integer> sourceFragmentIds = new ArrayList<>();
            for (int i = 0; i < union.getInputs().size(); i++) {
                SubPlan subPlan = generateFragment(union, i, shuffleHandle, keys, orderByOptions);
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
            Set<Integer> commonIds =
                runtimeFilterIdCollector.filterIds.stream().filter(runtimeFilterIdCollector.produceFilterIds::contains)
                    .collect(Collectors.toSet());
            SubPlan subPlan = generateSubPlan(root, currentProperties, outputTypes, partitioningScheme,
                runtimeFilterIdCollector.filterIds,
                runtimeFilterIdCollector.produceFilterIds);
            if (InstConfUtil.getBool(ConnectionParams.ENABLE_LOCAL_RUNTIME_FILTER) && commonIds.equals(
                new HashSet<>(runtimeFilterIdCollector.filterIds)) &&
                commonIds.equals(new HashSet<>(runtimeFilterIdCollector.produceFilterIds))) {
                subPlan.getFragment().setLocalBloomFilter(true);
            }
            return subPlan;
        }

        private SubPlan generateSubPlan(
            RelNode root, FragmentProperties currentProperties, List<SerializeDataType> outputTypes,
            PartitioningScheme partitioningScheme, List<Integer> filterIds, List<Integer> produceFilterIds) {
            Pair<List<Integer>, List<Integer>> pairs = getPartitionSourceIds(currentProperties);

            // collect the logical table name and its split count.
            Map<String, Integer> splitCountMap = new HashMap<>();

            List<SplitInfo> splitInfos = new ArrayList<>();
            if (currentProperties.getLogicalViews().size() > 0 && !session.isIgnoreSplitInfo()) {
                for (LogicalView logicalView : currentProperties.getLogicalViews()) {
                    SplitInfo splitInfo = null;
                    if (logicalView.fromTableOperation() != null) {
                        splitInfo = splitManager.getSingleSplit(logicalView, session.getClientContext());
                    } else {
                        RangeScanMode rangeScanMode = null;
                        ExecutionContext context = session.getClientContext();
                        if (logicalView.pushedRelNodeIsSort()) {
                            rangeScanMode = RangeScanUtils.useRangeScan(logicalView, context);
                        }

                        int mergeUnionSize = context.getParamManager().getInt(ConnectionParams.MERGE_UNION_SIZE);

                        if (rangeScanMode != null) {
                            context.putIntoHintCmds(ConnectionProperties.MERGE_UNION_SIZE, 1);
                        }

                        splitInfo = splitManager.getSplits(logicalView, context, !lowConcurrencyQuery);

                        if (rangeScanMode != null) {
                            // reset merge union size
                            context.putIntoHintCmds(ConnectionProperties.MERGE_UNION_SIZE, mergeUnionSize);
                        }
                    }
                    splitCountMap.put(logicalView.getLogicalTableName(), splitInfo.getSplitCount());
                    session.getGroups().putAll(splitInfo.getGroups());
                    splitInfos.add(splitInfo);
                }
            }

            PlanFragment planFragment = null;
            int outerParallelism = getOuterParallelism(splitInfos, currentProperties);

            List<SplitInfo> expandSplitInfos = new ArrayList<>();
            Integer bkaJoinParallelism = -1;
            if (currentProperties.getExpandView().size() > 0) {
                for (LogicalView logicalView : currentProperties.getExpandView()) {
                    SplitInfo info;
                    if (logicalView.fromTableOperation() != null) {
                        info = splitManager.getSingleSplit(logicalView, session.getClientContext());
                    } else {
                        info = splitManager.getSplits(
                            logicalView, session.getClientContext(), !lowConcurrencyQuery);
                    }
                    splitCountMap.put(logicalView.getLogicalTableName(), info.getSplitCount());

                    session.getGroups().putAll(info.getGroups());
                    expandSplitInfos.add(info);

                    ParamManager paramManager = session.getClientContext().getParamManager();
                    int lookupJoinParallelismFactor =
                        paramManager.getInt(ConnectionParams.LOOKUP_JOIN_PARALLELISM_FACTOR);
                    bkaJoinParallelism = Math.max(outerParallelism * lookupJoinParallelismFactor, bkaJoinParallelism);
                    bkaJoinParallelism = Math.max(Math.min(bkaJoinParallelism,
                            ExecUtils
                                .getMppMaxParallelism(session.getClientContext().getParamManager(), isColumnar)),
                        ExecUtils.getMppMinParallelism(session.getClientContext().getParamManager()));
                }
            }

            planFragment = new PlanFragment(nextFragmentId++, root, outputTypes,
                currentProperties.getPartitionHandle().setPartitionCount(outerParallelism),
                currentProperties.isRemotePairWise(), pairs.getKey(),
                pairs.getValue(), partitioningScheme, bkaJoinParallelism, filterIds, produceFilterIds,
                currentProperties.isLocalPairWise(), splitCountMap, currentProperties.isPruneExchangePartition());

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
            return new SubPlan(planFragment, splitInfos, expandSplitInfos, currentProperties.getChildren());
        }

        private int getOuterParallelism(List<SplitInfo> splitInfos, FragmentProperties currentProperties) {
            if (currentProperties.getPartitionHandle().isSingleTon()) {
                return 1;
            }

            // fragment not contain scan
            if (splitInfos.size() == 0) {
                return calcParallelism(currentProperties);
            }

            boolean containsOss = splitInfos.stream().anyMatch(this::containsOssSplit);
            // for innodb
            if (!containsOss) {
                int outerParallelism = 1;
                for (SplitInfo splitInfo : splitInfos) {
                    outerParallelism =
                        Math.max(outerParallelism, calcScanParallelism(currentProperties.rootIo, splitInfo));
                }
                return outerParallelism;
            }

            // coroner case: all tables in this fragment is empty
            boolean allEmpty = splitInfos.stream().allMatch(splitInfo -> splitInfo.getSplits().isEmpty());
            if (allEmpty) {
                int parallelism =
                    session.getClientContext().getParamManager().getInt(ConnectionParams.PARALLELISM_FOR_EMPTY_TABLE);
                if (parallelism > 0) {
                    return parallelism;
                }
            }

            int outerParallelism = 1;
            for (SplitInfo splitInfo : splitInfos) {
                outerParallelism = Math.max(outerParallelism,
                    calcColumnarScanParallelism(currentProperties.isSimpleOssScan(), splitInfo));
            }
            return outerParallelism;
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
                    Math.max(Math.min(parallelism, ExecUtils.getMppMaxParallelism(paramManager, isColumnar)),
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
                        Math.min(parallelism, ExecUtils.getMppMaxParallelism(paramManager, isColumnar)),
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
                int maxParallelism = ExecUtils.getMppMaxParallelism(paramManager, isColumnar);
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
                        Math.min(parallelsim, ExecUtils.getMppMaxParallelism(paramManager, isColumnar)),
                        ExecUtils.getMppMinParallelism(paramManager));
                }
            }

            int dbParallelism = ExecUtils.getPolarDbCores(paramManager);
            parallelsim = Math.max(Math.min(Math.min(
                splitInfo.getSplitCount(), dbParallelism * splitInfo.getInsCount()), parallelsim), 1);

            return parallelsim;
        }

        private boolean containsOssSplit(SplitInfo splitInfo) {
            return splitInfo != null && splitInfo.getSplits().stream()
                .anyMatch(splits -> splits.stream().anyMatch(split -> split.getConnectorSplit() instanceof OssSplit));
        }

        private int calcColumnarScanParallelism(boolean simpleOssScan, SplitInfo splitInfo) {
            if (lowConcurrencyQuery) {
                return 1;
            }

            ParamManager paramManager = session.getClientContext().getParamManager();

            int parallelism = paramManager.getInt(ConnectionParams.MPP_PARALLELISM);

            if (parallelism < 0) {
                MppScope mppScope = ExecUtils.getMppSchedulerScope(!isColumnar);
                int mppNodeSize = paramManager.getInt(ConnectionParams.MPP_NODE_SIZE);
                if (mppNodeSize <= 0) {
                    mppNodeSize = ServiceProvider.getInstance().getServer()
                        .getNodeManager().getAllNodes().getAllWorkers(mppScope).size();
                }
                // default parallelism is cores of all compute node
                parallelism = mppNodeSize * ExecUtils.getPolarDBXCNCores(paramManager, mppScope);
            }

            if (simpleOssScan && splitInfo.getSplitCount() > 0) {
                parallelism = Math.min(parallelism, splitInfo.getSplitCount());
            }
            return parallelism;
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
            for (LogicalView logicalView : properties.getLogicalViews()) {
                partitionSources.add(logicalView.getRelatedId());
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
        private List<LogicalView> logicalViews = new ArrayList<>();
        private List<LogicalView> expandViews = new ArrayList<>();
        private double rootRowCnt;
        private double rootIo;
        //节点属性
        private Optional<PartitionHandle> partitionHandle = Optional.empty();

        private int innerChildParallelism = 0;

        private int singleChildParallelism = 0;

        private boolean localPairWise = false;

        private boolean remotePairWise = false;

        private boolean simpleOssScan = false;

        private boolean pruneExchangePartition = true;

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

        public boolean isSimpleOssScan() {
            return simpleOssScan;
        }

        public void setSimpleOssScan(boolean simpleOssScan) {
            this.simpleOssScan = simpleOssScan;
        }

        public boolean isPruneExchangePartition() {
            return pruneExchangePartition;
        }

        public void setPruneExchangePartition(boolean pruneExchangePartition) {
            this.pruneExchangePartition = pruneExchangePartition;
        }

        public List<LogicalView> getLogicalViews() {
            return logicalViews;
        }

        public void addLogicalView(LogicalView logicalView) {
            this.logicalViews.add(logicalView);
        }

        public void updatePairWiseInfo(OSSTableScan ossTableScan, boolean enableLocalPairWise) {
            if (enableLocalPairWise) {
                this.localPairWise = localPairWise | ossTableScan.getTraitSet().getPartitionWise().isLocalPartition();
            }
            this.remotePairWise = remotePairWise | ossTableScan.getTraitSet().getPartitionWise().isRemotePartition();
        }

        public boolean isLocalPairWise() {
            return localPairWise;
        }

        public boolean isRemotePairWise() {
            return remotePairWise;
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
            if (session.getClientContext().getParamManager()
                .getBoolean(ConnectionParams.CHECK_RUNTIME_FILTER_SAME_FRAGMENT)) {
                verify();
            }
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
