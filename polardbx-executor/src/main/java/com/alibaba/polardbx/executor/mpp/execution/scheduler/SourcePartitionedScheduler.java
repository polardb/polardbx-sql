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

package com.alibaba.polardbx.executor.mpp.execution.scheduler;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.mpp.execution.RemoteTask;
import com.alibaba.polardbx.executor.mpp.execution.SqlStageExecution;
import com.alibaba.polardbx.executor.mpp.metadata.Split;
import com.alibaba.polardbx.executor.mpp.split.OssSplit;
import com.alibaba.polardbx.executor.mpp.split.SplitInfo;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.gms.node.Node;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.utils.QueryConcurrencyPolicy;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class SourcePartitionedScheduler implements StageScheduler {

    private static final Logger log = LoggerFactory.getLogger(SourcePartitionedScheduler.class);

    private final SqlStageExecution stage;

    private Map<Node, Multimap<Integer, Split>> splitAssignments;

    private final int driverParallelism;

    private final int requireChildOutput;

    private HashSet<Integer> prunePartitions = new HashSet<>();

    public SourcePartitionedScheduler(
        SqlStageExecution stageExecution, SplitPlacementPolicy splitPlacementPolicy,
        List<SplitInfo> splitInfos, List<SplitInfo> expandSplitInfos, ExecutionContext context,
        List<Node> nodes) {

        int columnarMaxShard = context.getColumnarMaxShard();
        // if we have 4 partition, while we have 6 nodes, we can only schedule to 4 nodes, so child should output four partition
        // we should avoid this situation, for the reset two nodes will idle
        this.requireChildOutput =
            columnarMaxShard > 0 ? Math.min(columnarMaxShard, nodes.size()) : nodes.size();

        this.stage = stageExecution;
        this.splitAssignments = new HashMap<>();

        // example of partition pruning, fragment as following:
        //            Join
        //        |         |
        //      Join       Join
        //    |      |   |      |
        //   t1     ex   t2    t3
        // splitInfos is {t1:1, t2:1,3, t3:0,1}
        // partitions of t1, t2 and t3 should be equal, and suggest partitions is 6
        // then pruning result is {0,2,3,4,5}
        for (SplitInfo splitInfo : splitInfos) {
            List<Split> splits = Iterables.getOnlyElement(splitInfo.getSplits());

            if (stage.getFragment().isRemotePairWise() || stage.getFragment().isLocalPairWise()) {

                if (!splits.isEmpty()) {

                    int partitions = getTablePartitions((OssSplit) splits.get(0).getConnectorSplit());

                    // if splits not contains all partition, then we should not use local partition wise join
                    // and optimize this situation has little meaning
                    if (stage.getFragment().isLocalPairWise()) {
                        updateLocalPairWiseInfo(partitions, splits, stageExecution);
                        // just reset all partitions here
                        // after schedule, we will set real local partition for local pair wise
                        stage.getFragment().setLocalPartitionCount(partitions);

                        // when pipeline is in local partition mode, record it's total partition count across all nodes.
                        stage.getFragment().setTotalPartitionCount(partitions);
                    }

                    // intersect visited partitions
                    if (context.getParamManager().getBoolean(ConnectionParams.ENABLE_PRUNE_EXCHANGE_PARTITION)
                        && stage.getFragment().isRemotePairWise() && stage.getFragment().isPruneExchangePartition()) {
                        updatePrunePartitionInfo(partitions, splits);
                    }

                } else {
                    // if split is empty, not use local partition wise
                    stage.getFragment().setLocalPairWise(Boolean.FALSE);
                }
            }

            // Notice: after assigned, part index of oss split will be under node scope rather than global
            Multimap<Node, Split> splitAssignment = splitPlacementPolicy.computeAssignments(splits);

            // e.g. for outer join under partition wise
            if (stage.getFragment().isRemotePairWise() && !stage.getFragment().isPruneExchangePartition()) {
                List<Node> shouldScheduleNodes = nodes.subList(0, requireChildOutput);
                for (Node node : shouldScheduleNodes) {
                    if (!splitAssignment.containsKey(node)) {
                        splitAssignment.put(node, Split.EMPTY_SPLIT);
                    }
                }
            }

            for (Map.Entry<Node, Split> entry : splitAssignment.entries()) {
                if (!splitAssignments.containsKey(entry.getKey())) {
                    Multimap<Integer, Split> multimap = HashMultimap.create();
                    splitAssignments.put(entry.getKey(), multimap);
                }
                splitAssignments.get(entry.getKey()).put(splitInfo.getSourceId(), entry.getValue());
            }
        }

        if (expandSplitInfos != null && expandSplitInfos.size() > 0) {
            if (stage.getFragment().isLocalPairWise()) {
                throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                    "partition wise join should not support bka yet");
            }
            for (SplitInfo info : expandSplitInfos) {
                List<Split> expandSplits = Iterables.getOnlyElement(info.getSplits());
                for (Node node : splitAssignments.keySet()) {
                    splitAssignments.get(node).putAll(info.getSourceId(), expandSplits);
                }
            }
        }

        int totalParallelism = stageExecution.getFragment().getPartitioning().getPartitionCount();
        int taskNum = splitAssignments.keySet().size();

        // multi splits should only occurs in partition wise join, and should not have sort trait under this situation
        if (splitInfos.size() == 1 && splitInfos.get(0).isUnderSort()) {
            this.driverParallelism = 1;
        } else {
            this.driverParallelism = totalParallelism % taskNum > 0 ? totalParallelism / taskNum + 1 :
                totalParallelism / taskNum;
        }

        int totalPrefetch = 0;
        if (splitInfos.size() > 0 && splitInfos.stream()
            .allMatch(splitInfo -> splitInfo.getConcurrencyPolicy() == QueryConcurrencyPolicy.SEQUENTIAL)) {
            totalPrefetch = splitInfos.size();
        } else {
            totalPrefetch = context.getParamManager().getInt(ConnectionParams.PREFETCH_SHARDS);
            if (totalPrefetch < 0) {
                for (SplitInfo splitInfo : splitInfos) {
                    totalPrefetch = Math.max(
                        totalPrefetch, ExecUtils.getMppPrefetchNumForLogicalView(splitInfo.getSplitParallelism()));
                }
                if (expandSplitInfos != null && expandSplitInfos.size() > 0) {
                    for (SplitInfo info : expandSplitInfos) {
                        totalPrefetch = Math.max(
                            totalPrefetch, ExecUtils.getMppPrefetchNumForLogicalView(info.getSplitParallelism()));
                    }
                }
            }
        }

        totalPrefetch = totalPrefetch > totalParallelism ? totalPrefetch : totalParallelism;

        int prefetch = totalPrefetch % taskNum > 0 ? totalPrefetch / taskNum + 1 :
            totalPrefetch / taskNum;

        for (SplitInfo splitInfo : splitInfos) {
            log.debug(
                "sourceId: " + splitInfo.getSourceId() + "splits num: " + splitInfo.getSplitCount() + ", prefetch: "
                    + prefetch);
        }

        int bkaJoinParallelism = stage.getFragment().getBkaJoinParallelism() % taskNum > 0 ?
            stage.getFragment().getBkaJoinParallelism() / taskNum + 1 :
            stage.getFragment().getBkaJoinParallelism() / taskNum;

        stageExecution.getFragment().setPrefetch(prefetch);
        stageExecution.getFragment().setBkaJoinParallelism(bkaJoinParallelism);

        int realPartitionCount = updateLocalPartitionCount(taskNum, context);

        updateOssInfoAfterSchedule(splitInfos, realPartitionCount);
    }

    private int getTablePartitions(OssSplit split) {
        String logicalSchema = split.getLogicalSchema();
        String logicalTable = split.getLogicalTableName();
        int allPartition = OptimizerContext.getContext(logicalSchema).getPartitionInfoManager()
            .getPartitionInfo(logicalTable).getPartitionBy().getPartitions().size();
        return allPartition;
    }

    private void updateLocalPairWiseInfo(int allPartition, List<Split> splits, SqlStageExecution stageExecution) {
        stageExecution.getFragment()
            .setLocalPairWise(
                stageExecution.getFragment().isLocalPairWise() & ossSplitContainsAllPart(splits, allPartition));
    }

    private boolean ossSplitContainsAllPart(List<Split> splits, int allPartitions) {
        Set<Integer> realPartition = splits.stream().map(split -> ((OssSplit) split.getConnectorSplit()).getPartIndex())
            .collect(Collectors.toSet());
        return realPartition.size() == allPartitions;
    }

    private void updatePrunePartitionInfo(int allPartition, List<Split> splits) {
        Set<Integer> realPartition =
            splits.stream().map(split -> ((OssSplit) split.getConnectorSplit()).getPartIndex())
                .collect(Collectors.toSet());

        Set<Integer> notVisitPartition =
            IntStream.range(0, allPartition).filter(i -> !realPartition.contains(i)).boxed()
                .collect(Collectors.toSet());
        prunePartitions.addAll(notVisitPartition);
    }

    /**
     * update local partition count for local partition wise mode
     * return -1 if update failed, and will not use local pairwise
     */
    private int updateLocalPartitionCount(int taskNum, ExecutionContext context) {
        if (stage.getFragment().isLocalPairWise()) {
            int partitions = stage.getFragment().getLocalPartitionCount();
            if (partitions > 0) {
                // when pipeline is in local partition mode, record it's total partition count across all nodes.
                stage.getFragment().setTotalPartitionCount(partitions);

                int localPartitionCount = partitions % taskNum == 0 ? partitions / taskNum : partitions / taskNum + 1;
                stage.getFragment().setLocalPartitionCount(localPartitionCount);
                return localPartitionCount;
            } else {
                // wired, should not access here
                log.warn(String.format("unable to use local partition wise, trace id is %s, should check this",
                    context.getTraceId()));
                stage.getFragment().setLocalPartitionCount(-1);
                stage.getFragment().setLocalPairWise(Boolean.FALSE);
            }
        } else {
            // set local partition count to default, although we will not use this value under non local pairwise mode
            stage.getFragment().setLocalPartitionCount(-1);
        }
        return -1;
    }

    private void updateOssInfoAfterSchedule(List<SplitInfo> splitInfos, int realPartitionCount) {
        if (realPartitionCount > 0) {
            for (SplitInfo splitInfo : splitInfos) {
                List<Split> splits = Iterables.getOnlyElement(splitInfo.getSplits());
                for (Split split : splits) {
                    ((OssSplit) split.getConnectorSplit()).setNodePartCount(realPartitionCount);
                }
            }
        } else {
            // disable local partition wise on all oss splits
            for (SplitInfo splitInfo : splitInfos) {
                List<Split> splits = Iterables.getOnlyElement(splitInfo.getSplits());
                for (Split split : splits) {
                    if (split.getConnectorSplit() instanceof OssSplit) {
                        ((OssSplit) split.getConnectorSplit()).setLocalPairWise(Boolean.FALSE);
                    }
                }
            }
        }
    }

    @Override
    public ScheduleResult schedule() {

        List<RemoteTask> newTasks = new ArrayList<>();
        for (Map.Entry<Node, Multimap<Integer, Split>> taskSplits : splitAssignments.entrySet()) {
            newTasks.addAll(stage.scheduleSplits(
                taskSplits.getKey(), taskSplits.getValue(), true));
        }
        return new ScheduleResult(true, newTasks, 0);
    }

    @Override
    public int getTaskNum() {
        return splitAssignments.keySet().size();
    }

    @Override
    public int requireChildOutputNum() {
        return stage.getFragment().isRemotePairWise() ? requireChildOutput : getTaskNum();
    }

    @Override
    public int getDriverParallelism() {
        return driverParallelism;
    }

    @Override
    public List<Integer> getPrunePartitions() {
        return new ArrayList<>(prunePartitions);
    }
}
