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

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.executor.mpp.execution.RemoteTask;
import com.alibaba.polardbx.executor.mpp.execution.SqlStageExecution;
import com.alibaba.polardbx.executor.mpp.metadata.Split;
import com.alibaba.polardbx.executor.mpp.split.SplitInfo;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.gms.node.Node;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.utils.QueryConcurrencyPolicy;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class FixedExpandSourceScheduler implements StageScheduler {

    private final SqlStageExecution stage;
    private int driverParallelism;
    private Map<Node, Multimap<Integer, Split>> splitAssignments;

    public FixedExpandSourceScheduler(
        SqlStageExecution stage, Map<Integer, Node> partitionToNode, List<SplitInfo> expandInfos,
        ExecutionContext context) {
        requireNonNull(stage, "stage is null");
        this.stage = stage;
        int totalParallelism = stage.getFragment().getPartitioning().getPartitionCount();

        this.splitAssignments = new HashMap<>();

        for (Node node : partitionToNode.values()) {
            splitAssignments.put(node, HashMultimap.create());
        }

        for (SplitInfo info : expandInfos) {
            //每个Node 拥有所有的splits
            List<Split> splits = Iterables.getOnlyElement(info.getSplits());
            for (Node node : partitionToNode.values()) {
                splitAssignments.get(node).putAll(info.getSourceId(), splits);
            }
        }

        int taskNum = partitionToNode.size();

        this.driverParallelism = totalParallelism % taskNum > 0 ? totalParallelism / taskNum + 1 :
            totalParallelism / taskNum;

        int totalPrefetch = context.getParamManager().getInt(ConnectionParams.PREFETCH_SHARDS);
        if (totalPrefetch < 0) {
            for (SplitInfo info : expandInfos) {
                if (info.getConcurrencyPolicy() == QueryConcurrencyPolicy.SEQUENTIAL) {
                    totalPrefetch = 1;
                    break;
                }
                totalPrefetch = Math.max(
                    totalPrefetch, ExecUtils.getMppPrefetchNumForLogicalView(info.getSplitParallelism()));
            }
        }
        totalPrefetch = totalPrefetch > totalParallelism ? totalPrefetch : totalParallelism;

        int prefetch = totalPrefetch % taskNum > 0 ? totalPrefetch / taskNum + 1 :
            totalPrefetch / taskNum;

        int bkaJoinParallelism = stage.getFragment().getBkaJoinParallelism() % taskNum > 0 ?
            stage.getFragment().getBkaJoinParallelism() / taskNum + 1 :
            stage.getFragment().getBkaJoinParallelism() / taskNum;

        stage.getFragment().setPrefetch(prefetch);
        stage.getFragment().setBkaJoinParallelism(bkaJoinParallelism);
    }

    @Override
    public ScheduleResult schedule() {

        List<RemoteTask> newTasks = new ArrayList<>();
        for (Map.Entry<Node, Multimap<Integer, Split>> taskSplits : splitAssignments.entrySet()) {
            newTasks.addAll(stage.scheduleSplits(taskSplits.getKey(), taskSplits.getValue(), true));
        }
        return new ScheduleResult(true, newTasks, 0);
    }

    @Override
    public int getTaskNum() {
        return splitAssignments.size();
    }

    @Override
    public int requireChildOutputNum() {
        return getTaskNum();
    }

    @Override
    public int getDriverParallelism() {
        return driverParallelism;
    }
}
