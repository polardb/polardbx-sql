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
import com.alibaba.polardbx.common.partition.MurmurHashUtils;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.mpp.execution.NodeTaskMap;
import com.alibaba.polardbx.executor.mpp.metadata.Split;
import com.alibaba.polardbx.executor.mpp.split.OssSplit;
import com.alibaba.polardbx.gms.node.InternalNode;
import com.alibaba.polardbx.gms.node.InternalNodeManager;
import com.alibaba.polardbx.gms.node.Node;
import com.alibaba.polardbx.optimizer.utils.PartitionUtils;
import com.google.common.collect.Multimap;
import io.airlift.slice.XxHash64;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class ColumnarNodeSelector extends SimpleNodeSelector {
    private static final Logger log = LoggerFactory.getLogger(ColumnarNodeSelector.class);

    private final boolean enableTwoChoiceSchedule;

    private Set<Integer> nodeUsedByPairWise = new HashSet<>();

    private boolean scheduleByPartition = false;

    public ColumnarNodeSelector(InternalNodeManager nodeManager, NodeTaskMap nodeTaskMap, Set<InternalNode> nodes,
                                int limitCandidates, int maxSplitsPerNode, boolean enableOssRoundRobin,
                                boolean randomNode, boolean enableTwoChoiceSchedule, boolean preferLocal) {
        super(nodeManager, nodeTaskMap, nodes, limitCandidates, maxSplitsPerNode, enableOssRoundRobin, randomNode,
            preferLocal);
        this.enableTwoChoiceSchedule = enableTwoChoiceSchedule;
    }

    @Override
    public Multimap<Node, Split> scheduleOssSplit(List<Split> splits, List<Node> candidateNodes,
                                                  NodeAssignmentStats assignmentStats,
                                                  Multimap<Node, Split> assignment) {
        if (scheduleByPartition) {
            return scheduleColumnarByPartition(splits, candidateNodes, assignmentStats, assignment);
        }
        Pair<List<Split>, List<Split>> splitsPair = splitByDefinedPartNum(splits);
        // assign non partition first, and handle schedule skew if happened
        assignNonPartition(splitsPair.getValue(), candidateNodes, assignmentStats, assignment);
        assignByPartition(splitsPair.getKey(), candidateNodes, assignmentStats, assignment, false);
        return assignment;
    }

    private Multimap<Node, Split> scheduleColumnarByPartition(List<Split> splits, List<Node> candidateNodes,
                                                              NodeAssignmentStats assignmentStats,
                                                              Multimap<Node, Split> assignment) {
        assignByPartition(splits, candidateNodes, assignmentStats, assignment, true);
        return assignment;
    }

    private void assignNonPartition(List<Split> splits, List<Node> candidateNodes,
                                    NodeAssignmentStats assignmentStats, Multimap<Node, Split> assignment) {
        super.scheduleOssSplit(splits, candidateNodes, assignmentStats, assignment);
    }

    @Override
    public void assignToNode(List<Node> candidateNodes, NodeAssignmentStats assignmentStats,
                             Multimap<Node, Split> assignment, Map<Integer, List<Split>> randomAssign) {
        int maxSplitCount = randomAssign.values().stream().mapToInt(List::size).max().getAsInt();
        int minSplitCount = randomAssign.values().stream().mapToInt(List::size).min().getAsInt();

        if (maxSplitCount > 2 * minSplitCount) {
            if (log.isDebugEnabled()) {
                logScheduleSkewMessage(assignment, maxSplitCount, minSplitCount);
            }
            if (enableTwoChoiceSchedule) {
                scheduleByTwoChoice(candidateNodes, assignmentStats, assignment, randomAssign);
                return;
            }
        }
        super.assignToNode(candidateNodes, assignmentStats, assignment, randomAssign);
    }

    private void scheduleByTwoChoice(List<Node> candidateNodes, NodeAssignmentStats assignmentStats,
                                     Multimap<Node, Split> assignment, Map<Integer, List<Split>> randomAssign) {
        Map<Integer, Integer> balancedAssign = new HashMap<>();

        for (List<Split> splits : randomAssign.values()) {
            for (Split split : splits) {
                int bucket = chooseBucketByTwoChoice(((OssSplit) split.getConnectorSplit()).getDesignatedFile(),
                    candidateNodes.size(), balancedAssign);
                doAssign(candidateNodes, assignmentStats, assignment, split, bucket);
            }
        }
        if (log.isDebugEnabled()) {
            log.debug("schedule by two phase choice. detail schedule result is " + assignment.entries().stream()
                .map(entry -> entry.getKey().getHostPort() + " : " + String.join(",",
                    ((OssSplit) entry.getValue().getConnectorSplit()).getDesignatedFile()))
                .collect(Collectors.joining("; ")));
        }
    }

    /**
     * return choosed bucket, and update balanced assign result
     */
    public static int chooseBucketByTwoChoice(List<String> files, int allNodes, Map<Integer, Integer> balancedAssign) {
        long fileNameCode = files.hashCode();
        long hashCode1 = MurmurHashUtils.murmurHash128WithZeroSeed(fileNameCode);
        long hashCode2 = XxHash64.hash(fileNameCode);

        // calculate two bucket
        int bucket1 = (int) ((hashCode1 & Long.MAX_VALUE) % allNodes);
        int bucket2 = (int) ((hashCode2 & Long.MAX_VALUE) % allNodes);

        // pick min count
        int count1 = balancedAssign.getOrDefault(bucket1, Integer.MIN_VALUE),
            count2 = balancedAssign.getOrDefault(bucket2, Integer.MIN_VALUE);

        int bucket = count1 < count2 ? bucket1 : bucket2;
        balancedAssign.compute(bucket, (key, count) -> (count == null) ? 1 : count + 1);
        return bucket;
    }

    private void logScheduleSkewMessage(Multimap<Node, Split> assignment, int maxSplitCount, int minSplitCount) {
        log.debug(String.format(
            "schedule split skewed under non pair-wise, max split count is %s, while min split count is %s",
            maxSplitCount, minSplitCount));

        log.debug("schedule by file. detail schedule result is " + assignment.entries().stream()
            .map(entry -> entry.getKey().getHostPort() + " : " + String.join(",",
                ((OssSplit) entry.getValue().getConnectorSplit()).getDesignatedFile()))
            .collect(Collectors.joining("; ")));
    }

    private Pair<List<Split>, List<Split>> splitByDefinedPartNum(List<Split> splits) {
        List<Split> splitsHasPartNum = new ArrayList<>();
        List<Split> restSplits = new ArrayList<>();
        for (Split split : splits) {
            if (((OssSplit) split.getConnectorSplit()).getPartIndex() > -1) {
                splitsHasPartNum.add(split);
            } else {
                restSplits.add(split);
            }
        }
        return Pair.of(splitsHasPartNum, restSplits);
    }

    private void assignByPartition(List<Split> splits, List<Node> candidateNodes,
                                   NodeAssignmentStats assignmentStats,
                                   Multimap<Node, Split> assignment, boolean forceGenPart) {
        if (splits.isEmpty()) {
            return;
        }
        // TODO can be replaced with consistency hash for better locality
        candidateNodes.sort(NODE_COMPARATOR);

        if (log.isDebugEnabled()) {
            log.debug("start distribute split under partition wise join\n");
            log.debug(
                "selected nodes: " + candidateNodes.stream().map(Node::getHostPort).collect(Collectors.joining(","))
                    + "\n");
        }

        Map<Integer, List<Integer>> nodeIdToPartitions = new HashMap<>();
        Map<Integer, Integer> partToNodeId = new HashMap<>();
        for (Split split : splits) {
            OssSplit ossSplit = (OssSplit) (split.getConnectorSplit());
            int partNum = ossSplit.getPartIndex();
            if (partNum < 0 && forceGenPart) {
                partNum = PartitionUtils.calcPartition(ossSplit.getLogicalSchema(), ossSplit.getLogicalTableName(),
                    ossSplit.getPhysicalSchema(), ossSplit.getPhyTableNameList().get(0));
            }
            int nodeId = partNum % candidateNodes.size();
            if (log.isDebugEnabled()) {
                log.debug("oss split: " + ossSplit + " part number: " + partNum + " node id: " + nodeId + "\n");
            }
            partToNodeId.put(partNum, nodeId);
            boolean alreadyAdd = nodeIdToPartitions.computeIfAbsent(nodeId, k -> new ArrayList<>()).contains(partNum);
            if (!alreadyAdd) {
                nodeIdToPartitions.get(nodeId).add(partNum);
            }
            doAssign(candidateNodes, assignmentStats, assignment, split, nodeId);
        }
        if (log.isDebugEnabled()) {
            log.debug("finish distribute split under partition wise join\n");
        }

        nodeUsedByPairWise.addAll(nodeIdToPartitions.keySet());
        // sort the partition
        nodeIdToPartitions.values().forEach(Collections::sort);
        updateOssSplitPartInfo(splits, nodeIdToPartitions, partToNodeId);
    }

    /**
     * change partNum in ossSplit to node scope instead of global scope
     */
    private void updateOssSplitPartInfo(List<Split> splits, Map<Integer, List<Integer>> nodeIdToPartitions,
                                        Map<Integer, Integer> partToNodeId) {
        for (Split split : splits) {
            OssSplit ossSplit = (OssSplit) (split.getConnectorSplit());
            // reset partition info to default if not local partition wise
            if (!ossSplit.isLocalPairWise()) {
                ossSplit.setNodePartCount(-1);
                ossSplit.setPartIndex(-1);
            } else {
                int partNum = ossSplit.getPartIndex();
                Integer nodeId = partToNodeId.get(partNum);
                List<Integer> nodeParts = nodeIdToPartitions.get(nodeId);
                ossSplit.setPartIndex(nodeParts.indexOf(partNum));
            }
        }
    }

    public void setScheduleByPartition(boolean scheduleByPartition) {
        this.scheduleByPartition = scheduleByPartition;
    }
}
