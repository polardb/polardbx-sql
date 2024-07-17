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
import com.alibaba.polardbx.executor.mpp.execution.RemoteTask;
import com.alibaba.polardbx.executor.mpp.metadata.Split;
import com.alibaba.polardbx.executor.mpp.split.OssSplit;
import com.alibaba.polardbx.gms.node.InternalNode;
import com.alibaba.polardbx.gms.node.InternalNodeManager;
import com.alibaba.polardbx.gms.node.Node;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class SimpleNodeSelector implements NodeSelector {

    private static final Logger log = LoggerFactory.getLogger(SimpleNodeSelector.class);

    protected static final Comparator<Node> NODE_COMPARATOR = Comparator.comparing(Node::getHostPort);

    protected final InternalNodeManager nodeManager;
    protected final NodeTaskMap nodeTaskMap;
    protected final int limitCandidates;
    protected final int maxSplitsPerNode;
    private final boolean enableOssRoundRobin;

    private final List<Node> workerNodes;

    private final boolean preferLocal;

    public SimpleNodeSelector(InternalNodeManager nodeManager, NodeTaskMap nodeTaskMap, Set<InternalNode> nodes,
                              int limitCandidates, int maxSplitsPerNode, boolean enableOssRoundRobin,
                              boolean randomNode, boolean preferLocal) {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.nodeTaskMap = requireNonNull(nodeTaskMap, "nodeTaskMap is null");
        this.limitCandidates = limitCandidates;
        this.maxSplitsPerNode = maxSplitsPerNode;
        this.workerNodes = selectSuitableNodes(limitCandidates, nodes, randomNode);
        this.enableOssRoundRobin = enableOssRoundRobin;
        this.preferLocal = preferLocal;
    }

    private <T extends Node> List<Node> selectSuitableNodes(int limit, Collection<T> internalNodes,
                                                            boolean randomNode) {
        checkArgument(limit > 0, "limit must be at least 1");
        List<Node> selected = new ArrayList<>(limit);

        if (preferLocal && limit == 1) {
            Node current = selectCurrentNode();
            if (current.isWorker() && internalNodes.contains(current)) {
                selected.add(current);
                return selected;
            }
        }

        Iterator<T> candidates = randomNode ? new ResettableRandomizedIterator<T>(internalNodes) :
            internalNodes.stream().sorted(NODE_COMPARATOR).iterator();
        while (selected.size() < limit && candidates.hasNext()) {
            Node node = candidates.next();
            if (node.isWorker()) {
                selected.add(node);
            }
        }
        if (!randomNode) {
            log.debug("selected nodes under non random mode: " + selected.stream().map(Node::getHostPort).collect(
                Collectors.joining(",")) + "\n");
        }
        return selected;
    }

    @Override
    public Node selectCurrentNode() {
        return nodeManager.getCurrentNode();
    }

    @Override
    public List<Node> selectRandomNodes(int limit) {
        return selectSuitableNodes(limit, workerNodes, true);
    }

    private List<Node> selectNodes(int limit, Iterator<Node> candidates, NodeAssignmentStats assignmentStats) {
        checkArgument(limit > 0, "limit must be at least 1");
        List<Node> selected = new ArrayList<>(limit);
        while (selected.size() < limit && candidates.hasNext()) {
            Node ownerNode = candidates.next();
            if (assignmentStats.getTotalSplitCount(ownerNode) < maxSplitsPerNode || maxSplitsPerNode == 0) {
                selected.add(ownerNode);
            }
        }
        return selected;
    }

    @Override
    public Multimap<Node, Split> computeAssignments(List<Split> splits, List<RemoteTask> existingTasks) {
        NodeAssignmentStats assignmentStats = new NodeAssignmentStats(nodeTaskMap, workerNodes, existingTasks);

        ResettableRandomizedIterator<Node> randomCandidates = new ResettableRandomizedIterator<>(workerNodes);
        List<Node> candidateNodes = selectNodes(limitCandidates, randomCandidates, assignmentStats);
        if (candidateNodes.isEmpty()) {
            log.error(String.format("No nodes available to schedule."));
            throw new TddlRuntimeException(ErrorCode.ERR_NO_NODES_AVAILABLE, "No nodes available to run query");
        }

        if (splits.stream().allMatch(x -> x.getConnectorSplit() instanceof OssSplit)) {
            Multimap<Node, Split> assignment = HashMultimap.create();
            return scheduleOssSplit(splits, candidateNodes, assignmentStats, assignment);
        }

        // normal schedule. e.g. for innodb
        Multimap<Node, Split> assignment = HashMultimap.create();
        ResettableRandomizedIterator<Node> nodeIterator = new ResettableRandomizedIterator(candidateNodes);
        for (Split split : splits) {
            if (!nodeIterator.hasNext()) {
                nodeIterator.reset();
            }
            Node chosenNode = nodeIterator.next();
            if (chosenNode != null) {
                assignment.put(chosenNode, split);
                assignmentStats.addAssignedSplit(chosenNode);
            } else {
                throw new TddlRuntimeException(ErrorCode.ERR_NO_NODES_AVAILABLE, "No nodes available to run query");
            }
        }
        return assignment;
    }

    protected Multimap<Node, Split> scheduleOssSplit(List<Split> splits, List<Node> candidateNodes,
                                                     NodeAssignmentStats assignmentStats,
                                                     Multimap<Node, Split> assignment) {
        if (splits.isEmpty()) {
            return assignment;
        }
        candidateNodes.sort((a, b) -> a.getNodeIdentifier().compareTo(b.getNodeIdentifier()));

        log.debug("distribute simple oss split, selected nodes: " + candidateNodes.stream().map(Node::getHostPort)
            .collect(Collectors.joining(",")) + "\n");

        final boolean allSplitFileCurrent = splits
            .stream()
            .allMatch(split -> ((OssSplit) split.getConnectorSplit()).getDesignatedFile() != null);

        // should be normal case under oss or columnar
        if (allSplitFileCurrent) {
            if (enableOssRoundRobin) {
                return assignRoundRobin(splits, candidateNodes, assignmentStats, assignment);
            } else {
                return assignAllByFileName(splits, candidateNodes, assignmentStats, assignment);
            }
        }

        for (Split split : splits) {
            long hashCode;
            if (((OssSplit) split.getConnectorSplit()).getDesignatedFile() != null) {
                hashCode = ((OssSplit) split.getConnectorSplit()).getDesignatedFile().hashCode();
                hashCode = MurmurHashUtils.murmurHash128WithZeroSeed(hashCode);
            } else {
                List<String> phyTableNameList = ((OssSplit) split.getConnectorSplit()).getPhyTableNameList();
                hashCode = phyTableNameList.stream().map(x -> x.hashCode()).reduce(31, (a, b) -> a + b).longValue();
                hashCode = MurmurHashUtils.murmurHash128WithZeroSeed(hashCode);
            }

            int position = (int) hashCode % candidateNodes.size();
            if (position < 0) {
                position += candidateNodes.size();
            }
            doAssign(candidateNodes, assignmentStats, assignment, split, position);
        }
        return assignment;
    }

    protected Multimap<Node, Split> assignRoundRobin(List<Split> splits, List<Node> candidateNodes,
                                                     NodeAssignmentStats assignmentStats,
                                                     Multimap<Node, Split> assignment) {
        // use round robin for oss query
        int currentId = 0;
        for (Split split : splits) {
            int position = (currentId++) % candidateNodes.size();
            doAssign(candidateNodes, assignmentStats, assignment, split, position);
        }
        return assignment;
    }

    protected Multimap<Node, Split> assignAllByFileName(List<Split> splits, List<Node> candidateNodes,
                                                        NodeAssignmentStats assignmentStats,
                                                        Multimap<Node, Split> assignment) {
        Map<Integer, List<Split>> randomAssign = new HashMap<>();
        for (Split split : splits) {
            long hashCode = ((OssSplit) split.getConnectorSplit()).getDesignatedFile().hashCode();
            hashCode = MurmurHashUtils.murmurHash128WithZeroSeed(hashCode);

            int position = (int) hashCode % candidateNodes.size();
            if (position < 0) {
                position += candidateNodes.size();
            }
            randomAssign.putIfAbsent(position, new ArrayList<>());
            randomAssign.get(position).add(split);
        }

        assignToNode(candidateNodes, assignmentStats, assignment, randomAssign);

        return assignment;
    }

    protected void assignToNode(List<Node> candidateNodes, NodeAssignmentStats assignmentStats,
                                Multimap<Node, Split> assignment, Map<Integer, List<Split>> randomAssign) {
        for (Map.Entry<Integer, List<Split>> entry : randomAssign.entrySet()) {
            Integer pos = entry.getKey();
            for (Split split : entry.getValue()) {
                doAssign(candidateNodes, assignmentStats, assignment, split, pos);
            }
        }
    }

    protected void doAssign(List<Node> candidateNodes, NodeAssignmentStats assignmentStats,
                            Multimap<Node, Split> assignment, Split split, int position) {
        Node chosenNode = candidateNodes.get(position);
        if (chosenNode != null) {
            assignment.put(chosenNode, split);
            assignmentStats.addAssignedSplit(chosenNode);
        } else {
            throw new TddlRuntimeException(ErrorCode.ERR_NO_NODES_AVAILABLE, "No nodes available to run query");
        }
    }

    @Override
    public List<Node> getOrderedNode() {
        return workerNodes.stream().sorted(NODE_COMPARATOR).collect(Collectors.toList());
    }
}
