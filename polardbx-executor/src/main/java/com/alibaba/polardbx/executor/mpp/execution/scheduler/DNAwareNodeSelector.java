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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.mpp.execution.RemoteTask;
import com.alibaba.polardbx.executor.mpp.metadata.Split;
import com.alibaba.polardbx.gms.node.InternalNode;
import com.alibaba.polardbx.gms.node.InternalNodeManager;
import com.alibaba.polardbx.gms.node.Node;
import com.alibaba.polardbx.gms.node.PolarDBXStatusManager;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * Take into account the DN's load, and scheduler the sourceTasks.
 */
public class DNAwareNodeSelector implements NodeSelector {

    private static final Logger log = LoggerFactory.getLogger(SimpleNodeSelector.class);

    private final InternalNodeManager nodeManager;
    private final int limitCandidates;
    private final List<Node> workerNodes;
    private final boolean awareLoad;
    private final boolean awareDelay;
    private final PolarDBXStatusManager statusNodeManager;

    public DNAwareNodeSelector(InternalNodeManager nodeManager, Set<InternalNode> nodes,
                               int limitCandidates, boolean awareLoad, boolean awareDelay) {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.limitCandidates = limitCandidates;
        this.awareDelay = awareDelay;
        this.awareLoad = awareLoad;
        this.statusNodeManager = nodeManager.getPolarDBXStatusManager();
        this.workerNodes = selectSuitableWorkerNodes(limitCandidates, nodes);
    }

    private List<Node> selectSuitableWorkerNodes(int limit, Set<InternalNode> internalNodes) {
        ResettableRandomizedIterator<InternalNode> candidates1 = new ResettableRandomizedIterator<>(internalNodes);
        checkArgument(limit > 0, "The limit must be at least 1");

        List<Node> selected1 = new ArrayList<>(internalNodes.size());

        if (awareDelay) {
            //exclude the delay nodes
            while (candidates1.hasNext()) {
                Node node = candidates1.next();
                if (node.isWorker() && !statusNodeManager.isDelay(node.getInstId())) {
                    selected1.add(node);
                }
            }
        }

        if (selected1.size() == 0) {
            candidates1.reset();
            while (candidates1.hasNext()) {
                Node node = candidates1.next();
                if (node.isWorker()) {
                    selected1.add(node);
                }
            }
        }

        List<Node> candidates;
        List<Node> selected = new ArrayList<>(limit);
        if (awareLoad) {
            //exclude the high load nodes
            List<Node> selected2 = new ArrayList<>(internalNodes.size());
            ResettableRandomizedIterator<Node> candidates2 = new ResettableRandomizedIterator<>(selected1);
            while (candidates2.hasNext()) {
                Node node = candidates2.next();
                if (node.isWorker() && !statusNodeManager.isHighLoad(node.getInstId())) {
                    selected2.add(node);
                }
            }

            if (selected2.size() == 0) {
                candidates2.reset();
                while (candidates2.hasNext()) {
                    Node node = candidates2.next();
                    if (node.isWorker()) {
                        selected2.add(node);
                    }
                }
            }
            candidates = selected2;
        } else {
            candidates = selected1;
        }

        for (int i = 0; i < candidates.size(); i++) {
            if (i < limit) {
                selected.add(candidates.get(i));
            } else {
                break;
            }
        }
        return selected;
    }

    @Override
    public Node selectCurrentNode() {
        return nodeManager.getCurrentNode();
    }

    @Override
    public List<Node> selectRandomNodes(int limit) {
        List<Node> selected = new ArrayList<>(limit);
        Iterator<Node> candidates = new ResettableRandomizedIterator<>(workerNodes);
        while (selected.size() < limit && candidates.hasNext()) {
            selected.add(candidates.next());
        }
        return selected;
    }

    private List<Node> selectNodes(int limit, Iterator<Node> candidates) {
        checkArgument(limit > 0, "limit must be at least 1");
        List<Node> selected = new ArrayList<>(limit);
        while (selected.size() < limit && candidates.hasNext()) {
            Node ownerNode = candidates.next();
            selected.add(ownerNode);
        }
        return selected;
    }

    @Override
    public Multimap<Node, Split> computeAssignments(List<Split> splits, List<RemoteTask> existingTasks) {
        Multimap<Node, Split> assignment = HashMultimap.create();

        ResettableRandomizedIterator<Node> randomCandidates = new ResettableRandomizedIterator<>(workerNodes);
        List<Node> candidateNodes = selectNodes(limitCandidates, randomCandidates);
        if (candidateNodes.isEmpty()) {
            log.error(String.format("No nodes available to schedule."));
            throw new TddlRuntimeException(ErrorCode.ERR_NO_NODES_AVAILABLE, "No nodes available to run query");
        }

        ResettableRandomizedIterator<Node> nodeIterator = new ResettableRandomizedIterator(candidateNodes);
        for (Split split : splits) {
            if (!nodeIterator.hasNext()) {
                nodeIterator.reset();
            }
            Node chosenNode = nodeIterator.next();
            if (chosenNode != null) {
                assignment.put(chosenNode, split);
            } else {
                throw new TddlRuntimeException(ErrorCode.ERR_NO_NODES_AVAILABLE, "No nodes available to run query");
            }
        }
        return assignment;
    }
}