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

import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.mpp.execution.scheduler.NodeSelector;
import com.alibaba.polardbx.executor.mpp.util.Failures;
import com.alibaba.polardbx.gms.node.Node;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;

/**
 * Per node only start the single same task which can start multi driver.
 */
public class NodePartitioningManager {

    private static final Logger logger = LoggerFactory.getLogger(NodePartitioningManager.class);

    @Inject
    public NodePartitioningManager() {
    }

    public Map<Integer, Node> getNodePartitioningMap(NodeSelector nodeSelector, PartitionHandle partitioningHandle) {
        List<Node> nodes;
        if (partitioningHandle.getPartitionMode() == PartitionHandle.PartitionMode.COORDINATOR_ONLY) {
            nodes = ImmutableList.of(nodeSelector.selectCurrentNode());
        } else {
            nodes = nodeSelector.selectRandomNodes(partitioningHandle.getPartitionCount());
        }

        Failures.checkCondition(!nodes.isEmpty(), ErrorCode.ERR_NO_NODES_AVAILABLE, "No worker nodes available");

        ImmutableMap.Builder<Integer, Node> partitionToNode = ImmutableMap.builder();

        for (int i = 0; i < nodes.size(); i++) {
            Node node = nodes.get(i);
            partitionToNode.put(i, node);
        }
        return partitionToNode.build();
    }
}
