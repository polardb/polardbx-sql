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

import com.alibaba.polardbx.executor.mpp.metadata.Split;
import com.alibaba.polardbx.gms.node.Node;
import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.function.ToIntFunction;

import static java.util.Objects.requireNonNull;

public class NodePartitionMap {
    private final Map<Integer, Node> partitionToNode;

    public NodePartitionMap(Map<Integer, Node> partitionToNode, ToIntFunction<Split> splitToBucket) {
        this.partitionToNode = ImmutableMap.copyOf(requireNonNull(partitionToNode, "partitionToNode is null"));
    }

    public Map<Integer, Node> getPartitionToNode() {
        return partitionToNode;
    }
}