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

/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.polardbx.gms.node;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class AllNodes {

    protected static final Logger logger = LoggerFactory.getLogger(AllNodes.class);

    private Set<InternalNode> activeNodes;
    private Set<InternalNode> otherActiveRowNodes;
    private Set<InternalNode> otherActiveColumnarNodes;
    private Set<InternalNode> inactiveNodes;
    private Set<InternalNode> shuttingDownNodes;

    public static final AllNodes EMPTY =
        new AllNodes(ImmutableSet.of(), ImmutableSet.of(), ImmutableSet.of(), ImmutableSet.of(), ImmutableSet.of());

    @JsonCreator
    public AllNodes(@JsonProperty("activeNodes") Set<InternalNode> activeNodes,
                    @JsonProperty("otherActiveNodes") Set<InternalNode> otherActiveNodes,
                    @JsonProperty("otherActiveColumnarNodes") Set<InternalNode> otherActiveColumnarNodes,
                    @JsonProperty("inactiveNodes") Set<InternalNode> inactiveNodes,
                    @JsonProperty("shuttingDownNodes") Set<InternalNode> shuttingDownNodes) {
        updateActiveNodes(activeNodes, otherActiveNodes, otherActiveColumnarNodes, inactiveNodes, shuttingDownNodes);
    }

    @JsonProperty
    public Set<InternalNode> getActiveNodes() {
        return activeNodes;
    }

    @JsonIgnore
    public Set<InternalNode> getOtherActiveRowNodes() {
        return otherActiveRowNodes;
    }

    @JsonIgnore
    public Set<InternalNode> getOtherActiveColumnarNodes() {
        return otherActiveColumnarNodes;
    }

    @JsonProperty
    public Set<InternalNode> getInactiveNodes() {
        return inactiveNodes;
    }

    @JsonProperty
    public Set<InternalNode> getShuttingDownNodes() {
        return shuttingDownNodes;
    }

    public void updateActiveNodes(Set<InternalNode> activeNodes, Set<InternalNode> otherActiveNodes, Set<InternalNode>
        otherActiveColumnarNodes, Set<InternalNode> inactiveNodes, Set<InternalNode> shuttingDownNodes) {
        this.activeNodes = new HashSet<>(activeNodes);
        this.otherActiveRowNodes = new HashSet<>(otherActiveNodes);
        this.otherActiveColumnarNodes = new HashSet<>(otherActiveColumnarNodes);
        this.inactiveNodes = new HashSet<>(inactiveNodes);
        this.shuttingDownNodes = new HashSet<>(shuttingDownNodes);
    }

    public List<InternalNode> getAllWorkers(MppScope scope) {
        List<InternalNode> workers = new ArrayList<>();
        switch (scope) {
        case SLAVE:
            for (InternalNode node : otherActiveRowNodes) {
                if (node.isWorker()) {
                    workers.add(node);
                }
            }
            break;
        case COLUMNAR:
            for (InternalNode node : otherActiveColumnarNodes) {
                if (node.isWorker()) {
                    workers.add(node);
                }
            }
            break;
        case CURRENT:
            for (InternalNode node : activeNodes) {
                if (node.isWorker()) {
                    workers.add(node);
                }
            }
            break;
        case ALL:
            for (InternalNode node : otherActiveColumnarNodes) {
                if (node.isWorker()) {
                    workers.add(node);
                }
            }
            for (InternalNode node : otherActiveRowNodes) {
                if (node.isWorker()) {
                    workers.add(node);
                }
            }
            for (InternalNode node : activeNodes) {
                if (node.isWorker()) {
                    workers.add(node);
                }
            }
            break;
        default:
            for (InternalNode node : activeNodes) {
                if (node.isWorker()) {
                    workers.add(node);
                }
            }
            break;
        }
        return workers;
    }

    public List<Node> getAllCoordinators() {
        List<Node> coordinators = new ArrayList<>();
        for (Node node : activeNodes) {
            if (node.isCoordinator()) {
                coordinators.add(node);
            }
        }
        return coordinators;
    }

}
