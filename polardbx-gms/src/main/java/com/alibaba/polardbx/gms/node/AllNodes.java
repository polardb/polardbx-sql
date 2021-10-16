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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class AllNodes {
    private Set<InternalNode> activeNodes;
    private Set<InternalNode> otherActiveNodes;
    private Set<InternalNode> inactiveNodes;
    private Set<InternalNode> shuttingDownNodes;

    private int activeHashcode;

    public static final AllNodes EMPTY = new AllNodes(null, null, null, -1);

    public AllNodes(Set<InternalNode> activeNodes, Set<InternalNode> otherActiveNodes, Set<InternalNode> inactiveNodes,
                    Set<InternalNode> shuttingDownNodes) {
        this.activeNodes = new HashSet<>(requireNonNull(activeNodes, "activeNodes is null"));
        this.otherActiveNodes = new HashSet<>(requireNonNull(otherActiveNodes, "activeNodes is null"));
        this.inactiveNodes = new HashSet<>(requireNonNull(inactiveNodes, "inactiveNodes is null"));
        this.shuttingDownNodes = new HashSet<>(requireNonNull(shuttingDownNodes, "shuttingDownNodes is null"));
    }

    @JsonCreator
    public AllNodes(@JsonProperty("activeNodes") Set<InternalNode> activeNodes,
                    @JsonProperty("inactiveNodes") Set<InternalNode> inactiveNodes,
                    @JsonProperty("shuttingDownNodes") Set<InternalNode> shuttingDownNodes,
                    @JsonProperty("activeHashcode") int activeHashcode) {
        this.activeNodes = activeNodes;
        this.inactiveNodes = inactiveNodes;
        this.shuttingDownNodes = shuttingDownNodes;
        this.activeHashcode = activeHashcode;
    }

    @JsonProperty
    public Set<InternalNode> getActiveNodes() {
        return activeNodes;
    }

    @JsonIgnore
    public Set<InternalNode> getOtherActiveNodes() {
        return otherActiveNodes;
    }

    @JsonProperty
    public Set<InternalNode> getInactiveNodes() {
        return inactiveNodes;
    }

    @JsonProperty
    public Set<InternalNode> getShuttingDownNodes() {
        return shuttingDownNodes;
    }

    @JsonProperty
    public int getActiveHashcode() {
        return activeHashcode;
    }

    public void setActiveNodes(Set<InternalNode> activeNodes, Set<InternalNode> otherActiveNodes) {
        this.activeNodes = new HashSet<>(requireNonNull(activeNodes, "activeNodes is null"));
        this.otherActiveNodes = otherActiveNodes;
    }

    public void setInactiveNodes(Set<InternalNode> inactiveNodes) {
        this.inactiveNodes = inactiveNodes;
    }

    public void setShuttingDownNodes(Set<InternalNode> shuttingDownNodes) {
        this.shuttingDownNodes = shuttingDownNodes;
    }

    public void setActiveHashcode(int activeHashcode) {
        this.activeHashcode = activeHashcode;
    }

    public List<Node> getAllWorkers(boolean slaveFirst) {
        List<Node> workers = new ArrayList<>();
        if (slaveFirst && otherActiveNodes != null && !otherActiveNodes.isEmpty()) {
            for (Node node : otherActiveNodes) {
                if (node.isWorker()) {
                    workers.add(node);
                }
            }
        } else {
            for (Node node : activeNodes) {
                if (node.isWorker()) {
                    workers.add(node);
                }
            }
        }

        /*
        for (Node node : inactiveNodes) {
            if (node.isWorker()) {
                workers.add(node);
            }
        }
        for (Node node : shuttingDownNodes) {
            if (node.isWorker()) {
                workers.add(node);
            }
        }
         */
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
