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

import java.util.List;
import java.util.Set;

public interface InternalNodeManager {

    /**
     * @param slaveFirst true - 只读实例的节点优先
     */
    Set<InternalNode> getNodes(NodeState state, boolean slaveFirst);

    Node getCurrentNode();

    Set<Node> getCoordinators();

    AllNodes getAllNodes();

    void refreshNodes();

    /**
     * @param slaveFirst true - 只读实例的节点优先
     */
    List<Node> getAllWorkers(boolean slaveFirst);

    List<Node> getAllCoordinators();

    void removeNode(NodeServer node, String schema);

    void updateNodes(Set<InternalNode> activeNodes, Set<InternalNode> otherActiveNodes, Set<InternalNode> inactiveNodes,
                     Set<InternalNode> shuttingDownNodes);

    PolarDBXStatusManager getPolarDBXStatusManager();
}
