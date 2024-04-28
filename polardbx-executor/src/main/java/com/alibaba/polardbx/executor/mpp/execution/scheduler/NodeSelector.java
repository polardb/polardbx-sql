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
package com.alibaba.polardbx.executor.mpp.execution.scheduler;

import com.alibaba.polardbx.executor.mpp.execution.RemoteTask;
import com.alibaba.polardbx.executor.mpp.metadata.Split;
import com.alibaba.polardbx.gms.node.Node;
import com.google.common.collect.Multimap;

import java.util.List;

public interface NodeSelector {

    Node selectCurrentNode();

    /**
     * 用于hash shuffle的上下游关系，limit此时是表示下游的并发度，计算出下游运行的nodes
     */
    List<Node> selectRandomNodes(int limit);

    /**
     * Identifies the nodes for running the specified splits.
     *
     * @param splits the splits that need to be assigned to nodes
     * @return a multimap from node to splits only for splits for which we could identify a node to schedule on.
     * If we cannot find an assignment for a split, it is not included in the map. Also returns a future indicating when
     * to reattempt scheduling of this batch of splits, if some of them could not be scheduled.
     * <p>
     * 用户计算source节点执行nodes
     */
    Multimap<Node, Split> computeAssignments(List<Split> splits, List<RemoteTask> existingTasks);

    /**
     * get ordered node for remote shuffle under partition wise join
     */
    List<Node> getOrderedNode();
}
