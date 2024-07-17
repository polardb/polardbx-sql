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
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.mpp.Session;
import com.alibaba.polardbx.executor.mpp.execution.NodeTaskMap;
import com.alibaba.polardbx.gms.node.InternalNode;
import com.alibaba.polardbx.gms.node.InternalNodeManager;
import com.alibaba.polardbx.gms.node.NodeState;

import javax.inject.Inject;
import java.util.Set;

public class NodeScheduler {

    private final InternalNodeManager nodeManager;
    private final NodeTaskMap nodeTaskMap;

    @Inject
    public NodeScheduler(InternalNodeManager nodeManager, NodeTaskMap nodeTaskMap) {
        this.nodeManager = nodeManager;
        this.nodeTaskMap = nodeTaskMap;
    }

    public NodeSelector createNodeSelector(Session session, int limit, boolean randomNode) {
        int maxSplitsPerNode =
            session.getClientContext().getParamManager().getInt(ConnectionParams.MPP_SCHEDULE_MAX_SPLITS_PER_NODE);
        boolean slaveFirst =
            session.getClientContext().getParamManager().getBoolean(ConnectionParams.POLARDBX_SLAVE_INSTANCE_FIRST);

        boolean enableOSSRoundRobin =
            session.getClientContext().getParamManager()
                .getBoolean(ConnectionParams.ENABLE_OSS_FILE_CONCURRENT_SPLIT_ROUND_ROBIN);

        Set<InternalNode> nodes = nodeManager.getNodes(NodeState.ACTIVE, ConfigDataMode.isMasterMode() && slaveFirst);

        boolean columnarMode = session.getClientContext().getParamManager()
            .getBoolean(ConnectionParams.ENABLE_COLUMNAR_SCHEDULE);

        boolean preferLocal =
            session.getClientContext().getParamManager().getBoolean(ConnectionParams.MPP_PREFER_LOCAL_NODE);

        if (columnarMode) {
            boolean enableTwoChoiceSchedule = session.getClientContext().getParamManager()
                .getBoolean(ConnectionParams.ENABLE_TWO_CHOICE_SCHEDULE);

            return new ColumnarNodeSelector(nodeManager, nodeTaskMap, nodes, limit, maxSplitsPerNode,
                enableOSSRoundRobin,
                randomNode, enableTwoChoiceSchedule, preferLocal);
        } else {
            return new SimpleNodeSelector(nodeManager, nodeTaskMap, nodes, limit, maxSplitsPerNode, enableOSSRoundRobin,
                randomNode, preferLocal);
        }
    }
}
