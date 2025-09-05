/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.gms.topology;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;
import com.alibaba.polardbx.gms.node.NodeStatusManager;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NodeInfoAccessor extends AbstractAccessor {
    private static final Logger logger = LoggerFactory.getLogger(NodeInfoAccessor.class);
    private static final String NODE_INFO_TABLE = GmsSystemTables.NODE_INFO;

    private static final String ALL_COLUMNS =
        "id, cluster, inst_id, nodeid, version, ip, port, rpc_port, role, status, gmt_created, gmt_modified";
    private static final String WHERE_GMT_MODIFIED = " WHERE gmt_modified > subtime(now(), ?)";

    private static final String WHERE_GMT_MODIFIED_AND_MASTER =
        " WHERE gmt_modified > subtime(now(), ?) and (role & " + NodeStatusManager.ROLE_MASTER + " ) <> 0 ";
    private static final String SELECT_NODE_INFO_BY_GMT_MODIFIED = "select " + ALL_COLUMNS + " from " + NODE_INFO_TABLE
        + WHERE_GMT_MODIFIED;

    private static final String SELECT_NODE_INFO_BY_GMT_MODIFIED_AND_MASTER =
        "select " + ALL_COLUMNS + " from " + NODE_INFO_TABLE
            + WHERE_GMT_MODIFIED_AND_MASTER;

    private static final String SELECT_NODE_INFO_BY_GMT_MODIFIED_AND_LEADER =
        "select " + ALL_COLUMNS + " from " + NODE_INFO_TABLE
            + " WHERE gmt_modified > subtime(now(), ?) "
            + "and role & " + NodeStatusManager.ROLE_LEADER + " = " + NodeStatusManager.ROLE_LEADER;

    public List<NodeInfoRecord> queryLatestActive() {
        Map<Integer, ParameterContext> params = new HashMap<>(1);
        MetaDbUtil.setParameter(1, params, ParameterMethod.setString, "0:2:0");
        return query(SELECT_NODE_INFO_BY_GMT_MODIFIED, NODE_INFO_TABLE, NodeInfoRecord.class, params);
    }

    /**
     * 获取主实例的节点
     */
    public List<NodeInfoRecord> queryLatestMasterActive() {
        Map<Integer, ParameterContext> params = new HashMap<>(1);
        MetaDbUtil.setParameter(1, params, ParameterMethod.setString, "0:2:0");
        return query(SELECT_NODE_INFO_BY_GMT_MODIFIED_AND_MASTER, NODE_INFO_TABLE, NodeInfoRecord.class, params);
    }

    /**
     * 获取主实例的 Leader 节点
     */
    public List<NodeInfoRecord> queryLatestLeaderActive() {
        Map<Integer, ParameterContext> params = new HashMap<>(1);
        MetaDbUtil.setParameter(1, params, ParameterMethod.setString, "0:2:0");
        return query(SELECT_NODE_INFO_BY_GMT_MODIFIED_AND_LEADER, NODE_INFO_TABLE, NodeInfoRecord.class, params);
    }
}
