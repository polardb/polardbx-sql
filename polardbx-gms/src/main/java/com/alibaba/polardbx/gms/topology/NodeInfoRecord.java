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

import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;
import com.alibaba.polardbx.gms.node.NodeStatusManager;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

/**
 * Check {@link NodeStatusManager#injectNode} for definition of {@link #role} and {@link #status}
 */
public class NodeInfoRecord implements SystemTableRecord {
    public long id;
    public String cluster;
    public String instId;
    public String nodeId;
    public String version;
    public String ip;
    public int port;
    public long rpcPort;
    public long role;
    public int status;
    public Timestamp gmtCreated;
    public Timestamp gmtModified;

    @Override
    public NodeInfoRecord fill(ResultSet rs) throws SQLException {
        this.id = rs.getLong("id");
        this.cluster = rs.getString("cluster");
        this.instId = rs.getString("inst_id");
        this.nodeId = rs.getString("nodeid");
        this.version = rs.getString("version");
        this.ip = rs.getString("ip");
        this.port = rs.getInt("port");
        this.rpcPort = rs.getLong("rpc_port");
        this.role = rs.getLong("role");
        this.status = rs.getInt("status");
        this.gmtCreated = rs.getTimestamp("gmt_created");
        this.gmtModified = rs.getTimestamp("gmt_modified");
        return this;
    }
}
