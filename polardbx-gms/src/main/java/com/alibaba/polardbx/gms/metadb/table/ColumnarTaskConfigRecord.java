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

package com.alibaba.polardbx.gms.metadb.table;

import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author wenki
 */
public class ColumnarTaskConfigRecord implements SystemTableRecord {
    public String clusterId;
    public String containerId;
    public String taskName;
    public String ip;
    public long port;
    public String config;
    public String role;

    public ColumnarTaskConfigRecord() {
    }

    public ColumnarTaskConfigRecord(String clusterId, String containerId, String taskName, String ip, long port,
                                    String config, String role) {
        this.clusterId = clusterId;
        this.containerId = containerId;
        this.taskName = taskName;
        this.ip = ip;
        this.port = port;
        this.config = config;
        this.role = role;
    }

    @Override
    public ColumnarTaskConfigRecord fill(ResultSet rs) throws SQLException {
        this.clusterId = rs.getString("cluster_id");
        this.containerId = rs.getString("container_id");
        this.taskName = rs.getString("task_name");
        this.ip = rs.getString("ip");
        this.port = rs.getLong("port");
        this.config = rs.getString("config");
        this.role = rs.getString("role");

        return this;
    }
}
