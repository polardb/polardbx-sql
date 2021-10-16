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

package com.alibaba.polardbx.gms.topology;

import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

/**
 * @author chenghui.lch
 */
public class ServerInfoRecord implements SystemTableRecord {

    public static int INST_TYPE_MASTER = 0;
    public static int INST_TYPE_SLAVE = 1;
    public static int INST_TYPE_HTAP_SLAVE = 2;
    public static int SERVER_STATUS_READY = 0;
    public static int SERVER_STATUS_NOT_READY = 1;
    public static int SERVER_STATUS_REMOVED = 2;

    public long id;
    public Timestamp gmtCreated;
    public Timestamp gmtModified;
    public String instId;
    // 0:master, 1:slave, 2: htap slave
    public int instType;
    public String ip;
    public int port;
    public int htapPort;
    public int mgrPort;
    public int mppPort;
    // 0:server ready, 1:server not_ready, 2:server removed
    public int status;
    public String regionId;
    public String azoneId;
    public String idcId;
    public int cpuCore;
    public int memSize;
    public String extras;

    @Override
    public ServerInfoRecord fill(ResultSet rs) throws SQLException {
        this.id = rs.getLong("id");
        this.gmtCreated = rs.getTimestamp("gmt_created");
        this.gmtModified = rs.getTimestamp("gmt_modified");
        this.instId = rs.getString("inst_id");
        this.instType = rs.getInt("inst_type");
        this.ip = rs.getString("ip");
        this.port = rs.getInt("port");
        this.htapPort = rs.getInt("htap_port");
        this.mgrPort = rs.getInt("mgr_port");
        this.mppPort = rs.getInt("mpp_port");
        this.status = rs.getInt("status");
        this.regionId = rs.getString("region_id");
        this.azoneId = rs.getString("azone_id");
        this.idcId = rs.getString("idc_id");
        this.cpuCore = rs.getInt("cpu_core");
        this.memSize = rs.getInt("mem_size"); // Unit: MB
        this.extras = rs.getString("extras");
        return this;
    }
}
