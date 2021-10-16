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
public class ConfigListenerRecord implements SystemTableRecord {

    public static int DATA_ID_STATUS_NORMAL = 0;
    public static int DATA_ID_STATUS_REMOVED = 1;

    public long id;
    public Timestamp gmtCreated;
    public Timestamp gmtModified;
    public String dataId;
    /**
     * 0: normal, 1: removing, 2:removed
     */
    public int status;
    public long opVersion;
    public String extras;

    @Override
    public ConfigListenerRecord fill(ResultSet rs) throws SQLException {
        this.id = rs.getLong("id");
        this.gmtCreated = rs.getTimestamp("gmt_created");
        this.gmtModified = rs.getTimestamp("gmt_modified");
        this.dataId = rs.getString("data_id");
        this.status = rs.getInt("status");
        this.opVersion = rs.getLong("op_version");
        this.extras = rs.getString("extras");
        return this;
    }
}
