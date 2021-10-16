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
import lombok.Data;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

/**
 * @author chenghui.lch
 */
@Data
public class GroupDetailInfoRecord implements SystemTableRecord {
    public long id;
    public Timestamp gmtCreated;
    public Timestamp gmtModified;
    public String instId;
    public String dbName;
    public String groupName;
    public String storageInstId;

    @Override
    public GroupDetailInfoRecord fill(ResultSet rs) throws SQLException {
        this.id = rs.getLong("id");
        this.gmtCreated = rs.getTimestamp("gmt_created");
        this.gmtModified = rs.getTimestamp("gmt_modified");
        this.instId = rs.getString("inst_id");
        this.dbName = rs.getString("db_name");
        this.groupName = rs.getString("group_name");
        this.storageInstId = rs.getString("storage_inst_id");
        return this;
    }

}
