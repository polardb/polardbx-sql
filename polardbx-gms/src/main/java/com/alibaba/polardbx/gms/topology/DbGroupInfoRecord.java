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
public class DbGroupInfoRecord implements SystemTableRecord {

    public static final int GROUP_TYPE_NORMAL = 0;
    public static final int GROUP_TYPE_ADDED = 1;
    public static final int GROUP_TYPE_ADDING = 2;
    public static final int GROUP_TYPE_REMOVING = 3;
    public static final int GROUP_TYPE_SCALEOUT_FINISHED = 4;

    public long id;
    public Timestamp gmtCreated;
    public Timestamp gmtModified;
    public String dbName;
    public String groupName;

    /**
     * 0: normal group
     * 1: scale out group
     * 2: adding group (the group is adding)
     * 3: removing group (the group is removing)
     */
    public int groupType;

    public String phyDbName;

    @Override
    public DbGroupInfoRecord fill(ResultSet rs) throws SQLException {
        this.id = rs.getLong("id");
        this.gmtCreated = rs.getTimestamp("gmt_created");
        this.gmtModified = rs.getTimestamp("gmt_modified");
        this.dbName = rs.getString("db_name");
        this.groupName = rs.getString("group_name");
        this.phyDbName = rs.getString("phy_db_name");
        this.groupType = rs.getInt("group_type");
        return this;
    }
}
