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
import java.util.HashMap;
import java.util.Map;

/**
 * @author chenghui.lch
 */
public class DbGroupInfoRecord implements SystemTableRecord {

    public static final int GROUP_TYPE_NORMAL = 0;

    public static final int GROUP_TYPE_ADDED = 1;
    public static final int GROUP_TYPE_ADDING = 2;
    public static final int GROUP_TYPE_SCALEOUT_FINISHED = 4;

    /**
     * NORMAL -> BEFORE_REMOVE -> REMOVING
     * BEFORE_REMOVE: stop place any partition on this group, and avoid broadcast-table read
     * REMOVING: almost removed
     */
    public static final int GROUP_TYPE_BEFORE_REMOVE = 5;
    public static final int GROUP_TYPE_REMOVING = 3;

    private static final Map<Integer, String> grpTypeNameMap = new HashMap<>();
    static  {
        grpTypeNameMap.put(GROUP_TYPE_NORMAL, "GROUP_TYPE_NORMAL");
        grpTypeNameMap.put(GROUP_TYPE_ADDED, "GROUP_TYPE_ADDED");
        grpTypeNameMap.put(GROUP_TYPE_ADDING, "GROUP_TYPE_ADDING");
        grpTypeNameMap.put(GROUP_TYPE_SCALEOUT_FINISHED, "GROUP_TYPE_SCALEOUT_FINISHED");
        grpTypeNameMap.put(GROUP_TYPE_BEFORE_REMOVE, "GROUP_TYPE_BEFORE_REMOVE");
        grpTypeNameMap.put(GROUP_TYPE_REMOVING, "GROUP_TYPE_REMOVING");
    }

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

    public boolean isNormal() {
        return this.groupType == GROUP_TYPE_NORMAL;
    }

    public boolean isVisible() {
        return this.groupType == GROUP_TYPE_NORMAL || this.groupType == GROUP_TYPE_BEFORE_REMOVE;
    }

    public boolean isRemoving() {
        return this.groupType == GROUP_TYPE_REMOVING;
    }

    public boolean isRemovable() {
        return this.groupType == GROUP_TYPE_BEFORE_REMOVE || this.groupType == GROUP_TYPE_REMOVING;
    }

    public String getGroupTypeStr() {
        return grpTypeNameMap.get(this.groupType);
    }
}
