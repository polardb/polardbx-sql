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

package com.alibaba.polardbx.gms.locality;

import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

/**
 *
 */
public class LocalityInfoRecord implements SystemTableRecord {
    public static final int LOCALITY_TYPE_DEFAULT = 0;
    public static final int LOCALITY_TYPE_DATABASE = 1;
    public static final int LOCALITY_TYPE_TABLE = 2;
    public static final int LOCALITY_TYPE_TABLEGROUP = 3;
    public static final int LOCALITY_TYPE_PARTITIONGROUP = 4;

    public static final int LOCALITY_ID_DEFAULT = 0;

    public long id;
    public Timestamp gmtCreated;
    public Timestamp gmtModified;
    public int objectType;
    public long objectId;

    public String primaryZone;
    public String locality;

    public static String typeName(int type) {
        switch (type) {
        case LOCALITY_TYPE_DEFAULT:
            return "default";
        case LOCALITY_TYPE_DATABASE:
            return "database";
        case LOCALITY_TYPE_TABLE:
            return "table";
        case LOCALITY_TYPE_TABLEGROUP:
            return "tablegroup";
        case LOCALITY_TYPE_PARTITIONGROUP:
            return "partitiongroup";
        default:
            return "unknown";
        }
    }

    @Override
    public LocalityInfoRecord fill(ResultSet result) throws SQLException {
        this.id = result.getLong("id");
        this.gmtCreated = result.getTimestamp("gmt_created");
        this.gmtModified = result.getTimestamp("gmt_modified");
        this.objectType = result.getInt("object_type");
        this.objectId = result.getLong("object_id");

        this.primaryZone = result.getString("primary_zone");
        this.locality = result.getString("locality");
        return this;
    }
}
