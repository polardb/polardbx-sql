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
import com.alibaba.polardbx.gms.topology.GroupDetailInfoRecord;
import lombok.Data;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

@Data
public class LocalityDetailInfoRecord implements SystemTableRecord {
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
    public String objectName;
    public String locality;

    public LocalityDetailInfoRecord(long id, int objectType, long objectId, String objectName, String locality) {
        this.id = id;
        this.objectType = objectType;
        this.objectName = objectName;
        this.locality = locality;
        this.objectId = objectId;
    }

    public LocalityDetailInfoRecord(LocalityDetailInfoRecord localityDetailInfoRecord) {
        this.id = localityDetailInfoRecord.id;
        this.objectType = localityDetailInfoRecord.objectType;
        this.objectName = localityDetailInfoRecord.objectName;
        this.locality = localityDetailInfoRecord.locality;
        this.objectId = localityDetailInfoRecord.objectId;
    }

    @Override
    public LocalityDetailInfoRecord fill(ResultSet rs) throws SQLException {
        this.id = rs.getLong("id");
        this.gmtCreated = rs.getTimestamp("gmt_created");
        this.gmtModified = rs.getTimestamp("gmt_modified");
        this.id = rs.getLong("id");
        this.objectType = rs.getInt("object_type");
        this.objectName = rs.getString("object_name");
        this.objectId = rs.getLong("object_id");
        this.locality = rs.getString("locality");
        return this;
    }
}
