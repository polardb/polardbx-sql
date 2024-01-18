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
public class StoragePoolInfoRecord implements SystemTableRecord {

    public long id;
    public String name;
    public Timestamp gmtCreated;
    public Timestamp gmtModified;
    public String dnIds;
    public String undeletableDnId;
    public String extras;

    @Override
    public StoragePoolInfoRecord fill(ResultSet result) throws SQLException {
        this.id = result.getLong("id");
        this.name = result.getString("name");
        this.gmtCreated = result.getTimestamp("gmt_created");
        this.gmtModified = result.getTimestamp("gmt_modified");
        this.dnIds = result.getString("dn_ids");
        this.undeletableDnId = result.getString("undeletable_dn_id");
        this.extras = result.getString("extras");
        return this;
    }
}
