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
public class DbInfoRecord implements SystemTableRecord {

    /**
     * The db type of logical db
     */
    // Db type for old partition db (drds)
    public static final int DB_TYPE_PART_DB = 0;
    public static final int DB_TYPE_DEFAULT_DB = 1;
    public static final int DB_TYPE_SYSTEM_DB = 2;
    public static final int DB_TYPE_CDC_DB = 3;
    // Db type for new partition db 
    public static final int DB_TYPE_NEW_PART_DB = 4;

    /**
     * The db status of logical db
     */
    public static final int DB_STATUS_RUNNING = 0;
    public static final int DB_STATUS_CREATING = 1;
    public static final int DB_STATUS_DROPPING = 2;

    public long id;
    public Timestamp gmtCreated;
    public Timestamp gmtModified;
    public String dbName;
    public String appName;
    public String charset;
    public String collation;
    public int dbType;
    public int dbStatus;

    public boolean isUserDb() {
        return this.dbType == DB_TYPE_PART_DB || this.dbType == DB_TYPE_NEW_PART_DB;
    }

    public boolean isPartition() {
        return this.dbType == DB_TYPE_NEW_PART_DB;
    }

    public boolean isSharding() {
        return this.dbType != DB_TYPE_PART_DB;
    }

    @Override
    public DbInfoRecord fill(ResultSet rs) throws SQLException {
        this.id = rs.getLong("id");
        this.gmtCreated = rs.getTimestamp("gmt_created");
        this.gmtModified = rs.getTimestamp("gmt_modified");
        this.dbName = rs.getString("db_name");
        this.appName = rs.getString("app_name");
        this.charset = rs.getString("charset");
        this.collation = rs.getString("collation");
        this.dbType = rs.getInt("db_type");
        this.dbStatus = rs.getInt("db_status");
        return this;
    }

}
