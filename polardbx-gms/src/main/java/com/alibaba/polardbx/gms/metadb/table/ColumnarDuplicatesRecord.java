/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.gms.metadb.table;

import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;
import lombok.Getter;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

public class ColumnarDuplicatesRecord implements SystemTableRecord {
    @Getter
    public enum Type {
        DOUBLE_DELETE("double_delete"),
        DOUBLE_INSERT("double_insert");

        private final String name;

        Type(String name) {
            this.name = name;
        }
    }

    public long id;
    public String engine;
    public String logicalSchema;
    public String logicalTable;
    public String partitionName;
    public Long longPk;
    public byte[] bytesPk;
    public String type;
    public Long beforeFileId;
    public Long beforePos;
    public Long afterFileId;
    public Long afterPos;
    public String extra;

    public Timestamp createTime;
    public Timestamp updateTime;

    @Override
    public ColumnarDuplicatesRecord fill(ResultSet rs) throws SQLException {
        this.id = rs.getLong("id");
        this.engine = rs.getString("engine");
        this.logicalSchema = rs.getString("logical_schema");
        this.logicalTable = rs.getString("logical_table");
        this.partitionName = rs.getString("partition_name");
        this.longPk = rs.getLong("long_pk");
        if (rs.wasNull()) {
            this.longPk = null;
        }
        this.bytesPk = rs.getBytes("bytes_pk");
        this.type = rs.getString("type");
        this.beforeFileId = rs.getLong("before_file_id");
        if (rs.wasNull()) {
            this.beforeFileId = null;
        }
        this.beforePos = rs.getLong("before_pos");
        if (rs.wasNull()) {
            this.beforePos = null;
        }
        this.afterFileId = rs.getLong("after_file_id");
        if (rs.wasNull()) {
            this.afterFileId = null;
        }
        this.afterPos = rs.getLong("after_pos");
        if (rs.wasNull()) {
            this.afterPos = null;
        }
        this.extra = rs.getString("extra");
        this.createTime = rs.getTimestamp("gmt_created");
        this.updateTime = rs.getTimestamp("gmt_modified");
        return this;
    }
}
