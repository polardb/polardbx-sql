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

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

public class ColumnarPurgeHistoryRecord implements SystemTableRecord {

    public long id;
    public long tso;
    public String status;
    public String info;
    public String extra;

    public Timestamp createTime;
    public Timestamp updateTime;

    @Override
    public ColumnarPurgeHistoryRecord fill(ResultSet rs) throws SQLException {
        this.id = rs.getLong("id");
        this.tso = rs.getLong("tso");
        this.status = rs.getString("status");
        this.info = rs.getString("info");
        this.extra = rs.getString("extra");
        this.createTime = rs.getTimestamp("gmt_created");
        this.updateTime = rs.getTimestamp("gmt_modified");
        return this;
    }

    public Map<Integer, ParameterContext> buildInsertParams() {
        Map<Integer, ParameterContext> params = new HashMap<>(8);
        int index = 0;
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.tso);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.status);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.info);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.extra);
        return params;
    }

    public enum PurgeStatus {
        NONE,
        START,
        FINISHED,
        FAILED;

        public String toString() {
            switch (this) {
            case NONE:
                return "None";
            case START:
                return "Start";
            case FINISHED:
                return "Finished";
            case FAILED:
                return "Failed";
            default:
                throw new IllegalArgumentException("Unsupported enum constant: " + this);
            }
        }

        public static PurgeStatus fromString(String value) {
            switch (value.toLowerCase()) {
            case "none":
                return NONE;
            case "start":
                return START;
            case "finished":
                return FINISHED;
            case "failed":
                return FAILED;
            default:
                throw new IllegalArgumentException("Invalid string value for PurgeStatus enum: " + value);
            }
        }
    }
}
