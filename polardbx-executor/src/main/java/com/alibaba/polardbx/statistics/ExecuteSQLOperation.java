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

package com.alibaba.polardbx.statistics;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.alibaba.polardbx.common.jdbc.Parameters;

import java.text.MessageFormat;

public class ExecuteSQLOperation extends AbstractSQLOperation {

    private String sql;
    public final static MessageFormat message = new MessageFormat("Execute sql on {0}, sql is: {1}, params is: {2}");

    @JSONCreator
    public ExecuteSQLOperation(String groupName, String dbKeyName, String sql, Long timestamp) {
        super(groupName, dbKeyName, timestamp);
        this.sql = sql;
    }

    @JsonCreator
    public ExecuteSQLOperation(@JsonProperty("groupName") String groupName,
                               @JsonProperty("dbKeyName") String dbKeyName,
                               @JsonProperty("timestamp") Long timestamp,
                               @JsonProperty("sql") String sql,
                               @JsonProperty("params") Parameters params,
                               @JsonProperty("rowsCount") Long rowsCount,
                               @JsonProperty("timeCost") Long timeCost,
                               @JsonProperty("getConnectionTimeCost") float getConnectionTimeCost,
                               @JsonProperty("thread") String thread) {
        super(params, timeCost, groupName, dbKeyName, thread, getConnectionTimeCost, rowsCount, timestamp);
        this.sql = sql;
    }

    @Override
    public String getOperationString() {
        return message.format(new String[] {groupName, sql});
    }

    @Override
    @JsonProperty("sql")
    public String getSqlOrResult() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public String getSql() {
        return sql;
    }

    @Override
    public String getOperationType() {
        return "Query";
    }

}
