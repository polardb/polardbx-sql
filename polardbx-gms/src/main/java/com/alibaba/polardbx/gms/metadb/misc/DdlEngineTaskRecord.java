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

package com.alibaba.polardbx.gms.metadb.misc;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class DdlEngineTaskRecord implements SystemTableRecord {

    public long jobId;
    public long taskId;
    public String schemaName;
    public String name;
    public String state;
    public String exceptionAction;
    public String value;
    public String extra;

    @Override
    public DdlEngineTaskRecord fill(ResultSet rs) throws SQLException {
        this.jobId = rs.getLong("job_id");
        this.taskId = rs.getLong("task_id");
        this.schemaName = rs.getString("schema_name");
        this.name = rs.getString("name");
        this.state = rs.getString("state");
        this.exceptionAction = rs.getString("exception_action");
        this.value = rs.getString("value");
        this.extra = rs.getString("extra");
        return this;
    }

    public Map<Integer, ParameterContext> buildParams() {
        Map<Integer, ParameterContext> params = new HashMap<>(16);
        int index = 0;
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.jobId);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.taskId);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.schemaName);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.name);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.state);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.exceptionAction);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.value);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.extra);
        return params;
    }

    public long getJobId() {
        return this.jobId;
    }

    public void setJobId(final long jobId) {
        this.jobId = jobId;
    }

    public long getTaskId() {
        return this.taskId;
    }

    public void setTaskId(final long taskId) {
        this.taskId = taskId;
    }

    public String getSchemaName() {
        return this.schemaName;
    }

    public void setSchemaName(final String schemaName) {
        this.schemaName = schemaName;
    }

    public String getName() {
        return this.name;
    }

    public void setName(final String name) {
        this.name = name;
    }

    public String getState() {
        return this.state;
    }

    public void setState(final String state) {
        this.state = state;
    }

    public String getExceptionAction() {
        return this.exceptionAction;
    }

    public void setExceptionAction(final String exceptionAction) {
        this.exceptionAction = exceptionAction;
    }

    public String getValue() {
        return this.value;
    }

    public void setValue(final String value) {
        this.value = value;
    }

    public String getExtra() {
        return extra;
    }

    public void setExtra(String extra) {
        this.extra = extra;
    }
}
