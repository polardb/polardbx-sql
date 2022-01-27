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

public class DdlEngineRecord implements SystemTableRecord {

    public DdlEngineRecord() {
    }

    public static final long FLAG_SUPPORT_CONTINUE = 0x1;
    public static final long FLAG_SUPPORT_CANCEL = 0x2;

    public long jobId;
    public String ddlType;
    public String schemaName;
    public String objectName;
    public String responseNode;
    public String executionNode;
    public String state;
    public String resources;
    public int progress;
    public String traceId;
    public String context;
    public String taskGraph;
    public String result;
    public String ddlStmt;
    public long gmtCreated;
    public long gmtModified;
    public int maxParallelism;
    public int supportedCommands;
    public String pausedPolicy;
    public String rollbackPausedPolicy;

    @Override
    public DdlEngineRecord fill(ResultSet rs) throws SQLException {
        this.jobId = rs.getLong("job_id");
        this.ddlType = rs.getString("ddl_type");
        this.schemaName = rs.getString("schema_name");
        this.objectName = rs.getString("object_name");
        this.responseNode = rs.getString("response_node");
        this.executionNode = rs.getString("execution_node");
        this.state = rs.getString("state");
        this.resources = rs.getString("resources");
        this.progress = rs.getInt("progress");
        this.traceId = rs.getString("trace_id");
        this.context = rs.getString("context");
        this.taskGraph = rs.getString("task_graph");
        this.result = rs.getString("result");
        this.ddlStmt = rs.getString("ddl_stmt");
        this.gmtCreated = rs.getLong("gmt_created");
        this.gmtModified = rs.getLong("gmt_modified");
        this.maxParallelism = rs.getInt("max_parallelism");
        this.supportedCommands = rs.getInt("supported_commands");
        this.pausedPolicy = rs.getString("paused_policy");
        this.rollbackPausedPolicy = rs.getString("rollback_paused_policy");
        return this;
    }

    public Map<Integer, ParameterContext> buildParams() {
        Map<Integer, ParameterContext> params = new HashMap<>(16);
        int index = 0;
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.jobId);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.ddlType);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.schemaName);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.objectName);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.responseNode);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.executionNode);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.state);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.resources);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setInt, this.progress);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.traceId);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.context);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.taskGraph);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.result);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.ddlStmt);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.gmtCreated);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.gmtModified);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setInt, this.maxParallelism);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setInt, this.supportedCommands);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.pausedPolicy);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.rollbackPausedPolicy);
        return params;
    }

    public boolean isSupportContinue() {
        return (supportedCommands & FLAG_SUPPORT_CONTINUE) != 0L;
    }

    public void setSupportContinue() {
        supportedCommands |= FLAG_SUPPORT_CONTINUE;
    }

    public void clearSupportContinue() {
        supportedCommands &= ~FLAG_SUPPORT_CONTINUE;
    }

    public boolean isSupportCancel() {
        return (supportedCommands & FLAG_SUPPORT_CANCEL) != 0L;
    }

    public void setSupportCancel() {
        supportedCommands |= FLAG_SUPPORT_CANCEL;
    }

    public void clearSupportCancel() {
        supportedCommands &= ~FLAG_SUPPORT_CANCEL;
    }

    public static boolean isSupportContinue(int flag) {
        return (flag & FLAG_SUPPORT_CONTINUE) != 0L;
    }

    public static boolean isSupportCancel(int flag) {
        return (flag & FLAG_SUPPORT_CANCEL) != 0L;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || this.getClass() != o.getClass()) {
            return false;
        }

        final DdlEngineRecord record = (DdlEngineRecord) o;

        return this.jobId == record.jobId;
    }

    @Override
    public int hashCode() {
        return (int) (this.jobId ^ (this.jobId >>> 32));
    }
}
