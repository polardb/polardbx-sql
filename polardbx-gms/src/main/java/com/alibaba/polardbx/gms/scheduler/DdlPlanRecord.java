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

package com.alibaba.polardbx.gms.scheduler;

import com.alibaba.polardbx.common.ddl.newengine.DdlState;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class DdlPlanRecord implements SystemTableRecord {
    private long    id;
    private long    planId;
    private long    jobId;
    private String  tableSchema;
    private String  ddlStmt;
    private String  state;
    private String  ddlType;
    private int     progress;
    private int     retryCount;
    private String  result;
    private String  extras;
    public Date     gmtCreate;
    public Date     gmtModified;
    private String  resource;

    @Override
    public DdlPlanRecord fill(ResultSet rs) throws SQLException {

        this.id = rs.getLong("id");
        this.planId = rs.getLong("plan_id");
        this.jobId = rs.getLong("job_id");
        this.tableSchema = rs.getString("table_schema");
        this.ddlStmt = rs.getString("ddl_stmt");
        this.state = rs.getString("state");
        this.ddlType = rs.getString("ddl_type");
        this.progress = rs.getInt("progress");
        this.retryCount = rs.getInt("retry_count");
        this.extras = rs.getString("extras");
        this.result = rs.getString("result");
        this.gmtCreate = rs.getTimestamp("gmt_created");
        this.gmtModified = rs.getTimestamp("gmt_modified");
        this.resource = rs.getString("resource");

        return this;
    }

    public Map<Integer, ParameterContext> buildParams() {
        Map<Integer, ParameterContext> params = new HashMap<>(16);
        int index = 0;
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong,   this.planId);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.tableSchema);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.ddlStmt);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString,  this.state);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString,  this.ddlType);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setInt,  this.progress);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setInt,  this.retryCount);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.result);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.extras);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.resource);
        return params;
    }

    public long getId() {
        return id;
    }

    public long getPlanId() {
        return planId;
    }

    public void setPlanId(long planId) {
        this.planId = planId;
    }

    public long getJobId() {
        return jobId;
    }

    public void setJobId(long jobId) {
        this.jobId = jobId;
    }

    public String getTableSchema() {
        return tableSchema;
    }

    public void setTableSchema(String tableSchema) {
        this.tableSchema = tableSchema;
    }

    public String getDdlStmt() {
        return ddlStmt;
    }

    public void setDdlStmt(String ddlStmt) {
        this.ddlStmt = ddlStmt;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public String getDdlType() {
        return ddlType;
    }

    public void setDdlType(String ddlType) {
        this.ddlType = ddlType;
    }

    public int getProgress() {
        return progress;
    }

    public void setProgress(int progress) {
        this.progress = progress;
    }

    public int getRetryCount() {
        return this.retryCount;
    }

    public void setRetryCount(final int retryCount) {
        this.retryCount = retryCount;
    }

    public String getResource() {
        return resource;
    }

    public void setResource(String resource) {
        this.resource = resource;
    }

    public String getResult() {
        return result;
    }

    public void setResult(String result) {
        this.result = result;
    }

    public Date getGmtCreate() {
        return gmtCreate;
    }

    public Date getGmtModified() {
        return gmtModified;
    }

    public String getExtras() {
        return extras;
    }

    public void setExtras(String extras) {
        this.extras = extras;
    }

    public static DdlPlanRecord constructNewDdlPlanRecord(String tableSchema, long planId, String ddlType, String ddlStmt) {
        DdlPlanRecord ddlPlanRecord = new DdlPlanRecord();
        ddlPlanRecord.setPlanId(planId);
        ddlPlanRecord.setDdlType(ddlType);
        ddlPlanRecord.setDdlStmt(ddlStmt);
        ddlPlanRecord.setJobId(0);
        ddlPlanRecord.setState("INIT");
        ddlPlanRecord.setProgress(0);
        ddlPlanRecord.setRetryCount(0);
        ddlPlanRecord.setTableSchema(tableSchema);
        ddlPlanRecord.setResult("");
        ddlPlanRecord.setExtras("");
        ddlPlanRecord.setResource("");
        return ddlPlanRecord;
    }
}