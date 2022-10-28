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

import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;
import lombok.Data;

import java.sql.ResultSet;
import java.sql.SQLException;

public class ExecutableScheduledJob implements SystemTableRecord {

    private long scheduleId;
    private String tableSchema;
    private String tableName;
    private String tableGroupName;

    private String scheduleName;
    private String scheduleComment;
    private String executorType;
    private String scheduleContext;
    private String executorContents;
    private String status;
    private String scheduleType;
    private String scheduleExpr;
    private String timeZone;
    private String schedulePolicy;

    private long fireTime;
    private long startTime;
    private long finishTime;
    private String state;
    private String remark;
    private String result;

    @Override
    public ExecutableScheduledJob fill(ResultSet rs) throws SQLException {

        this.scheduleId = rs.getLong("schedule_id");
        this.tableSchema = rs.getString("table_schema");
        this.tableName = rs.getString("table_name");
        this.tableGroupName = rs.getString("table_group_name");
        this.scheduleName = rs.getString("schedule_name");
        this.scheduleComment = rs.getString("schedule_comment");
        this.executorType = rs.getString("executor_type");
        this.scheduleContext = rs.getString("schedule_context");
        this.executorContents = rs.getString("executor_contents");
        this.status = rs.getString("status");
        this.scheduleType = rs.getString("schedule_type");
        this.scheduleExpr = rs.getString("schedule_expr");
        this.timeZone = rs.getString("time_zone");
        this.schedulePolicy = rs.getString("schedule_policy");

        this.fireTime = rs.getLong("fire_time");
        this.startTime = rs.getLong("start_time");
        this.finishTime = rs.getLong("finish_time");
        this.state = rs.getString("state");
        this.remark = rs.getString("remark");
        this.result = rs.getString("result");

        return this;
    }

    public long getScheduleId() {
        return this.scheduleId;
    }

    public void setScheduleId(final long scheduleId) {
        this.scheduleId = scheduleId;
    }

    public String getTableSchema() {
        return this.tableSchema;
    }

    public void setTableSchema(final String tableSchema) {
        this.tableSchema = tableSchema;
    }

    public String getTableName() {
        return this.tableName;
    }

    public void setTableName(final String tableName) {
        this.tableName = tableName;
    }

    public String getScheduleName() {
        return this.scheduleName;
    }

    public void setScheduleName(final String scheduleName) {
        this.scheduleName = scheduleName;
    }

    public String getScheduleComment() {
        return this.scheduleComment;
    }

    public void setScheduleComment(final String scheduleComment) {
        this.scheduleComment = scheduleComment;
    }

    public String getExecutorType() {
        return this.executorType;
    }

    public void setExecutorType(final String executorType) {
        this.executorType = executorType;
    }

    public String getScheduleContext() {
        return this.scheduleContext;
    }

    public void setScheduleContext(final String scheduleContext) {
        this.scheduleContext = scheduleContext;
    }

    public String getExecutorContents() {
        return this.executorContents;
    }

    public void setExecutorContents(final String executorContents) {
        this.executorContents = executorContents;
    }

    public String getStatus() {
        return this.status;
    }

    public void setStatus(final String status) {
        this.status = status;
    }

    public String getScheduleType() {
        return this.scheduleType;
    }

    public void setScheduleType(final String scheduleType) {
        this.scheduleType = scheduleType;
    }

    public String getScheduleExpr() {
        return this.scheduleExpr;
    }

    public void setScheduleExpr(final String scheduleExpr) {
        this.scheduleExpr = scheduleExpr;
    }

    public String getTimeZone() {
        return this.timeZone;
    }

    public void setTimeZone(final String timeZone) {
        this.timeZone = timeZone;
    }

    public String getSchedulePolicy() {
        return this.schedulePolicy;
    }

    public void setSchedulePolicy(final String schedulePolicy) {
        this.schedulePolicy = schedulePolicy;
    }

    public long getFireTime() {
        return this.fireTime;
    }

    public void setFireTime(final long fireTime) {
        this.fireTime = fireTime;
    }

    public long getStartTime() {
        return this.startTime;
    }

    public void setStartTime(final long startTime) {
        this.startTime = startTime;
    }

    public long getFinishTime() {
        return this.finishTime;
    }

    public void setFinishTime(final long finishTime) {
        this.finishTime = finishTime;
    }

    public String getState() {
        return this.state;
    }

    public void setState(final String state) {
        this.state = state;
    }

    public String getRemark() {
        return this.remark;
    }

    public void setRemark(final String remark) {
        this.remark = remark;
    }

    public String getResult() {
        return this.result;
    }

    public void setResult(final String result) {
        this.result = result;
    }

    public String getTableGroupName() {
        return tableGroupName;
    }

    public void setTableGroupName(String tableGroupName) {
        this.tableGroupName = tableGroupName;
    }
}