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

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.time.core.OriginalTimestamp;
import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class ScheduledJobsRecord implements SystemTableRecord {

    private long scheduleId;
    private Date createTime;
    private Date updateTime;
    private String tableSchema;
    private String tableName;
    private String scheduleName;
    private String scheduleComment;
    private String executorType;
    private String scheduleContext;
    private String executorContents;
    private String status;
    private String scheduleType;
    private String scheduleExpr;
    private String timeZone;
    private long lastFireTime;
    private long nextFireTime;
    private long starts;
    private long ends;
    private String schedulePolicy;
    private String tableGroupName;

    @Override
    public ScheduledJobsRecord fill(ResultSet rs) throws SQLException {

        this.scheduleId = rs.getLong("schedule_id");
        this.createTime = rs.getTimestamp("create_time");
        this.updateTime = rs.getTimestamp("update_time");
        this.tableSchema = rs.getString("table_schema");
        this.tableName = rs.getString("table_name");
        this.scheduleName = rs.getString("schedule_name");
        this.scheduleComment = rs.getString("schedule_comment");
        this.executorType = rs.getString("executor_type");
        this.scheduleContext = rs.getString("schedule_context");
        this.executorContents = rs.getString("executor_contents");
        this.status = rs.getString("status");
        this.scheduleType = rs.getString("schedule_type");
        this.scheduleExpr = rs.getString("schedule_expr");
        this.timeZone = rs.getString("time_zone");
        this.lastFireTime = rs.getLong("last_fire_time");
        this.nextFireTime = rs.getLong("next_fire_time");
        this.starts = rs.getLong("starts");
        this.ends = rs.getLong("ends");
        this.schedulePolicy = rs.getString("schedule_policy");
        this.tableGroupName = rs.getString("table_group_name");

        return this;
    }

    public Map<Integer, ParameterContext> buildParams() {
        Map<Integer, ParameterContext> params = new HashMap<>(16);
        int index = 0;
//        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.scheduleId);
//        MetaDbUtil.setParameter(++index, params, ParameterMethod.setTimestamp1, this.createTime);
//        MetaDbUtil.setParameter(++index, params, ParameterMethod.setTimestamp1, this.updateTime);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.tableSchema);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.tableName);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.scheduleName);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.scheduleComment);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.executorType);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.scheduleContext);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.executorContents);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.status);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.scheduleType);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.scheduleExpr);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.timeZone);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.lastFireTime);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.nextFireTime);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.starts);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.ends);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.schedulePolicy);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.tableGroupName);
        return params;
    }

    public long getScheduleId() {
        return this.scheduleId;
    }

    public void setScheduleId(final long scheduleId) {
        this.scheduleId = scheduleId;
    }

    public Date getCreateTime() {
        return this.createTime;
    }

    public void setCreateTime(final Date createTime) {
        this.createTime = createTime;
    }

    public Date getUpdateTime() {
        return this.updateTime;
    }

    public void setUpdateTime(final Date updateTime) {
        this.updateTime = updateTime;
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

    public long getLastFireTime() {
        return this.lastFireTime;
    }

    public void setLastFireTime(final long lastFireTime) {
        this.lastFireTime = lastFireTime;
    }

    public long getNextFireTime() {
        return this.nextFireTime;
    }

    public void setNextFireTime(final long nextFireTime) {
        this.nextFireTime = nextFireTime;
    }

    public long getStarts() {
        return this.starts;
    }

    public void setStarts(final long starts) {
        this.starts = starts;
    }

    public long getEnds() {
        return this.ends;
    }

    public void setEnds(final long ends) {
        this.ends = ends;
    }

    public String getSchedulePolicy() {
        return this.schedulePolicy;
    }

    public void setSchedulePolicy(final String schedulePolicy) {
        this.schedulePolicy = schedulePolicy;
    }

    public String getTableGroupName() {
        return tableGroupName;
    }

    public void setTableGroupName(String tableGroupName) {
        this.tableGroupName = tableGroupName;
    }

    public String toString() {
        return executorType + "," + getStatus() + "," + getScheduleComment();
    }
}