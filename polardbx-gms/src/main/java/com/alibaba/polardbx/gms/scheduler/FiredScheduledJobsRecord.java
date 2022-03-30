package com.alibaba.polardbx.gms.scheduler;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class FiredScheduledJobsRecord implements SystemTableRecord {

    private long    scheduleId;
    private String  tableSchema;
    private String  tableName;
    private long    fireTime;
    private long    startTime;
    private long    finishTime;
    private String  state;
    private String  remark;
    private String  result;

    @Override
    public FiredScheduledJobsRecord fill(ResultSet rs) throws SQLException {

        this.scheduleId = rs.getLong("schedule_id");
        this.tableSchema = rs.getString("table_schema");
        this.tableName = rs.getString("table_name");
        this.fireTime = rs.getLong("fire_time");
        this.startTime = rs.getLong("start_time");
        this.finishTime = rs.getLong("finish_time");
        this.state = rs.getString("state");
        this.remark = rs.getString("remark");
        this.result = rs.getString("result");

        return this;
    }

    public Map<Integer, ParameterContext> buildParams() {
        Map<Integer, ParameterContext> params = new HashMap<>(16);
        int index = 0;
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong,   this.scheduleId);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.tableSchema);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.tableName);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong,  this.fireTime);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong,  this.startTime);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong,  this.finishTime);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.state);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.remark);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.result);
        return params;
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
}