package com.alibaba.polardbx.gms.metadb.table;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import lombok.Data;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

/**
 * Record wrapper for table: spm_baseline/spm_plan
 */
@Data
public class BaselineInfoRecord implements SystemTableRecord {

    // common field for baseline and plan
    public String instId;
    public String schemaName;

    // baseline info
    public int id;
    public String sql;
    public String tableSet;
    public String extendField;

    // plan info
    public int tablesHashCode;
    public int planId;
    public String plan;
    public long lastExecuteTime;
    public int chooseCount;
    public double cost;
    public double estimateExecutionTime;
    public boolean fixed;
    public String traceId;
    public long createTime;
    public String origin;
    public String planExtend;

    @Override
    public BaselineInfoRecord fill(ResultSet rs) throws SQLException {
        this.id = rs.getInt("BASELINE_INFO.ID");
        this.schemaName = rs.getString("BASELINE_INFO.SCHEMA_NAME");
        this.sql = rs.getString("BASELINE_INFO.SQL");
        this.tableSet = rs.getString("BASELINE_INFO.TABLE_SET");
        this.extendField = rs.getString("BASELINE_INFO.EXTEND_FIELD");

        this.tablesHashCode = rs.getInt("PLAN_INFO.TABLES_HASHCODE");
        this.planId = rs.getInt("PLAN_INFO.ID");
        this.plan = rs.getString("PLAN_INFO.PLAN");
        this.lastExecuteTime = rs.getLong("UNIX_TIMESTAMP(PLAN_INFO.LAST_EXECUTE_TIME)");
        this.chooseCount = rs.getInt("PLAN_INFO.CHOOSE_COUNT");
        this.cost = rs.getDouble("PLAN_INFO.COST");
        this.estimateExecutionTime = rs.getDouble("PLAN_INFO.ESTIMATE_EXECUTION_TIME");
        this.fixed = rs.getBoolean("PLAN_INFO.FIXED");
        this.traceId = rs.getString("PLAN_INFO.TRACE_ID");
        this.createTime = rs.getLong("UNIX_TIMESTAMP(PLAN_INFO.GMT_CREATED)");
        this.origin = rs.getString("PLAN_INFO.ORIGIN");
        this.planExtend = rs.getString("PLAN_EXTEND");
        return this;
    }

    public Map<Integer, ParameterContext> buildInsertParamsForBaseline() {
        Map<Integer, ParameterContext> params = new HashMap<>(16);
        int index = 0;
        // skip auto increment primary-index
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.schemaName);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.instId);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setInt, this.id);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.sql);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.tableSet);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.extendField);
        // skip automatically updated column: create_time and update_time
        return params;
    }

    public Map<Integer, ParameterContext> buildInsertParamsForPlan() {
        Map<Integer, ParameterContext> params = new HashMap<>(16);
        int index = 0;
        // skip auto increment primary-index
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.instId);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.schemaName);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setInt, this.planId);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setInt, this.id);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setTimestamp1,
            this.lastExecuteTime == -1 ? null : new Timestamp(this.lastExecuteTime * 1000));
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.plan);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setInt, this.chooseCount);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setDouble, this.cost);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setDouble, this.estimateExecutionTime);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setBoolean, true);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setBoolean, this.fixed);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.traceId);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.origin);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setInt, this.tablesHashCode);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.planExtend);
        // skip automatically updated column: create_time and update_time
        return params;
    }
}
