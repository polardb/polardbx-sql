package com.alibaba.polardbx.gms.metadb.misc;

/**
 * Created by luoyanxin.
 */

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Represents the metadata for a task importing table space information statistics.
 */
public class ImportTableSpaceInfoStatRecord implements SystemTableRecord {

    /**
     * Unique identifier for the import task.
     */
    private long taskId;

    /**
     * Database schema where the table belongs.
     */
    private String tableSchema;

    /**
     * Name of the table being imported.
     */
    private String tableName;

    /**
     * Group key representing the physical database.
     */
    private String physicalDb;

    /**
     * Physical table name in the storage system.
     */
    private String physicalTable;

    /**
     * Size of the ibd file in bytes.
     */
    private long dataSize;

    /**
     * Start time of the import task.
     */
    private long startTime;

    /**
     * End time of the import task, updated on completion or update.
     */
    private long endTime;

    public long getTaskId() {
        return taskId;
    }

    public void setTaskId(long taskId) {
        this.taskId = taskId;
    }

    public String getTableSchema() {
        return tableSchema;
    }

    public void setTableSchema(String tableSchema) {
        this.tableSchema = tableSchema;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getPhysicalDb() {
        return physicalDb;
    }

    public void setPhysicalDb(String physicalDb) {
        this.physicalDb = physicalDb;
    }

    public String getPhysicalTable() {
        return physicalTable;
    }

    public void setPhysicalTable(String physicalTable) {
        this.physicalTable = physicalTable;
    }

    public long getDataSize() {
        return dataSize;
    }

    public void setDataSize(long dataSize) {
        this.dataSize = dataSize;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    @Override
    public ImportTableSpaceInfoStatRecord fill(ResultSet rs) throws SQLException {
        this.taskId = rs.getLong("task_id");
        this.tableSchema = rs.getString("table_schema");
        this.tableName = rs.getString("table_name");
        this.physicalDb = rs.getString("physical_db");
        this.physicalTable = rs.getString("physical_table");
        this.dataSize = rs.getLong("data_size");
        this.startTime = rs.getLong("start_time");
        this.endTime = rs.getLong("end_time");
        return this;
    }

    public Map<Integer, ParameterContext> buildParams() {
        Map<Integer, ParameterContext> params = new HashMap<>(16);
        int index = 0;
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.taskId);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.tableSchema);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.tableName);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.physicalDb);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.physicalTable);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.dataSize);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.startTime);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.endTime);
        return params;
    }
}
