package com.alibaba.polardbx.gms.metadb.table;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import lombok.Data;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * Record wrapper for table: columnar_appended_files
 */
@Data
public class ColumnarAppendedFilesRecord implements SystemTableRecord {
    public long id;
    public long checkpointTso;
    public String logicalSchema;
    /**
     * After supporting DDL, logicalTable here means table_id
     */
    public String logicalTable;
    public String physicalSchema;
    public String physicalTable;
    public String partName;
    public String fileName;
    public String fileType;
    public long fileLength;
    public String engine;
    public long appendOffset;
    public long appendLength;
    public long totalRows;
    public String createTime;
    public String updateTime;

    @Override
    public ColumnarAppendedFilesRecord fill(ResultSet rs) throws SQLException {
        this.id = rs.getLong("id");
        this.checkpointTso = rs.getLong("checkpoint_tso");
        this.logicalSchema = rs.getString("logical_schema");
        this.logicalTable = rs.getString("logical_table");
        this.physicalSchema = rs.getString("physical_schema");
        this.physicalTable = rs.getString("physical_table");
        this.partName = rs.getString("part_name");
        this.fileName = rs.getString("file_name");
        this.fileType = rs.getString("file_type");
        this.fileLength = rs.getLong("file_length");
        this.engine = rs.getString("engine");
        this.appendOffset = rs.getLong("append_offset");
        this.appendLength = rs.getLong("append_length");
        this.totalRows = rs.getLong("total_rows");
        this.createTime = rs.getString("create_time");
        this.updateTime = rs.getString("update_time");
        return this;
    }

    public Map<Integer, ParameterContext> buildInsertParams() {
        Map<Integer, ParameterContext> params = new HashMap<>(16);
        int index = 0;
        // skip auto increment primary-index
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.checkpointTso);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.logicalSchema);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.logicalTable);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.physicalSchema);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.physicalTable);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.partName);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.fileName);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.fileType);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.fileLength);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.engine);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.appendOffset);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.appendLength);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.totalRows);
        // skip automatically updated column: create_time and update_time
        return params;
    }
}
