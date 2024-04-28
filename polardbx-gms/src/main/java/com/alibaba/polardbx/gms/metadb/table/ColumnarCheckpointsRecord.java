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
 * Record wrapper for table: columnar_checkpoints
 */
@Data
public class ColumnarCheckpointsRecord implements SystemTableRecord {
    public long id;
    public String logicalSchema;
    public String logicalTable;
    public String partitionName;
    public long checkpointTso;
    public String offset;
    public String checkpointType;
    public String extra;
    public String createTime;
    public String updateTime;

    @Override
    public ColumnarCheckpointsRecord fill(ResultSet rs) throws SQLException {
        this.id = rs.getLong("id");
        this.logicalSchema = rs.getString("logical_schema");
        this.logicalTable = rs.getString("logical_table");
        this.partitionName = rs.getString("partition_name");
        this.checkpointTso = rs.getLong("checkpoint_tso");
        this.offset = rs.getString("offset");
        this.checkpointType = rs.getString("checkpoint_type");
        this.extra = rs.getString("extra");
        this.createTime = rs.getString("create_time");
        this.updateTime = rs.getString("update_time");
        return this;
    }

    public Map<Integer, ParameterContext> buildInsertParams() {
        Map<Integer, ParameterContext> params = new HashMap<>(8);
        int index = 0;
        // skip auto increment primary-index
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.logicalSchema);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.logicalTable);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.partitionName);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.checkpointTso);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.offset);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.checkpointType);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.extra);
        // skip automatically updated column: create_time and update_time
        return params;
    }
}
