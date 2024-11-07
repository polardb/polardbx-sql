package com.alibaba.polardbx.gms.metadb.table;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

/**
 * Data of information_schema.tables, but only for SHOW DAL
 */
public class ShowTablesSchemaRecord implements SystemTableRecord {

    public long id;
    public String tableSchema;
    public String tableName;
    public String tableType;
    public String engine;
    public long version;
    public String rowFormat;
    public long tableRows;
    public long avgRowLength;
    public long dataLength;
    public long maxDataLength;
    public long indexLength;
    public long dataFree;
    public long autoIncrement;
    public String createTime;
    public String updateTime;
    public String checkTime;
    public String tableCollation;
    public long checkSum;
    public String createOptions;
    public String tableComment;

    @Override
    public ShowTablesSchemaRecord fill(ResultSet rs) throws SQLException {
        try {
            this.id = rs.getLong("id");
        } catch (Exception ignored) {
        }
        this.tableSchema = rs.getString("table_schema");
        this.tableName = rs.getString("table_name");
        this.tableType = rs.getString("table_type");
        this.engine = rs.getString("engine");
        this.version = rs.getLong("version");
        this.rowFormat = rs.getString("row_format");
        this.tableRows = rs.getLong("table_rows");
        this.avgRowLength = rs.getLong("avg_row_length");
        this.dataLength = rs.getLong("data_length");
        this.maxDataLength = rs.getLong("max_data_length");
        this.indexLength = rs.getLong("index_length");
        this.dataFree = rs.getLong("data_free");
        this.autoIncrement = rs.getLong("auto_increment");
        this.createTime = rs.getString("create_time");
        this.updateTime = rs.getString("update_time");
        this.checkTime = rs.getString("check_time");
        this.tableCollation = rs.getString("table_collation");
        this.checkSum = rs.getLong("checksum");
        this.createOptions = rs.getString("create_options");
        this.tableComment = rs.getString("table_comment");
        return this;
    }

    private void setCommonParams(Map<Integer, ParameterContext> params, int index) {
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.tableName);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.tableType);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.engine);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.version);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.rowFormat);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.tableRows);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.avgRowLength);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.dataLength);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.maxDataLength);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.indexLength);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.dataFree);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.autoIncrement);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.createTime);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.updateTime);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.checkTime);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.tableCollation);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.checkSum);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.createOptions);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.tableComment);
    }

    @Override
    public String toString() {
        return "ShowTablesSchemaRecord{" +
            "id=" + id +
            ", tableSchema='" + tableSchema + '\'' +
            ", tableName='" + tableName + '\'' +
            ", tableType='" + tableType + '\'' +
            ", engine='" + engine + '\'' +
            ", version=" + version +
            ", rowFormat='" + rowFormat + '\'' +
            ", tableRows=" + tableRows +
            ", avgRowLength=" + avgRowLength +
            ", dataLength=" + dataLength +
            ", maxDataLength=" + maxDataLength +
            ", indexLength=" + indexLength +
            ", dataFree=" + dataFree +
            ", autoIncrement=" + autoIncrement +
            ", createTime=" + createTime + '\'' +
            ", updateTime=" + updateTime + '\'' +
            ", checkTime=" + checkTime + '\'' +
            ", tableCollation='" + tableCollation + '\'' +
            ", checkSum=" + checkSum +
            ", createOptions='" + createOptions + '\'' +
            ", tableComment='" + tableComment + '\'' +
            '}';
    }

}
