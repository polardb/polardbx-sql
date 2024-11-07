package com.alibaba.polardbx.gms.metadb.table;

import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;
import lombok.Data;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * 和FilesRecord类似，但是不包含file_meta列，主要是为了查fileInfo，file_meta列太长了
 */
@Data
public class FileInfoRecord implements SystemTableRecord {

    public long fileId;
    public String fileName;
    public String fileType;
    public String tablespaceName;
    public String tableCatalog;
    public String tableSchema;
    public String tableName;
    public String logfileGroupName;
    public long logfileGroupNumber;
    public String engine;
    public String fulltextKeys;
    public long deletedRows;
    public long updateCount;
    public long freeExtents;
    public long totalExtents;
    public long extentSize;
    public long initialSize;
    public long maximumSize;
    public long autoextendSize;
    public String creationTime;
    public String lastUpdateTime;
    public String lastAccessTime;
    public long recoverTime;
    public long transactionCounter;
    public long version;
    public String rowFormat;
    public long tableRows;
    public long avgRowLength;
    public long dataLength;
    public long maxDataLength;
    public long indexLength;
    public long dataFree;
    public String createTime;
    public String updateTime;
    public String checkTime;
    public long checksum;
    public Long deletedChecksum;
    public String status;
    public String extra;
    public long taskId;
    public long lifeCycle;
    public String localPath;
    public String logicalSchemaName;
    /**
     * After supporting DDL, logicalTableName here means table id
     */
    public String logicalTableName;
    public Long commitTs;
    public Long removeTs;
    public Long fileHash;
    public String localPartitionName;
    public String partitionName;
    public Long schemaTs;

    @Override
    public FileInfoRecord fill(ResultSet rs) throws SQLException {
        this.fileId = rs.getLong("file_id");
        this.fileName = rs.getString("file_name");
        this.fileType = rs.getString("file_type");
        this.tablespaceName = rs.getString("tablespace_name");
        this.tableCatalog = rs.getString("table_catalog");
        this.tableSchema = rs.getString("table_schema");
        this.tableName = rs.getString("table_name");
        this.logfileGroupName = rs.getString("logfile_group_name");
        this.logfileGroupNumber = rs.getLong("logfile_group_number");
        this.engine = rs.getString("engine");
        this.fulltextKeys = rs.getString("fulltext_keys");
        this.deletedRows = rs.getLong("deleted_rows");
        this.updateCount = rs.getLong("update_count");
        this.freeExtents = rs.getLong("free_extents");
        this.totalExtents = rs.getLong("total_extents");
        this.extentSize = rs.getLong("extent_size");
        this.initialSize = rs.getLong("initial_size");
        this.maximumSize = rs.getLong("maximum_size");
        this.autoextendSize = rs.getLong("autoextend_size");
        this.creationTime = rs.getString("creation_time");
        this.lastUpdateTime = rs.getString("last_update_time");
        this.lastAccessTime = rs.getString("last_access_time");
        this.recoverTime = rs.getLong("recover_time");
        this.transactionCounter = rs.getLong("transaction_counter");
        this.version = rs.getLong("version");
        this.rowFormat = rs.getString("row_format");
        this.tableRows = rs.getLong("table_rows");
        this.avgRowLength = rs.getLong("avg_row_length");
        this.dataLength = rs.getLong("data_length");
        this.maxDataLength = rs.getLong("max_data_length");
        this.indexLength = rs.getLong("index_length");
        this.dataFree = rs.getLong("data_free");
        this.createTime = rs.getString("create_time");
        this.updateTime = rs.getString("update_time");
        this.checkTime = rs.getString("check_time");
        this.checksum = rs.getLong("checksum");
        this.deletedChecksum = rs.getLong("deleted_checksum");
        this.status = rs.getString("status");
        this.extra = rs.getString("extra");
        this.taskId = rs.getLong("task_id");
        this.lifeCycle = rs.getLong("life_cycle");
        this.localPath = rs.getString("local_path");
        this.logicalSchemaName = rs.getString("logical_schema_name");
        this.logicalTableName = rs.getString("logical_table_name");
        this.commitTs = rs.getLong("commit_ts");
        if (rs.wasNull()) {
            this.commitTs = null;
        }
        this.removeTs = rs.getLong("remove_ts");
        if (rs.wasNull()) {
            this.removeTs = null;
        }
        this.fileHash = rs.getLong("file_hash");
        this.localPartitionName = rs.getString("local_partition_name");
        this.partitionName = rs.getString("partition_name");
        this.schemaTs = rs.getLong("schema_ts");
        if (rs.wasNull()) {
            this.schemaTs = null;
        }
        return this;
    }
}
