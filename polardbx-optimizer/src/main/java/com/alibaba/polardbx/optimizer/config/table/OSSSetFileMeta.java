package com.alibaba.polardbx.optimizer.config.table;

import com.alibaba.polardbx.common.Engine;

/**
 * File meta for table-file with suffix .del
 */
public class OSSSetFileMeta extends FileMeta {
    public OSSSetFileMeta(String logicalSchemaName, String logicalTableName, String physicalTableSchema,
                          String physicalTableName, String partitionName, String fileName,
                          long fileSize, long tableRows, Long commitTs, Long removeTs, Long schemaTs,
                          String createTime, String updateTime, Engine engine, Long fileHash) {
        super(logicalSchemaName, logicalTableName, physicalTableSchema, physicalTableName, partitionName, fileName,
            fileSize, tableRows, commitTs, removeTs, schemaTs, createTime, updateTime, engine, fileHash);
    }
}
