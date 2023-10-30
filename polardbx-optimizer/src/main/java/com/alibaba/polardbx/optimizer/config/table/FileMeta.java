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

package com.alibaba.polardbx.optimizer.config.table;

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TreeMaps;
import com.alibaba.polardbx.gms.metadb.table.FilesRecord;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public class FileMeta {
    protected String logicalTableSchema;
    protected String logicalTableName;

    protected String physicalTableSchema;
    protected String physicalTableName;

    protected String fileName;
    protected long fileSize;
    protected long tableRows;

    protected Long commitTs;

    protected Map<String, ColumnMeta> columnMetaMap = TreeMaps.caseInsensitiveMap();

    public FileMeta(String logicalSchemaName, String logicalTableName, String physicalTableSchema,
                    String physicalTableName, String fileName, long fileSize,
                    long tableRows, Long commitTs) {
        this.logicalTableSchema = logicalSchemaName;
        this.logicalTableName = logicalTableName;
        this.physicalTableSchema = physicalTableSchema;
        this.physicalTableName = physicalTableName;
        this.fileName = fileName;
        this.fileSize = fileSize;
        this.tableRows = tableRows;
        this.commitTs = commitTs;
    }

    public static FileMeta parseFrom(FilesRecord filesRecord) {
        String engineStr = filesRecord.engine;
        Engine engine = Engine.of(engineStr);
        if (engine == null) {
            GeneralUtil.nestedException("Unknown engine: " + engineStr);
        }
        switch (engine) {
        case OSS:
        case S3:
        case LOCAL_DISK:
        case EXTERNAL_DISK:
        case NFS: {
            String logicalSchemaName = filesRecord.getLogicalSchemaName();
            String logicalTableName = filesRecord.getLogicalTableName();
            String physicalTableSchema = filesRecord.getTableSchema();
            String physicalTableName = filesRecord.getTableName();
            String fileName = filesRecord.getFileName();
            long fileSize = filesRecord.getExtentSize();
            long tableRows = filesRecord.getTableRows();
            String createTime = filesRecord.getCreateTime();
            String updateTime = filesRecord.getUpdateTime();
            ByteBuffer orcTailBuffer = ByteBuffer.wrap(filesRecord.getFileMeta());
            Long commitTs = filesRecord.getCommitTs();
            Long removeTs = filesRecord.getRemoveTs();
            Long fileHash = filesRecord.getFileHash();
            return new OSSOrcFileMeta(logicalSchemaName, logicalTableName, physicalTableSchema, physicalTableName,
                fileName, fileSize, tableRows,
                orcTailBuffer, createTime, updateTime, engine, commitTs, removeTs, fileHash);
        }
        default:
            GeneralUtil.nestedException("Unsupported file format with engine " + engineStr);
        }
        return null;
    }

    public void initColumnMetas(TableMeta tableMeta) {
        if (tableMeta.isOldFileStorage()) {
            for (ColumnMeta columnMeta : tableMeta.getPhysicalColumns()) {
                this.columnMetaMap.put(columnMeta.getName(), columnMeta);
            }
        }
    }

    public Map<String, ColumnMeta> getColumnMetaMap() {
        return columnMetaMap;
    }

    public String getLogicalTableSchema() {
        return logicalTableSchema;
    }

    public String getLogicalTableName() {
        return logicalTableName;
    }

    public String getPhysicalTableSchema() {
        return physicalTableSchema;
    }

    public String getPhysicalTableName() {
        return physicalTableName;
    }

    public String getFileName() {
        return fileName;
    }

    public long getFileSize() {
        return fileSize;
    }

    public long getTableRows() {
        return tableRows;
    }

    public void setLogicalTableSchema(String logicalTableSchema) {
        this.logicalTableSchema = logicalTableSchema;
    }

    public void setLogicalTableName(String logicalTableName) {
        this.logicalTableName = logicalTableName;
    }

    @Override
    public String toString() {
        return "FileMeta{" +
            "logicalTableSchema='" + logicalTableSchema + '\'' +
            ", logicalTableName='" + logicalTableName + '\'' +
            ", physicalTableSchema='" + physicalTableSchema + '\'' +
            ", physicalTableName='" + physicalTableName + '\'' +
            ", fileName='" + fileName + '\'' +
            ", fileSize=" + fileSize +
            ", tableRows=" + tableRows +
            '}';
    }

    public Long getCommitTs() {
        return commitTs;
    }

    // todo(shengyu): don't store file meta in table meta
    public static Map<String, List<FileMeta>> getFlatFileMetas(TableMeta tableMeta) {
        return tableMeta.getFlatFileMetas();
    }

    public TableMeta getTableMeta(ExecutionContext ec) {
        return ec.getSchemaManager(logicalTableSchema).getTable(logicalTableName);
    }
}
