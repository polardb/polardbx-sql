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
import com.alibaba.polardbx.common.oss.ColumnarFileType;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TreeMaps;
import com.alibaba.polardbx.gms.metadb.table.FilesRecord;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import org.apache.orc.impl.OrcTail;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

// todo need concurrency-safe
// read-only
public class FileMeta {

    protected final String logicalTableSchema;
    protected final String logicalTableName;

    protected final String physicalTableSchema;
    protected final String physicalTableName;

    protected final String fileName;
    protected final long fileSize;
    protected final long tableRows;

    // number of implicit column BEFORE real physical column
    protected int implicitColumnCnt = 0;
    protected List<ColumnMeta> columnMetas;

    protected Map<String, ColumnMeta> columnMetaMap = TreeMaps.caseInsensitiveMap();
    protected Long commitTs;
    protected AtomicReference<Long> removeTs;
    protected Long schemaTs;
    protected String createTime;
    protected String updateTime;
    protected Engine engine;
    protected Long fileHash;
    protected String partitionName;

    public FileMeta(String logicalSchemaName, String logicalTableName, String physicalTableSchema,
                    String physicalTableName, String partitionName, String fileName, long fileSize,
                    long tableRows, Long commitTs, Long removeTs, Long schemaTs, String createTime, String updateTime,
                    Engine engine, Long fileHash) {
        this.logicalTableSchema = logicalSchemaName;
        this.logicalTableName = logicalTableName;
        this.physicalTableSchema = physicalTableSchema;
        this.physicalTableName = physicalTableName;
        this.partitionName = partitionName;
        this.fileName = fileName;
        this.fileSize = fileSize;
        this.tableRows = tableRows;
        this.commitTs = commitTs;
        this.removeTs = new AtomicReference<>(removeTs);
        this.schemaTs = schemaTs;
        this.createTime = createTime;
        this.updateTime = updateTime;
        this.engine = engine;
        this.fileHash = fileHash;
    }

    public boolean updateRemoveTs(Long updateTs) {
        // This method can only be called by columnar mode.
        return removeTs.compareAndSet(null, updateTs);
    }

    public Long getCommitTs() {
        return commitTs;
    }

    public Long getRemoveTs() {
        return removeTs.get();
    }

    public Long getSchemaTs() {
        return schemaTs;
    }

    public static FileMeta parseFrom(FilesRecord filesRecord) {
        return parseFrom(filesRecord, OSSOrcFileMeta.DEFAULT_FETCH_FUNCTION);
    }

    public static FileMeta parseFrom(FilesRecord filesRecord, Function<String, OrcTail> fetchFunction) {
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
            // Identify the file with its suffix (orc, csv or del).
            String fileName = filesRecord.getFileName();
            String suffix = fileName.substring(fileName.lastIndexOf('.') + 1);
            ColumnarFileType fileType = ColumnarFileType.of(suffix);

            switch (fileType) {
            case ORC:
                return buildOrcFileMeta(filesRecord, fetchFunction);
            case CSV:
                return buildCsvFileMeta(filesRecord);
            case DEL:
                return buildDelFileMeta(filesRecord);
            case SET:
                return buildSetFileMeta(filesRecord);
            }
        }
        default:
            GeneralUtil.nestedException("Unsupported file format with engine " + engineStr);
        }
        return null;
    }

    private static OSSDelFileMeta buildDelFileMeta(FilesRecord filesRecord) {
        String engineStr = filesRecord.engine;
        Engine engine = Engine.of(engineStr);
        String fileName = filesRecord.getFileName();
        String logicalSchemaName = filesRecord.getLogicalSchemaName();
        String logicalTableName = filesRecord.getLogicalTableName();
        String physicalTableSchema = filesRecord.getTableSchema();
        String physicalTableName = filesRecord.getTableName();
        String partitionName = filesRecord.getPartitionName();

        long fileSize = filesRecord.getExtentSize();
        long tableRows = filesRecord.getTableRows();
        String createTime = filesRecord.getCreateTime();
        String updateTime = filesRecord.getUpdateTime();

        Long commitTs = filesRecord.getCommitTs();
        Long removeTs = filesRecord.getRemoveTs();
        Long schemaTs = filesRecord.getSchemaTs();
        Long fileHash = filesRecord.getFileHash();
        return new OSSDelFileMeta(
            logicalSchemaName, logicalTableName, physicalTableSchema, physicalTableName, partitionName, fileName,
            fileSize, tableRows, commitTs, removeTs, schemaTs, createTime, updateTime, engine, fileHash
        );
    }

    private static OSSCsvFileMeta buildCsvFileMeta(FilesRecord filesRecord) {
        String engineStr = filesRecord.engine;
        Engine engine = Engine.of(engineStr);
        String fileName = filesRecord.getFileName();
        String logicalSchemaName = filesRecord.getLogicalSchemaName();
        String logicalTableName = filesRecord.getLogicalTableName();
        String physicalTableSchema = filesRecord.getTableSchema();
        String physicalTableName = filesRecord.getTableName();
        String partitionName = filesRecord.getPartitionName();

        long fileSize = filesRecord.getExtentSize();
        long tableRows = filesRecord.getTableRows();
        String createTime = filesRecord.getCreateTime();
        String updateTime = filesRecord.getUpdateTime();

        Long commitTs = filesRecord.getCommitTs();
        Long removeTs = filesRecord.getRemoveTs();
        Long schemaTs = filesRecord.getSchemaTs();
        Long fileHash = filesRecord.getFileHash();
        return new OSSCsvFileMeta(
            logicalSchemaName, logicalTableName, physicalTableSchema, physicalTableName, partitionName, fileName,
            fileSize, tableRows, commitTs, removeTs, schemaTs, createTime, updateTime, engine, fileHash
        );
    }

    private static OSSSetFileMeta buildSetFileMeta(FilesRecord filesRecord) {
        String engineStr = filesRecord.engine;
        Engine engine = Engine.of(engineStr);
        String fileName = filesRecord.getFileName();
        String logicalSchemaName = filesRecord.getLogicalSchemaName();
        String logicalTableName = filesRecord.getLogicalTableName();
        String physicalTableSchema = filesRecord.getTableSchema();
        String physicalTableName = filesRecord.getTableName();
        String partitionName = filesRecord.getPartitionName();

        long fileSize = filesRecord.getExtentSize();
        long tableRows = filesRecord.getTableRows();
        String createTime = filesRecord.getCreateTime();
        String updateTime = filesRecord.getUpdateTime();

        Long commitTs = filesRecord.getCommitTs();
        Long removeTs = filesRecord.getRemoveTs();
        Long schemaTs = filesRecord.getSchemaTs();
        Long fileHash = filesRecord.getFileHash();
        return new OSSSetFileMeta(
            logicalSchemaName, logicalTableName, physicalTableSchema, physicalTableName, partitionName, fileName,
            fileSize, tableRows, commitTs, removeTs, schemaTs, createTime, updateTime, engine, fileHash
        );
    }

    private static OSSOrcFileMeta buildOrcFileMeta(FilesRecord filesRecord, Function<String, OrcTail> fetchFunction) {
        String engineStr = filesRecord.engine;
        Engine engine = Engine.of(engineStr);
        String fileName = filesRecord.getFileName();
        String logicalSchemaName = filesRecord.getLogicalSchemaName();
        String logicalTableName = filesRecord.getLogicalTableName();
        String physicalTableSchema = filesRecord.getTableSchema();
        String physicalTableName = filesRecord.getTableName();
        String partitionName = filesRecord.getPartitionName();

        long fileSize = filesRecord.getExtentSize();
        long tableRows = filesRecord.getTableRows();
        String createTime = filesRecord.getCreateTime();
        String updateTime = filesRecord.getUpdateTime();

        Long commitTs = filesRecord.getCommitTs();
        Long removeTs = filesRecord.getRemoveTs();
        Long schemaTs = filesRecord.getSchemaTs();
        Long fileHash = filesRecord.getFileHash();
        return new OSSOrcFileMeta(
            logicalSchemaName, logicalTableName, physicalTableSchema, physicalTableName, partitionName, fileName,
            fileSize, tableRows, fetchFunction, createTime, updateTime, engine, commitTs, removeTs, schemaTs, fileHash);
    }

    public List<ColumnMeta> getColumnMetas() {
        return columnMetas;
    }

    // todo get column metas of that tso
    public void initColumnMetas(TableMeta tableMeta) {
        this.columnMetas = tableMeta.getPhysicalColumns();
        if (tableMeta.isOldFileStorage()) {
            for (ColumnMeta columnMeta : tableMeta.getPhysicalColumns()) {
                this.columnMetaMap.put(columnMeta.getName(), columnMeta);
            }
        }
    }

    public void setImplicitColumnCnt(int implicitColumnCnt) {
        this.implicitColumnCnt = implicitColumnCnt;
    }

    public int getImplicitColumnCnt() {
        return implicitColumnCnt;
    }

    public void initColumnMetas(int implicitColumnCnt, List<ColumnMeta> physicalColumns) {
        setImplicitColumnCnt(implicitColumnCnt);
        this.columnMetas = physicalColumns;
        for (ColumnMeta columnMeta : physicalColumns) {
            this.columnMetaMap.put(columnMeta.getName(), columnMeta);
        }
    }

    // todo get column metas of that tso
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

    public String getPartitionName() {
        return partitionName;
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

    public Engine getEngine() {
        return engine;
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

    // todo(shengyu): don't store file meta in table meta
    public static Map<String, List<FileMeta>> getFlatFileMetas(TableMeta tableMeta) {
        return tableMeta.getFlatFileMetas();
    }

    public TableMeta getTableMeta(ExecutionContext ec) {
        return ec.getSchemaManager(logicalTableSchema).getTable(logicalTableName);
    }
}
