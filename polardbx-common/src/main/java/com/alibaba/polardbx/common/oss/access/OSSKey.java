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

package com.alibaba.polardbx.common.oss.access;

import com.alibaba.polardbx.common.oss.OSSFileType;

import static com.alibaba.polardbx.common.oss.OSSFileType.EXPORT_ORC_FILE;
import static com.alibaba.polardbx.common.oss.OSSFileType.TABLE_FILE;
import static com.alibaba.polardbx.common.oss.OSSFileType.TABLE_FORMAT;
import static com.alibaba.polardbx.common.oss.OSSFileType.TABLE_META;

/**
 * OSS Unique Key and it's local file path
 */
public class OSSKey {
    private static final String FILE_SUFFIX_FORMAT = "%s_%s";
    private final OSSFileType fileType;
    private String schemaName;
    private String tableName;
    private String tableFileId;
    private String suffix;

    private String columnName;
    private long stripeIndex;

    private OSSKey(OSSFileType fileType, String schemaName, String tableName, String tableFileId, String columnName,
                   long stripeIndex) {
        this.fileType = fileType;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.tableFileId = tableFileId;
        this.columnName = columnName;
        this.stripeIndex = stripeIndex;
        this.suffix = fileType.getSuffix();
    }

    public static OSSKey createBloomFilterFileOSSKey(String schemaName, String tableName, String tableFileId,
                                                     String columnName, long stripeIndex) {
        return new OSSKey(TABLE_META, schemaName, tableName, tableFileId, columnName, stripeIndex);
    }

    public static String localMetaPath(String schemaName, String tableName, String tableFileId, String columnName,
                                       long stripeIndex) {
        return String.format(TABLE_META.getLocalPathFormat(), schemaName, tableName, tableFileId, columnName,
            stripeIndex, TABLE_META.getSuffix());
    }

    public static OSSKey createTableFileOSSKey(String schemaName, String tableName, String tableFileId) {
        return new OSSKey(OSSFileType.TABLE_FILE, schemaName, tableName, tableFileId, null, 0);
    }

    public static OSSKey createTableFileOSSKey(String schemaName, String tableName, String fileSuffix,
                                               String tableFileId) {
        return new OSSKey(OSSFileType.TABLE_FILE, schemaName, tableName,
            String.format(FILE_SUFFIX_FORMAT, fileSuffix, tableFileId),
            null, 0);
    }

    public static OSSKey createExportOrcFileOSSKey(String path, String uniqueId) {
        return new OSSKey(OSSFileType.EXPORT_ORC_FILE, path, null, uniqueId,
            null, 0);
    }

    public static String localFilePath(String schemaName, String tableName, String tableFileId) {
        return String.format(TABLE_FILE.getLocalPathFormat(), schemaName, tableName, tableFileId,
            TABLE_FILE.getSuffix());
    }

    public static OSSKey createFormatFileOSSKey(String schemaName, String tableName, String tableFileId) {
        return new OSSKey(OSSFileType.TABLE_FORMAT, schemaName, tableName, tableFileId, null, 0);
    }

    public static String localFormatPath(String schemaName, String tableName) {
        return String.format(TABLE_FORMAT.getLocalPathFormat(), schemaName, tableName, TABLE_FORMAT.getSuffix());
    }

    public String localPath() {
        switch (fileType) {
        case TABLE_FILE:
            return String.format(TABLE_FILE.getLocalPathFormat(), schemaName, tableName, tableFileId,
                TABLE_FILE.getSuffix());
        case TABLE_FORMAT:
            return String.format(TABLE_FORMAT.getLocalPathFormat(), schemaName, tableName, tableFileId,
                TABLE_FORMAT.getSuffix());
        case TABLE_META:
            return String.format(TABLE_META.getLocalPathFormat(), schemaName, tableName, tableFileId, columnName,
                stripeIndex, TABLE_META.getSuffix());
        case EXPORT_ORC_FILE:
            return String.format(EXPORT_ORC_FILE.getLocalPathFormat(), schemaName, EXPORT_ORC_FILE.getSuffix());
        }
        return null;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public String getTableFileId() {
        return tableFileId;
    }

    public String getSuffix() {
        return suffix;
    }

    public OSSFileType getFileType() {
        return fileType;
    }

    public String getColumnName() {
        return columnName;
    }

    public long getStripeIndex() {
        return stripeIndex;
    }

    @Override
    public String toString() {
        switch (fileType) {
        case TABLE_FORMAT:
            return String.format(fileType.getRemotePathFormat(), schemaName, tableName, tableFileId, suffix);
        case TABLE_FILE:
            return String.format(fileType.getRemotePathFormat(), schemaName, tableName, tableFileId, suffix);
        case TABLE_META:
            return String.format(fileType.getRemotePathFormat(), schemaName, tableName, tableFileId, columnName,
                stripeIndex, suffix);
        case EXPORT_ORC_FILE:
            return String.format(fileType.getRemotePathFormat(), schemaName, tableFileId, suffix);
        }
        return super.toString();
    }
}
