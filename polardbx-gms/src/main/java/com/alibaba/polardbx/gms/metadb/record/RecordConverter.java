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

package com.alibaba.polardbx.gms.metadb.record;

import com.alibaba.polardbx.common.utils.version.InstanceVersion;
import com.alibaba.polardbx.gms.metadb.table.ColumnStatus;
import com.alibaba.polardbx.gms.metadb.table.ColumnsAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnsInfoSchemaRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnsRecord;
import com.alibaba.polardbx.gms.metadb.table.IndexStatus;
import com.alibaba.polardbx.gms.metadb.table.IndexesInfoSchemaRecord;
import com.alibaba.polardbx.gms.metadb.table.IndexesRecord;
import com.alibaba.polardbx.gms.metadb.table.TableStatus;
import com.alibaba.polardbx.gms.metadb.table.TablesInfoSchemaRecord;
import com.alibaba.polardbx.gms.metadb.table.TablesRecord;
import com.alibaba.polardbx.rpc.result.XResultUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class RecordConverter {

    public static final String SEMICOLON = ";";
    public static final String EMPTY_CONTENT = "";

    public static TablesRecord convertTable(TablesInfoSchemaRecord infoSchemaRecord, String logicalTableSchema,
                                            String logicalTableName) {
        TablesRecord record = new TablesRecord();
        record.tableSchema = logicalTableSchema;
        record.tableName = logicalTableName;
        record.newTableName = EMPTY_CONTENT;
        record.tableType = infoSchemaRecord.tableType;
        record.engine = infoSchemaRecord.engine;
        record.version = 1;
        record.rowFormat = infoSchemaRecord.rowFormat;
        record.tableRows = infoSchemaRecord.tableRows;
        record.avgRowLength = infoSchemaRecord.avgRowLength;
        record.dataLength = infoSchemaRecord.dataLength;
        record.maxDataLength = infoSchemaRecord.maxDataLength;
        record.indexLength = infoSchemaRecord.indexLength;
        record.dataFree = infoSchemaRecord.dataFree;
        record.autoIncrement = infoSchemaRecord.autoIncrement;
        record.tableCollation = infoSchemaRecord.tableCollation;
        record.checkSum = infoSchemaRecord.checkSum;
        record.createOptions = infoSchemaRecord.createOptions;
        record.tableComment = infoSchemaRecord.tableComment;
        record.status = TableStatus.ABSENT.getValue();
        record.flag = 0;
        return record;
    }

    public static List<ColumnsRecord> convertColumn(List<ColumnsInfoSchemaRecord> infoSchemaRecords,
                                                    Map<String, Map<String, Object>> jdbcExtInfo,
                                                    String logicalTableSchema, String logicalTableName) {
        List<ColumnsRecord> records = new ArrayList<>(infoSchemaRecords.size());
        for (ColumnsInfoSchemaRecord infoSchemaRecord : infoSchemaRecords) {
            ColumnsRecord record = new ColumnsRecord();
            record.tableSchema = logicalTableSchema;
            record.tableName = logicalTableName;
            record.columnName = infoSchemaRecord.columnName;
            record.ordinalPosition = infoSchemaRecord.ordinalPosition;
            record.columnDefault = infoSchemaRecord.columnDefault;
            record.isNullable = infoSchemaRecord.isNullable;
            record.dataType = infoSchemaRecord.dataType;
            record.characterMaximumLength = infoSchemaRecord.characterMaximumLength;
            record.characterOctetLength = infoSchemaRecord.characterOctetLength;
            record.numericPrecision = infoSchemaRecord.numericPrecision;
            record.numericScale = infoSchemaRecord.numericScale;
            if (infoSchemaRecord.numericScaleNull && (infoSchemaRecord.dataType.equalsIgnoreCase("float")
                || infoSchemaRecord.dataType.equalsIgnoreCase("double"))) {
                // 对于 ColumnMeta 中的 float 和 double 数据，存储其精度数据，scale 来自 information_schema columns 中
                // 没有指定精度时，存储为 DECIMAL_NOT_SPECIFIED(31)，作为 magic number
                record.numericScale = XResultUtil.DECIMAL_NOT_SPECIFIED;
            }
            record.datetimePrecision = infoSchemaRecord.datetimePrecision;
            record.characterSetName = infoSchemaRecord.characterSetName;
            record.collationName = infoSchemaRecord.collationName;
            record.columnType = infoSchemaRecord.columnType;
            record.columnKey = infoSchemaRecord.columnKey;
            record.extra = infoSchemaRecord.extra;
            record.privileges = infoSchemaRecord.privileges;
            record.columnComment = infoSchemaRecord.columnComment;
            record.generationExpression = infoSchemaRecord.generationExpression;
            record.jdbcType = (int) jdbcExtInfo.get(infoSchemaRecord.columnName).get(ColumnsAccessor.JDBC_TYPE);
            record.jdbcTypeName = (String) jdbcExtInfo.get(infoSchemaRecord.columnName).get(
                ColumnsAccessor.JDBC_TYPE_NAME);
            record.fieldLength = (long) jdbcExtInfo.get(infoSchemaRecord.columnName).get(ColumnsAccessor.FIELD_LENGTH);
            record.status = ColumnStatus.ABSENT.getValue();
            record.version = 1;
            record.flag = 0;
            records.add(record);
        }
        return records;
    }

    public static List<IndexesRecord> convertIndex(List<IndexesInfoSchemaRecord> infoSchemaRecords,
                                                   String logicalTableSchema, String logicalTableName) {
        List<IndexesRecord> records = new ArrayList<>(infoSchemaRecords.size());
        for (IndexesInfoSchemaRecord infoSchemaRecord : infoSchemaRecords) {
            IndexesRecord record = new IndexesRecord();
            record.tableSchema = logicalTableSchema;
            record.tableName = logicalTableName;
            record.nonUnique = infoSchemaRecord.nonUnique;
            record.indexSchema = logicalTableSchema;
            record.indexName = infoSchemaRecord.indexName;
            record.seqInIndex = infoSchemaRecord.seqInIndex;
            record.columnName = infoSchemaRecord.columnName;
            if (InstanceVersion.isMYSQL80() && infoSchemaRecord.columnName == null) {
                //mysql 80 函数索引列名为null，这里mock一下
                record.columnName = "null";
            }
            record.collation = infoSchemaRecord.collation;
            record.cardinality = infoSchemaRecord.cardinality;
            record.subPart = infoSchemaRecord.subPart;
            record.packed = infoSchemaRecord.packed;
            record.nullable = infoSchemaRecord.nullable;
            record.indexType = infoSchemaRecord.indexType;
            record.comment = infoSchemaRecord.comment;
            record.indexComment = infoSchemaRecord.indexComment;
            record.indexColumnType = 0;
            record.indexLocation = 0;
            record.indexTableName = "";
            record.indexStatus = IndexStatus.ABSENT.getValue();
            record.version = 1;
            record.flag = 0;
            records.add(record);
        }
        return records;
    }

}
