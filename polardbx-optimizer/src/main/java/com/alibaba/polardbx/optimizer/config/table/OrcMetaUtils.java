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

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.TddlRelDataTypeSystemImpl;
import com.alibaba.polardbx.optimizer.core.TddlTypeFactoryImpl;
import com.alibaba.polardbx.optimizer.core.datatype.BigBitType;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.datatype.SliceType;
import com.google.common.base.Preconditions;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.orc.ColumnStatistics;
import org.apache.orc.CompressionCodec;
import org.apache.orc.CompressionKind;
import org.apache.orc.FileFormatException;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcProto;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.BufferChunk;
import org.apache.orc.impl.ColumnStatisticsImpl;
import org.apache.orc.impl.InStream;
import org.apache.orc.impl.OrcCodecPool;
import org.apache.orc.impl.OrcTail;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class OrcMetaUtils {
    public static final TddlTypeFactoryImpl TYPE_FACTORY =
        new TddlTypeFactoryImpl(TddlRelDataTypeSystemImpl.getInstance());
    public static final String ORC_ROW_INDEX_STRIDE = "orc.row.index.stride";
    public static final String ORC_BLOOM_FILTER_COLUMNS = "orc.bloom.filter.columns";
    public static final String ORC_BLOOM_FILTER_FPP = "orc.bloom.filter.fpp";
    public static final String ORC_COMPRESS = "orc.compress";
    private static final String REDUNDANT_SUFFIX = "__redundant__";
    private static final String REDUNDANT_FORMAT = "%s__redundant__";

    public static String redundantColumnOf(String columnName) {
        return String.format(REDUNDANT_FORMAT, columnName);
    }

    public static boolean isRedundantColumn(String columnName) {
        return columnName != null && columnName.endsWith(REDUNDANT_SUFFIX);
    }

    /**
     * Build orc schema from source table meta.
     *
     * @param sourceTableMeta source table meta.
     */
    @NotNull
    public static PolarDBXOrcSchema buildPolarDBXOrcSchema(TableMeta sourceTableMeta) {
        List<ColumnMeta> allColumns = sourceTableMeta.getAllColumns();
        List<Field> fieldList = sourceTableMeta.getAllColumns().stream().map(columnMeta -> columnMeta.getField())
            .collect(Collectors.toList());
        // all string columns from single or composite key
        Set<String> columnsWithSortKey = sourceTableMeta.getIndexes().stream()
            .map(indexMeta -> indexMeta.getKeyColumns())
            .flatMap(List::stream)
            .filter(columnMeta -> columnMeta.getDataType() instanceof SliceType)
            .map(ColumnMeta::getName)
            .collect(Collectors.toSet());
        if (sourceTableMeta.getGsiPublished() != null) {
            Set<String> gsiColumnsWithSortKey = sourceTableMeta.getGsiPublished().values().stream()
                .map(gsiIndexMetaBean -> gsiIndexMetaBean.indexColumns)
                .flatMap(List::stream)
                .map(gsiIndexColumnMetaBean -> gsiIndexColumnMetaBean.columnName)
                .filter(columnName -> sourceTableMeta.getColumn(columnName).getDataType() instanceof SliceType)
                .collect(Collectors.toSet());
            columnsWithSortKey.addAll(gsiColumnsWithSortKey);
        }
        int[] redundantMap = initRedundantMap(allColumns);
        final int redundantColumnId = allColumns.size() + 1;
        List<ColumnMeta> redundantColumnMetas = new ArrayList<>();
        int currentRedundantId = redundantColumnId;
        for (int i = 0; i < redundantMap.length; i++) {
            if (columnsWithSortKey.contains(allColumns.get(i).getOriginColumnName())) {
                redundantMap[i] = currentRedundantId++;
                ColumnMeta columnMeta = allColumns.get(i);
                String redundantColumnName = redundantColumnOf(columnMeta.getName());
                ColumnMeta redundantColumnMeta = new ColumnMeta(
                    columnMeta.getTableName(),
                    redundantColumnName,
                    redundantColumnName,
                    new Field(
                        columnMeta.getTableName(),
                        redundantColumnName,
                        TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR))
                );
                redundantColumnMetas.add(redundantColumnMeta);
            }
        }
        List<ColumnMeta> columnMetas = sourceTableMeta.getPhysicalColumns();
        // No varchar index column, because it's redundant column have bloom filter.
        Set<ColumnMeta> bfColumnMetas = sourceTableMeta.getSecondaryIndexes().stream()
            .map(indexMeta -> indexMeta.getKeyColumns())
            .flatMap(List::stream)
            .filter(columnMeta -> !(columnMeta.getDataType() instanceof SliceType))
            .collect(Collectors.toSet());
        if (sourceTableMeta.getGsiPublished() != null) {
            List<ColumnMeta> gsiBfColumnMetas = sourceTableMeta.getGsiPublished().values().stream()
                .map(gsiIndexMetaBean -> gsiIndexMetaBean.indexColumns)
                .flatMap(List::stream)
                .map(gsiIndexColumnMetaBean -> gsiIndexColumnMetaBean.columnName)
                .map(columnName -> sourceTableMeta.getColumn(columnName))
                .filter(columnMeta -> !(columnMeta.getDataType() instanceof SliceType))
                .collect(Collectors.toList());
            bfColumnMetas.addAll(gsiBfColumnMetas);
        }
        List<String> orcKeyColumnNames = bfColumnMetas.stream().map(ColumnMeta::getName).collect(Collectors.toList());
        TypeDescription schema = getTypeDescription(fieldList, redundantMap);
        // build bloom filter schema
        TypeDescription bfSchema = getBfTypeDescription(orcKeyColumnNames, schema, redundantColumnId);
        return new PolarDBXOrcSchema(
            schema, bfSchema,
            columnMetas, bfColumnMetas.stream().collect(Collectors.toList()), redundantColumnMetas,
            redundantColumnId, redundantMap
        );
    }

    /**
     * Build orc schema from source table meta.
     *
     * @param sourceTableMeta source table meta.
     * @param columnToFieldIdMap the column mapping from name to fieldId, use the map in sourceTableMeta if it's empty
     */
    @NotNull
    public static PolarDBXOrcSchema buildPolarDBXOrcSchema(TableMeta sourceTableMeta,
                                                           Optional<Map<String, String>> columnToFieldIdMap,
                                                           boolean oldFileStorage) {
        List<ColumnMeta> allColumns = sourceTableMeta.getAllColumns();
        List<Field> fieldList = sourceTableMeta.getAllColumns().stream().map(columnMeta -> columnMeta.getField())
            .collect(Collectors.toList());

        // all string columns from single or composite key
        Set<String> columnsWithSortKey = sourceTableMeta.getIndexes().stream()
            .map(indexMeta -> indexMeta.getKeyColumns())
            .flatMap(List::stream)
            .filter(columnMeta -> columnMeta.getDataType() instanceof SliceType)
            .map(ColumnMeta::getName)
            .collect(Collectors.toSet());

        if (sourceTableMeta.getGsiPublished() != null) {
            Set<String> gsiColumnsWithSortKey = sourceTableMeta.getGsiPublished().values().stream()
                .map(gsiIndexMetaBean -> gsiIndexMetaBean.indexColumns)
                .flatMap(List::stream)
                .map(gsiIndexColumnMetaBean -> gsiIndexColumnMetaBean.columnName)
                .filter(columnName -> sourceTableMeta.getColumn(columnName).getDataType() instanceof SliceType)
                .collect(Collectors.toSet());
            columnsWithSortKey.addAll(gsiColumnsWithSortKey);
        }

        int[] redundantMap = initRedundantMap(allColumns);
        final int redundantColumnId = allColumns.size() + 1;
        List<ColumnMeta> redundantColumnMetas = new ArrayList<>();

        int currentRedundantId = redundantColumnId;
        for (int i = 0; i < redundantMap.length; i++) {
            if (columnsWithSortKey.contains(allColumns.get(i).getOriginColumnName())) {
                redundantMap[i] = currentRedundantId++;

                ColumnMeta columnMeta = allColumns.get(i);
                String redundantColumnName = redundantColumnOf(columnMeta.getName());

                ColumnMeta redundantColumnMeta =
                    buildRedundantColumnMeta(columnMeta.getTableName(), redundantColumnName);

                redundantColumnMetas.add(redundantColumnMeta);
            }
        }

        List<ColumnMeta> columnMetas = sourceTableMeta.getPhysicalColumns();

        // No varchar index column, because it's redundant column have bloom filter.
        Set<ColumnMeta> bfColumnMetas = sourceTableMeta.getSecondaryIndexes().stream()
            .map(indexMeta -> indexMeta.getKeyColumns())
            .flatMap(List::stream)
            .filter(columnMeta -> !(columnMeta.getDataType() instanceof SliceType))
            .collect(Collectors.toSet());

        if (sourceTableMeta.getGsiPublished() != null) {
            List<ColumnMeta> gsiBfColumnMetas = sourceTableMeta.getGsiPublished().values().stream()
                .map(gsiIndexMetaBean -> gsiIndexMetaBean.indexColumns)
                .flatMap(List::stream)
                .map(gsiIndexColumnMetaBean -> gsiIndexColumnMetaBean.columnName)
                .map(columnName -> sourceTableMeta.getColumn(columnName))
                .filter(columnMeta -> !(columnMeta.getDataType() instanceof SliceType))
                .collect(Collectors.toList());
            bfColumnMetas.addAll(gsiBfColumnMetas);
        }

        List<String> orcKeyColumnNames = bfColumnMetas.stream().map(ColumnMeta::getName).collect(Collectors.toList());

        TypeDescription schema =
            getTypeDescription(fieldList, redundantMap, sourceTableMeta, columnToFieldIdMap, oldFileStorage);

        // build bloom filter schema
        TypeDescription bfSchema = getBfTypeDescription(orcKeyColumnNames, schema, redundantColumnId,
            sourceTableMeta, columnToFieldIdMap, oldFileStorage);

        return new PolarDBXOrcSchema(
            schema, bfSchema,
            columnMetas, bfColumnMetas.stream().collect(Collectors.toList()), redundantColumnMetas,
            redundantColumnId, redundantMap
        );
    }

    public static ColumnMeta buildRedundantColumnMeta(String tableName, String redundantColumnName) {
        return new ColumnMeta(
            tableName,
            redundantColumnName,
            redundantColumnName,
            new Field(
                tableName,
                redundantColumnName,
                TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR))
        );
    }
    public static int[] initRedundantMap(List<ColumnMeta> allColumns) {
        int[] redundantMap = new int[allColumns.size()];
        for (int i = 0; i < redundantMap.length; i++) {
            redundantMap[i] = -1;
        }
        return redundantMap;
    }

    @NotNull
    public static TypeDescription getBfTypeDescription(List<String> keyColumnNames, TypeDescription schema,
                                                       int redundantId,
                                                       TableMeta tableMeta,
                                                       Optional<Map<String, String>> columnToFieldIdMap,
                                                       boolean oldFileStorage) {
        TypeDescription bfSchema = TypeDescription.createStruct();
        for (int i = 0; i < keyColumnNames.size(); i++) {
            String colName = keyColumnNames.get(i);
            if (oldFileStorage) {
                TypeDescription child = schema.findSubtype(colName);
                bfSchema.addField(colName, child.clone());
                continue;
            }
            String filedId = columnToFieldIdMap.isPresent() ?
                columnToFieldIdMap.get().get(colName) :
                tableMeta.getColumnFieldId(colName);
            TypeDescription child = schema.findSubtype(filedId);
            bfSchema.addField(filedId, child.clone());
        }

        // build bf schema for redundant columns
        for (int i = redundantId; i <= schema.getMaximumId(); i++) {
            String colName = schema.getFieldNames().get(i - 1);
            TypeDescription child = schema.findSubtype(i);
            bfSchema.addField(colName, child.clone());
        }

        return bfSchema;
    }

    @NotNull
    public static TypeDescription getBfTypeDescription(List<String> keyColumnNames, TypeDescription schema,
                                                       int redundantId) {
        TypeDescription bfSchema = TypeDescription.createStruct();
        for (int i = 0; i < keyColumnNames.size(); i++) {
            String colName = keyColumnNames.get(i);
            TypeDescription child = schema.findSubtype(colName);
            bfSchema.addField(colName, child.clone());
        }
        // build bf schema for redundant columns
        for (int i = redundantId; i <= schema.getMaximumId(); i++) {
            String colName = schema.getFieldNames().get(i - 1);
            TypeDescription child = schema.findSubtype(i);
            bfSchema.addField(colName, child.clone());
        }
        return bfSchema;
    }

    @NotNull
    public static Configuration getConfiguration(ExecutionContext executionContext, PolarDBXOrcSchema orcSchema) {
        Configuration conf = new Configuration();
        ParamManager paramManager = executionContext.getParamManager();
        List<String> orcKeyColumnNames = orcSchema.getBfSchema().getFieldNames();
        String orcBloomFilterColumns = String.join(",", orcKeyColumnNames);
        conf.setLong(ORC_ROW_INDEX_STRIDE, paramManager.getLong(ConnectionParams.OSS_ORC_INDEX_STRIDE));
        conf.set(ORC_BLOOM_FILTER_COLUMNS, orcBloomFilterColumns);
        conf.setDouble(ORC_BLOOM_FILTER_FPP, paramManager.getFloat(ConnectionParams.OSS_BLOOM_FILTER_FPP));
        conf.set(ORC_COMPRESS, paramManager.getString(ConnectionParams.OSS_ORC_COMPRESSION));
        return conf;
    }

    @NotNull
    public static Configuration getConfiguration(ExecutionContext executionContext) {
        Configuration conf = new Configuration();
        ParamManager paramManager = executionContext.getParamManager();

        conf.setLong(ORC_ROW_INDEX_STRIDE, paramManager.getLong(ConnectionParams.OSS_ORC_INDEX_STRIDE));
        conf.setDouble(ORC_BLOOM_FILTER_FPP, paramManager.getFloat(ConnectionParams.OSS_BLOOM_FILTER_FPP));
        conf.set(ORC_COMPRESS, paramManager.getString(ConnectionParams.OSS_ORC_COMPRESSION));
        return conf;
    }

    /**
     * From PolarDB-X column data type meta to Orc column meta.
     *
     * @param fieldList original column data type info
     * @param redundantMap {original col pos -> redundant col pos}. redundant col pos = -1 means no redundant col for original col.
     * @return Orc column meta.
     */
    @NotNull
    public static TypeDescription getTypeDescription(List<Field> fieldList, int[] redundantMap,
                                                     TableMeta tableMeta,
                                                     Optional<Map<String, String>> columnToFieldIdMap,
                                                     boolean oldFileStorage) {
        Preconditions.checkArgument(fieldList.size() == redundantMap.length);

        TypeDescription schema = TypeDescription.createStruct();
        fieldList.stream()
            .forEach(col -> {
                DataType t = col.getDataType();
                TypeDescription typeDescription = getTypeDescription(t);
                if (oldFileStorage) {
                    schema.addField(col.getOriginColumnName(), typeDescription);
                    return;
                }
                String filedId = columnToFieldIdMap.isPresent() ?
                    columnToFieldIdMap.get().get(col.getOriginColumnName()) :
                    tableMeta.getColumnFieldId(col.getOriginColumnName());
                schema.addField(filedId, typeDescription);
            });

        // for redundant column
        for (int i = 0; i < redundantMap.length; i++) {
            if (redundantMap[i] == -1) {
                // no redundant.
                continue;
            }

            // redundant sort key column.
            DataType t = DataTypes.VarcharType;
            TypeDescription typeDescription = getTypeDescription(t);

            String filedId = columnToFieldIdMap.isPresent() ?
                columnToFieldIdMap.get().get(fieldList.get(i).getOriginColumnName()) :
                tableMeta.getColumnFieldId(fieldList.get(i).getOriginColumnName());
            String redundantColumn = redundantColumnOf(filedId);

            schema.addField(redundantColumn, typeDescription);
        }

        // invoke id allocation
        schema.getId();
        return schema;
    }

    /**
     * From PolarDB-X column data type meta to Orc column meta.
     *
     * @param fieldList original column data type info
     * @param redundantMap {original col pos -> redundant col pos}. redundant col pos = -1 means no redundant col for original col.
     * @return Orc column meta.
     */
    @NotNull
    public static TypeDescription getTypeDescription(List<Field> fieldList, int[] redundantMap) {
        Preconditions.checkArgument(fieldList.size() == redundantMap.length);
        TypeDescription schema = TypeDescription.createStruct();
        fieldList.stream()
            .forEach(col -> {
                DataType t = col.getDataType();
                TypeDescription typeDescription = getTypeDescription(t);
                schema.addField(col.getOriginColumnName(), typeDescription);
            });
        // for redundant column
        for (int i = 0; i < redundantMap.length; i++) {
            if (redundantMap[i] == -1) {
                // no redundant.
                continue;
            }
            // redundant sort key column.
            DataType t = DataTypes.VarcharType;
            TypeDescription typeDescription = getTypeDescription(t);
            String originalColumn = fieldList.get(i).getOriginColumnName();
            String redundantColumn = redundantColumnOf(originalColumn);
            schema.addField(redundantColumn, typeDescription);
        }
        // invoke id allocation
        schema.getId();
        return schema;
    }

    @NotNull
    public static TypeDescription getTypeDescription(List<Field> fieldList) {
        TypeDescription schema = TypeDescription.createStruct();
        fieldList.stream()
            .forEach(col -> {
                DataType t = col.getDataType();
                TypeDescription typeDescription = getTypeDescription(t);
                schema.addField(col.getOriginColumnName(), typeDescription);
            });
        // invoke id allocation
        schema.getId();
        return schema;
    }

    public static TypeDescription getTypeDescription(DataType dataType) {
        final boolean isUnsigned = dataType.isUnsigned();
        switch (dataType.fieldType()) {
        /* =========== Temporal ============ */
        //timestamp
        case MYSQL_TYPE_TIMESTAMP:
        case MYSQL_TYPE_TIMESTAMP2:
            // for datetime
        case MYSQL_TYPE_DATETIME:
        case MYSQL_TYPE_DATETIME2:
            // for date
        case MYSQL_TYPE_DATE:
        case MYSQL_TYPE_NEWDATE:
            // for time
        case MYSQL_TYPE_TIME:
            // for year
        case MYSQL_TYPE_YEAR:
            // for bigint
        case MYSQL_TYPE_LONGLONG:
            return TypeDescription.createLong();
        /* =========== Decimal ============ */
        case MYSQL_TYPE_DECIMAL:
        case MYSQL_TYPE_NEWDECIMAL:
        case MYSQL_TYPE_ENUM:
            // for enum
        case MYSQL_TYPE_JSON:
            // for blob
        case MYSQL_TYPE_BLOB:
            /* =========== String ============ */
            // for varchar/char
        case MYSQL_TYPE_VAR_STRING:
        case MYSQL_TYPE_STRING: {
            TypeDescription typeDescription = TypeDescription.createVarchar();
            typeDescription.withMaxLength(Integer.MAX_VALUE);
            return typeDescription;
        }
        /* =========== Fixed-point Numeric ============ */
        case MYSQL_TYPE_LONG:
            if (isUnsigned) {
                // for int unsigned
                return TypeDescription.createLong();
            } else {
                // for int signed
                return TypeDescription.createInt();
            }
        case MYSQL_TYPE_INT24:
            return TypeDescription.createInt();
        case MYSQL_TYPE_SHORT:
            if (isUnsigned) {
                // for smallint unsigned
                return TypeDescription.createInt();
            } else {
                // for smallint signed
                return TypeDescription.createShort();
            }
        case MYSQL_TYPE_TINY:
            return TypeDescription.createShort();
        case MYSQL_TYPE_BIT:
            if (dataType instanceof BigBitType) {
                return TypeDescription.createLong();
            } else {
                // for bit
                return TypeDescription.createInt();
            }
            /* =========== Float-point Numeric ============ */
        case MYSQL_TYPE_DOUBLE:
            // for double
            return TypeDescription.createDouble();
        case MYSQL_TYPE_FLOAT:
            // for float
            return TypeDescription.createFloat();
        default:
            return null;
        }
    }

    public static ColumnStatistics[] deserializeStats(
        TypeDescription schema,
        List<OrcProto.ColumnStatistics> fileStats) {
        ColumnStatistics[] result = new ColumnStatistics[fileStats.size()];
        for (int i = 0; i < result.length; ++i) {
            TypeDescription subschema = schema == null ? null : schema.findSubtype(i);
            result[i] = ColumnStatisticsImpl.deserialize(subschema, fileStats.get(i),
                false,
                false);
        }
        return result;
    }

    public static OrcTail extractFileTail(ByteBuffer buffer) {
        try {
            return extractFileTail(buffer, -1, -1);
        } catch (IOException e) {
            GeneralUtil.nestedException("Fail to parse the file meta! ", e);
        }
        return null;
    }

    private static OrcTail extractFileTail(ByteBuffer buffer, long fileLen, long modificationTime)
        throws IOException {
        OrcProto.PostScript ps;
        long readSize = fileLen != -1 ? fileLen : buffer.limit();
        OrcProto.FileTail.Builder fileTailBuilder = OrcProto.FileTail.newBuilder();
        fileTailBuilder.setFileLength(readSize);
        int psLen = buffer.get((int) (readSize - 1)) & 0xff;
        int psOffset = (int) (readSize - 1 - psLen);
        ensureOrcFooter(buffer, psLen);
        byte[] psBuffer = new byte[psLen];
        System.arraycopy(buffer.array(), psOffset, psBuffer, 0, psLen);
        ps = OrcProto.PostScript.parseFrom(psBuffer);
        int footerSize = (int) ps.getFooterLength();
        CompressionKind compressionKind =
            CompressionKind.valueOf(ps.getCompression().name());
        fileTailBuilder.setPostscriptLength(psLen).setPostscript(ps);
        InStream.StreamOptions compression = new InStream.StreamOptions();
        try (CompressionCodec codec = OrcCodecPool.getCodec(compressionKind)) {
            if (codec != null) {
                compression.withCodec(codec)
                    .withBufferSize((int) ps.getCompressionBlockSize());
            }
            OrcProto.Footer footer =
                OrcProto.Footer.parseFrom(
                    InStream.createCodedInputStream(
                        InStream.create("footer", new BufferChunk(buffer, 0), psOffset - footerSize, footerSize,
                            compression)));
            fileTailBuilder.setPostscriptLength(psLen).setFooter(footer);
        }
        // clear does not clear the contents but sets position to 0 and limit = capacity
        buffer.clear();
        return new OrcTail(fileTailBuilder.build(), new BufferChunk(buffer.slice(), 0), modificationTime);
    }

    /**
     * Ensure this is an ORC file to prevent users from trying to read text
     * files or RC files as ORC files.
     */
    private static void ensureOrcFooter(ByteBuffer buffer, int psLen) throws IOException {
        int magicLength = OrcFile.MAGIC.length();
        int fullLength = magicLength + 1;
        if (psLen < fullLength || buffer.remaining() < fullLength) {
            throw new FileFormatException("Malformed ORC file. Invalid postscript length " + psLen);
        }
        int offset = buffer.arrayOffset() + buffer.position() + buffer.limit() - fullLength;
        byte[] array = buffer.array();
        // now look for the magic string at the end of the postscript.
        if (!Text.decode(array, offset, magicLength).equals(OrcFile.MAGIC)) {
            // if it isn't there, this may be 0.11.0 version of the ORC file.
            // Read the first 3 bytes from the buffer to check for the header
            if (!Text.decode(buffer.array(), 0, magicLength).equals(OrcFile.MAGIC)) {
                throw new FileFormatException("Malformed ORC file. Invalid postscript length " + psLen);
            }
        }
    }
}