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

package com.alibaba.polardbx.executor.gsi.utils;

import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.google.common.base.Preconditions;
import com.google.common.io.BaseEncoding;
import com.mysql.jdbc.StringUtils;
import com.mysql.jdbc.ZeroDate;
import com.mysql.jdbc.ZeroTime;
import com.mysql.jdbc.ZeroTimestamp;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.properties.PropUtil;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.expression.bean.EnumValue;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import com.google.common.base.Preconditions;
import com.google.common.io.BaseEncoding;
import io.airlift.slice.Slice;

import java.math.BigInteger;
import java.nio.charset.Charset;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.BiFunction;

/**
 * Convert between extractor's output and loader's input
 */
public class Transformer {

    /**
     * Build upper bound parameter for data extraction, from the results of select top n
     *
     * @param cursor Result cursor of select top n
     * @param defaultGen Default upper bound generator for empty source table
     * @return Parameter list for data extraction
     */
    public static List<Map<Integer, ParameterContext>> convertUpperBoundWithDefault(Cursor cursor, boolean useBinary,
                                                                                    BiFunction<ColumnMeta, Integer,
                                                                                        ParameterContext> defaultGen) {
        final List<Map<Integer, ParameterContext>> batchParams = new ArrayList<>();

        Row row;
        while ((row = cursor.next()) != null) {
            final List<ColumnMeta> columns = row.getParentCursorMeta().getColumns();

            final Map<Integer, ParameterContext> params = new HashMap<>(columns.size());
            for (int i = 0; i < columns.size(); i++) {

                ParameterContext pc = buildColumnParam(row, i, useBinary);

                final DataType columnType = columns.get(i).getDataType();
                if (DataTypeUtil.anyMatchSemantically(columnType, DataTypes.FloatType, DataTypes.DoubleType)) {
                    if (null != pc.getArgs()[1]) {
                        // For float value like "-100.003", query like "c_float <= -100.003" returns nothing.
                        // Should replace upper bound with "c_float <= -100"
                        pc = new ParameterContext(pc.getParameterMethod(),
                            new Object[] {pc.getArgs()[0], Math.ceil((Double) pc.getArgs()[1])});
                    }
                }

                params.put(i + 1, pc);
            }
            batchParams.add(params);
        }

        if (batchParams.isEmpty()) {
            // Build default
            final List<ColumnMeta> columns = cursor.getReturnColumns();
            final int columnCount = columns.size();

            final Map<Integer, ParameterContext> params = new HashMap<>(columnCount);
            for (int i = 0; i < columnCount; i++) {

                final ColumnMeta columnMeta = columns.get(i);

                params.put(i + 1, defaultGen.apply(columnMeta, i + 1));
            }

            batchParams.add(params);
        }

        return batchParams;
    }

    public static List<Map<Integer, ParameterContext>> convertUpperBoundWithDefaultForFastChecker(Cursor cursor,
                                                                                                  boolean useBinary,
                                                                                                  List<List<Object>> rowValues) {
        final List<Map<Integer, ParameterContext>> batchParams = new ArrayList<>();

        Row row;
        while ((row = cursor.next()) != null) {
            final List<ColumnMeta> columns = row.getParentCursorMeta().getColumns();

            final Map<Integer, ParameterContext> params = new HashMap<>(columns.size());
            final List<Object> rowValue = new ArrayList<>();
            for (int i = 0; i < columns.size(); i++) {
                rowValue.add(row.getObject(i));
                if (row.getObject(i) == null) {
                    throw new TddlRuntimeException(ErrorCode.ERR_FAST_CHECKER,
                        "fastchecker failed because found null value in sampled primary keys");
                }
                ParameterContext pc = buildColumnParam(row, i, useBinary);

                final DataType columnType = columns.get(i).getDataType();
                if (DataTypeUtil.anyMatchSemantically(columnType, DataTypes.FloatType, DataTypes.DoubleType)) {
                    if (null != pc.getArgs()[1]) {
                        // For float value like "-100.003", query like "c_float <= -100.003" returns nothing.
                        // Should replace upper bound with "c_float <= -100"
                        pc = new ParameterContext(pc.getParameterMethod(),
                            new Object[] {pc.getArgs()[0], Math.ceil((Double) pc.getArgs()[1])});
                    }
                }

                params.put(i + 1, pc);
            }
            rowValues.add(rowValue);
            batchParams.add(params);
        }
        return batchParams;
    }

    /**
     * Build batch insert parameter, from the results of select
     *
     * @param cursor result cursor of select
     * @return batch parameters for insert
     */
    public static List<Map<Integer, ParameterContext>> buildBatchParam(Cursor cursor, boolean useBinary) {
        return buildBatchParam(cursor, useBinary, null);
    }

    public static List<Map<Integer, ParameterContext>> buildBatchParam(Cursor cursor, boolean useBinary,
                                                                       Set<String> notConvertColumns) {
        final List<Map<Integer, ParameterContext>> batchParams = new ArrayList<>();

        Row row;
        while ((row = cursor.next()) != null) {
            final List<ColumnMeta> columns = row.getParentCursorMeta().getColumns();

            final Map<Integer, ParameterContext> params = new HashMap<>(columns.size());
            for (int i = 0; i < columns.size(); i++) {
                ColumnMeta columnMeta = columns.get(i);
                String colName = columnMeta.getName();
                boolean canConvert = useBinary && (notConvertColumns == null || !notConvertColumns.contains(colName));

                final ParameterContext parameterContext = buildColumnParam(row, i, canConvert);

                params.put(i + 1, parameterContext);
            }
            batchParams.add(params);
        }

        return batchParams;
    }

    public static List<Map<Integer, ParameterContext>> buildBatchParam(List<ColumnMeta> columnMetas,
                                                                       ResultSet resultSet, boolean useBinary,
                                                                       Set<String> notConvertColumns)
        throws SQLException {
        final List<Map<Integer, ParameterContext>> batchParams = new ArrayList<>();

        int columnSize = columnMetas.size();
        while (resultSet.next()) {
            final TreeMap<Integer, ParameterContext> params = new TreeMap<>();
            for (int i = 0; i < columnSize; i++) {
                String colName = columnMetas.get(i).getName();
                DataType dataType = columnMetas.get(i).getDataType();
                boolean canConvert = useBinary && (notConvertColumns == null || !notConvertColumns.contains(colName));

                final ParameterContext parameterContext =
                    buildColumnParam(resultSet, i + 1, dataType, canConvert);

                params.put(i + 1, parameterContext);
            }
            batchParams.add(params);
        }
        return batchParams;
    }

    public static ParameterContext buildColumnParam(ResultSet resultSet, int i, DataType columnType,
                                                    boolean strToBinary) throws SQLException {
        ParameterMethod method = ParameterMethod.setObject1;
        Object value = resultSet.getObject(i);
        try {
            if (value instanceof ZeroDate || value instanceof ZeroTimestamp || value instanceof ZeroTime
                || value instanceof Decimal) {
                // 针对 0000-00-00 的时间类型 setObject 会失败，setString 没问题
                value = value.toString();
                method = ParameterMethod.setString;
            } else if (value instanceof Slice) {
                value = ((Slice) value).toStringUtf8();
                method = ParameterMethod.setString;
            } else if (value instanceof EnumValue) {
                value = ((EnumValue) value).value;
                method = ParameterMethod.setString;
            } else if (value != null) {
                if (DataTypeUtil.anyMatchSemantically(columnType, DataTypes.DateType, DataTypes.TimestampType,
                    DataTypes.DatetimeType, DataTypes.TimeType, DataTypes.YearType)) {
                    // 针对 0000-00-00 01:01:01.12 的时间类型或 0000 的year 类型，
                    // getObject 返回的结果错误，getBytes 后转为 String 没问题
                    value = new String(resultSet.getBytes(i));
                    method = ParameterMethod.setString;
                } else if (DataTypeUtil.anyMatchSemantically(columnType, DataTypes.BitType, DataTypes.BigBitType)) {
                    // 使用表示范围更大的类型，规避序列化/反序列化上下界时丢失数据
                    value = new BigInteger(resultSet.getString(i));
                    method = ParameterMethod.setBit;
                } else if (DataTypeUtil.anyMatchSemantically(columnType, DataTypes.FloatType, DataTypes.DoubleType)) {
                    // 使用表示范围更大的类型，规避序列化/反序列化上下界时丢失数据
                    value = resultSet.getDouble(i);
                    method = ParameterMethod.setDouble;
                } else if (DataTypeUtil.anyMatchSemantically(columnType, DataTypes.ULongType)) {
                    // BIGINT(64) UNSIGNED
                    value = resultSet.getString(i);
                    method = ParameterMethod.setString;
                } else if (DataTypeUtil
                    .anyMatchSemantically(columnType, DataTypes.BinaryType, DataTypes.BlobType,
                        DataTypes.BinaryStringType)) {
                    // 使用 setBytes 标记，序列化时使用16进制字符串
                    value = resultSet.getBytes(i);
                    method = ParameterMethod.setBytes;
                } else if (strToBinary && DataTypeUtil.isStringType(columnType)) {
                    // 字符串类型，直接select binary得到二进制数，直接setBytes，避免字符集转换带来的损失
                    value = resultSet.getBytes(i);
                    method = ParameterMethod.setBytes;
                }
            }
        } catch (TddlNestableRuntimeException e) {
            SQLRecorderLogger.ddlLogger.warn("Convert data type failed, use getBytes. message: " + e.getMessage());

            // 类似 -01:01:01 的时间类型 getObject 会抛异常，getBytes 没问题
            // Ignore exception, use getBytes instead
            value = resultSet.getBytes(i);
            method = ParameterMethod.setBytes;
        }
        return new ParameterContext(method, new Object[] {i + 1, value, columnType});
    }

    public static ParameterContext buildColumnParam(Row row, int i) {
        return buildColumnParam(row, i, false);
    }

    public static Map<Integer, ParameterContext> buildColumnsParam(Row row) {
        Map<Integer, ParameterContext> params = new TreeMap<>();
        if (row == null || row.getValues().isEmpty()) {
            return params;
        }
        int columnSize = row.getColNum();
        for (int i = 0; i < columnSize; i++) {
            params.put(i, buildColumnParam(row, i));
        }
        return params;
    }

    /**
     * Build column parameter for insert，from the results of select
     *
     * @param row result set of select
     * @param i column index, start from 0
     * @return ParameterContext for specified column
     */
    public static ParameterContext buildColumnParam(Row row, int i, boolean strToBinary) {
        DataType columnType = DataTypes.BinaryType;
        Object value = null;
        ParameterMethod method = ParameterMethod.setObject1;
        try {
            columnType = row.getParentCursorMeta().getColumnMeta(i).getDataType();
            value = row.getObject(i);

            if (value instanceof ZeroDate || value instanceof ZeroTimestamp || value instanceof ZeroTime
                || value instanceof Decimal) {
                // 针对 0000-00-00 的时间类型 setObject 会失败，setString 没问题
                value = value.toString();
                method = ParameterMethod.setString;
            } else if (value instanceof Slice) {
                value = ((Slice) value).toStringUtf8();
                method = ParameterMethod.setString;
            } else if (value instanceof Slice) {
                value = ((Slice) value).toStringUtf8();
                method = ParameterMethod.setString;
            } else if (value instanceof EnumValue) {
                value = ((EnumValue) value).value;
                method = ParameterMethod.setString;
            } else if (value != null) {
                if (DataTypeUtil.anyMatchSemantically(columnType, DataTypes.DateType, DataTypes.TimestampType,
                    DataTypes.DatetimeType, DataTypes.TimeType, DataTypes.YearType)) {
                    // 针对 0000-00-00 01:01:01.12 的时间类型或 0000 的year 类型，
                    // getObject 返回的结果错误，getBytes 后转为 String 没问题
                    value = new String(row.getBytes(i));
                    method = ParameterMethod.setString;
                } else if (DataTypeUtil.anyMatchSemantically(columnType, DataTypes.BitType, DataTypes.BigBitType)) {
                    // 使用表示范围更大的类型，规避序列化/反序列化上下界时丢失数据
                    value = new BigInteger(row.getString(i));
                    method = ParameterMethod.setBit;
                } else if (DataTypeUtil.anyMatchSemantically(columnType, DataTypes.FloatType, DataTypes.DoubleType)) {
                    // 使用表示范围更大的类型，规避序列化/反序列化上下界时丢失数据
                    value = row.getDouble(i);
                    method = ParameterMethod.setDouble;
                } else if (DataTypeUtil.anyMatchSemantically(columnType, DataTypes.ULongType)) {
                    // BIGINT(64) UNSIGNED
                    value = row.getString(i);
                    method = ParameterMethod.setString;
                } else if (DataTypeUtil
                    .anyMatchSemantically(columnType, DataTypes.BinaryType, DataTypes.BlobType,
                        DataTypes.BinaryStringType)) {
                    // 使用 setBytes 标记，序列化时使用16进制字符串
                    value = row.getBytes(i);
                    method = ParameterMethod.setBytes;
                } else if (strToBinary && DataTypeUtil.isStringType(columnType)) {
                    // 字符串类型，直接select binary得到二进制数，直接setBytes，避免字符集转换带来的损失
                    value = row.getBytes(i);
                    method = ParameterMethod.setBytes;
                }
            }
        } catch (TddlNestableRuntimeException e) {
            SQLRecorderLogger.ddlLogger.warn("Convert data type failed, use getBytes. message: " + e.getMessage());

            // 类似 -01:01:01 的时间类型 getObject 会抛异常，getBytes 没问题
            // Ignore exception, use getBytes instead
            value = row.getBytes(i);
            method = ParameterMethod.setBytes;
        }
        return new ParameterContext(method, new Object[] {i + 1, value, columnType});
    }

    /**
     * here must keep compatible with the buildColumnParam(Row row, int i）
     */
    public static Map<Integer, ParameterContext> buildColumnParam(
        List<ColumnMeta> columnMetaList, List<String> values, Charset charset, PropUtil.LOAD_NULL_MODE defaultMode) {
        Preconditions.checkArgument(
            values.size() == columnMetaList.size(),
            String.format("The column's length is %s, while the value's length is %s", columnMetaList.size(),
                values.size()));
        final Map<Integer, ParameterContext> parameterContexts = new HashMap<>();
        ParameterMethod method = null;
        for (int i = 0; i < columnMetaList.size(); i++) {
            String stringVal = null;
            ColumnMeta meta = columnMetaList.get(i);
            if (StringUtils.isNullOrEmpty(values.get(i)) &&
                (defaultMode == PropUtil.LOAD_NULL_MODE.DEFAULT_VALUE_MODE ||
                    defaultMode == PropUtil.LOAD_NULL_MODE.DEFAULT_VALUE_AND_N_MODE)) {
                if (meta.getField().getDefault() != null) {
                    stringVal = meta.getField().getDefault();
                } else {
                    if (meta.isNullable()) {
                        stringVal = null;
                    } else {
                        stringVal = values.get(i);
                    }
                }
            } else {
                if ((PropUtil.LOAD_NULL_MODE.N_MODE == defaultMode ||
                    defaultMode == PropUtil.LOAD_NULL_MODE.DEFAULT_VALUE_AND_N_MODE)
                    && "\\N".equalsIgnoreCase(values.get(i))) {
                    if (!meta.isNullable()) {
                        stringVal = values.get(i);
                    } else {
                        stringVal = null;
                    }
                } else {
                    stringVal = values.get(i);
                }
            }

            Object value = null;
            try {
                DataType columnType = columnMetaList.get(i).getDataType();
                if (DataTypeUtil.anyMatchSemantically(columnType, DataTypes.DateType, DataTypes.TimestampType,
                    DataTypes.DatetimeType, DataTypes.TimeType, DataTypes.YearType)) {
                    // 针对 0000-00-00 01:01:01.12 的时间类型或 0000 的year 类型，
                    // getObject 返回的结果错误，getBytes 后转为 String 没问题
                    value = stringVal;
                    method = ParameterMethod.setString;
                } else if (DataTypeUtil.anyMatchSemantically(columnType, DataTypes.BitType, DataTypes.BigBitType)
                    && stringVal != null) {
                    // 使用表示范围更大的类型，规避序列化/反序列化上下界时丢失数据
                    value = new BigInteger(stringVal);
                    method = ParameterMethod.setBit;
                } else if (DataTypeUtil.anyMatchSemantically(columnType, DataTypes.FloatType, DataTypes.DoubleType)
                    && stringVal != null) {
                    // 使用表示范围更大的类型，规避序列化/反序列化上下界时丢失数据
                    value = Double.valueOf(stringVal);
                    method = ParameterMethod.setDouble;
                } else if (DataTypeUtil.anyMatchSemantically(columnType, DataTypes.ULongType)) {
                    // BIGINT(64) UNSIGNED
                    value = stringVal;
                    method = ParameterMethod.setString;
                } else if (DataTypeUtil.anyMatchSemantically(columnType, DataTypes.BinaryType, DataTypes.BlobType,
                    DataTypes.BinaryStringType) && stringVal != null) {
                    // 使用 setBytes 标记，序列化时使用16进制字符串
                    value = stringVal.getBytes(charset);
                    method = ParameterMethod.setBytes;
                } else if (DataTypeUtil.isStringType(columnType)) {
                    value = stringVal;
                    method = ParameterMethod.setString;
                } else {
                    value = stringVal;
                    method = ParameterMethod.setObject1;
                }
            } catch (TddlNestableRuntimeException e) {
                SQLRecorderLogger.ddlLogger.warn(
                    "Convert backfill data failed, use getBytes. message: " + e.getMessage());

                // 类似 -01:01:01 的时间类型 getObject 会抛异常，getBytes 没问题
                // Ignore exception, use getBytes instead
                value = stringVal.getBytes(charset);
                method = ParameterMethod.setBytes;
            }
            parameterContexts.put(i + 1, new ParameterContext(method, new Object[] {i + 1, value}));
        }
        return parameterContexts;
    }

    public static ParameterContext buildParamByType(long index, String method, String value) {
        return buildParamByType(index, ParameterMethod.valueOf(method), value);
    }

    public static ParameterContext buildParamByType(long index, ParameterMethod method, String value) {
        return new ParameterContext(method, new Object[] {index, deserializeParam(method, value)});
    }

    public static String serializeParam(ParameterContext pc) {
        if (pc == null) {
            return null;
        }
        final ParameterMethod method = pc.getParameterMethod();
        switch (method) {
        case setBytes:
            return BaseEncoding.base16().encode((byte[]) pc.getArgs()[1]);
        default:
            return pc.getArgs()[1].toString();
        }
    }

    public static Object deserializeParam(ParameterMethod method, String value) {
        switch (method) {
        case setInt:
            return Integer.valueOf(value);
        case setLong:
            return Long.valueOf(value);
        case setDouble:
            return Double.valueOf(value);
        case setBytes:
            return BaseEncoding.base16().decode(value);
        default:
            return value;
        }
    }
}
