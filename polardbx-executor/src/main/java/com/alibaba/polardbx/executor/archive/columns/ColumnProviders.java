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

package com.alibaba.polardbx.executor.archive.columns;

import com.alibaba.polardbx.common.charset.CollationName;
import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.optimizer.config.table.PolarDBXOrcSchema;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.core.datatype.BigBitType;
import com.alibaba.polardbx.optimizer.core.datatype.BinaryType;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;

import java.nio.charset.Charset;
import java.sql.Blob;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ColumnProviders {
    public static final String UTF_8 = "utf8";
    public static final byte[] EMPTY_BYTES = new byte[0];
    public static final Charset LATIN1 = Charset.forName("LATIN1");
    public static final Charset UTF8 = Charset.forName("UTF-8");

    public static final Map<CollationName, ColumnProvider<String>>
        VARCHAR_COLUMN_PROVIDERS = new HashMap<>();

    static {
        // init collation handlers for all collation names.
        for (CollationName collationName : CollationName.values()) {
            VARCHAR_COLUMN_PROVIDERS.put(
                collationName, new VarcharColumnProvider(collationName));
        }
    }

    public static final ColumnProvider<Long> LONG_COLUMN_PROVIDER = new LongColumnProvider();

    public static final ColumnProvider<Long> UNSIGNED_LONG_COLUMN_PROVIDER = new UnsignedLongColumnProvider();

    public static final ColumnProvider<Integer> INTEGER_COLUMN_PROVIDER = new IntegerColumnProvider();

    public static final ColumnProvider<Integer> BIT_COLUMN_PROVIDER = new BitColumnProvider();

    public static final ColumnProvider<Short> SHORT_COLUMN_PROVIDER = new ShortColumnProvider();

    public static final ColumnProvider<Long> DATETIME_COLUMN_PROVIDER = new DatetimeColumnProvider();

    public static final ColumnProvider<Long> TIMESTAMP_COLUMN_PROVIDER = new TimestampColumnProvider();

    public static final ColumnProvider<Long> TIME_COLUMN_PROVIDER = new TimeColumnProvider();

    public static final ColumnProvider<Long> DATE_COLUMN_PROVIDER = new DateColumnProvider();

    public static final ColumnProvider<Decimal> DECIMAL_COLUMN_PROVIDER = new DecimalColumnProvider();

    public static final ColumnProvider<Double> DOUBLE_COLUMN_PROVIDER = new DoubleColumnProvider();

    public static final ColumnProvider<Float> FLOAT_COLUMN_PROVIDER = new FloatColumnProvider();

    public static final ColumnProvider<Blob> BLOB_COLUMN_PROVIDER = new BlobColumnProvider();

    public static final ColumnProvider<String> STRING_COLUMN_PROVIDER = new StringColumnProvider();

    public static final ColumnProvider<Long> BIG_BIT_COLUMN_PROVIDER = new BigBitColumnProvider();

    public static final ColumnProvider<String> BINARY_COLUMN_PROVIDER = new BinaryColumnProvider();

    public static List<ColumnProvider> getColumnProviders(PolarDBXOrcSchema orcSchema) {
        return orcSchema.getColumnMetas().stream()
            .map(t -> ColumnProviders.getProvider(t)).collect(Collectors.toList());
    }

    public static List<ColumnProvider> getBfColumnProviders(PolarDBXOrcSchema orcSchema) {
        List<ColumnProvider> bfColumnProviders =
            orcSchema.getBfColumnMetas().stream()
                .map(t -> ColumnProviders.getProvider(t)).collect(Collectors.toList());

        List<ColumnProvider> redundantColumnProviders = orcSchema.getRedundantColumnMetas().stream()
            .map(columnMeta -> ColumnProviders.getProvider(columnMeta)).collect(Collectors.toList());

        bfColumnProviders.addAll(redundantColumnProviders);
        return bfColumnProviders;
    }

    public static ColumnProvider<?> getProvider(ColumnMeta columnMeta) {
        return getProvider(columnMeta.getDataType());
    }

    public static ColumnProvider<?> getProvider(DataType dataType) {
        final boolean isUnsigned = dataType.isUnsigned();
        switch (dataType.fieldType()) {
        /* =========== Temporal ============ */
        // for datetime/timestamp
        case MYSQL_TYPE_DATETIME:
        case MYSQL_TYPE_DATETIME2:
            return DATETIME_COLUMN_PROVIDER;
        case MYSQL_TYPE_TIMESTAMP:
        case MYSQL_TYPE_TIMESTAMP2:
            return TIMESTAMP_COLUMN_PROVIDER;
        // for date
        case MYSQL_TYPE_DATE:
        case MYSQL_TYPE_NEWDATE:
            return DATE_COLUMN_PROVIDER;

        // for time
        case MYSQL_TYPE_TIME:
            return TIME_COLUMN_PROVIDER;

        // for year
        case MYSQL_TYPE_YEAR:
            return LONG_COLUMN_PROVIDER;

        /* =========== Fixed-point Numeric ============ */
        case MYSQL_TYPE_DECIMAL:
        case MYSQL_TYPE_NEWDECIMAL:
            // for decimal
            return DECIMAL_COLUMN_PROVIDER;

        case MYSQL_TYPE_LONGLONG:
            if (isUnsigned) {
                // for bigint unsigned
                return UNSIGNED_LONG_COLUMN_PROVIDER;
            } else {
                // for bigint signed
                return LONG_COLUMN_PROVIDER;
            }
        case MYSQL_TYPE_LONG:
            if (isUnsigned) {
                // for int unsigned
                return LONG_COLUMN_PROVIDER;
            } else {
                // for int signed
                return INTEGER_COLUMN_PROVIDER;
            }

        case MYSQL_TYPE_INT24:
            if (isUnsigned) {
                // for mediumint unsigned
                return INTEGER_COLUMN_PROVIDER;
            } else {
                // for mediumint signed
                return INTEGER_COLUMN_PROVIDER;
            }

        case MYSQL_TYPE_SHORT:
            if (isUnsigned) {
                // for smallint unsigned
                return INTEGER_COLUMN_PROVIDER;
            } else {
                // for smallint signed
                return SHORT_COLUMN_PROVIDER;
            }

        case MYSQL_TYPE_TINY:
            if (isUnsigned) {
                // for tinyint unsigned
                return SHORT_COLUMN_PROVIDER;
            } else {
                // for tinyint signed
                return SHORT_COLUMN_PROVIDER;
            }

        case MYSQL_TYPE_BIT:
            if (dataType instanceof BigBitType) {
                return BIG_BIT_COLUMN_PROVIDER;
            } else {
                // for bit
                return BIT_COLUMN_PROVIDER;
            }
            /* =========== Float-point Numeric ============ */
        case MYSQL_TYPE_DOUBLE:
            // for double
            return DOUBLE_COLUMN_PROVIDER;
        case MYSQL_TYPE_FLOAT:
            // for float
            return FLOAT_COLUMN_PROVIDER;

        /* =========== String ============ */
        case MYSQL_TYPE_VAR_STRING:
        case MYSQL_TYPE_STRING:
            // for varchar/char
            if (dataType instanceof BinaryType) {
                return BINARY_COLUMN_PROVIDER;
            } else {
                return VARCHAR_COLUMN_PROVIDERS.get(dataType.getCollationName());
            }
        case MYSQL_TYPE_BLOB:
            // for blob
            return BLOB_COLUMN_PROVIDER;

        case MYSQL_TYPE_ENUM:
        case MYSQL_TYPE_JSON:
            // for enum
            return STRING_COLUMN_PROVIDER;

        default:
            return null;
        }
    }

}
