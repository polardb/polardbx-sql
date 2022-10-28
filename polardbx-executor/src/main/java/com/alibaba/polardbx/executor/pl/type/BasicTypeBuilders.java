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

package com.alibaba.polardbx.executor.pl.type;

public class BasicTypeBuilders {
    public static BasicSqlTypeBuilder getTypeBuilder(String typeName) {
        typeName = typeName.toUpperCase();
        switch (typeName) {
        case "BIT":
            return BitTypeBuilder.INSTANCE;
        case "TINYINT":
            return TinyIntTypeBuilder.INSTANCE;
        case "SMALLINT":
            return SmallIntTypeBuilder.INSTANCE;
        case "MEDIUMINT":
            return MediumIntTypeBuilder.INSTANCE;
        case "INT":
        case "INTEGER":
            return IntTypeBuilder.INSTANCE;
        case "BIGINT":
            return BigIntTypeBuilder.INSTANCE;
        case "DECIMAL":
            return DecimalTypeBuilder.INSTANCE;
        case "FLOAT":
            return FloatTypeBuilder.INSTANCE;
        case "DOUBLE":
            return DoubleTypeBuilder.INSTANCE;
        case "DATE":
            return DateTypeBuilder.INSTANCE;
        case "DATETIME":
            return DatetimeTypeBuilder.INSTANCE;
        case "TIMESTAMP":
            return TimestampTypeBuilder.INSTANCE;
        case "TIME":
            return TimeTypeBuilder.INSTANCE;
        case "YEAR":
            return YearTypeBuilder.INSTANCE;
        case "CHAR":
            return CharTypeBuilder.INSTANCE;
        case "VARCHAR":
            return VarcharTypeBuilder.INSTANCE;
        case "BINARY":
            return BinaryTypeBuidler.INSTANCE;
        case "VARBINARY":
            return VarbinaryTypeBuilder.INSTANCE;
        case "TINYBLOB":
            return TinyBlobTypeBuilder.INSTANCE;
        case "BLOB":
            return BlobTypeBuilder.INSTANCE;
        case "MEDIUMBLOB":
            return MediumBlobTypeBuilder.INSTANCE;
        case "LONGBLOB":
            return LongBlobTypeBuilder.INSTANCE;
        case "TINYTEXT":
            return TinyTextTypeBuilder.INSTANCE;
        case "TEXT":
            return TextTypeBuilder.INSTANCE;
        case "MEDIUMTEXT":
            return MediumTextTypeBuilder.INSTANCE;
        case "LONGTEXT":
            return LongTextTypeBuilder.INSTANCE;
        case "JSON":
            return JsonTypeBuilder.INSTANCE;
        case "ENUM":
            return EnumTypeBuilder.INSTANCE;
        default:
            throw new RuntimeException(typeName + " not support yet!");
        }
    }
}
