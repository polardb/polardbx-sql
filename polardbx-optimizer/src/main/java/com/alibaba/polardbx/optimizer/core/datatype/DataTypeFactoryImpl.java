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

package com.alibaba.polardbx.optimizer.core.datatype;

import com.alibaba.polardbx.common.charset.CharsetName;
import com.alibaba.polardbx.common.charset.CollationName;
import com.alibaba.polardbx.common.utils.time.MySQLTimeTypeUtil;
import com.alibaba.polardbx.rpc.result.XResultUtil;
import com.google.common.base.Preconditions;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.type.EnumSqlType;
import org.apache.calcite.sql.type.SetSqlType;
import org.apache.calcite.sql.type.SqlTypeUtil;

import java.util.Optional;

public class DataTypeFactoryImpl implements DataTypeFactory {

    @Override
    public DataType<?> create(RelDataType relDataType) {
        Preconditions.checkNotNull(relDataType);
        if (relDataType.isStruct()) {
            throw new UnsupportedOperationException("structured type not supported");
        }
        CharsetName charsetName = Optional.ofNullable(relDataType)
            .filter(SqlTypeUtil::isCharacter)
            .map(RelDataType::getCharset)
            .map(CharsetName::of)
            .orElse(CharsetName.defaultCharset());
        CollationName collationName = Optional.ofNullable(relDataType)
            .filter(SqlTypeUtil::isCharacter)
            .map(RelDataType::getCollation)
            .map(SqlCollation::getCollationName)
            .map(CollationName::of)
            .orElseGet(
                () -> charsetName.getDefaultCollationName()
            );
        int scale = relDataType.getScale();
        int precision = relDataType.getPrecision();
        DataType ret;

        switch (relDataType.getSqlTypeName()) {
        case BOOLEAN:
            return DataTypes.BooleanType;
        case TINYINT:
            return DataTypes.TinyIntType;
        case TINYINT_UNSIGNED:
            return DataTypes.UTinyIntType;
        case SMALLINT:
            return DataTypes.SmallIntType;
        case SMALLINT_UNSIGNED:
            return DataTypes.USmallIntType;
        case MEDIUMINT:
            return DataTypes.MediumIntType;
        case MEDIUMINT_UNSIGNED:
            return DataTypes.UMediumIntType;
        case INTEGER:
            return DataTypes.IntegerType;
        case INTEGER_UNSIGNED:
            return DataTypes.UIntegerType;
        case BIGINT:
        case SIGNED:
            return DataTypes.LongType;
        case BIGINT_UNSIGNED:
        case UNSIGNED:
            return DataTypes.ULongType;
        case DECIMAL:
            return new DecimalType(precision, scale);
        // 注意目前只处理 scale 大于 0 的情况，因为之前没有办法区分0位小数保留和null的情况，
        // 目前会存储未指定精度并存储为DECIMAL_NOT_SPECIFIED
        case FLOAT:
            if (scale > 0 && scale < XResultUtil.DECIMAL_NOT_SPECIFIED) {
                return new FloatType(scale);
            } else {
                return DataTypes.FloatType;
            }
        case DOUBLE:
            if (scale > 0 && scale < XResultUtil.DECIMAL_NOT_SPECIFIED) {
                return new DoubleType(scale);
            } else {
                return DataTypes.DoubleType;
            }
        case DATETIME:
            return new DateTimeType(MySQLTimeTypeUtil.normalizeScale(scale));
        case ENUM:
            return new EnumType(((EnumSqlType) relDataType).getStringValues());
        case DATE:
            return DataTypes.DateType;
        case TIME:
            return new TimeType(MySQLTimeTypeUtil.normalizeScale(scale));
        case TIMESTAMP:
            return new TimestampType(MySQLTimeTypeUtil.normalizeScale(scale));
        case YEAR:
            return DataTypes.YearType;
        case SYMBOL:
        case VARCHAR:
            return new VarcharType(charsetName, collationName, precision);
        case CHAR:
            if (relDataType instanceof SetSqlType) {
                return new SetType(((SetSqlType) relDataType).getSetValues());
            } else {
                return new CharType(charsetName, collationName, precision);
            }
        case BLOB:
            return DataTypes.BlobType;
        case BINARY:
            return new BinaryType(precision);
        case VARBINARY:
        case GEOMETRY:
            return DataTypes.BinaryType;
        case BINARY_VARCHAR:
            return DataTypes.BinaryStringType;
        case BIT:
            return DataTypes.BitType;
        case BIG_BIT:
            return DataTypes.BigBitType;
        case NULL:
            // singleton
            return DataTypes.NullType;
        case JSON:
            return DataTypes.JsonType;
        case INTERVAL:
        case INTERVAL_YEAR_MONTH:
        case INTERVAL_YEAR:
        case INTERVAL_MONTH:
        case INTERVAL_DAY:
        case INTERVAL_DAY_HOUR:
        case INTERVAL_DAY_MINUTE:
        case INTERVAL_DAY_SECOND:
        case INTERVAL_DAY_MICROSECOND:
        case INTERVAL_HOUR:
        case INTERVAL_HOUR_MINUTE:
        case INTERVAL_HOUR_SECOND:
        case INTERVAL_HOUR_MICROSECOND:
        case INTERVAL_MINUTE:
        case INTERVAL_MINUTE_SECOND:
        case INTERVAL_MINUTE_MICROSECOND:
        case INTERVAL_SECOND_MICROSECOND:
        case INTERVAL_SECOND:
        case INTERVAL_MICROSECOND:
            return DataTypes.IntervalType;
        default:
            throw new UnsupportedOperationException(
                "unsupported data type: " + relDataType.getSqlTypeName().toString());
        }
    }
}
