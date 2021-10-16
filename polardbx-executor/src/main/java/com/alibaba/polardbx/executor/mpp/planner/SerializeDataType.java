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

package com.alibaba.polardbx.executor.mpp.planner;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.alibaba.polardbx.common.charset.CharsetName;
import com.alibaba.polardbx.common.charset.CollationName;
import com.alibaba.polardbx.common.utils.time.MySQLTimeTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.CharType;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.datatype.DateTimeType;
import com.alibaba.polardbx.optimizer.core.datatype.EnumType;
import com.alibaba.polardbx.optimizer.core.datatype.TimeType;
import com.alibaba.polardbx.optimizer.core.datatype.TimestampType;
import com.alibaba.polardbx.optimizer.core.datatype.VarcharType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.type.EnumSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Util;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.alibaba.polardbx.optimizer.core.datatype.DataTypes.IntervalType;

public class SerializeDataType {

    private String typeName;
    private int precision;
    private int scale;
    private boolean sensitive;
    private List<String> enumValues;

    // for char / varchar type.
    private String charsetName;
    private String collationName;

    @JsonCreator
    public SerializeDataType(
        @JsonProperty("typeName") String typeName,
        @JsonProperty("sensitive") boolean sensitive,
        @JsonProperty("precision") int precision,
        @JsonProperty("scale") int scale,
        @JsonProperty("enumValues") List<String> enumValues,
        @JsonProperty("charsetName") String charsetName,
        @JsonProperty("collationName") String collationName
    ) {
        this.typeName = typeName;
        this.sensitive = sensitive;
        this.precision = precision;
        this.scale = scale;
        this.enumValues = enumValues;
        this.charsetName = charsetName;
        this.collationName = collationName;
    }

    @JsonProperty
    public String getTypeName() {
        return typeName;
    }

    @JsonProperty
    public int getPrecision() {
        return precision;
    }

    @JsonProperty
    public int getScale() {
        return scale;
    }

    @JsonProperty
    public boolean isSensitive() {
        return sensitive;
    }

    @JsonProperty
    public List<String> getEnumValues() {
        return enumValues;
    }

    @JsonProperty
    public String getCharsetName() {
        return charsetName;
    }

    @JsonProperty
    public String getCollationName() {
        return collationName;
    }

    @Override
    public String toString() {
        if (enumValues == null) {
            return typeName + "(" + precision + "," + scale + ")";
        } else {
            return typeName + enumValues.toString().replace('[', '(').replace(']', ')');
        }
    }

    public static List<SerializeDataType> convertToSerilizeType(List<RelDataTypeField> relDataTypeList) {
        List<SerializeDataType> dataTypes = new ArrayList<>(relDataTypeList.size());
        for (RelDataTypeField relDataTypeField : relDataTypeList) {
            RelDataType relDataType = relDataTypeField.getType();
            SqlTypeName sqlTypeName = relDataType.getSqlTypeName();
            List<String> stringValues = null;
            int precision = -1;
            int scale = -1;
            if (sqlTypeName.allowsPrec()) {
                precision = relDataType.getPrecision();
            }
            if (sqlTypeName.allowsScale()) {
                scale = relDataType.getScale();
            }
            if (relDataType instanceof EnumSqlType) {
                stringValues = ((EnumSqlType) relDataType).getStringValues();
            }
            boolean sensitive = relDataType.getCollation() == null ? false : relDataType.getCollation().isSensitive();

            // serialize charset & collation info
            String charsetName = Optional.ofNullable(relDataType.getCharset())
                .map(CharsetName::of)
                .map(Enum::name)
                .orElseGet(
                    () -> CharsetName.defaultCharset().name()
                );
            String collation = Optional.ofNullable(relDataType.getCollation())
                .map(SqlCollation::getCollationName)
                .map(CollationName::of)
                .map(Enum::name)
                .orElseGet(
                    () -> CollationName.defaultCollation().name()
                );

            dataTypes.add(
                new SerializeDataType(sqlTypeName.toString(), sensitive, precision, scale, stringValues, charsetName,
                    collation));
        }
        return dataTypes;
    }

    public static List<DataType> convertToDataType(List<SerializeDataType> relDataTypeList) {
        List<DataType> dataTypes = new ArrayList<>();
        for (int i = 0; i < relDataTypeList.size(); i++) {
            SerializeDataType serializeDataType = relDataTypeList.get(i);
            final List<String> enumValues = serializeDataType.getEnumValues();
            final SqlTypeName sqlTypeName = Util.enumVal(SqlTypeName.class, serializeDataType.getTypeName());
            final String charsetName = serializeDataType.getCharsetName();
            final String collationName = serializeDataType.getCollationName();
            DataType dt = relTypeToTddlType(sqlTypeName,
                serializeDataType.getPrecision(),
                serializeDataType.getScale(),
                true,
                serializeDataType.sensitive,
                enumValues,
                charsetName,
                collationName);
            dataTypes.add(dt);
        }
        return dataTypes;
    }

    // Moved from CalciteUtils
    @Deprecated
    private static DataType relTypeToTddlType(SqlTypeName typeName, int precision, int scale, boolean hasBooleanType,
                                              boolean sensitive, List<String> enumValues, String charset,
                                              String collation) {
        // TODO FIX ME
        DataType dataType = null;
        CharsetName charsetName = Optional.ofNullable(charset)
            .map(CharsetName::of)
            .orElseGet(CharsetName::defaultCharset);
        CollationName collationName = Optional.ofNullable(collation)
            .map(CollationName::of)
            .orElseGet(CollationName::defaultCollation);
        switch (typeName) {
        case DECIMAL:
            dataType = DataTypes.DecimalType;
            break;
        case BOOLEAN:
            if (hasBooleanType) {
                dataType = DataTypes.BooleanType;
            } else {
                dataType = DataTypes.TinyIntType;
            }
            break;
        case TINYINT:
            dataType = DataTypes.TinyIntType;
            break;
        case SMALLINT:
            dataType = DataTypes.SmallIntType;
            break;
        case INTEGER:
            dataType = DataTypes.IntegerType;
            break;
        case MEDIUMINT:
            dataType = DataTypes.MediumIntType;
            break;
        case BIGINT:
            dataType = DataTypes.LongType;
            break;
        case FLOAT:
            dataType = DataTypes.FloatType;
            break;
        case DATETIME:
            dataType = new DateTimeType(MySQLTimeTypeUtil.normalizeScale(scale));
            break;
        case REAL:
        case DOUBLE:
            dataType = DataTypes.DoubleType;
            break;
        case DATE:
            dataType = DataTypes.DateType;
            break;
        case TIME:
            dataType = new TimeType(MySQLTimeTypeUtil.normalizeScale(scale));
            break;
        case TIMESTAMP:
            dataType = new TimestampType(MySQLTimeTypeUtil.normalizeScale(scale));
            break;
        case YEAR:
            dataType = DataTypes.YearType;
            break;
        case INTERVAL_YEAR:
            dataType = DataTypes.IntervalType;
            break;
        case INTERVAL_YEAR_MONTH:
            dataType = DataTypes.YearType;
            break;
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
            dataType = IntervalType;
            break;
        case SIGNED:
            dataType = DataTypes.LongType;
            break;
        case UNSIGNED:
            dataType = DataTypes.ULongType;
            break;
        case CHAR:
            dataType = new CharType(charsetName, collationName);
            break;
        case VARCHAR:
            dataType = new VarcharType(charsetName, collationName);
            break;
        case BIT:
            dataType = DataTypes.BitType;
            break;
        case BIG_BIT:
            dataType = DataTypes.BigBitType;
            break;
        case BINARY_VARCHAR:
            dataType = DataTypes.BinaryStringType;
            break;
        case BLOB:
            dataType = DataTypes.BlobType;
            break;
        case VARBINARY:
        case BINARY:
            dataType = DataTypes.BinaryType;
            break;
        case NULL:
            dataType = DataTypes.NullType;
            break;
        case INTEGER_UNSIGNED:
            dataType = DataTypes.UIntegerType;
            break;
        case TINYINT_UNSIGNED:
            dataType = DataTypes.UTinyIntType;
            break;
        case BIGINT_UNSIGNED:
            dataType = DataTypes.ULongType;
            break;
        case SMALLINT_UNSIGNED:
            dataType = DataTypes.USmallIntType;
            break;
        case MEDIUMINT_UNSIGNED:
            dataType = DataTypes.UMediumIntType;
        case ANY:
            break;
        case SYMBOL:
            break;
        case MULTISET:
            break;
        case ARRAY:
            break;
        case MAP:
            break;
        case DISTINCT:
            break;
        case STRUCTURED:
            break;
        case ROW:
            break;
        case OTHER:
            break;
        case CURSOR:
            break;
        case COLUMN_LIST:
            break;
        case DYNAMIC_STAR:
            break;
        case JSON:
            dataType = DataTypes.JsonType;
            break;
        case INTERVAL:
            dataType = DataTypes.IntervalType;
            break;
        case ENUM:
            dataType = new EnumType(enumValues);
            break;
        default:
            dataType = null;
        }
        return dataType;
    }
}
