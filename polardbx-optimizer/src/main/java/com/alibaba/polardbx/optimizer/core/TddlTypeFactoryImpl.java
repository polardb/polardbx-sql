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

package com.alibaba.polardbx.optimizer.core;

import com.alibaba.polardbx.common.charset.CharsetName;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFamily;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.type.IntervalSqlType;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;

import java.nio.charset.Charset;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by lingce.ldm on 2017/1/19.
 */
public class TddlTypeFactoryImpl extends SqlTypeFactoryImpl {

    public TddlTypeFactoryImpl(RelDataTypeSystem typeSystem) {
        super(typeSystem);
    }

    @Override
    public Charset getDefaultCharset() {
        return CharsetName.defaultCharset().toJavaCharset();
    }

    @Override
    public RelDataType leastRestrictive(List<RelDataType> types) {
        assert types != null && types.size() > 0;
        RelDataType type0 = types.get(0);
        RelDataType dataType = tddlLeastRestrictive(types);
        if (type0.getSqlTypeName() != null && dataType == null) {
            return leastRestrictiveByCast(types);
        }
        return dataType;
    }

    @Override
    public RelDataType leastRestrictiveForDML(List<RelDataType> types) {
        assert types != null && types.size() > 0;
        return types.get(0);
    }

    // In this inference rule, it will try type casting when there is no result
    // type inferred.
    // This is designed for arithmetic expressions and filter expressions that
    // will treat non-numeric
    // type as double, achieved by being chained with ReturnTypes.DOUBLE.
    //
    @Override
    public RelDataType arithmeticCastLeastRestrictive(List<RelDataType> types) {
        assert types != null;
        assert types.size() >= 1;
//        RelDataType type0 = types.get(0);
        RelDataType resultType = super.arithmeticCastLeastRestrictive(types);
//        if (type0.getSqlTypeName() != null && resultType == null) {
//            return leastRestrictiveByCast(types);
//        }
        return resultType;
    }

    // In this inference rule, it will not try type casting when there is no
    // result type inferred.
    public RelDataType tddlLeastRestrictive(List<RelDataType> types) {
        assert types != null;
        assert types.size() >= 1;

        RelDataType type0 = types.get(0);
        if (type0.getSqlTypeName() != null) {
            RelDataType resultType = leastRestrictiveSqlType(types);
            return resultType;
        }
        return super.leastRestrictive(types);
    }

    @Override
    protected RelDataType leastRestrictiveByCast(List<RelDataType> types) {
        RelDataType resultType = types.get(0);
        boolean anyNullable = resultType.isNullable();
        for (int i = 1; i < types.size(); i++) {
            RelDataType type = types.get(i);
            if (type.getSqlTypeName() == SqlTypeName.NULL) {
                anyNullable = true;
                continue;
            }

            if (type.isNullable()) {
                anyNullable = true;
            }

            if (SqlTypeUtil.canCastFrom(type, resultType, true)) {
                resultType = type;
            } else {
                if (!SqlTypeUtil.canCastFrom(resultType, type, true)) {
                    return null;
                }
            }
        }
        if (anyNullable) {
            return createTypeWithNullability(resultType, true);
        } else {
            return resultType;
        }
    }

    @Override
    protected RelDataType leastRestrictiveSqlType(List<RelDataType> types) {
        RelDataType resultType = null;
        int nullCount = 0;
        int nullableCount = 0;
        int javaCount = 0;
        int anyCount = 0;

        for (RelDataType type : types) {
            final SqlTypeName typeName = type.getSqlTypeName();
            if (typeName == null) {
                return null;
            }
            if (typeName == SqlTypeName.ANY) {
                anyCount++;
            }
            if (type.isNullable()) {
                ++nullableCount;
            }
            if (typeName == SqlTypeName.NULL) {
                ++nullCount;
            }
            if (isJavaType(type)) {
                ++javaCount;
            }
        }

        // if any of the inputs are ANY, the output is ANY
        if (anyCount > 0) {
            return createTypeWithNullability(createSqlType(SqlTypeName.ANY), nullCount > 0 || nullableCount > 0);
        }

        for (int i = 0; i < types.size(); ++i) {
            RelDataType type = types.get(i);
            RelDataTypeFamily family = type.getFamily();

            final SqlTypeName typeName = type.getSqlTypeName();
            if (typeName == SqlTypeName.NULL) {
                continue;
            }

            // Convert Java types; for instance, JavaType(int) becomes INTEGER.
            // Except if all types are either NULL or Java types.
            if (isJavaType(type) && javaCount + nullCount < types.size()) {
                final RelDataType originalType = type;
                type = typeName.allowsPrecScale(true, true) ? createSqlType(typeName,
                    type.getPrecision(),
                    type.getScale()) : typeName.allowsPrecScale(true, false) ? createSqlType(typeName,
                    type.getPrecision()) : createSqlType(typeName);
                type = createTypeWithNullability(type, originalType.isNullable());
            }

            if (resultType == null) {
                resultType = type;
                if (resultType.getSqlTypeName() == SqlTypeName.ROW) {
                    return leastRestrictiveStructuredType(types);
                }
            }

            RelDataTypeFamily resultFamily = resultType.getFamily();
            SqlTypeName resultTypeName = resultType.getSqlTypeName();

            if (resultFamily != family) {
                if (SqlTypeUtil.inCharFamily(resultType) || SqlTypeUtil.inCharFamily(type)) {
                    // varchar can be the
                    // least restrictive
                    // type for
                    // non-arithmetic
                    // funcs
                    resultType = SqlTypeUtil.inCharFamily(resultType) ? resultType : type;
                    continue;
                }
                // Deal with inference between datetime and numeric
                if (SqlTypeUtil.inDateTimeFamily(type)) {
                    if (SqlTypeUtil.isNumeric(resultType)) {
                        type = createSqlType(SqlTypeName.BIGINT);
                    } else if (SqlTypeUtil.inDateTimeFamily(resultType)) {
                        resultType = createSqlType(SqlTypeName.DATETIME);
                        continue;
                    }
                } else if (SqlTypeUtil.inDateTimeFamily(resultType) && SqlTypeUtil.isNumeric(type)) {
                    resultType = createSqlType(SqlTypeName.BIGINT);
                    continue;
                } else if (!SqlTypeFamily.NUMERIC_BOOLEAN.contains(resultType)
                    || !SqlTypeFamily.NUMERIC_BOOLEAN.contains(type)) {

                    return null;

                }
            }
            if (SqlTypeUtil.inCharOrBinaryFamilies(type)) {
                Charset charset1 = type.getCharset();
                Charset charset2 = resultType.getCharset();
                SqlCollation collation1 = type.getCollation();
                SqlCollation collation2 = resultType.getCollation();

                // TODO: refine collation combination rules
                final int precision = SqlTypeUtil.maxPrecision(resultType.getPrecision(), type.getPrecision());

                // If either type is LOB, then result is LOB with no precision.
                // Otherwise, if either is variable width, result is variable
                // width. Otherwise, result is fixed width.
                if (SqlTypeUtil.isLob(resultType)) {
                    resultType = createSqlType(resultType.getSqlTypeName());
                } else if (SqlTypeUtil.isLob(type)) {
                    resultType = createSqlType(type.getSqlTypeName());
                } else if (SqlTypeUtil.isBoundedVariableWidth(resultType)) {
                    resultType = createSqlType(resultType.getSqlTypeName(), precision);
                } else {
                    // this catch-all case covers type variable, and both fixed

                    SqlTypeName newTypeName = type.getSqlTypeName();

                    if (shouldRaggedFixedLengthValueUnionBeVariable()) {
                        if (resultType.getPrecision() != type.getPrecision()) {
                            if (newTypeName == SqlTypeName.CHAR) {
                                newTypeName = SqlTypeName.VARCHAR;
                            } else if (newTypeName == SqlTypeName.BINARY) {
                                newTypeName = SqlTypeName.VARBINARY;
                            }
                        }
                    }

                    resultType = createSqlType(newTypeName, precision);
                }
                Charset charset = null;
                SqlCollation collation = null;
                if ((charset1 != null) || (charset2 != null)) {
                    if (charset1 == null) {
                        charset = charset2;
                        collation = collation2;
                    } else if (charset2 == null) {
                        charset = charset1;
                        collation = collation1;
                    } else if (charset1.equals(charset2)) {
                        charset = charset1;
                        collation = collation1;
                    } else if (charset1.contains(charset2)) {
                        charset = charset1;
                        collation = collation1;
                    } else {
                        charset = charset2;
                        collation = collation2;
                    }
                }
                if (charset != null) {
                    resultType = createTypeWithCharsetAndCollation(resultType, charset, collation);
                }
            } else if (SqlTypeUtil.isExactNumeric(type)) {
                if (SqlTypeUtil.isExactNumeric(resultType)) {
                    // TODO: come up with a cleaner way to support
                    // interval + datetime = datetime
                    // if (types.size() > (i + 1)) {
                    // RelDataType type1 = types.get(i + 1);
                    // if (SqlTypeUtil.isDatetime(type1)) {
                    // resultType = type1;
                    // return createTypeWithNullability(resultType,
                    // nullCount > 0 || nullableCount > 0);
                    // }
                    // }
                    if (!type.equals(resultType)) {
                        if (SqlTypeUtil.exactNumberRestrcitiveLevel(type) > SqlTypeUtil
                            .exactNumberRestrcitiveLevel(resultType)) {
                            resultType = type;
                        } else if (typeName != SqlTypeName.DECIMAL && resultTypeName != SqlTypeName.DECIMAL) {
                            // use the bigger primitive
                            if (type.getPrecision() > resultType.getPrecision()) {
                                resultType = type;
                            }
                        } else {
                            // Let the result type have precision (p), scale (s)
                            // and number of whole digits (d) as follows: d =
                            // max(p1 - s1, p2 - s2) s <= max(s1, s2) p = s + d

                            int p1 = resultType.getPrecision();
                            int p2 = type.getPrecision();
                            int s1 = resultType.getScale();
                            int s2 = type.getScale();
                            final int maxPrecision = typeSystem.getMaxNumericPrecision();
                            final int maxScale = typeSystem.getMaxNumericScale();

                            int dout = Math.max(p1 - s1, p2 - s2);
                            dout = Math.min(dout, maxPrecision);

                            int scale = Math.max(s1, s2);
                            scale = Math.min(scale, maxPrecision - dout);
                            scale = Math.min(scale, maxScale);

                            int precision = dout + scale;
                            assert precision <= maxPrecision;
                            assert precision > 0
                                || (resultType.getSqlTypeName() == SqlTypeName.DECIMAL && precision == 0 && scale == 0);

                            resultType = createSqlType(SqlTypeName.DECIMAL, precision, scale);
                        }
                    }
                } else if (SqlTypeUtil.isApproximateNumeric(resultType)) {
                    // already approximate; promote to double just in case
                    // TODO: only promote when required
                    if (SqlTypeUtil.isDecimal(type)) {
                        // Only promote to double for decimal types
                        resultType = createDoublePrecisionType();
                    }
                } else {
                    return null;
                }
            } else if (SqlTypeUtil.isApproximateNumeric(type)) {
                if (SqlTypeUtil.isApproximateNumeric(resultType)) {
                    if (type.getPrecision() > resultType.getPrecision()) {
                        resultType = type;
                    }
                } else if (SqlTypeUtil.isExactNumeric(resultType)) {
                    if (SqlTypeUtil.isDecimal(resultType)) {
                        resultType = createDoublePrecisionType();
                    } else {
                        resultType = type;
                    }
                } else {
                    return null;
                }
            } else if (SqlTypeUtil.isInterval(type)) {
                // TODO: come up with a cleaner way to support
                // interval + datetime = datetime
                if (types.size() > (i + 1)) {
                    RelDataType type1 = types.get(i + 1);
                    if (SqlTypeUtil.isDatetime(type1)) {
                        resultType = type1;
                        return createTypeWithNullability(resultType, nullCount > 0 || nullableCount > 0);
                    }
                }

                if (!type.equals(resultType)) {
                    // TODO jvs 4-June-2005: This shouldn't be necessary;
                    // move logic into IntervalSqlType.combine
                    Object type1 = resultType;
                    resultType = ((IntervalSqlType) resultType).combine(this, (IntervalSqlType) type);
                    resultType = ((IntervalSqlType) resultType).combine(this, (IntervalSqlType) type1);
                }
            } else if (SqlTypeUtil.isDatetime(type)) {
                // TODO: come up with a cleaner way to support
                // datetime +/- interval (or integer) = datetime
                if (types.size() > (i + 1)) {
                    RelDataType type1 = types.get(i + 1);
                    if (SqlTypeUtil.isInterval(type1) || SqlTypeUtil.isIntType(type1)) {
                        if (SqlTypeUtil.isDatetime(type1)) {
                            resultType = this.createSqlType(SqlTypeName.BIGINT);

                        } else if (SqlTypeUtil.isNumeric(type1)) {
                            if (SqlTypeUtil.isDecimal(type1) || SqlTypeUtil.isApproximateNumeric(type1)) {
                                resultType = this.createSqlType(SqlTypeName.DECIMAL);
                            } else {
                                resultType = this.createSqlType(SqlTypeName.BIGINT);
                            }
                        } else {
                            resultType = this.createSqlType(SqlTypeName.DATETIME);
                        }
                        return createTypeWithNullability(resultType, nullCount > 0 || nullableCount > 0);
                    }
                }
            } else {
                // TODO: datetime precision details; for now we let
                // leastRestrictiveByCast handle it
                return null;
            }
        }
        if (resultType != null && nullableCount > 0) {
            resultType = createTypeWithNullability(resultType, true);
        }
        return resultType;
    }

    @Override
    protected RelDataType leastRestrictiveStructuredType(final List<RelDataType> types) {
        final RelDataType type0 = types.get(0);
        final int fieldCount = type0.getFieldCount();

        // precheck that all types are structs with same number of fields
        for (RelDataType type : types) {
            if (!type.isStruct()) {
                return null;
            }
            if (type.getFieldList().size() != fieldCount) {
                throw new RuntimeException("The used SELECT statements have a different number of columns");
            }
        }

        // recursively compute column-wise least restrictive
        final Builder builder = builder();
        for (int j = 0; j < fieldCount; ++j) {
            // REVIEW jvs 22-Jan-2004: Always use the field name from the
            // first type?
            List<RelDataType> typeCandidates = new LinkedList<>();
            for (RelDataType recordType : types) {
                typeCandidates.add(recordType.getFieldList().get(j).getType());
            }
            RelDataType columnType = leastRestrictive(typeCandidates);
            // Noted: Tddl leastRestrictive inference will not try to do type
            // casting,
            // such that leastRestrictiveByCast is necessary when inferred type
            // is null,
            // otherwise there will be null type exist in record type which will
            // fail the sql validation.
            if (columnType == null) {
                columnType = leastRestrictiveByCast(typeCandidates);
            }
            builder.add(type0.getFieldList().get(j).getName(), columnType);
        }
        return builder.build();
    }
}
