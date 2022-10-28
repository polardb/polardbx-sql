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

package com.alibaba.polardbx.optimizer.core.function.calc.scalar.cast;

import com.alibaba.polardbx.common.charset.CharsetName;
import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.common.datatype.DecimalConverter;
import com.alibaba.polardbx.common.datatype.DecimalStructure;
import com.alibaba.polardbx.common.datatype.UInt64;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.datatype.DateTimeType;
import com.alibaba.polardbx.optimizer.core.datatype.SliceType;
import com.alibaba.polardbx.optimizer.core.datatype.StringType;
import com.alibaba.polardbx.optimizer.core.datatype.TimeType;
import com.alibaba.polardbx.optimizer.core.datatype.TimestampType;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;
import com.alibaba.polardbx.optimizer.utils.FunctionUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 对应mysql的cast函数
 *
 * @author jianghang 2014-7-1 上午11:08:14
 * @since 5.1.7
 */
public class Cast extends AbstractScalarFunction {
    public Cast(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    public static final BigInteger BIGINT_MAX_VALUE = new BigInteger("18446744073709551616");
    public static final BigInteger LONG_MAX_VALUE = new BigInteger("9223372036854775807");

    public static CastType parseCastType(List<Object> args) {
        CastType castType = new CastType();
        int startIndex = 0;
        String type = DataTypes.StringType.convertFrom(args.get(startIndex));
        if (type.equalsIgnoreCase("BINARY")) {
            if (args.size() > startIndex + 1) {
                castType.type1 = DataTypes.IntegerType.convertFrom(args.get(startIndex + 1));
            }
            castType.type = DataTypes.BytesType;
        } else if (type.equalsIgnoreCase("CHAR")) {
            if (args.size() > startIndex + 1) {
                castType.type1 = DataTypes.IntegerType.convertFrom(args.get(startIndex + 1));
            }
            castType.type = DataTypes.StringType;
        } else if (type.equalsIgnoreCase("DATE")) {
            castType.type = DataTypes.DateType;
        } else if (type.equalsIgnoreCase("DATETIME")) {
            if (args.size() > startIndex + 1) {
                castType.type1 = DataTypes.IntegerType.convertFrom(args.get(startIndex + 1));
            }
            castType.type = new DateTimeType(castType.type1);
        } else if (type.equalsIgnoreCase("TIMESTAMP")) {
            if (args.size() > startIndex + 1) {
                castType.type1 = DataTypes.IntegerType.convertFrom(args.get(startIndex + 1));
            }
            castType.type = new TimestampType(castType.type1);
        } else if (type.equalsIgnoreCase("TIME")) {
            if (args.size() > startIndex + 1) {
                castType.type1 = DataTypes.IntegerType.convertFrom(args.get(startIndex + 1));
            }
            castType.type = new TimeType(castType.type1);
        } else if (type.equalsIgnoreCase("DECIMAL")) {
            castType.type = DataTypes.DecimalType;
            if (args.size() > startIndex + 1) {
                castType.type1 = DataTypes.IntegerType.convertFrom(args.get(startIndex + 1));
            }
            if (args.size() > startIndex + 2) {
                castType.type2 = DataTypes.IntegerType.convertFrom(args.get(startIndex + 2));
            }

            if (castType.type1 != null) {
                if (castType.type2 == null) {
                    castType.type2 = 0;
                }

                if (castType.type1 < castType.type2) {
                    throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                        "For float(M,D), double(M,D) or decimal(M,D), M must be >= D (column '')");
                }
            }

        } else if (type.equalsIgnoreCase("SIGNED") || type.equalsIgnoreCase("BIGINT")) {
            castType.type = DataTypes.LongType;
            castType.signed = true;
        } else if (type.equalsIgnoreCase("UNSIGNED")) {
            castType.type = DataTypes.ULongType;
            castType.signed = false;
        } else if (type.equalsIgnoreCase("INTEGER")) {
            castType.type = DataTypes.IntegerType;
            castType.signed = false;
        } else if (type.equalsIgnoreCase("VARCHAR")) {
            if (args.size() > startIndex + 1) {
                castType.type1 = DataTypes.IntegerType.convertFrom(args.get(startIndex + 1));
            }
            castType.type = DataTypes.StringType;
        } else if (type.equalsIgnoreCase("DOUBLE")) {
            castType.type = DataTypes.DoubleType;
            castType.signed = true;
        } else if (type.equalsIgnoreCase("JSON")) {
            castType.type = DataTypes.JsonType;
        } else if (type.equalsIgnoreCase("FLOAT")) {
            castType.type = DataTypes.FloatType;
            castType.signed = true;
        } else if (type.equalsIgnoreCase("YEAR")) {
            castType.type = DataTypes.YearType;
            castType.signed = false;
        } else if (type.equalsIgnoreCase("TINYINT")) {
            castType.type = DataTypes.TinyIntType;
            castType.signed = false;
        } else if (type.equalsIgnoreCase("SMALLINT")) {
            castType.type = DataTypes.SmallIntType;
            castType.signed = false;
        } else if (type.equalsIgnoreCase("BLOB")) {
            castType.type = DataTypes.BlobType;
            castType.signed = false;
        } else if (type.equalsIgnoreCase("BIT")) {
            castType.type = DataTypes.BitType;
            castType.signed = false;
        } else {
            throw new TddlRuntimeException(ErrorCode.ERR_NOT_SUPPORT, "cast type:" + type);
        }

        return castType;
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        if (FunctionUtils.isNull(args[0])) {
            return null;
        }
        Object obj = null;
        CastType castType = getType(Arrays.stream(args).collect(Collectors.toList()), 1);
        if (castType.type instanceof SliceType) {
            obj = DataTypes.StringType.convertFrom(args[0]);
        } else if (DataTypeUtil.isFractionalTimeType(castType.type)) {
            obj = castType.type.convertFrom(args[0]);
        } else {
            obj = castType.type.convertFrom(args[0]);
        }
        return computeInner(castType, obj);
    }

    protected Object computeInner(CastType castType, Object obj) {
        if (DataTypeUtil.equalsSemantically(DataTypes.BytesType, castType.type)) {
            if (castType.type1 != null) {
                // 如果超过byte数量，前面用0x00填充
                int length = ((byte[]) obj).length;
                byte[] bytes = new byte[castType.type1];
                int min = length > castType.type1 ? castType.type1 : length;
                System.arraycopy(obj, 0, bytes, 0, min);
                for (int i = min; i < castType.type1; i++) {
                    bytes[i] = 0;
                }
                return bytes;
            }
        } else if (castType.type instanceof StringType) {
            if (castType.type1 != null) {
                String str = DataTypes.StringType.convertFrom(obj);
                int length = str.length();
                int min = length > castType.type1 ? castType.type1 : length;
                return str.substring(0, min);
            }
        } else if (castType.type instanceof SliceType) {
            if (castType.type1 != null) {
                String str = DataTypes.StringType.convertFrom(obj);
                int length = str.length();
                int min = length > castType.type1 ? castType.type1 : length;
                return str.substring(0, min).getBytes(CharsetName.DEFAULT_STORAGE_CHARSET_IN_CHUNK);
            }
        } else if (DataTypeUtil.equalsSemantically(DataTypes.DecimalType, castType.type)) {
            BigDecimal bd = ((Decimal) obj).toBigDecimal();
            if (castType.type1 != null && castType.type2 != null && castType.type1 > 0) {
                DecimalStructure dec = ((Decimal) obj).getDecimalStructure();
                int precision = castType.type1;
                int scale = castType.type2;

                DecimalConverter.rescale(dec, dec, precision, scale, false);
                return obj;
            } else {
                bd = bd.setScale(0, BigDecimal.ROUND_HALF_UP);
                return Decimal.fromBigDecimal(bd);
            }
        } else if (DataTypeUtil.equalsSemantically(DataTypes.ULongType, castType.type)) {
            if (obj instanceof UInt64) {
                if (!castType.signed && UInt64.UINT64_ZERO.compareTo((UInt64) obj) > 0) {
                    return ((BigInteger) obj).add(BIGINT_MAX_VALUE);
                } else if (castType.signed && UInt64.MAX_UINT64.compareTo((UInt64) obj) < 0) {
                    return ((BigInteger) obj).subtract(BIGINT_MAX_VALUE);
                }
            } else if (obj instanceof BigInteger) {
                if (!castType.signed && BigInteger.ZERO.compareTo((BigInteger) obj) > 0) {
                    return ((BigInteger) obj).add(BIGINT_MAX_VALUE);
                } else if (castType.signed && LONG_MAX_VALUE.compareTo((BigInteger) obj) < 0) {
                    return ((BigInteger) obj).subtract(BIGINT_MAX_VALUE);
                }
            }
        }

        return obj;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"CAST"};
    }

    /**
     * <pre>
     * BINARY[(N)]
     * CHAR[(N)]
     * DATE
     * DATETIME
     * DECIMAL[(M[,D])]
     * SIGNED [INTEGER]
     * TIME
     * UNSIGNED [INTEGER]
     * </pre>
     */
    protected CastType getType(List<Object> args, int startIndex) {
        return parseCastType(args.subList(startIndex, args.size()));
    }

    public static class CastType {
        protected DataType type;
        protected Integer type1;
        protected Integer type2;
        protected boolean signed = true;

        public DataType getType() {
            return type;
        }

        public Integer getType1() {
            return type1;
        }

        public Integer getType2() {
            return type2;
        }

        public boolean isSigned() {
            return signed;
        }
    }
}
