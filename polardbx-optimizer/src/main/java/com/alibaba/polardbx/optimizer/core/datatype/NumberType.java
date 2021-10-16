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
import com.alibaba.polardbx.common.datatype.UInt64;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.convertor.Convertor;
import com.alibaba.polardbx.common.utils.convertor.ConvertorException;
import com.alibaba.polardbx.common.utils.convertor.ConvertorHelper;
import com.alibaba.polardbx.common.utils.time.MySQLTimeConverter;
import com.alibaba.polardbx.common.utils.time.MySQLTimeTypeUtil;
import com.alibaba.polardbx.common.utils.time.core.MysqlDateTime;
import com.alibaba.polardbx.optimizer.core.expression.bean.EnumValue;
import com.alibaba.polardbx.optimizer.core.expression.bean.NullValue;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.exception.TruncatedDoubleValueOverflowException;
import io.airlift.slice.Slice;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 常见类型的转化处理
 *
 * @author jianghang 2014-1-21 上午12:39:45
 * @since 5.0.0
 */
public abstract class NumberType<DATA> extends AbstractDataType<DATA> {

    /**
     * Match a valid number  e.g. 1234, 12.34, 1.23e5, -1.23e-5, 012
     * <p>
     * Ref: https://stackoverflow.com/questions/638565/parsing-scientific-notation-sensibly
     */
    private static final Pattern NUMBER_PATTERN =
        Pattern.compile("^[+\\-]?(?:\\d+)(?:\\.\\d*)?(?:[eE][+\\-]?\\d+)?");

    protected Convertor convertor = null;
    protected Convertor stringConvertor = null;

    public NumberType() {
        convertor = ConvertorHelper.commonToCommon;
        stringConvertor = ConvertorHelper.stringToCommon;
    }

    @Override
    public ResultGetter getResultGetter() {
        return new ResultGetter() {

            @Override
            public Object get(ResultSet rs, int index) throws SQLException {
                Class clazz = getDataClass();
                if (clazz.equals(Integer.class)) {
                    return rs.getInt(index);
                } else if (clazz.equals(Short.class)) {
                    return rs.getShort(index);
                } else if (clazz.equals(Long.class)) {
                    return rs.getLong(index);
                } else if (clazz.equals(Boolean.class)) {
                    return rs.getBoolean(index);
                } else if (clazz.equals(Byte.class)) {
                    return rs.getByte(index);
                } else if (clazz.equals(Float.class)) {
                    return rs.getFloat(index);
                } else if (clazz.equals(Double.class)) {
                    return rs.getDouble(index);
                } else if (clazz.equals(BigInteger.class)) {
                    return rs.getBigDecimal(index).toBigInteger();
                } else if (clazz.equals(BigDecimal.class)) {
                    return rs.getBigDecimal(index);
                } else {
                    throw new TddlRuntimeException(ErrorCode.ERR_NOT_SUPPORT, clazz.getSimpleName());
                }
            }

            @Override
            public Object get(Row rs, int index) {
                Object val = rs.getObject(index);
                return convertFrom(val);
            }

        };
    }

    @Override
    public int compare(Object o1, Object o2) {
        if (o1 == o2) {
            return 0;
        }

        DATA no1 = convertFrom(o1);
        DATA no2 = convertFrom(o2);

        if (no1 == null) {
            return -1;
        }

        if (no2 == null) {
            return 1;
        }

        return ((Comparable) no1).compareTo(no2);
    }

    @Override
    public DATA convertFrom(Object value) {
        if (value == null || value instanceof NullValue) {
            return null;
        }
        if (value instanceof UInt64) {
            value = ((UInt64) value).toBigInteger();
        } else if (value instanceof Timestamp) {
            // convert timestamp to long with round
            MysqlDateTime t = MySQLTimeTypeUtil.toMysqlDateTime((Timestamp) value);
            value = MySQLTimeConverter.convertTemporalToNumeric(t, Types.TIMESTAMP, getSqlType());
            value = truncate((Number) value);
        } else if (value instanceof Time) {
            // convert time to long with round
            MysqlDateTime t = MySQLTimeTypeUtil.toMysqlTime((Time) value);
            value = MySQLTimeConverter.convertTemporalToNumeric(t, Types.TIME, getSqlType());
            value = truncate((Number) value);
        } else if (value instanceof Date) {
            // convert date to long
            MysqlDateTime t = MySQLTimeTypeUtil.toMysqlDate((Date) value);
            value = MySQLTimeConverter.convertTemporalToNumeric(t, Types.DATE, getSqlType());
            value = truncate((Number) value);
        } else if (value instanceof Slice) {
            // converting from slice, is equal to convert from a string with utf-8 encoding.
            value = ((Slice) value).toString(CharsetName.DEFAULT_STORAGE_CHARSET_IN_CHUNK);
        } else if (value instanceof EnumValue) {
            return (DATA) ((EnumValue) value).type.convertTo(this, (EnumValue) value);
        }

        Class targetClass = getDataClass();
        try {
            try {
                if (value instanceof String && ConvertorHelper.commonTypes.containsKey(targetClass)) {
                    return (DATA) stringConvertor.convert(value, targetClass);
                } else {
                    return (DATA) convertor.convert(value, targetClass);
                }
            } catch (ConvertorException e) {
                Convertor cv = getConvertor(value.getClass());
                if (cv != null) {
                    return (DATA) cv.convert(value, targetClass);
                } else {
                    return (DATA) value;
                }
            }
        } catch (NumberFormatException e) {
            // 针对字符串转为integer类型,比如1.000f
            if (value instanceof String) {
                // 转为BigDecimal对象后再转一次
                BigDecimal truncatedVal = truncateIncorrectDoubleValue((String) value);
                // 判断val是否在当前类型的值范围内
                if (inValueRange(truncatedVal) || truncatedVal.equals(BigDecimal.ZERO)) {
                    return convertFrom(truncatedVal);
                } else {
                    throw new TruncatedDoubleValueOverflowException("not in column's value range for " + value);
                }
            } else {
                // mysql中针对不可识别的字符，返回数字0
                return (DATA) stringConvertor.convert("0", getDataClass());
            }
        }
    }

    /**
     * mysql对于不符合当前列类型的值尝试转换为double
     */
    private BigDecimal truncateIncorrectDoubleValue(String str) {
        Matcher matcher = NUMBER_PATTERN.matcher(str);
        if (matcher.find()) {
            assert matcher.start() == 0 : "bad pattern";
            return new BigDecimal(str.substring(0, matcher.end()));
        }
        return BigDecimal.ZERO;
    }

    /**
     * 判断val是否在当前类型的值范围内
     */
    protected final boolean inValueRange(BigDecimal val) {
        BigDecimal minVal = getMinValueToDecimal();
        BigDecimal maxVal = getMaxValueToDecimal();
        return maxVal.compareTo(val) >= 0 && minVal.compareTo(val) <= 0;
    }

    /**
     * 获取当前类型最大值对应的BigDecimal的形式
     */
    protected abstract BigDecimal getMaxValueToDecimal();

    /**
     * 获取当前类型最小值对应的BigDecimal的形式
     */
    protected abstract BigDecimal getMinValueToDecimal();

    private Number truncate(Number value) {
        // 为了避免随机实验室新增报错，暂且在这里将过长的value截断
        // 实际上应该优化下long to int的逻辑
        switch (getSqlType()) {
        case Types.TINYINT:
            return value.shortValue();
        case Types.SMALLINT:
        case Types.INTEGER:
        case Types.BOOLEAN:
            return value.intValue();
        case Types.BIGINT:
            return value.longValue();
        default:
            return value;
        }
    }
}
