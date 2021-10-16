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

import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.common.type.MySQLStandardFieldType;
import com.alibaba.polardbx.common.utils.convertor.Convertor;
import com.alibaba.polardbx.common.utils.time.MySQLTimeTypeUtil;
import com.alibaba.polardbx.common.utils.time.core.MysqlDateTime;
import com.alibaba.polardbx.common.utils.time.core.OriginalDate;
import com.alibaba.polardbx.common.utils.time.core.TimeStorage;
import com.alibaba.polardbx.common.utils.time.parser.TimeParserFlags;
import com.alibaba.polardbx.optimizer.core.row.Row;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Calendar;
import java.util.Optional;

/**
 * {@link Date}类型
 *
 * @author jianghang 2014-1-21 下午5:20:32
 * @since 5.0.0
 */
public class DateType extends AbstractDataType<java.sql.Date> {

    private static final Date maxDate = Date.valueOf("9999-12-31");
    private static final Date minDate = Date.valueOf("1900-01-01");

    public DateType() {
        this(0);
    }

    public DateType(int precision) {
        super();
    }

    private final Calculator calculator = new AbstractCalculator() {

        @Override
        public Object doAdd(Object v1, Object v2) {
            Calendar cal = Calendar.getInstance();
            if (v1 instanceof IntervalType) {
                Date i2 = convertFrom(v2);
                cal.setTime(i2);
                ((IntervalType) v1).process(cal, 1);
            } else if (v2 instanceof IntervalType) {
                Date i1 = convertFrom(v1);
                cal.setTime(i1);
                ((IntervalType) v2).process(cal, 1);
            } else if (v2 instanceof Integer) {
                return DataTypes.IntegerType.convertFrom(v1) + (Integer) v2;
            } else {
                throw new NotSupportException("时间类型不支持算术符操作");
            }

            return convertFrom(cal.getTime());
        }

        @Override
        public Object doSub(Object v1, Object v2) {
            Calendar cal = Calendar.getInstance();
            if (v1 instanceof IntervalType) {
                Date i2 = convertFrom(v2);
                cal.setTime(i2);
                ((IntervalType) v1).process(cal, -1);
            } else if (v2 instanceof IntervalType) {
                Date i1 = convertFrom(v1);
                cal.setTime(i1);
                ((IntervalType) v2).process(cal, -1);
            } else if (v2 instanceof Integer) {
                return DataTypes.IntegerType.convertFrom(v1) - (Integer) v2;
            } else {
                throw new NotSupportException("时间类型不支持算术符操作");
            }

            return convertFrom(cal.getTime());
        }

        @Override
        public Object doMultiply(Object v1, Object v2) {
            throw new NotSupportException("时间类型不支持算术符操作");
        }

        @Override
        public Object doDivide(Object v1, Object v2) {
            throw new NotSupportException("时间类型不支持算术符操作");
        }

        @Override
        public Object doMod(Object v1, Object v2) {
            throw new NotSupportException("时间类型不支持算术符操作");
        }

        @Override
        public Object doAnd(Object v1, Object v2) {
            throw new NotSupportException("时间类型不支持算术符操作");
        }

        @Override
        public Object doOr(Object v1, Object v2) {
            throw new NotSupportException("时间类型不支持算术符操作");
        }

        @Override
        public Object doNot(Object v1) {
            throw new NotSupportException("时间类型不支持算术符操作");
        }

        @Override
        public Object doBitAnd(Object v1, Object v2) {
            throw new NotSupportException("时间类型不支持算术符操作");
        }

        @Override
        public Object doBitOr(Object v1, Object v2) {
            throw new NotSupportException("时间类型不支持算术符操作");
        }

        @Override
        public Object doBitNot(Object v1) {
            throw new NotSupportException("时间类型不支持算术符操作");
        }

        @Override
        public Object doXor(Object v1, Object v2) {
            throw new NotSupportException("时间类型不支持算术符操作");
        }

        @Override
        public Object doBitXor(Object v1, Object v2) {
            throw new NotSupportException("时间类型不支持算术符操作");
        }
    };

    @Override
    public ResultGetter getResultGetter() {
        return new ResultGetter() {

            @Override
            public Object get(ResultSet rs, int index) throws SQLException {
                return rs.getDate(index);
            }

            @Override
            public Object get(Row rs, int index) {
                Object val = rs.getObject(index);
                return convertFrom(val);
            }
        };
    }

    @Override
    public Date getMaxValue() {
        return maxDate;
    }

    @Override
    public Date getMinValue() {
        return minDate;
    }

    @Override
    public int compare(Object o1, Object o2) {
        if (o1 == o2) {
            return 0;
        }

        // if one of the object is long type, compare by long.
        if (o1 instanceof Long || o2 instanceof Long) {
            Long packedLong1 = makePackedLong(o1);
            Long packedLong2 = makePackedLong(o2);
            if (packedLong1 == null) {
                return -1;
            }

            if (packedLong2 == null) {
                return 1;
            }
            return Long.compare(packedLong1, packedLong2);
        }

        Date d1 = convertFrom(o1);
        Date d2 = convertFrom(o2);

        if (d1 == null) {
            return -1;
        }

        if (d2 == null) {
            return 1;
        }

        long l1 = TimeStorage.packDate(d1);
        long l2 = TimeStorage.packDate(d2);

        return (l1 > l2) ? 1 : (l1 == l2 ? 0 : -1);
    }

    private Long makePackedLong(Object o) {
        if (o instanceof Long) {
            return (Long) o;
        } else {
            return Optional.ofNullable(o)
                .map(this::convertFrom)
                .map(TimeStorage::packDate)
                .orElse(null);
        }
    }

    @Override
    public Calculator getCalculator() {
        return calculator;
    }

    @Override
    public int getSqlType() {
        return java.sql.Types.DATE;
    }

    @Override
    public String getStringSqlType() {
        return "DATE";
    }

    @Override
    public Class getDataClass() {
        return Date.class;
    }

    @Override
    public Date convertFrom(Object value) {
        // Try to get mysql datetime structure
        MysqlDateTime t = DataTypeUtil.toMySQLDatetimeByFlags(
            value,
            Types.TIMESTAMP,
            TimeParserFlags.FLAG_TIME_FUZZY_DATE | TimeParserFlags.FLAG_TIME_NO_DATE_FRAC_WARN
        );

        if (t == null) {
            return null;
        }
        Date date = new OriginalDate(t);
        return date;
    }

    @Override
    public Object convertJavaFrom(Object value) {
        Date date = convertFrom(value);
        if (date == null) {
            // compatible to old sharding key
            return super.convertFrom(value);
        } else {
            return MySQLTimeTypeUtil.toJavaDate(date);
        }
    }

    public String toStandardString(Object value) {
        return Optional.ofNullable(value)
            .map(this::convertFrom)
            .map(MySQLTimeTypeUtil::toMysqlDate)
            .map(
                MysqlDateTime::toDateString
            )
            .orElseGet(
                () -> DataTypes.StringType.convertFrom(value)
            );
    }

    @Override
    public int getScale() {
        return 0;
    }

    @Override
    public int length() {
        return MySQLTimeTypeUtil.MAX_DATE_WIDTH;
    }

    @Override
    public MySQLStandardFieldType fieldType() {
        return MySQLStandardFieldType.MYSQL_TYPE_NEWDATE;
    }
}
