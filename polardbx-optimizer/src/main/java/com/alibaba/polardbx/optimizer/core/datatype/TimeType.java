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
import com.alibaba.polardbx.common.utils.time.calculator.MySQLTimeCalculator;
import com.alibaba.polardbx.common.utils.time.core.MysqlDateTime;
import com.alibaba.polardbx.common.utils.time.core.OriginalTime;
import com.alibaba.polardbx.common.utils.time.core.TimeStorage;
import com.alibaba.polardbx.optimizer.core.row.Row;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Types;
import java.util.Calendar;
import java.util.Optional;

/**
 * {@link Time}类型
 *
 * @author jianghang 2014-1-21 下午5:33:07
 * @since 5.0.0
 */
public class TimeType extends AbstractDataType<java.sql.Time> {
    public static final TimeType TIME_TYPE_0 = new TimeType(0);
    public static final TimeType TIME_TYPE_1 = new TimeType(1);
    public static final TimeType TIME_TYPE_2 = new TimeType(2);
    public static final TimeType TIME_TYPE_3 = new TimeType(3);
    public static final TimeType TIME_TYPE_4 = new TimeType(4);
    public static final TimeType TIME_TYPE_5 = new TimeType(5);
    public static final TimeType TIME_TYPE_6 = new TimeType(6);

    private static final int LENGTH_OF_SORT_KEY = 3;

    private static final Time maxTime = Time.valueOf("23:59:59");
    private static final Time minTime = Time.valueOf("00:00:00");
    private int scale;

    public TimeType() {
        this(0);
    }

    public TimeType(int scale) {
        super();
        this.scale = scale;
    }

    private final Calculator calculator = new AbstractCalculator() {

        @Override
        public Object doAdd(Object v1, Object v2) {
            Calendar cal = Calendar.getInstance();
            if (v1 instanceof IntervalType) {
                Time i2 = convertFrom(v2);
                cal.setTime(i2);
                ((IntervalType) v1).process(cal, 1);
            } else if (v2 instanceof IntervalType) {
                Time i1 = convertFrom(v1);
                cal.setTime(i1);
                ((IntervalType) v2).process(cal, 1);
            } else {
                throw new NotSupportException("时间类型不支持算术符操作");
            }

            return convertFrom(cal.getTime());
        }

        @Override
        public Object doSub(Object v1, Object v2) {
            Calendar cal = Calendar.getInstance();
            if (v1 instanceof IntervalType) {
                Time i2 = convertFrom(v2);
                cal.setTime(i2);
                ((IntervalType) v1).process(cal, -1);
            } else if (v2 instanceof IntervalType) {
                Time i1 = convertFrom(v1);
                cal.setTime(i1);
                ((IntervalType) v2).process(cal, -1);
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
                return rs.getTime(index);
            }

            @Override
            public Object get(Row rs, int index) {
                Object val = rs.getObject(index);
                return convertFrom(val);
            }
        };
    }

    @Override
    public Time getMaxValue() {
        return maxTime;
    }

    @Override
    public Time getMinValue() {
        return minTime;
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

        Time d1 = convertFrom(o1, false);
        Time d2 = convertFrom(o2, false);
        if (d1 == null) {
            return -1;
        }

        if (d2 == null) {
            return 1;
        }

        long l1 = TimeStorage.packTime(d1);
        long l2 = TimeStorage.packTime(d2);

        return (l1 > l2) ? 1 : (l1 == l2 ? 0 : -1);
    }

    private Long makePackedLong(Object o) {
        if (o instanceof Long) {
            return (Long) o;
        } else {
            return Optional.ofNullable(o)
                .map(this::convertFrom)
                .map(TimeStorage::packTime)
                .orElse(null);
        }
    }

    @Override
    public Calculator getCalculator() {
        return calculator;
    }

    @Override
    public int getSqlType() {
        return java.sql.Types.TIME;
    }

    @Override
    public String getStringSqlType() {
        return "TIME";
    }

    @Override
    public Class getDataClass() {
        return Time.class;
    }

    @Override
    public Time convertFrom(Object value) {
        return convertFrom(value, true);
    }

    @Override
    public Object convertJavaFrom(Object value) {
        Time time = convertFrom(value);
        if (time == null) {
            // compatible to old sharding key
            return super.convertFrom(value);
        } else {
            Time newTime = MySQLTimeTypeUtil.toJavaTime(time);
            return newTime;
        }
    }

    private Time convertFrom(Object value, boolean round) {
        // Try to get mysql datetime structure
        MysqlDateTime t = DataTypeUtil.toMySQLDatetime(value, Types.TIME);
        if (t == null) {
            return null;
        }
        Time time = new OriginalTime(t);
        if (!round) {
            return time;
        }

        // need to round ?
        boolean needToRound = MySQLTimeCalculator.needToRound(
            (int) t.getSecondPart(),
            scale);
        if (!needToRound) {
            return time;
        }

        // round: time -> MysqlDateTime -> OriginalTime
        return Optional.ofNullable(t)
            .map(
                mysqlDateTime -> MySQLTimeCalculator.roundTime(mysqlDateTime, scale)
            )
            .map(
                MySQLTimeTypeUtil::createOriginalTime
            )
            .orElse(null);
    }

    public String toStandardString(Object value) {
        return Optional.ofNullable(value)
            .map(this::convertFrom)
            .map(MySQLTimeTypeUtil::toMysqlTime)
            .map(
                t -> t.toTimeString(scale)
            )
            .orElseGet(
                () -> DataTypes.StringType.convertFrom(value)
            );
    }

    @Override
    public int getScale() {
        return scale;
    }

    @Override
    public int length() {
        return MySQLTimeTypeUtil.MAX_TIME_WIDTH + (scale != 0 ? scale + 1 : 0);
    }

    @Override
    public MySQLStandardFieldType fieldType() {
        return MySQLStandardFieldType.MYSQL_TYPE_TIME;
    }

    @Override
    public boolean equalDeeply(DataType that) {
        if (that == null || that.getClass() != this.getClass()) {
            return false;
        }

        TimeType thatType = (TimeType) that;
        return thatType.scale == this.scale;
    }
}
