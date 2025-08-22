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
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.convertor.Convertor;
import com.alibaba.polardbx.common.utils.time.MySQLTimeTypeUtil;
import com.alibaba.polardbx.common.utils.time.calculator.MySQLTimeCalculator;
import com.alibaba.polardbx.common.utils.time.core.MysqlDateTime;
import com.alibaba.polardbx.common.utils.time.core.OriginalTimestamp;
import com.alibaba.polardbx.common.utils.time.core.TimeStorage;
import com.alibaba.polardbx.common.utils.time.parser.StringTimeParser;
import com.alibaba.polardbx.common.utils.time.parser.TimeParserFlags;
import com.alibaba.polardbx.optimizer.core.row.Row;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDateTime;
import java.util.Optional;

/**
 * {@link Timestamp}类型
 *
 * @author jianghang 2014-1-21 下午5:36:26
 * @since 5.0.0
 */
public class TimestampType extends AbstractDataType<java.sql.Timestamp> {

    private static final Timestamp maxTimestamp = Timestamp.valueOf("9999-12-31 23:59:59");
    private static final Timestamp minTimestamp = Timestamp.valueOf("1900-01-01 00:00:00");
    private int scale;

    public TimestampType() {
        this(0);
    }

    public TimestampType(int scale) {
        super();
        this.scale = scale;
    }

    private final Calculator calculator = new AbstractCalculator() {

        @Override
        public Object doAdd(Object v1, Object v2) {
            LocalDateTime time;
            if (v1 instanceof IntervalType) {
                Timestamp i2 = convertFrom(v2);
                time = ((IntervalType) v1).process(i2.toLocalDateTime(), 1);
            } else if (v2 instanceof IntervalType) {
                Timestamp i1 = convertFrom(v1);
                time = ((IntervalType) v2).process(i1.toLocalDateTime(), 1);
            } else {
                throw new NotSupportException("时间类型不支持算术符操作");
            }
            return convertFrom(time);
        }

        @Override
        public Object doSub(Object v1, Object v2) {
            LocalDateTime time;
            if (v2 instanceof IntervalType) {
                Timestamp i1 = convertFrom(v1);
                time = ((IntervalType) v2).process(i1.toLocalDateTime(), -1);
            } else {
                throw new NotSupportException("时间类型不支持算术符操作");
            }

            return convertFrom(time);
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
                return rs.getTimestamp(index);
            }

            @Override
            public Object get(Row rs, int index) {
                Object val = rs.getObject(index);
                return convertFrom(val);
            }
        };
    }

    @Override
    public Timestamp getMaxValue() {
        return maxTimestamp;
    }

    @Override
    public Timestamp getMinValue() {
        return minTimestamp;
    }

    @Override
    public Object convertJavaFrom(Object value) {
        // only return java.sql.Timestamp
        Timestamp ts = convertFrom(value);
        if (ts == null) {
            // compatible to old sharding key
            return super.convertFrom(value);
        } else {
            Timestamp newTimestamp = MySQLTimeTypeUtil.toJavaTimestamp(ts);
            return newTimestamp;
        }
    }

    @Override
    public int compare(Object o1, Object o2) {
        if (o1 == o2) {
            return 0;
        }

        // when compare date.
        // in MySQL type system, the calculation of date/datetime is consistent
        if (o1 instanceof Date) {
            o1 = TimeStorage.packDate((Date) o1);
        }
        if (o2 instanceof Date) {
            o2 = TimeStorage.packDate((Date) o2);
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

        Timestamp d1 = convertFrom(o1, false);
        Timestamp d2 = convertFrom(o2, false);
        if (d1 == null) {
            return -1;
        }

        if (d2 == null) {
            return 1;
        }
        long l1 = TimeStorage.packDatetime(d1);
        long l2 = TimeStorage.packDatetime(d2);

        return (l1 > l2) ? 1 : (l1 == l2 ? 0 : -1);
    }

    private Long makePackedLong(Object o) {
        if (o instanceof Long) {
            return (Long) o;
        } else {
            return Optional.ofNullable(o)
                .map(this::convertFrom)
                .map(TimeStorage::packDatetime)
                .orElse(null);
        }
    }

    @Override
    public Calculator getCalculator() {
        return calculator;
    }

    @Override
    public int getSqlType() {
        return java.sql.Types.TIMESTAMP;
    }

    @Override
    public String getStringSqlType() {
        return "TIMESTAMP";
    }

    @Override
    public Class getDataClass() {
        return Timestamp.class;
    }

    @Override
    public Timestamp convertFrom(Object value) {
        return convertFrom(value, true);
    }

    private Timestamp convertFrom(Object value, boolean round) {
        // Try to get mysql datetime structure
        MysqlDateTime t = DataTypeUtil.toMySQLDatetimeByFlags(
            value,
            Types.TIMESTAMP,
            TimeParserFlags.FLAG_TIME_FUZZY_DATE | TimeParserFlags.FLAG_TIME_NO_DATE_FRAC_WARN);
        if (t == null) {
            return null;
        }
        OriginalTimestamp timestamp = new OriginalTimestamp(t);
        if (!round) {
            return timestamp;
        }

        // need to round ?
        boolean needToRound = MySQLTimeCalculator.needToRound((int) t.getSecondPart(), scale);
        if (!needToRound) {
            return timestamp;
        }

        // round: Timestamp -> MysqlDateTime -> OriginalTimestamp
        return Optional.ofNullable(timestamp.getMysqlDateTime())
            .map(
                mysqlDateTime -> MySQLTimeCalculator.roundDatetime(mysqlDateTime, scale)
            )
            .map(
                MySQLTimeTypeUtil::createOriginalTimestamp
            )
            .orElse(null);
    }

    /**
     * 按照scale 输出mysql 标准格式timestamp
     */
    public String fixScale(String timestamp) {
        // String -> byte[] -> MysqlDateTime -> String
        return Optional.ofNullable(timestamp)
            .filter(str -> !TStringUtil.isEmpty(str))
            .map(
                // string to mysql time
                s -> StringTimeParser.parseString(timestamp.getBytes(), getSqlType())
            )
            .map(
                mysqlDateTime -> {
                    if (MySQLTimeCalculator.needToRound((int) mysqlDateTime.getSecondPart(), scale)) {
                        // round if needed
                        return MySQLTimeCalculator.roundDatetime(mysqlDateTime, scale);
                    } else {
                        // don't round
                        return mysqlDateTime;
                    }
                }
            )
            .map(
                // get string with scale
                t -> t.toDatetimeString(scale)
            )
            .orElse(timestamp);
    }

    public String toStandardString(Object value) {
        return Optional.ofNullable(value)
            .map(this::convertFrom)
            .map(MySQLTimeTypeUtil::toMysqlDateTime)
            .map(
                t -> t.toDatetimeString(scale)
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
        return MySQLTimeTypeUtil.MAX_DATETIME_WIDTH + (scale != 0 ? scale + 1 : 0);
    }

    @Override
    public MySQLStandardFieldType fieldType() {
        return MySQLStandardFieldType.MYSQL_TYPE_TIMESTAMP;
    }

    @Override
    public boolean equalDeeply(DataType that) {
        if (that == null || that.getClass() != this.getClass()) {
            return false;
        }

        TimestampType thatType = (TimestampType) that;
        return thatType.scale == this.scale;
    }
}
