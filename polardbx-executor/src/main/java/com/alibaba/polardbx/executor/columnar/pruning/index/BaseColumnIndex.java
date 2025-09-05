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

package com.alibaba.polardbx.executor.columnar.pruning.index;

import com.alibaba.polardbx.common.utils.time.MySQLTimeConverter;
import com.alibaba.polardbx.common.utils.time.core.MySQLTimeVal;
import com.alibaba.polardbx.common.utils.time.core.MysqlDateTime;
import com.alibaba.polardbx.common.utils.time.parser.TimeParseStatus;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.rpc.result.XResultUtil;
import com.google.common.base.Preconditions;

import java.time.ZoneId;
import java.util.TimeZone;

/**
 * @author fangwu
 */
public abstract class BaseColumnIndex implements ColumnIndex {
    private static final ZoneId DEFAULT_TIME_ZONE = TimeZone.getTimeZone("GMT+08:00").toZoneId();

    private long rgNum;

    protected BaseColumnIndex(long rgNum) {
        this.rgNum = rgNum;
    }

    @Override
    public long rgNum() {
        return rgNum;
    }

    /**
     * @param value: input data type
     * @param dt: column data type
     * @param clazz: excepted input data type
     */
    protected <T> T paramTransform(Object value, DataType dt, Class<T> clazz) {
        Preconditions.checkArgument(value != null && dt != null, "value and DataType can't be null here");
        if (DataTypeUtil.isIntType(dt)) {
            if (clazz.equals(Long.class) && value instanceof Number) {
                return clazz.cast(((Number) value).longValue());
            }
        } else if (DataTypeUtil.isStringSqlType(dt)) {
            if (clazz.equals(String.class) && value instanceof String) {
                return clazz.cast(value);
            }
        } else if (DataTypeUtil.isDateType(dt)) {
            MysqlDateTime date = DataTypeUtil.toMySQLDatetime(value, dt.getSqlType());
            if (date == null) {
                return null;
            }
            if (clazz.equals(Long.class)) {
                if (DataTypeUtil.equalsSemantically(DataTypes.TimestampType, dt)) {
                    return clazz.cast(convertToLongFromMysqlDateTime(date));
                } else {
                    return clazz.cast(date.toPackedLong());
                }
            }
        }
        return null;
    }

    /**
     * 将字符串时间转成的MysqlDateTime转成MySQLTimeVal，再转成long, 与columnar写入对齐
     */
    public static long convertToLongFromMysqlDateTime(MysqlDateTime t) {
        TimeParseStatus timeParseStatus = new TimeParseStatus();
        MySQLTimeVal timeVal = MySQLTimeConverter.convertDatetimeToTimestampWithoutCheck(t, timeParseStatus,
            DEFAULT_TIME_ZONE);
        if (timeVal == null) {
            // for error time value, set to zero.
            timeVal = new MySQLTimeVal();
        }
        return XResultUtil.timeValToLong(timeVal);
    }

    public abstract long getSizeInBytes();
}
