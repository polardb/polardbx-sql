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

package com.alibaba.polardbx.optimizer.chunk;

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.time.MySQLTimeTypeUtil;
import com.alibaba.polardbx.common.utils.time.core.MysqlDateTime;
import com.alibaba.polardbx.common.utils.time.core.OriginalTimestamp;
import com.alibaba.polardbx.common.utils.time.core.TimeStorage;
import com.alibaba.polardbx.common.utils.time.parser.StringTimeParser;
import com.alibaba.polardbx.common.utils.timezone.InternalTimeZone;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.google.common.base.Preconditions;
import it.unimi.dsi.fastutil.longs.LongArrayList;

import java.sql.Timestamp;
import java.sql.Types;
import java.util.Optional;
import java.util.TimeZone;

/**
 * Timestamp Block Builder
 *
 */
public class TimestampBlockBuilder extends AbstractBlockBuilder {

    final DataType<? extends Timestamp> dataType;
    final LongArrayList packed;
    final ExecutionContext context;

    // timezone is mutable
    TimeZone timezone;

    public TimestampBlockBuilder(int capacity, DataType<? extends Timestamp> dataType, ExecutionContext context) {
        super(capacity);
        this.dataType = dataType;
        this.packed = new LongArrayList(capacity);
        this.context = context;
        // 当前执行器以外的时区处理逻辑，都认为时间戳以默认时区表示
        this.timezone = InternalTimeZone.DEFAULT_TIME_ZONE;
    }

    @Override
    public void writeTimestamp(Timestamp value) {
        // round to the scale.
        Timestamp ts = dataType.convertFrom(value);
        MysqlDateTime t = Optional.ofNullable(ts)
            .map(MySQLTimeTypeUtil::toMysqlDateTime)
            .orElse(null);

        writeMysqlDatetime(t);
    }

    @Override
    public void writeDatetimeRawLong(long val) {
        packed.add(val);
        valueIsNull.add(false);
    }

    @Override
    public Timestamp getTimestamp(int position) {
        checkReadablePosition(position);

        long l = packed.get(position);
        MysqlDateTime t = TimeStorage.readTimestamp(l);
        t.setTimezone(timezone);

        OriginalTimestamp originalTimestamp = new OriginalTimestamp(t);
        return originalTimestamp;
    }

    @Override
    public Object getObject(int position) {
        return isNull(position) ? null : getTimestamp(position);
    }

    @Override
    public void writeObject(Object value) {
        if (value == null) {
            appendNull();
            return;
        }
        Preconditions.checkArgument(value instanceof Timestamp);
        writeTimestamp((Timestamp) value);
    }

    @Override
    public void writeString(String value) {
        if (value == null) {
            appendNull();
            return;
        }
        writeByteArray(value.getBytes());
    }

    @Override
    public void writeByteArray(byte[] value) {
        if (value == null) {
            appendNull();
            return;
        }
        MysqlDateTime t = StringTimeParser.parseString(
            value,
            Types.TIMESTAMP);
        writeMysqlDatetime(t);
    }

    public void writeLong(long value) {
        // assume not null.
        packed.add(value);
        valueIsNull.add(false);
    }

    public long getPackedLong(int position) {
        // assume not null.
        checkReadablePosition(position);
        return packed.get(position);
    }

    public void writeMysqlDatetime(MysqlDateTime t) {
        if (t == null) {
            appendNull();
            return;
        }
        // For time-generation functions like NOW()/CURTIME()/UTC_TIME()/CONVERT_TZ() etc.
        timezone = GeneralUtil.coalesce(t.getTimezone(), timezone);

        long l = Optional.ofNullable(t)
            // pack to long value
            .map(TimeStorage::writeTimestamp)
            .orElse(0L);

        packed.add(l);
        valueIsNull.add(false);
    }

    @Override
    public void ensureCapacity(int capacity) {
        super.ensureCapacity(capacity);
        packed.ensureCapacity(capacity);
    }

    @Override
    public Block build() {
        return new TimestampBlock(0, getPositionCount(), mayHaveNull() ? valueIsNull.elements() : null,
            packed.elements(), dataType, timezone);
    }

    @Override
    public void appendNull() {
        appendNullInternal();
        packed.size(packed.size() + 1);
    }

    @Override
    public BlockBuilder newBlockBuilder() {
        return new TimestampBlockBuilder(getCapacity(), dataType, context);
    }

    @Override
    public int hashCode(int position) {
        if (isNull(position)) {
            return 0;
        }
        return Long.hashCode(packed.get(position));
    }

    public TimeZone getTimezone() {
        return timezone;
    }

    public void setTimezone(TimeZone timezone) {
        this.timezone = timezone;
    }
}
