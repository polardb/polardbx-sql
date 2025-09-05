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

package com.alibaba.polardbx.executor.chunk;

import com.alibaba.polardbx.common.memory.FastMemoryCounter;
import com.alibaba.polardbx.common.memory.FieldMemoryCounter;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.time.MySQLTimeTypeUtil;
import com.alibaba.polardbx.common.utils.time.core.MysqlDateTime;
import com.alibaba.polardbx.common.utils.time.core.OriginalTime;
import com.alibaba.polardbx.common.utils.time.core.TimeStorage;
import com.alibaba.polardbx.common.utils.time.parser.StringTimeParser;
import com.alibaba.polardbx.common.utils.timezone.InternalTimeZone;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.google.common.base.Preconditions;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.openjdk.jol.info.ClassLayout;

import java.sql.Time;
import java.sql.Types;
import java.util.Optional;
import java.util.TimeZone;

/**
 * Time Block
 *
 */
public class TimeBlockBuilder extends AbstractBlockBuilder {
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(TimeBlockBuilder.class).instanceSize();

    final MemoryCountableLongArrayList packed;

    @FieldMemoryCounter(value = false)
    final DataType<? extends Time> dataType;

    @FieldMemoryCounter(value = false)
    final ExecutionContext context;

    // timezone is mutable
    @FieldMemoryCounter(value = false)
    TimeZone timezone;

    public TimeBlockBuilder(int capacity, DataType<? extends Time> dataType, ExecutionContext context) {
        super(capacity);
        this.packed = new MemoryCountableLongArrayList(capacity);
        this.dataType = dataType;
        this.context = context;
        // 当前执行器以外的时区处理逻辑，都认为时间戳以默认时区表示
        this.timezone = InternalTimeZone.DEFAULT_TIME_ZONE;
    }

    @Override
    public long getMemoryUsage() {
        return INSTANCE_SIZE
            + FastMemoryCounter.sizeOf(packed)
            + FastMemoryCounter.sizeOf(valueIsNull);
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
            Types.TIME);
        writeMysqlDatetime(t);
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
            .map(TimeStorage::writeTime)
            .orElse(0L);

        packed.add(l);
        valueIsNull.add(false);
    }

    @Override
    public void writeTime(Time value) {
        // round to scale.
        Time time = dataType.convertFrom(value);

        // pack to long value
        MysqlDateTime t = Optional.ofNullable(time)
            .map(MySQLTimeTypeUtil::toMysqlTime)
            .orElse(null);

        writeMysqlDatetime(t);
    }

    @Override
    public void writeDatetimeRawLong(long val) {
        packed.add(val);
        valueIsNull.add(false);
    }

    @Override
    public Time getTime(int position) {
        checkReadablePosition(position);
        long packedLong = packed.getLong(position);
        MysqlDateTime t = TimeStorage.readTime(packedLong);
        t.setTimezone(timezone);
        return new OriginalTime(t);
    }

    @Override
    public Object getObject(int position) {
        return isNull(position) ? null : getTime(position);
    }

    @Override
    public void writeObject(Object value) {
        if (value == null) {
            appendNull();
            return;
        }
        Preconditions.checkArgument(value instanceof Time);
        writeTime((Time) value);
    }

    @Override
    public void ensureCapacity(int capacity) {
        super.ensureCapacity(capacity);
        packed.ensureCapacity(capacity);
    }

    @Override
    public Block build() {
        return new TimeBlock(0, getPositionCount(), mayHaveNull() ? valueIsNull.elements() : null, packed.elements(),
            dataType, timezone);
    }

    @Override
    public void appendNull() {
        appendNullInternal();
        packed.add(0L);
    }

    @Override
    public BlockBuilder newBlockBuilder() {
        return new TimeBlockBuilder(
            getCapacity(),
            dataType, context);
    }

    @Override
    public int hashCode(int position) {
        if (isNull(position)) {
            return 0;
        }
        return Long.hashCode(packed.getLong(position));
    }

    public long getPackedLong(int position) {
        checkReadablePosition(position);
        return packed.get(position);
    }
}
