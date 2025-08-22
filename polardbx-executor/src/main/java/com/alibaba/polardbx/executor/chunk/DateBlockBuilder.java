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

import com.alibaba.polardbx.common.memory.FieldMemoryCounter;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.time.MySQLTimeTypeUtil;
import com.alibaba.polardbx.common.utils.time.core.MysqlDateTime;
import com.alibaba.polardbx.common.utils.time.core.OriginalDate;
import com.alibaba.polardbx.common.utils.time.core.TimeStorage;
import com.alibaba.polardbx.common.utils.time.parser.StringTimeParser;
import com.alibaba.polardbx.common.utils.timezone.InternalTimeZone;
import com.alibaba.polardbx.executor.mpp.operator.DriverContext;
import com.alibaba.polardbx.executor.operator.util.DriverObjectPool;
import com.alibaba.polardbx.executor.operator.util.ObjectPools;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.google.common.base.Preconditions;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.openjdk.jol.info.ClassLayout;

import java.sql.Date;
import java.sql.Types;
import java.util.Optional;
import java.util.TimeZone;

/**
 * Date Block
 *
 */
public class DateBlockBuilder extends AbstractBlockBuilder {
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(DateBlockBuilder.class).instanceSize();

    final BatchedArrayList.BatchLongArrayList packed;

    @FieldMemoryCounter(value = false)
    final DataType<? extends Date> dataType;

    @FieldMemoryCounter(value = false)
    final ExecutionContext context;

    // timezone is mutable
    @FieldMemoryCounter(value = false)
    TimeZone timezone;

    @FieldMemoryCounter(value = false)
    private DriverObjectPool<long[]> objectPool;
    private int chunkLimit;

    public DateBlockBuilder(int capacity, DataType<? extends Date> dataType, ExecutionContext context) {
        super(capacity);
        this.packed = new BatchedArrayList.BatchLongArrayList(capacity);
        this.dataType = dataType;
        this.context = context;
        // 当前执行器以外的时区处理逻辑，都认为时间戳以默认时区表示
        this.timezone = InternalTimeZone.DEFAULT_TIME_ZONE;
        this.chunkLimit = context.getParamManager().getInt(ConnectionParams.CHUNK_SIZE);
    }

    public DateBlockBuilder(int capacity, DataType<? extends Date> dataType, ExecutionContext context,
                            DriverObjectPool<long[]> objectPool) {
        super(capacity);
        this.packed = new BatchedArrayList.BatchLongArrayList(capacity);
        this.dataType = dataType;
        this.context = context;
        this.timezone = InternalTimeZone.DEFAULT_TIME_ZONE;
        this.objectPool = objectPool;
        this.chunkLimit = context.getParamManager().getInt(ConnectionParams.CHUNK_SIZE);
    }

    @Override
    public long getMemoryUsage() {
        return INSTANCE_SIZE + packed.getMemoryUsage() + valueIsNull.getMemoryUsage();
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
            Types.DATE);
        writeMysqlDatetime(t);
    }

    public void writeMysqlDatetime(MysqlDateTime t) {
        if (t == null) {
            appendNull();
            return;
        }
        // For time-generation functions like NOW()/CURTIME()/UTC_TIME()/CONVERT_TZ() etc.
        timezone = GeneralUtil.coalesce(t.getTimezone(), timezone);
        long l = TimeStorage.writeDate(t);

        packed.add(l);
        valueIsNull.add(false);
    }

    @Override
    public void writeDate(Date value) {
        // round to scale.
        Date date = dataType.convertFrom(value);

        // pack to long value
        MysqlDateTime t = Optional.ofNullable(date)
            .map(MySQLTimeTypeUtil::toMysqlDate)
            .orElse(null);

        writeMysqlDatetime(t);
    }

    @Override
    public void writeDatetimeRawLong(long val) {
        packed.add(val);
        valueIsNull.add(false);
    }

    @Override
    public Date getDate(int position) {
        checkReadablePosition(position);

        // unpack the long value to original date object.
        final long packedLong = packed.get(position);
        MysqlDateTime t = TimeStorage.readDate(packedLong);
        t.setTimezone(timezone);

        // we assume the time read from packed long value is valid.
        Date date = new OriginalDate(t);
        return date;
    }

    @Override
    public Object getObject(int position) {
        return isNull(position) ? null : getDate(position);
    }

    @Override
    public void writeObject(Object value) {
        if (value == null) {
            appendNull();
            return;
        }
        Preconditions.checkArgument(value instanceof Date);
        writeDate((Date) value);
    }

    @Override
    public void ensureCapacity(int capacity) {
        super.ensureCapacity(capacity);
        packed.ensureCapacity(capacity);
    }

    @Override
    public Block build() {
        Block block =
            new DateBlock(0, getPositionCount(), mayHaveNull() ? valueIsNull.elements() : null, packed.elements(),
                dataType, timezone);
        if (objectPool != null) {
            block.setRecycler(objectPool.getRecycler(chunkLimit));
        }
        return block;
    }

    @Override
    public void appendNull() {
        appendNullInternal();
        packed.add(0L);
    }

    @Override
    public BlockBuilder newBlockBuilder() {
        if (objectPool != null) {
            return new DateBlockBuilder(getCapacity(), dataType, context, objectPool);
        } else {
            return new DateBlockBuilder(getCapacity(), dataType, context);
        }
    }

    @Override
    public BlockBuilder newBlockBuilder(ObjectPools objectPools, int chunkLimit) {
        return new DateBlockBuilder(getCapacity(), dataType, context, objectPools.getLongArrayPool());
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
