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

import com.alibaba.polardbx.common.utils.hash.IStreamingHasher;
import com.alibaba.polardbx.common.utils.time.core.MysqlDateTime;
import com.alibaba.polardbx.common.utils.time.core.OriginalTimestamp;
import com.alibaba.polardbx.common.utils.time.core.TimeStorage;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;

import com.google.common.base.Preconditions;
import org.openjdk.jol.info.ClassLayout;

import java.sql.Timestamp;
import java.util.TimeZone;

import static com.alibaba.polardbx.common.utils.memory.SizeOf.sizeOf;

/**
 * Timestamp Block
 *
 */
public class TimestampBlock extends AbstractCommonBlock {
    private static final long NULL_VALUE = 0L;
    private static final long INSTANCE_SIZE = ClassLayout.parseClass(TimestampBlock.class).instanceSize();

    public static final long ZERO_TIMESTAMP_MILLIS = -1;
    public static final long ZERO_TIMESTAMP_NANOS = -1;

    /**
     * Store the timestamp as long value.
     */
    private final long[] packed;

    private TimeZone timezone;

    TimestampBlock(int arrayOffset, int positionCount, boolean[] valueIsNull, long[] packed,
                   DataType<? extends Timestamp> dataType, TimeZone timezone) {
        super(dataType, positionCount, valueIsNull, valueIsNull != null);
        this.dataType = dataType;
        this.timezone = timezone;
        this.packed = Preconditions.checkNotNull(packed);
        estimatedSize = INSTANCE_SIZE + sizeOf(valueIsNull) + sizeOf(packed);
        sizeInBytes = (Long.BYTES + Byte.BYTES) * positionCount;
    }

    TimestampBlock(int positionCount, boolean[] valueIsNull, long[] packed,
                   DataType<? extends Timestamp> dataType, TimeZone timezone, boolean hasNull) {
        super(dataType, positionCount, valueIsNull, hasNull);
        this.dataType = dataType;
        this.timezone = timezone;
        this.packed = Preconditions.checkNotNull(packed);
        estimatedSize = INSTANCE_SIZE + sizeOf(valueIsNull) + sizeOf(packed);
        sizeInBytes = (Long.BYTES + Byte.BYTES) * positionCount;
    }

    @Override
    public OriginalTimestamp getTimestamp(int position) {
        checkReadablePosition(position);

        // unpacked from long, and make original timestamp object.
        long l = packed[arrayOffset + position];
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
    public Object getObjectForCmp(int position) {
        return isNull(position) ? null : packed[arrayOffset + position];
    }

    @Override
    public void writePositionTo(int position, BlockBuilder blockBuilder) {
        if (isNull(position)) {
            blockBuilder.appendNull();
        } else {
            // write packed long.
            blockBuilder.writeLong(packed[arrayOffset + position]);
        }
    }

    @Override
    public int hashCode(int position) {
        if (isNull(position)) {
            return 0;
        }
        return Long.hashCode(packed[arrayOffset + position]);
    }

    public long[] getPacked() {
        return packed;
    }

    @Override
    public void addToHasher(IStreamingHasher sink, int position) {
        if (isNull(position)) {
            sink.putLong(NULL_VALUE);
        } else {
            sink.putLong(packed[position]);
        }
    }

    public DataType<? extends Timestamp> getDataType() {
        return dataType;
    }

    @Override
    public long getPackedLong(int position) {
        // assume not null
        checkReadablePosition(position);

        long l = packed[arrayOffset + position];
        return l;
    }

    public TimeZone getTimezone() {
        return timezone;
    }

    @Override
    public boolean equals(int position, Block other, int otherPosition) {
        if (other instanceof TimestampBlock) {
            return equals(position, (TimestampBlock) other, otherPosition);
        } else if (other instanceof TimestampBlockBuilder) {
            return equals(position, (TimestampBlockBuilder) other, otherPosition);
        } else {
            throw new AssertionError();
        }
    }

    boolean equals(int position, TimestampBlock other, int otherPosition) {
        boolean n1 = isNull(position);
        boolean n2 = other.isNull(otherPosition);
        if (n1 && n2) {
            return true;
        } else if (n1 != n2) {
            return false;
        }

        // by packed long value
        long l1 = getPackedLong(position);
        long l2 = other.getPackedLong(otherPosition);
        return l1 == l2;
    }

    boolean equals(int position, TimestampBlockBuilder other, int otherPosition) {
        boolean n1 = isNull(position);
        boolean n2 = other.isNull(otherPosition);
        if (n1 && n2) {
            return true;
        } else if (n1 != n2) {
            return false;
        }

        // by packed long value
        long l1 = getPackedLong(position);
        long l2 = other.getPackedLong(otherPosition);
        return l1 == l2;
    }
}