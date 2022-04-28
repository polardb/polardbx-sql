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
import com.alibaba.polardbx.common.utils.time.core.OriginalTime;
import com.alibaba.polardbx.common.utils.time.core.TimeStorage;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;

import org.openjdk.jol.info.ClassLayout;

import java.sql.Time;
import java.util.TimeZone;

import static com.alibaba.polardbx.common.utils.memory.SizeOf.sizeOf;

/**
 * Time Block
 *
 */
public class TimeBlock extends AbstractCommonBlock {
    private static final long NULL_VALUE = 0L;
    private static final long INSTANCE_SIZE = ClassLayout.parseClass(TimeBlock.class).instanceSize();

    public static final long ZERO_TIME_MILLIS = -1;

    private final long[] packed;
    private final TimeZone timezone;

    TimeBlock(int arrayOffset, int positionCount, boolean[] valueIsNull, long[] packed,
              DataType<? extends Time> dataType, TimeZone timezone) {
        super(dataType, positionCount, valueIsNull, valueIsNull != null);
        this.packed = packed;
        this.timezone = timezone;
        estimatedSize = INSTANCE_SIZE + sizeOf(valueIsNull) + sizeOf(packed);
        sizeInBytes = (Long.BYTES + Byte.BYTES) * positionCount;
    }

    @Override
    public Time getTime(int position) {
        checkReadablePosition(position);

        // unpack the long value to original time object.
        final long packedLong = packed[arrayOffset + position];
        MysqlDateTime t = TimeStorage.readTime(packedLong);
        t.setTimezone(timezone);
        // we assume the time read from packed long value is valid.
        Time time = new OriginalTime(t);
        return time;
    }

    @Override
    public Object getObject(int position) {
        return isNull(position) ? null : getTime(position);
    }

    @Override
    public Object getObjectForCmp(int position) {
        return isNull(position) ? null : getPackedLong(position);
    }

    /**
     * The getLong of TimeBlock is ambiguous.
     */
    @Override
    @Deprecated
    public long getLong(int position) {
        checkReadablePosition(position);
        // this method means get long value of millis second ?
        long millis = getTime(position).getTime();
        return millis;
    }

    @Override
    public long getPackedLong(int position) {
        checkReadablePosition(position);

        return packed[arrayOffset + position];
    }

    @Override
    public void writePositionTo(int position, BlockBuilder blockBuilder) {
        if (blockBuilder instanceof TimeBlockBuilder) {
            writePositionTo(position, (TimeBlockBuilder) blockBuilder);
        } else {
            throw new AssertionError();
        }
    }

    private void writePositionTo(int position, TimeBlockBuilder b) {
        if (isNull(position)) {
            b.appendNull();
        } else {
            b.valueIsNull.add(false);
            b.packed.add(packed[arrayOffset + position]);
        }
    }

    @Override
    public int hashCode(int position) {
        if (isNull(position)) {
            return 0;
        }
        return Long.hashCode(packed[arrayOffset + position]);
    }

    @Override
    public void addToHasher(IStreamingHasher sink, int position) {
        if (isNull(position)) {
            sink.putLong(NULL_VALUE);
        } else {
            sink.putLong(packed[arrayOffset + position]);
        }
    }

    public DataType<? extends Time> getDataType() {
        return dataType;
    }

    @Override
    public boolean equals(int position, Block other, int otherPosition) {
        if (other instanceof TimeBlock) {
            return equals(position, (TimeBlock) other, otherPosition);
        } else if (other instanceof TimeBlockBuilder) {
            return equals(position, (TimeBlockBuilder) other, otherPosition);
        } else {
            throw new AssertionError();
        }
    }

    boolean equals(int position, TimeBlock other, int otherPosition) {
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

    boolean equals(int position, TimeBlockBuilder other, int otherPosition) {
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

    public long[] getPacked() {
        return packed;
    }

    public TimeZone getTimezone() {
        return timezone;
    }

}
