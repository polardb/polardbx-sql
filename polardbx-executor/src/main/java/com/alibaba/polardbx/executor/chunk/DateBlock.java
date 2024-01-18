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
import com.alibaba.polardbx.common.utils.time.core.OriginalDate;
import com.alibaba.polardbx.common.utils.time.core.TimeStorage;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;

import com.google.common.base.Preconditions;
import org.openjdk.jol.info.ClassLayout;

import java.sql.Date;
import java.util.TimeZone;

import static com.alibaba.polardbx.common.utils.memory.SizeOf.sizeOf;

/**
 * Date Block
 */
public class DateBlock extends AbstractCommonBlock {
    private static final long NULL_VALUE = 0L;
    private static final byte[] NULL_VALUE_FOR_HASHER = new byte[0];
    private static final long INSTANCE_SIZE = ClassLayout.parseClass(DateBlock.class).instanceSize();

    public static final long ZERO_DATE_MILLIS = -1;

    private final long[] packed;

    private final TimeZone timezone;

    private int[] selection;

    public DateBlock(int arrayOffset, int positionCount, boolean[] valueIsNull, long[] data, DataType<? extends Date> dataType,
                     TimeZone timezone, int[] selection) {
        super(dataType, positionCount, valueIsNull, valueIsNull != null);
        this.packed = Preconditions.checkNotNull(data);
        this.timezone = timezone;
        this.selection = selection;
        updateSizeInfo();
    }

    DateBlock(int arrayOffset, int positionCount, boolean[] valueIsNull, long[] data, DataType<? extends Date> dataType,
              TimeZone timezone) {
        super(dataType, positionCount, valueIsNull, valueIsNull != null);
        this.packed = Preconditions.checkNotNull(data);
        this.timezone = timezone;
        updateSizeInfo();
    }

    private int realPositionOf(int position) {
        if (selection == null) {
            return position;
        }
        return selection[position];
    }

    @Override
    public boolean isNull(int position) {
        position = realPositionOf(position);
        return isNull != null && isNull[position + arrayOffset];
    }

    @Override
    public Date getDate(int position) {
        // unpack the long value to original date object.
        final long packedLong = getPackedLong(position);
        MysqlDateTime t = TimeStorage.readDate(packedLong);
        t.setTimezone(timezone);
        // we assume the time read from packed long value is valid.
        Date date = new OriginalDate(t);
        return date;
    }

    /**
     * The getLong of DateBlock is ambiguous.
     */
    @Override
    @Deprecated
    public long getLong(int position) {
        // this method means get long value of millis second ?
        long millis = getDate(position).getTime();
        return millis;
    }

    @Override
    public long getPackedLong(int position) {
        position = realPositionOf(position);

        return packed[arrayOffset + position];
    }

    @Override
    public Object getObject(int position) {
        return isNull(position) ? null : getDate(position);
    }

    @Override
    public Object getObjectForCmp(int position) {
        return isNull(position) ? null : getPackedLong(position);
    }

    @Override
    public void writePositionTo(int position, BlockBuilder blockBuilder) {
        if (blockBuilder instanceof DateBlockBuilder) {
            writePositionTo(position, (DateBlockBuilder) blockBuilder);
        } else {
            throw new AssertionError();
        }
    }

    private void writePositionTo(int position, DateBlockBuilder b) {
        if (isNull(position)) {
            b.appendNull();
        } else {
            b.valueIsNull.add(false);
            b.packed.add(getPackedLong(position));
        }
    }

    @Override
    public int hashCode(int position) {
        if (isNull(position)) {
            return 0;
        }
        return Long.hashCode(getPackedLong(position));
    }

    @Override
    public void addToHasher(IStreamingHasher sink, int position) {
        if (isNull(position)) {
            sink.putBytes(NULL_VALUE_FOR_HASHER);
        } else {
            sink.putString(getDate(position).toString());
        }
    }

    @Override
    public boolean equals(int position, Block other, int otherPosition) {
        if (other instanceof DateBlock) {
            return equals(position, (DateBlock) other, otherPosition);
        } else if (other instanceof DateBlockBuilder) {
            return equals(position, (DateBlockBuilder) other, otherPosition);
        } else {
            throw new AssertionError();
        }
    }

    boolean equals(int position, DateBlock other, int otherPosition) {
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

    boolean equals(int position, DateBlockBuilder other, int otherPosition) {
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

    public int[] getSelection() {
        return selection;
    }

    @Override
    public void updateSizeInfo() {
        estimatedSize = INSTANCE_SIZE + sizeOf(isNull) + sizeOf(packed);
        elementUsedBytes = Byte.BYTES * positionCount + Long.BYTES * positionCount;
    }
}
