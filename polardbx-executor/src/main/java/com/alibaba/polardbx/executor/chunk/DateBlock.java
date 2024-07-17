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

import com.alibaba.polardbx.common.utils.XxhashUtils;
import com.alibaba.polardbx.common.utils.hash.IStreamingHasher;
import com.alibaba.polardbx.common.utils.hash.IStreamingHasher;
import com.alibaba.polardbx.common.utils.time.core.MysqlDateTime;
import com.alibaba.polardbx.common.utils.time.core.OriginalDate;
import com.alibaba.polardbx.common.utils.time.core.TimeStorage;
import com.alibaba.polardbx.executor.operator.util.BatchBlockWriter;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;

import com.google.common.base.Preconditions;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.XxHash64;
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

    public DateBlock(int slotLen, TimeZone timezone) {
        super(DataTypes.DateType, slotLen);
        this.packed = new long[slotLen];
        this.selection = null;
        this.timezone = timezone;
        updateSizeInfo();
    }

    DateBlock(int arrayOffset, int positionCount, boolean[] valueIsNull, long[] data,
              DataType<? extends Date> dataType,
              TimeZone timezone, int[] selection) {
        super(dataType, positionCount, valueIsNull, valueIsNull != null);
        this.packed = Preconditions.checkNotNull(data);
        this.timezone = timezone;
        this.selection = selection;
        updateSizeInfo();
    }

    public DateBlock(int arrayOffset, int positionCount, boolean[] valueIsNull, long[] data,
                     DataType<? extends Date> dataType,
                     TimeZone timezone) {
        super(dataType, positionCount, valueIsNull, valueIsNull != null);
        this.packed = Preconditions.checkNotNull(data);
        this.timezone = timezone;
        updateSizeInfo();
    }

    public static DateBlock from(DateBlock dateBlock, int selSize, int[] selection, boolean useSelection) {
        if (useSelection) {
            return new DateBlock(0, selSize, dateBlock.isNull, dateBlock.packed, dateBlock.dataType,
                dateBlock.timezone, selection);
        }

        return new DateBlock(0, selSize,
            BlockUtils.copyNullArray(dateBlock.isNull, selection, selSize),
            BlockUtils.copyLongArray(dateBlock.packed, selection, selSize),
            dateBlock.dataType,
            dateBlock.timezone,
            null);
    }

    @Override
    public void recycle() {
        if (recycler != null) {
            recycler.recycle(packed);
        }
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
        return isNullInner(position);
    }

    @Override
    public Date getDate(int position) {
        position = realPositionOf(position);
        return getDateInner(position);
    }

    /**
     * The getLong of DateBlock is ambiguous.
     */
    @Override
    @Deprecated
    public long getLong(int position) {
        position = realPositionOf(position);
        // this method means get long value of millis second ?
        return getDateInner(position).getTime();
    }

    @Override
    public Object getObject(int position) {
        position = realPositionOf(position);
        return isNullInner(position) ? null : getDateInner(position);
    }

    @Override
    public Object getObjectForCmp(int position) {
        position = realPositionOf(position);
        return isNullInner(position) ? null : getPackedLongInner(position);
    }

    @Override
    public void writePositionTo(int[] selection, int offsetInSelection, int positionCount, BlockBuilder blockBuilder) {
        if (this.selection != null || !(blockBuilder instanceof DateBlockBuilder)) {
            // don't support it when selection in use.
            super.writePositionTo(selection, offsetInSelection, positionCount, blockBuilder);
            return;
        }

        if (!mayHaveNull()) {
            ((DateBlockBuilder) blockBuilder).packed
                .add(this.packed, selection, offsetInSelection, positionCount);

            ((DateBlockBuilder) blockBuilder).valueIsNull
                .add(false, positionCount);

            return;
        }

        DateBlockBuilder dateBlockBuilder = (DateBlockBuilder) blockBuilder;
        for (int i = 0; i < positionCount; i++) {
            int position = selection[i + offsetInSelection];

            if (isNull != null && isNull[position + arrayOffset]) {
                dateBlockBuilder.appendNull();
            } else {
                dateBlockBuilder.writeDatetimeRawLong(packed[position + arrayOffset]);
            }
        }
    }

    @Override
    public void writePositionTo(int position, BlockBuilder blockBuilder) {
        position = realPositionOf(position);
        writePositionToInner(position, blockBuilder);
    }

    @Override
    public int hashCode(int position) {
        position = realPositionOf(position);
        return hashCodeInner(position);
    }

    @Override
    public long hashCodeUseXxhash(int pos) {
        int realPos = realPositionOf(pos);
        if (isNull(realPos)) {
            return NULL_HASH_CODE;
        } else {
            return XxhashUtils.finalShuffle(getPackedLongInner(realPos));
        }
    }

    @Override
    public void addToHasher(IStreamingHasher sink, int position) {
        position = realPositionOf(position);
        addToHasherInner(sink, position);
    }

    @Override
    public boolean equals(int position, Block other, int otherPosition) {
        position = realPositionOf(position);
        if (other instanceof DateBlock) {
            return equalsInner(position, other.cast(DateBlock.class), otherPosition);
        } else if (other instanceof DateBlockBuilder) {
            return equalsInner(position, (DateBlockBuilder) other, otherPosition);
        } else {
            throw new AssertionError();
        }
    }

    private boolean equalsInner(int realPosition, DateBlock other, int otherPosition) {
        boolean n1 = isNullInner(realPosition);
        boolean n2 = other.isNull(otherPosition);
        if (n1 && n2) {
            return true;
        } else if (n1 != n2) {
            return false;
        }

        // by packed long value
        long l1 = getPackedLongInner(realPosition);
        long l2 = other.getPackedLong(otherPosition);
        return l1 == l2;
    }

    private boolean equalsInner(int realPosition, DateBlockBuilder other, int otherPosition) {
        boolean n1 = isNullInner(realPosition);
        boolean n2 = other.isNull(otherPosition);
        if (n1 && n2) {
            return true;
        } else if (n1 != n2) {
            return false;
        }

        // by packed long value
        long l1 = getPackedLongInner(realPosition);
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

    public void writeLong(SliceOutput sliceOutput, int position) {
        position = realPositionOf(position);
        sliceOutput.writeLong(getPackedLongInner(position));
    }

    public long getPackedLong(int position) {
        position = realPositionOf(position);
        return packed[arrayOffset + position];
    }

    private long getPackedLongInner(int position) {
        return packed[arrayOffset + position];
    }

    private Date getDateInner(int position) {
        // unpack the long value to original date object.
        final long packedLong = getPackedLongInner(position);
        MysqlDateTime t = TimeStorage.readDate(packedLong);
        t.setTimezone(timezone);
        // we assume the time read from packed long value is valid.
        Date date = new OriginalDate(t);
        return date;
    }

    private int hashCodeInner(int position) {
        if (isNullInner(position)) {
            return 0;
        }
        return Long.hashCode(getPackedLongInner(position));
    }

    private void writePositionToInner(int position, BlockBuilder blockBuilder) {
        if (blockBuilder instanceof DateBlockBuilder) {
            DateBlockBuilder b = (DateBlockBuilder) blockBuilder;
            if (isNullInner(position)) {
                b.appendNull();
            } else {
                b.valueIsNull.add(false);
                b.packed.add(getPackedLongInner(position));
            }
        } else if (blockBuilder instanceof BatchBlockWriter.BatchDateBlockBuilder) {
            BatchBlockWriter.BatchDateBlockBuilder b = (BatchBlockWriter.BatchDateBlockBuilder) blockBuilder;
            if (isNullInner(position)) {
                b.appendNull();
            } else {
                b.writePackedLong(getPackedLongInner(position));
            }
        } else {
            throw new AssertionError();
        }
    }

    private void addToHasherInner(IStreamingHasher sink, int position) {
        if (isNullInner(position)) {
            sink.putBytes(NULL_VALUE_FOR_HASHER);
        } else {
            sink.putString(getDateInner(position).toString());
        }
    }

    private boolean isNullInner(int position) {
        return isNull != null && isNull[position + arrayOffset];
    }

    @Override
    public void updateSizeInfo() {
        estimatedSize = INSTANCE_SIZE + sizeOf(isNull) + sizeOf(packed);
        elementUsedBytes = Byte.BYTES * positionCount + Long.BYTES * positionCount;
    }
}
