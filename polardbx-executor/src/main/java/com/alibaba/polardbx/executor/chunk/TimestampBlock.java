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
import com.alibaba.polardbx.common.type.MySQLStandardFieldType;
import com.alibaba.polardbx.common.utils.hash.IStreamingHasher;
import com.alibaba.polardbx.common.utils.time.MySQLTimeConverter;
import com.alibaba.polardbx.common.utils.time.core.MySQLTimeVal;
import com.alibaba.polardbx.common.utils.hash.IStreamingHasher;
import com.alibaba.polardbx.common.utils.time.core.MysqlDateTime;
import com.alibaba.polardbx.common.utils.time.core.OriginalTimestamp;
import com.alibaba.polardbx.common.utils.time.core.TimeStorage;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;

import com.google.common.base.Preconditions;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.XxHash64;
import org.openjdk.jol.info.ClassLayout;
import org.openjdk.jol.util.VMSupport;

import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Objects;
import java.util.TimeZone;

import static com.alibaba.polardbx.common.utils.memory.SizeOf.sizeOf;

/**
 * Timestamp Block
 */
public class TimestampBlock extends AbstractCommonBlock {
    private static final long NULL_VALUE = 0L;
    private static final byte[] NULL_VALUE_FOR_HASHER = new byte[0];
    private static final long INSTANCE_SIZE = ClassLayout.parseClass(TimestampBlock.class).instanceSize();
    /**
     * Store the timestamp as long value.
     */
    private final long[] packed;

    @FieldMemoryCounter(value = false)
    private TimeZone timezone;

    private int[] selection;

    // random access
    public TimestampBlock(DataType dataType, int positionCount, TimeZone timezone) {
        super(dataType, positionCount);
        this.dataType = dataType;
        this.timezone = timezone;
        this.packed = new long[positionCount];
        updateSizeInfo();
    }

    TimestampBlock(int arrayOffset, int positionCount, boolean[] valueIsNull, long[] packed,
                   DataType<? extends Timestamp> dataType, TimeZone timezone, int[] selection) {
        super(dataType, positionCount, valueIsNull, valueIsNull != null);
        this.dataType = dataType;
        this.timezone = timezone;
        this.packed = Preconditions.checkNotNull(packed);
        this.selection = selection;
        updateSizeInfo();
    }

    public TimestampBlock(int arrayOffset, int positionCount, boolean[] valueIsNull, long[] packed,
                          DataType<? extends Timestamp> dataType, TimeZone timezone) {
        super(dataType, positionCount, valueIsNull, valueIsNull != null);
        this.dataType = dataType;
        this.timezone = timezone;
        this.packed = Preconditions.checkNotNull(packed);
        updateSizeInfo();
    }

    TimestampBlock(int positionCount, boolean[] valueIsNull, long[] packed,
                   DataType<? extends Timestamp> dataType, TimeZone timezone, boolean hasNull) {
        super(dataType, positionCount, valueIsNull, hasNull);
        this.dataType = dataType;
        this.timezone = timezone;
        this.packed = Preconditions.checkNotNull(packed);
        updateSizeInfo();
    }

    public static TimestampBlock from(TimestampBlock timestampBlock, int selSize, int[] selection,
                                      boolean useSelection, TimeZone timeZone) {
        // Only convert timezone for timestamp type
        MySQLStandardFieldType fieldType = timestampBlock.getDataType().fieldType();
        boolean notTimestampType = (fieldType != MySQLStandardFieldType.MYSQL_TYPE_TIMESTAMP &&
            fieldType != MySQLStandardFieldType.MYSQL_TYPE_TIMESTAMP2);
        boolean isSameTimeZone = Objects.equals(timestampBlock.timezone.toZoneId(), timeZone.toZoneId());

        if (notTimestampType || isSameTimeZone) {
            // no need to do timezone conversion
            if (useSelection) {
                return new TimestampBlock(0, selSize, timestampBlock.isNull, timestampBlock.packed,
                    timestampBlock.dataType,
                    timestampBlock.timezone, selection);
            } else {
                return new TimestampBlock(0, selSize,
                    BlockUtils.copyNullArray(timestampBlock.isNull, selection, selSize),
                    BlockUtils.copyLongArray(timestampBlock.packed, selection, selSize),
                    timestampBlock.dataType,
                    timestampBlock.timezone, null);
            }
        }

        // do timezone conversion
        // no need to keep selection since we have to rewrite the data
        return convertTimeZone(timestampBlock, timeZone, selSize);
    }

    /**
     * Time zone conversion for csv chunk, assuming that the selection is null and the offset is zero
     */
    public static TimestampBlock from(TimestampBlock timestampBlock, TimeZone timeZone) {
        // Only convert timezone for timestamp type
        MySQLStandardFieldType fieldType = timestampBlock.getDataType().fieldType();
        if (fieldType != MySQLStandardFieldType.MYSQL_TYPE_TIMESTAMP &&
            fieldType != MySQLStandardFieldType.MYSQL_TYPE_TIMESTAMP2) {
            return timestampBlock;
        }

        ZoneId sourceZoneId = timestampBlock.getTimezone().toZoneId();
        ZoneId targetZoneId = timeZone.toZoneId();

        if (sourceZoneId.equals(targetZoneId)) {
            return timestampBlock;
        }

        assert timestampBlock.selection == null && timestampBlock.arrayOffset == 0;
        return convertTimeZone(timestampBlock, timeZone, timestampBlock.positionCount);
    }

    public static TimestampBlock convertTimeZone(TimestampBlock timestampBlock, TimeZone targetTimeZone,
                                                 int positionCount) {
        ZoneId sourceZoneId = timestampBlock.getTimezone().toZoneId();
        ZoneId targetZoneId = targetTimeZone.toZoneId();

        long[] convertPacked = Arrays.copyOf(timestampBlock.packed, positionCount);

        for (int i = 0; i < positionCount; i++) {
            if (timestampBlock.isNull(i)) {
                continue;
            }
            MySQLTimeVal timeVal =
                MySQLTimeConverter.convertValidDatetimeToTimestamp(
                    timestampBlock.getTimestamp(i).getMysqlDateTime(),
                    null,
                    sourceZoneId
                );

            if (timeVal.getSeconds() == 0) {
                convertPacked[i] = TimeStorage.writeTimestamp(MysqlDateTime.zeroDateTime());
            } else {
                MysqlDateTime mysqlDateTime = MySQLTimeConverter.convertTimestampToDatetime(timeVal, targetZoneId);
                convertPacked[i] = TimeStorage.writeTimestamp(mysqlDateTime);
            }
        }

        return new TimestampBlock(
            0,
            positionCount,
            timestampBlock.isNull,
            convertPacked,
            timestampBlock.dataType,
            targetTimeZone,
            null
        );
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

    public OriginalTimestamp getTimestamp(int position) {
        position = realPositionOf(position);
        return getTimestampInner(position);
    }

    private OriginalTimestamp getTimestampInner(int position) {
        // unpacked from long, and make original timestamp object.
        long l = packed[arrayOffset + position];
        MysqlDateTime t = TimeStorage.readTimestamp(l);
        t.setTimezone(timezone);

        OriginalTimestamp originalTimestamp = new OriginalTimestamp(t);
        return originalTimestamp;
    }

    @Override
    public Object getObject(int position) {
        position = realPositionOf(position);
        return getObjectInner(position);
    }

    private Object getObjectInner(int position) {
        return isNullInner(position) ? null : getTimestampInner(position);
    }

    @Override
    public Object getObjectForCmp(int position) {
        position = realPositionOf(position);
        return getObjectForCmpInner(position);
    }

    private Object getObjectForCmpInner(int position) {
        return isNullInner(position) ? null : packed[arrayOffset + position];
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
            byte[] rawBytes = getTimestampInner(realPos).toString().getBytes(StandardCharsets.UTF_8);
            return XxHash64.hash(rawBytes, 0, rawBytes.length);
        }
    }

    public long[] getPacked() {
        return packed;
    }

    @Override
    public void addToHasher(IStreamingHasher sink, int position) {
        position = realPositionOf(position);
        addToHasherInner(sink, position);
    }

    public DataType<? extends Timestamp> getDataType() {
        return dataType;
    }

    public TimeZone getTimezone() {
        return timezone;
    }

    @Override
    public boolean equals(int position, Block other, int otherPosition) {
        position = realPositionOf(position);
        if (other instanceof TimestampBlock) {
            return equalsInner(position, other.cast(TimestampBlock.class), otherPosition);
        } else if (other instanceof TimestampBlockBuilder) {
            return equalsInner(position, (TimestampBlockBuilder) other, otherPosition);
        } else {
            throw new AssertionError();
        }
    }

    private boolean equalsInner(int position, TimestampBlock other, int otherPosition) {
        boolean n1 = isNullInner(position);
        boolean n2 = other.isNull(otherPosition);
        if (n1 && n2) {
            return true;
        } else if (n1 != n2) {
            return false;
        }

        // by packed long value
        long l1 = getPackedLongInner(position);
        long l2 = other.getPackedLong(otherPosition);
        return l1 == l2;
    }

    private boolean equalsInner(int position, TimestampBlockBuilder other, int otherPosition) {
        boolean n1 = isNullInner(position);
        boolean n2 = other.isNull(otherPosition);
        if (n1 && n2) {
            return true;
        } else if (n1 != n2) {
            return false;
        }

        // by packed long value
        long l1 = getPackedLongInner(position);
        long l2 = other.getPackedLong(otherPosition);
        return l1 == l2;
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

    private int hashCodeInner(int position) {
        if (isNullInner(position)) {
            return 0;
        }
        return Long.hashCode(getPackedLongInner(position));
    }

    private void writePositionToInner(int position, BlockBuilder blockBuilder) {
        if (isNullInner(position)) {
            blockBuilder.appendNull();
        } else {
            // write packed long.
            blockBuilder.writeLong(getPackedLongInner(position));
        }
    }

    private void addToHasherInner(IStreamingHasher sink, int position) {
        if (isNullInner(position)) {
            sink.putBytes(NULL_VALUE_FOR_HASHER);
        } else {
            sink.putString(getTimestampInner(position).toString());
        }
    }

    private boolean isNullInner(int position) {
        return isNull != null && isNull[position + arrayOffset];
    }

    @Override
    public void updateSizeInfo() {
        elementUsedBytes = INSTANCE_SIZE
            + VMSupport.align((int) sizeOf(isNull))
            + VMSupport.align((int) sizeOf(packed))
            + (selection == null ? 0 : VMSupport.align((int) sizeOf(selection)));
        estimatedSize = elementUsedBytes;
    }
}