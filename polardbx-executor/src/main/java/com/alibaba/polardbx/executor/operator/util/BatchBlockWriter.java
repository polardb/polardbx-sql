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

package com.alibaba.polardbx.executor.operator.util;

import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.common.datatype.DecimalConverter;
import com.alibaba.polardbx.common.datatype.DecimalStructure;
import com.alibaba.polardbx.common.datatype.FastDecimalUtils;
import com.alibaba.polardbx.common.memory.FastMemoryCounter;
import com.alibaba.polardbx.common.memory.FieldMemoryCounter;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.type.MySQLStandardFieldType;
import com.alibaba.polardbx.common.utils.memory.SizeOf;
import com.alibaba.polardbx.common.utils.time.MySQLTimeTypeUtil;
import com.alibaba.polardbx.common.utils.time.core.MysqlDateTime;
import com.alibaba.polardbx.common.utils.time.core.OriginalDate;
import com.alibaba.polardbx.common.utils.time.core.TimeStorage;
import com.alibaba.polardbx.common.utils.time.parser.StringTimeParser;
import com.alibaba.polardbx.common.utils.timezone.InternalTimeZone;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.executor.chunk.BlockBuilders;
import com.alibaba.polardbx.executor.chunk.DateBlock;
import com.alibaba.polardbx.executor.chunk.DecimalBlock;
import com.alibaba.polardbx.executor.chunk.IntegerBlock;
import com.alibaba.polardbx.executor.chunk.LongBlock;
import com.alibaba.polardbx.executor.chunk.SegmentedDecimalBlock;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.datatype.DecimalType;
import com.google.common.base.Preconditions;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import org.openjdk.jol.info.ClassLayout;
import org.openjdk.jol.util.VMSupport;

import java.sql.Date;
import java.sql.Types;
import java.util.Optional;

import static com.alibaba.polardbx.common.datatype.DecimalTypeBase.DECIMAL_MEMORY_SIZE;
import static com.alibaba.polardbx.executor.chunk.SegmentedDecimalBlock.DecimalBlockState.DECIMAL_128;
import static com.alibaba.polardbx.executor.chunk.SegmentedDecimalBlock.DecimalBlockState.DECIMAL_64;
import static com.alibaba.polardbx.executor.chunk.SegmentedDecimalBlock.DecimalBlockState.UNSET_STATE;

public interface BatchBlockWriter {
    static BlockBuilder create(DataType type, ExecutionContext context, int chunkSize, ObjectPools objectPools) {
        if (objectPools == null) {
            return create(type, context, chunkSize);
        }
        MySQLStandardFieldType fieldType = type.fieldType();
        int chunkLimit = context.getParamManager().getInt(ConnectionParams.CHUNK_SIZE);
        switch (fieldType) {
        case MYSQL_TYPE_LONGLONG:
            if (type.isUnsigned()) {
                // for bigint unsigned
                return BlockBuilders.create(type, context, context.getBlockBuilderCapacity(), objectPools);
            } else {
                // for bigint
                return new BatchLongBlockBuilder(chunkSize, chunkLimit, objectPools.getLongArrayPool());
            }
        case MYSQL_TYPE_LONG:
            if (type.isUnsigned()) {
                // for int unsigned
                return new BatchLongBlockBuilder(chunkSize, chunkLimit, objectPools.getLongArrayPool());
            } else {
                // for int
                return new BatchIntegerBlockBuilder(chunkSize, chunkLimit, objectPools.getIntArrayPool());
            }

        case MYSQL_TYPE_DATE:
        case MYSQL_TYPE_NEWDATE:
            return new BatchDateBlockBuilder(chunkSize, chunkLimit, objectPools.getLongArrayPool());
        case MYSQL_TYPE_DECIMAL:
        case MYSQL_TYPE_NEWDECIMAL:
            return new BatchDecimalBlockBuilder(chunkSize, type);
        default:
            return BlockBuilders.create(type, context, context.getBlockBuilderCapacity(), objectPools);
        }
    }

    static BlockBuilder create(DataType type, ExecutionContext context, int chunkSize) {
        MySQLStandardFieldType fieldType = type.fieldType();
        switch (fieldType) {
        case MYSQL_TYPE_LONGLONG:
            if (type.isUnsigned()) {
                // for bigint unsigned
                return BlockBuilders.create(type, context, context.getBlockBuilderCapacity());
            } else {
                // for bigint
                return new BatchLongBlockBuilder(chunkSize);
            }
        case MYSQL_TYPE_LONG:
            if (type.isUnsigned()) {
                // for int unsigned
                return new BatchLongBlockBuilder(chunkSize);
            } else {
                // for int
                return new BatchIntegerBlockBuilder(chunkSize);
            }
        case MYSQL_TYPE_DATE:
        case MYSQL_TYPE_NEWDATE:
            return new BatchDateBlockBuilder(chunkSize);
        case MYSQL_TYPE_DECIMAL:
        case MYSQL_TYPE_NEWDECIMAL:
            return new BatchDecimalBlockBuilder(chunkSize, type);
        default:
            return BlockBuilders.create(type, context, context.getBlockBuilderCapacity());
        }
    }

    void copyBlock(Block sourceBlock, int[] positions, int offsetInPositionArray, int positionOffset,
                   int positionCount);

    default void copyBlock(Block sourceBlock, int[] positions, int positionOffset, int positionCount) {
        copyBlock(sourceBlock, positions, 0, positionOffset, positionCount);
    }

    default void copyBlock(Block sourceBlock, int[] positions, int positionCount) {
        copyBlock(sourceBlock, positions, 0, positionCount);
    }

    void copyBlock(Block sourceBlock, int positionCount);

    class BatchIntegerBlockBuilder extends AbstractBatchBlockBuilder implements BatchBlockWriter {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(BatchIntegerBlockBuilder.class).instanceSize();
        private int[] values;

        @FieldMemoryCounter(value = false)
        private DriverObjectPool<int[]> objectPool;
        private int chunkLimit;

        public BatchIntegerBlockBuilder(int capacity, int chunkLimit, DriverObjectPool<int[]> intArrayPool) {
            super(capacity);
            this.objectPool = intArrayPool;
            this.chunkLimit = chunkLimit;
            int[] result = intArrayPool.poll();
            if (result == null || result.length < capacity) {
                result = new int[capacity];
            }
            this.values = result;
        }

        public BatchIntegerBlockBuilder(int capacity) {
            super(capacity);
            this.values = new int[capacity];
        }

        @Override
        public long getMemoryUsage() {
            return INSTANCE_SIZE
                + VMSupport.align((int) SizeOf.sizeOf(values))
                + VMSupport.align((int) SizeOf.sizeOf(valueIsNull));
        }

        @Override
        public void copyBlock(Block sourceBlock, int[] positions, int offsetInPositionArray, int positionOffset,
                              int positionCount) {
            IntegerBlock block = sourceBlock.cast(IntegerBlock.class);
            int[] selection = block.getSelection();
            int[] intArray = block.intArray();

            int nullArrayIndex = currentIndex;
            if (selection != null) {
                for (int i = 0; i < positionCount; i++) {
                    int j = selection[positions[i + offsetInPositionArray] + positionOffset];
                    values[currentIndex++] = intArray[j];
                }

                if (block.mayHaveNull()) {
                    allocateNulls();
                    boolean[] nullArray = block.nulls();
                    for (int i = 0; i < positionCount; i++) {
                        int j = selection[positions[i + offsetInPositionArray] + positionOffset];
                        containsNull |= (valueIsNull[nullArrayIndex++] = nullArray[j]);
                    }
                }

            } else {
                for (int i = 0; i < positionCount; i++) {
                    values[currentIndex++] = intArray[positions[i + offsetInPositionArray] + positionOffset];
                }

                if (block.mayHaveNull()) {
                    allocateNulls();
                    boolean[] nullArray = block.nulls();
                    for (int i = 0; i < positionCount; i++) {
                        containsNull |= (valueIsNull[nullArrayIndex++] =
                            nullArray[positions[i + offsetInPositionArray] + positionOffset]);
                    }
                }
            }
        }

        @Override
        public void copyBlock(Block sourceBlock, int positionCount) {
            IntegerBlock block = sourceBlock.cast(IntegerBlock.class);
            int[] selection = block.getSelection();
            int[] intArray = block.intArray();

            int nullArrayIndex = currentIndex;
            if (selection != null) {
                for (int i = 0; i < positionCount; i++) {
                    int j = selection[i];
                    values[currentIndex++] = intArray[j];
                }

                if (block.mayHaveNull()) {
                    allocateNulls();
                    boolean[] nullArray = block.nulls();
                    for (int i = 0; i < positionCount; i++) {
                        int j = selection[i];
                        containsNull |= (valueIsNull[nullArrayIndex++] = nullArray[j]);
                    }
                }

            } else {
                for (int i = 0; i < positionCount; i++) {
                    values[currentIndex++] = intArray[i];
                }

                if (block.mayHaveNull()) {
                    allocateNulls();
                    boolean[] nullArray = block.nulls();
                    for (int i = 0; i < positionCount; i++) {
                        containsNull |= (valueIsNull[nullArrayIndex++] = nullArray[i]);
                    }
                }
            }
        }

        @Override
        public void writeInt(int value) {
            values[currentIndex] = value;
            if (valueIsNull != null) {
                valueIsNull[currentIndex] = false;
            }
            currentIndex++;
        }

        @Override
        public int getInt(int position) {
            checkReadablePosition(position);
            return values[position];
        }

        @Override
        public Object getObject(int position) {
            return isNull(position) ? null : getInt(position);
        }

        @Override
        public void writeObject(Object value) {
            if (value == null) {
                appendNull();
                return;
            }
            Preconditions.checkArgument(value instanceof Integer);
            writeInt((Integer) value);
        }

        @Override
        public Block build() {
            Block result = new IntegerBlock(0, getPositionCount(), mayHaveNull() ? valueIsNull : null, values);
            if (objectPool != null) {
                result.setRecycler(objectPool.getRecycler(chunkLimit));
            }
            return result;
        }

        @Override
        public void appendNull() {
            appendNullInternal();
            values[currentIndex - 1] = 0;
        }

        @Override
        public BlockBuilder newBlockBuilder() {
            if (objectPool != null) {
                return new BatchIntegerBlockBuilder(getCapacity(), chunkLimit, objectPool);
            } else {
                return new BatchIntegerBlockBuilder(getCapacity());
            }
        }

        @Override
        public BlockBuilder newBlockBuilder(ObjectPools objectPools, int chunkLimit) {
            return new BatchIntegerBlockBuilder(getCapacity(), chunkLimit, objectPools.getIntArrayPool());
        }

        @Override
        public int hashCode(int position) {
            if (isNull(position)) {
                return 0;
            }
            return values[position];
        }
    }

    class BatchLongBlockBuilder extends AbstractBatchBlockBuilder implements BatchBlockWriter {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(BatchLongBlockBuilder.class).instanceSize();

        private long[] values;

        @FieldMemoryCounter(value = false)
        private DriverObjectPool<long[]> objectPool;
        private int chunkLimit;

        public BatchLongBlockBuilder(int initialCapacity) {
            super(initialCapacity);
            this.values = new long[initialCapacity];
        }

        public BatchLongBlockBuilder(int capacity, int chunkLimit, DriverObjectPool<long[]> longArrayPool) {
            super(capacity);
            this.objectPool = longArrayPool;
            this.chunkLimit = chunkLimit;

            long[] result = longArrayPool.poll();
            if (result == null || result.length < capacity) {
                result = new long[capacity];
            }
            this.values = result;
        }

        @Override
        public long getMemoryUsage() {
            return INSTANCE_SIZE
                + VMSupport.align((int) SizeOf.sizeOf(values))
                + VMSupport.align((int) SizeOf.sizeOf(valueIsNull));
        }

        @Override
        public void copyBlock(Block sourceBlock, int[] positions, int offsetInPositionArray, int positionOffset,
                              int positionCount) {
            LongBlock block = sourceBlock.cast(LongBlock.class);
            int[] selection = block.getSelection();
            long[] longArray = block.longArray();

            int nullArrayIndex = currentIndex;
            if (selection != null) {
                for (int i = 0; i < positionCount; i++) {
                    int j = selection[positions[i + offsetInPositionArray] + positionOffset];
                    values[currentIndex++] = longArray[j];
                }

                if (block.mayHaveNull()) {
                    allocateNulls();
                    boolean[] nullArray = block.nulls();
                    for (int i = 0; i < positionCount; i++) {
                        int j = selection[positions[i + offsetInPositionArray] + positionOffset];
                        containsNull |= (valueIsNull[nullArrayIndex++] = nullArray[j]);
                    }
                }

            } else {
                for (int i = 0; i < positionCount; i++) {
                    values[currentIndex++] = longArray[positions[i + offsetInPositionArray] + positionOffset];
                }

                if (block.mayHaveNull()) {
                    allocateNulls();
                    boolean[] nullArray = block.nulls();
                    for (int i = 0; i < positionCount; i++) {
                        containsNull |= (valueIsNull[nullArrayIndex++] =
                            nullArray[positions[i + offsetInPositionArray] + positionOffset]);
                    }
                }
            }
        }

        @Override
        public void copyBlock(Block sourceBlock, int positionCount) {
            LongBlock block = sourceBlock.cast(LongBlock.class);
            int[] selection = block.getSelection();
            long[] longArray = block.longArray();

            int nullArrayIndex = currentIndex;
            if (selection != null) {
                for (int i = 0; i < positionCount; i++) {
                    int j = selection[i];
                    values[currentIndex++] = longArray[j];
                }

                if (block.mayHaveNull()) {
                    allocateNulls();
                    boolean[] nullArray = block.nulls();
                    for (int i = 0; i < positionCount; i++) {
                        int j = selection[i];
                        containsNull |= (valueIsNull[nullArrayIndex++] = nullArray[j]);
                    }
                }

            } else {
                for (int i = 0; i < positionCount; i++) {
                    values[currentIndex++] = longArray[i];
                }

                if (block.mayHaveNull()) {
                    allocateNulls();
                    boolean[] nullArray = block.nulls();
                    for (int i = 0; i < positionCount; i++) {
                        containsNull |= (valueIsNull[nullArrayIndex++] = nullArray[i]);
                    }
                }
            }
        }

        @Override
        public void writeLong(long value) {
            values[currentIndex] = value;
            if (valueIsNull != null) {
                valueIsNull[currentIndex] = false;
            }
            currentIndex++;
        }

        @Override
        public long getLong(int position) {
            checkReadablePosition(position);
            return values[position];
        }

        @Override
        public Object getObject(int position) {
            return isNull(position) ? null : getLong(position);
        }

        @Override
        public void writeObject(Object value) {
            if (value == null) {
                appendNull();
                return;
            }
            Preconditions.checkArgument(value instanceof Long);
            writeLong((Long) value);
        }

        @Override
        public Block build() {
            Block block = new LongBlock(0, getPositionCount(), mayHaveNull() ? valueIsNull : null, values);
            if (objectPool != null) {
                block.setRecycler(objectPool.getRecycler(chunkLimit));
            }
            return block;
        }

        @Override
        public void appendNull() {
            appendNullInternal();
            values[currentIndex - 1] = 0L;
        }

        @Override
        public BlockBuilder newBlockBuilder() {
            if (objectPool != null) {
                return new BatchLongBlockBuilder(getCapacity(), chunkLimit, objectPool);
            } else {
                return new BatchLongBlockBuilder(getCapacity());
            }
        }

        @Override
        public BlockBuilder newBlockBuilder(ObjectPools objectPools, int chunkLimit) {
            return new BatchLongBlockBuilder(getCapacity(), chunkLimit, objectPools.getLongArrayPool());
        }

        @Override
        public int hashCode(int position) {
            if (isNull(position)) {
                return 0;
            }
            return Long.hashCode(values[position]);
        }
    }

    class BatchDateBlockBuilder extends AbstractBatchBlockBuilder implements BatchBlockWriter {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(BatchDateBlockBuilder.class).instanceSize();

        long[] packed;

        @FieldMemoryCounter(value = false)
        DriverObjectPool<long[]> objectPool;

        int chunkLimit;

        public BatchDateBlockBuilder(int initialCapacity) {
            super(initialCapacity);
            packed = new long[initialCapacity];
        }

        public BatchDateBlockBuilder(int capacity, int chunkLimit, DriverObjectPool<long[]> longArrayPool) {
            super(capacity);
            this.objectPool = longArrayPool;
            this.chunkLimit = chunkLimit;
            long[] result = longArrayPool.poll();
            if (result == null || result.length < capacity) {
                result = new long[capacity];
            }
            this.packed = result;
        }

        @Override
        public long getMemoryUsage() {
            return INSTANCE_SIZE
                + VMSupport.align((int) SizeOf.sizeOf(packed))
                + VMSupport.align((int) SizeOf.sizeOf(valueIsNull));
        }

        @Override
        public void copyBlock(Block sourceBlock, int[] positions, int offsetInPositionArray, int positionOffset,
                              int positionCount) {
            DateBlock block = sourceBlock.cast(DateBlock.class);
            int[] selection = block.getSelection();
            long[] longArray = block.getPacked();

            int nullArrayIndex = currentIndex;
            if (selection != null) {
                for (int i = 0; i < positionCount; i++) {
                    int j = selection[positions[i + offsetInPositionArray] + positionOffset];
                    packed[currentIndex++] = longArray[j];
                }

                if (block.mayHaveNull()) {
                    allocateNulls();
                    boolean[] nullArray = block.nulls();
                    for (int i = 0; i < positionCount; i++) {
                        int j = selection[positions[i + offsetInPositionArray] + positionOffset];
                        containsNull |= (valueIsNull[nullArrayIndex++] = nullArray[j]);
                    }
                }

            } else {
                for (int i = 0; i < positionCount; i++) {
                    packed[currentIndex++] = longArray[positions[i + offsetInPositionArray] + positionOffset];
                }

                if (block.mayHaveNull()) {
                    allocateNulls();
                    boolean[] nullArray = block.nulls();
                    for (int i = 0; i < positionCount; i++) {
                        containsNull |= (valueIsNull[nullArrayIndex++] =
                            nullArray[positions[i + offsetInPositionArray] + positionOffset]);
                    }
                }
            }
        }

        @Override
        public void copyBlock(Block sourceBlock, int positionCount) {
            DateBlock block = sourceBlock.cast(DateBlock.class);
            int[] selection = block.getSelection();
            long[] longArray = block.getPacked();

            int nullArrayIndex = currentIndex;
            if (selection != null) {
                for (int i = 0; i < positionCount; i++) {
                    int j = selection[i];
                    packed[currentIndex++] = longArray[j];
                }

                if (block.mayHaveNull()) {
                    allocateNulls();
                    boolean[] nullArray = block.nulls();
                    for (int i = 0; i < positionCount; i++) {
                        int j = selection[i];
                        containsNull |= (valueIsNull[nullArrayIndex++] = nullArray[j]);
                    }
                }

            } else {
                for (int i = 0; i < positionCount; i++) {
                    packed[currentIndex++] = longArray[i];
                }

                if (block.mayHaveNull()) {
                    allocateNulls();
                    boolean[] nullArray = block.nulls();
                    for (int i = 0; i < positionCount; i++) {
                        containsNull |= (valueIsNull[nullArrayIndex++] = nullArray[i]);
                    }
                }
            }
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

        public void writePackedLong(long value) {
            packed[currentIndex] = value;
            if (valueIsNull != null) {
                valueIsNull[currentIndex] = false;
            }
            currentIndex++;
        }

        public void writeMysqlDatetime(MysqlDateTime t) {
            if (t == null) {
                appendNull();
                return;
            }
            long l = TimeStorage.writeDate(t);

            packed[currentIndex] = l;
            if (valueIsNull != null) {
                valueIsNull[currentIndex] = false;
            }
            currentIndex++;
        }

        @Override
        public void writeDate(Date value) {
            // round to scale.
            Date date = DataTypes.DateType.convertFrom(value);

            // pack to long value
            MysqlDateTime t = Optional.ofNullable(date)
                .map(MySQLTimeTypeUtil::toMysqlDate)
                .orElse(null);

            writeMysqlDatetime(t);
        }

        @Override
        public void writeDatetimeRawLong(long val) {
            packed[currentIndex] = val;
            if (valueIsNull != null) {
                valueIsNull[currentIndex] = false;
            }
            currentIndex++;
        }

        @Override
        public Date getDate(int position) {
            checkReadablePosition(position);

            // unpack the long value to original date object.
            final long packedLong = packed[position];
            MysqlDateTime t = TimeStorage.readDate(packedLong);
            t.setTimezone(InternalTimeZone.DEFAULT_TIME_ZONE);

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
        public Block build() {
            Block block = new DateBlock(0, getPositionCount(), mayHaveNull() ? valueIsNull : null, packed,
                DataTypes.DateType, InternalTimeZone.DEFAULT_TIME_ZONE);
            if (objectPool != null) {
                block.setRecycler(objectPool.getRecycler(chunkLimit));
            }
            return block;
        }

        @Override
        public void appendNull() {
            appendNullInternal();
            packed[currentIndex - 1] = 0L;
        }

        @Override
        public BlockBuilder newBlockBuilder() {
            if (objectPool != null) {
                return new BatchDateBlockBuilder(getCapacity(), chunkLimit, objectPool);
            } else {
                return new BatchDateBlockBuilder(getCapacity());
            }
        }

        @Override
        public BlockBuilder newBlockBuilder(ObjectPools objectPools, int chunkLimit) {
            return new BatchDateBlockBuilder(getCapacity(), chunkLimit, objectPools.getLongArrayPool());
        }

        @Override
        public int hashCode(int position) {
            if (isNull(position)) {
                return 0;
            }
            return Long.hashCode(packed[position]);
        }

        public long getPackedLong(int position) {
            checkReadablePosition(position);
            return packed[position];
        }
    }

    class BatchDecimalBlockBuilder extends AbstractBatchBlockBuilder
        implements BatchBlockWriter, SegmentedDecimalBlock {

        private static final int INSTANCE_SIZE = ClassLayout.parseClass(BatchDecimalBlockBuilder.class).instanceSize();

        private final int scale;
        SliceOutput sliceOutput;
        long[] decimal64List;
        long[] decimal128HighList;

        @FieldMemoryCounter(value = false)
        DecimalType decimalType;
        // collect state of decimal values.
        SegmentedDecimalBlock.DecimalBlockState state;
        private DecimalStructure decimalBuffer;
        private DecimalStructure decimalResult;

        public BatchDecimalBlockBuilder(int capacity, DataType type) {
            super(capacity);
            this.decimalType = (DecimalType) type;
            this.scale = decimalType.getScale();
            this.state = UNSET_STATE;
        }

        @Override
        public long getMemoryUsage() {
            return INSTANCE_SIZE
                + VMSupport.align((int) SizeOf.sizeOf(valueIsNull))
                + FastMemoryCounter.sizeOf(sliceOutput)
                + VMSupport.align((int) SizeOf.sizeOf(decimal64List))
                + VMSupport.align((int) SizeOf.sizeOf(decimal128HighList))
                + (state == null ? 0 : state.memorySize())
                + FastMemoryCounter.sizeOf(decimalBuffer)
                + FastMemoryCounter.sizeOf(decimalResult);
        }

        @Override
        public void copyBlock(Block sourceBlock, int[] positions, int offsetInPositionArray, int positionOffset,
                              int positionCount) {
            DecimalBlock block = sourceBlock.cast(DecimalBlock.class);

            if (isDecimal64()) {
                copyToDecimal64(block, positions, offsetInPositionArray, positionOffset, positionCount);
                return;
            }

            if (isDecimal128()) {
                copyToDecimal128(block, positions, offsetInPositionArray, positionOffset, positionCount);
                return;
            }

            // copy to a normal decimal block
            Slice output = DecimalStructure.allocateDecimalSlice();
            for (int i = 0; i < positionCount; i++) {
                block.writePositionTo(positions[i], this, output);
            }
        }

        private void copyToDecimal64(DecimalBlock sourceBlock, int[] positions, int offsetInPositionArray,
                                     int positionOffset, int positionCount) {
            if (this.isUnset()) {
                initDecimal64List();
            }
            if (sourceBlock.isDecimal64()) {
                copyDec64ToDec64(sourceBlock, positions, offsetInPositionArray, positionOffset, positionCount);
                return;
            }
            if (sourceBlock.isDecimal128()) {
                copyDec128ToDec64(sourceBlock, positions, offsetInPositionArray, positionOffset, positionCount);
                return;
            }

            // copy from a normal decimal block
            Slice output = sourceBlock.allocCachedSlice();
            for (int i = 0; i < positionCount; i++) {
                sourceBlock.writePositionTo(positions[i], this, output);
            }
        }

        private void copyToDecimal128(DecimalBlock sourceBlock, int[] positions, int offsetInPositionArray,
                                      int positionOffset, int positionCount) {
            if (this.isUnset()) {
                initDecimal128List();
            }
            if (sourceBlock.isDecimal64()) {
                copyDec64ToDec128(sourceBlock, positions, offsetInPositionArray, positionOffset, positionCount);
                return;
            }
            if (sourceBlock.isDecimal128()) {
                copyDec128ToDec128(sourceBlock, positions, offsetInPositionArray, positionOffset, positionCount);
                return;
            }

            // copy from a normal decimal block
            Slice output = sourceBlock.allocCachedSlice();
            for (int i = 0; i < positionCount; i++) {
                sourceBlock.writePositionTo(positions[i], this, output);
            }
        }

        private void copyDec64ToDec64(DecimalBlock sourceBlock, int[] positions, int offsetInPositionArray,
                                      int positionOffset, int positionCount) {
            int[] selection = sourceBlock.getSelection();
            long[] longArray = sourceBlock.getDecimal64Values();
            int nullArrayIndex = currentIndex;
            if (selection != null) {
                for (int i = 0; i < positionCount; i++) {
                    int j = selection[positions[i + offsetInPositionArray] + positionOffset];
                    decimal64List[currentIndex++] = longArray[j];
                }

                if (sourceBlock.mayHaveNull()) {
                    allocateNulls();
                    boolean[] nullArray = sourceBlock.nulls();
                    for (int i = 0; i < positionCount; i++) {
                        int j = selection[positions[i + offsetInPositionArray] + positionOffset];
                        containsNull |= (valueIsNull[nullArrayIndex++] = nullArray[j]);
                    }
                }

            } else {
                for (int i = 0; i < positionCount; i++) {
                    decimal64List[currentIndex++] = longArray[positions[i + offsetInPositionArray] + positionOffset];
                }

                if (sourceBlock.mayHaveNull()) {
                    allocateNulls();
                    boolean[] nullArray = sourceBlock.nulls();
                    for (int i = 0; i < positionCount; i++) {
                        containsNull |= (valueIsNull[nullArrayIndex++] =
                            nullArray[positions[i + offsetInPositionArray] + positionOffset]);
                    }
                }
            }
        }

        private void copyDec64ToDec128(DecimalBlock sourceBlock, int[] positions, int offsetInPositionArray,
                                       int positionOffset, int positionCount) {
            int[] selection = sourceBlock.getSelection();
            long[] longArray = sourceBlock.getDecimal64Values();
            int nullArrayIndex = currentIndex;
            if (selection != null) {
                for (int i = 0; i < positionCount; i++) {
                    int j = selection[positions[i + offsetInPositionArray] + positionOffset];
                    decimal64List[currentIndex] = longArray[j];
                    decimal128HighList[currentIndex] = decimal64List[currentIndex] >= 0 ? 0 : -1;
                    currentIndex++;
                }

                if (sourceBlock.mayHaveNull()) {
                    allocateNulls();
                    boolean[] nullArray = sourceBlock.nulls();
                    for (int i = 0; i < positionCount; i++) {
                        int j = selection[positions[i + offsetInPositionArray] + positionOffset];
                        containsNull |= (valueIsNull[nullArrayIndex++] = nullArray[j]);
                    }
                }

            } else {
                for (int i = 0; i < positionCount; i++) {
                    decimal64List[currentIndex] = longArray[positions[i + offsetInPositionArray] + positionOffset];
                    decimal128HighList[currentIndex] = decimal64List[currentIndex] >= 0 ? 0 : -1;
                    currentIndex++;
                }

                if (sourceBlock.mayHaveNull()) {
                    allocateNulls();
                    boolean[] nullArray = sourceBlock.nulls();
                    for (int i = 0; i < positionCount; i++) {
                        containsNull |= (valueIsNull[nullArrayIndex++] =
                            nullArray[positions[i + offsetInPositionArray] + positionOffset]);
                    }
                }
            }
        }

        private void copyDec128ToDec64(DecimalBlock sourceBlock, int[] positions, int offsetInPositionArray,
                                       int positionOffset, int positionCount) {
            this.initDecimal128List();
            copyDec128ToDec128(sourceBlock, positions, offsetInPositionArray, positionOffset, positionCount);
        }

        private void copyDec128ToDec128(DecimalBlock sourceBlock, int[] positions, int offsetInPositionArray,
                                        int positionOffset, int positionCount) {
            int[] selection = sourceBlock.getSelection();
            long[] srcDecimal128Low = sourceBlock.getDecimal128LowValues();
            long[] srcDecimal128High = sourceBlock.getDecimal128HighValues();
            int nullArrayIndex = currentIndex;
            if (selection != null) {
                for (int i = 0; i < positionCount; i++) {
                    int j = selection[positions[i + offsetInPositionArray] + positionOffset];
                    decimal64List[currentIndex] = srcDecimal128Low[j];
                    decimal128HighList[currentIndex] = srcDecimal128High[j];
                    currentIndex++;
                }

                if (sourceBlock.mayHaveNull()) {
                    allocateNulls();
                    boolean[] nullArray = sourceBlock.nulls();
                    for (int i = 0; i < positionCount; i++) {
                        int j = selection[positions[i + offsetInPositionArray] + positionOffset];
                        containsNull |= (valueIsNull[nullArrayIndex++] = nullArray[j]);
                    }
                }

            } else {
                for (int i = 0; i < positionCount; i++) {
                    decimal64List[currentIndex] =
                        srcDecimal128Low[positions[i + offsetInPositionArray] + positionOffset];
                    decimal128HighList[currentIndex] =
                        srcDecimal128High[positions[i + offsetInPositionArray] + positionOffset];
                    currentIndex++;
                }

                if (sourceBlock.mayHaveNull()) {
                    allocateNulls();
                    boolean[] nullArray = sourceBlock.nulls();
                    for (int i = 0; i < positionCount; i++) {
                        containsNull |= (valueIsNull[nullArrayIndex++] =
                            nullArray[positions[i + offsetInPositionArray] + positionOffset]);
                    }
                }
            }
        }

        @Override
        public void copyBlock(Block sourceBlock, int positionCount) {
            DecimalBlock block = sourceBlock.cast(DecimalBlock.class);

            if (isDecimal64()) {
                copyToDecimal64(block, positionCount);
                return;
            }

            if (isDecimal128()) {
                copyToDecimal128(block, positionCount);
                return;
            }

            // copy to a normal decimal block
            Slice output = DecimalStructure.allocateDecimalSlice();
            for (int i = 0; i < positionCount; i++) {
                block.writePositionTo(i, this, output);
            }
        }

        private void copyToDecimal64(DecimalBlock sourceBlock, int positionCount) {
            if (sourceBlock.isDecimal64()) {
                copyDec64ToDec64(sourceBlock, positionCount);
                return;
            }
            if (sourceBlock.isDecimal128()) {
                copyDec128ToDec64(sourceBlock, positionCount);
                return;
            }

            // copy from a normal decimal block
            Slice output = DecimalStructure.allocateDecimalSlice();
            for (int i = 0; i < positionCount; i++) {
                sourceBlock.writePositionTo(i, this, output);
            }
        }

        private void copyToDecimal128(DecimalBlock sourceBlock, int positionCount) {
            if (this.isUnset()) {
                initDecimal128List();
            }
            if (sourceBlock.isDecimal64()) {
                copyDec64ToDec128(sourceBlock, positionCount);
                return;
            }
            if (sourceBlock.isDecimal128()) {
                copyDec128ToDec128(sourceBlock, positionCount);
                return;
            }

            // copy from a normal decimal block
            Slice output = DecimalStructure.allocateDecimalSlice();
            for (int i = 0; i < positionCount; i++) {
                sourceBlock.writePositionTo(i, this, output);
            }
        }

        private void copyDec64ToDec64(DecimalBlock sourceBlock, int positionCount) {
            int[] selection = sourceBlock.getSelection();
            long[] longArray = sourceBlock.getDecimal64Values();
            int nullArrayIndex = currentIndex;
            if (selection != null) {
                for (int i = 0; i < positionCount; i++) {
                    int j = selection[i];
                    decimal64List[currentIndex++] = longArray[j];
                }

                if (sourceBlock.mayHaveNull()) {
                    allocateNulls();
                    boolean[] nullArray = sourceBlock.nulls();
                    for (int i = 0; i < positionCount; i++) {
                        int j = selection[i];
                        containsNull |= (valueIsNull[nullArrayIndex++] = nullArray[j]);
                    }
                }

            } else {
                for (int i = 0; i < positionCount; i++) {
                    decimal64List[currentIndex++] = longArray[i];
                }

                if (sourceBlock.mayHaveNull()) {
                    allocateNulls();
                    boolean[] nullArray = sourceBlock.nulls();
                    for (int i = 0; i < positionCount; i++) {
                        containsNull |= (valueIsNull[nullArrayIndex++] = nullArray[i]);
                    }
                }
            }
        }

        private void copyDec64ToDec128(DecimalBlock sourceBlock, int positionCount) {
            int[] selection = sourceBlock.getSelection();
            long[] longArray = sourceBlock.getDecimal64Values();
            int nullArrayIndex = currentIndex;
            if (selection != null) {
                for (int i = 0; i < positionCount; i++) {
                    int j = selection[i];
                    decimal64List[currentIndex] = longArray[j];
                    decimal128HighList[currentIndex] = longArray[j] >= 0 ? 0 : 1;
                    currentIndex++;
                }

                if (sourceBlock.mayHaveNull()) {
                    allocateNulls();
                    boolean[] nullArray = sourceBlock.nulls();
                    for (int i = 0; i < positionCount; i++) {
                        int j = selection[i];
                        containsNull |= (valueIsNull[nullArrayIndex++] = nullArray[j]);
                    }
                }

            } else {
                for (int i = 0; i < positionCount; i++) {
                    decimal64List[currentIndex++] = longArray[i];
                    decimal128HighList[currentIndex++] = longArray[i] >= 0 ? 0 : 1;
                    currentIndex++;
                }

                if (sourceBlock.mayHaveNull()) {
                    allocateNulls();
                    boolean[] nullArray = sourceBlock.nulls();
                    for (int i = 0; i < positionCount; i++) {
                        containsNull |= (valueIsNull[nullArrayIndex++] = nullArray[i]);
                    }
                }
            }
        }

        private void copyDec128ToDec64(DecimalBlock sourceBlock, int positionCount) {
            this.initDecimal128List();
            copyDec128ToDec128(sourceBlock, positionCount);
        }

        private void copyDec128ToDec128(DecimalBlock sourceBlock, int positionCount) {
            int[] selection = sourceBlock.getSelection();
            long[] srcDecimal128Low = sourceBlock.getDecimal128LowValues();
            long[] srcDecimal128High = sourceBlock.getDecimal128HighValues();
            int nullArrayIndex = currentIndex;
            if (selection != null) {
                for (int i = 0; i < positionCount; i++) {
                    int j = selection[i];
                    decimal64List[currentIndex] = srcDecimal128Low[j];
                    decimal128HighList[currentIndex] = srcDecimal128High[j];
                    currentIndex++;
                }

                if (sourceBlock.mayHaveNull()) {
                    allocateNulls();
                    boolean[] nullArray = sourceBlock.nulls();
                    for (int i = 0; i < positionCount; i++) {
                        int j = selection[i];
                        containsNull |= (valueIsNull[nullArrayIndex++] = nullArray[j]);
                    }
                }

            } else {
                for (int i = 0; i < positionCount; i++) {
                    decimal64List[currentIndex] = srcDecimal128Low[i];
                    decimal128HighList[currentIndex] = srcDecimal128High[i];
                    currentIndex++;
                }

                if (sourceBlock.mayHaveNull()) {
                    allocateNulls();
                    boolean[] nullArray = sourceBlock.nulls();
                    for (int i = 0; i < positionCount; i++) {
                        containsNull |= (valueIsNull[nullArrayIndex++] = nullArray[i]);
                    }
                }
            }
        }

        private void initSliceOutput() {
            if (this.sliceOutput == null) {
                this.sliceOutput = new DynamicSliceOutput(initialCapacity * DECIMAL_MEMORY_SIZE);
            }
        }

        private void initDecimal64List() {
            if (this.decimal64List == null) {
                this.decimal64List = new long[initialCapacity];
            }
            this.state = DECIMAL_64;
        }

        private void initDecimal128List() {
            if (this.decimal64List == null) {
                this.decimal64List = new long[initialCapacity];
            }
            if (this.decimal128HighList == null) {
                this.decimal128HighList = new long[initialCapacity];
            }
            this.state = DECIMAL_128;
        }

        @Override
        public void writeDecimal(Decimal value) {
            convertToNormalDecimal();
            if (valueIsNull != null) {
                valueIsNull[currentIndex] = false;
            }
            currentIndex++;
            sliceOutput.writeBytes(value.getMemorySegment());

            updateDecimalInfo(value.getDecimalStructure());
        }

        @Override
        public void writeLong(long value) {
            if (state.isUnset()) {
                initDecimal64List();
                state = DECIMAL_64;
            } else if (!state.isDecimal64()) {
                writeDecimal(new Decimal(value, decimalType.getScale()));
                return;
            }

            if (valueIsNull != null) {
                valueIsNull[currentIndex] = false;
            }
            decimal64List[currentIndex] = value;
            currentIndex++;
        }

        public void writeDecimal128(long low, long high) {
            if (state.isUnset()) {
                initDecimal128List();
                state = DECIMAL_128;
            } else if (state.isDecimal64()) {
                // convert decimal64 to decimal128
                initDecimal128List();
                state = DECIMAL_128;
                for (int i = 0; i < currentIndex; i++) {
                    if (decimal64List != null && decimal64List[i] < 0) {
                        decimal128HighList[i] = -1;
                    } else {
                        decimal128HighList[i] = 0;
                    }
                }
            } else if (!state.isDecimal128()) {
                // normal decimal
                DecimalStructure buffer = getDecimalBuffer();
                DecimalStructure result = getDecimalResult();
                FastDecimalUtils.setDecimal128WithScale(buffer, result, low, high, scale);
                writeDecimal(new Decimal(result));
                return;
            }

            if (valueIsNull != null) {
                valueIsNull[currentIndex] = false;
            }
            decimal64List[currentIndex] = low;
            decimal128HighList[currentIndex] = high;
            currentIndex++;
        }

        @Override
        public void writeByteArray(byte[] value) {
            writeByteArray(value, 0, value.length);
        }

        @Override
        public void writeByteArray(byte[] value, int offset, int length) {
            DecimalStructure d = new DecimalStructure();
            DecimalConverter.parseString(value, offset, length, d, false);
            writeDecimal(new Decimal(d));
        }

        @Override
        public void appendNull() {
            appendNullInternal();
            if (isUnset()) {
                initDecimal64List();
            }
            if (isDecimal64()) {
                decimal64List[currentIndex - 1] = 0L;
            } else if (isDecimal128()) {
                decimal64List[currentIndex - 1] = 0L;
                decimal128HighList[currentIndex - 1] = 0L;
            } else {
                // If null value, just skip 64-bytes
                sliceOutput.skipBytes(DECIMAL_MEMORY_SIZE);
            }
        }

        @Override
        public Decimal getDecimal(int position) {
            checkReadablePosition(position);
            if (state.isDecimal64()) {
                return new Decimal(getLong(position), scale);
            }
            if (state.isDecimal128()) {
                DecimalStructure buffer = getDecimalBuffer();
                DecimalStructure result = getDecimalResult();
                long low = decimal64List[position];
                long high = decimal128HighList[position];
                FastDecimalUtils.setDecimal128WithScale(buffer, result, low, high, scale);
                return new Decimal(result);
            }
            Slice segment = sliceOutput.slice().slice(position * DECIMAL_MEMORY_SIZE, DECIMAL_MEMORY_SIZE);
            return new Decimal(segment);
        }

        @Override
        public long getLong(int position) {
            checkDecimal64StoreType();
            checkReadablePosition(position);
            return decimal64List[position];
        }

        @Override
        public Object getObject(int position) {
            return isNull(position) ? null : getDecimal(position);
        }

        @Override
        public void writeObject(Object value) {
            if (value == null) {
                appendNull();
                return;
            }
            checkNormalDecimalType();
            Preconditions.checkArgument(value instanceof Decimal);
            writeDecimal((Decimal) value);
        }

        @Override
        public Block build() {
            if (isDecimal64()) {
                return new DecimalBlock(decimalType, getPositionCount(), mayHaveNull(),
                    mayHaveNull() ? valueIsNull : null, decimal64List);
            }
            if (isDecimal128()) {
                return DecimalBlock.buildDecimal128Block(decimalType, getPositionCount(), mayHaveNull(),
                    mayHaveNull() ? valueIsNull : null, decimal64List, decimal128HighList);
            }

            return new DecimalBlock(decimalType, getPositionCount(), mayHaveNull() ? valueIsNull : null,
                sliceOutput.slice(), state);
        }

        @Override
        public BlockBuilder newBlockBuilder() {
            return new BatchDecimalBlockBuilder(getCapacity(), decimalType);
        }

        @Override
        public int hashCode(int position) {
            if (isNull(position)) {
                return 0;
            }
            return getDecimal(position).hashCode();
        }

        @Override
        public Slice segmentUncheckedAt(int position) {
            return sliceOutput.slice().slice(position * DECIMAL_MEMORY_SIZE, DECIMAL_MEMORY_SIZE);
        }

        private void updateDecimalInfo(DecimalStructure d) {
            DecimalBlock.DecimalBlockState elementState = DecimalBlock.DecimalBlockState.stateOf(d);
            this.state = this.state.merge(elementState);
        }

        public DecimalBlock.DecimalBlockState getState() {
            return this.state;
        }

        public DataType getDecimalType() {
            return decimalType;
        }

        @Override
        public boolean isDecimal64() {
            return state.isDecimal64() || (state.isUnset() && decimalType.isDecimal64());
        }

        @Override
        public boolean isDecimal128() {
            return state.isDecimal128() || (state.isUnset() && decimalType.isDecimal128());
        }

        @Override
        public long getDecimal128Low(int position) {
            if (isDecimal128()) {
                return decimal64List[position];
            } else {
                throw new UnsupportedOperationException(
                    "Cannot get decimal128Low from DecimalBlock with state: " + state);
            }
        }

        @Override
        public long getDecimal128High(int position) {
            if (isDecimal128()) {
                return decimal128HighList[position];
            } else {
                throw new UnsupportedOperationException(
                    "Cannot get decimal128High from DecimalBlock with state: " + state);
            }
        }

        @Override
        public int getScale() {
            return scale;
        }

        public boolean isNormal() {
            return state.isNormal();
        }

        public void convertToNormalDecimal() {
            initSliceOutput();

            if (isNormal()) {
                return;
            }

            // unset 或 decimal64/decimal128 状态
            if (decimal64List != null && currentIndex > 0) {
                if (state.isDecimal64()) {
                    state = UNSET_STATE;
                    DecimalStructure tmpBuffer = getDecimalBuffer();
                    DecimalStructure resultBuffer = getDecimalResult();
                    // 可能已经有 DECIMAL64值
                    for (int pos = 0; pos < currentIndex; pos++) {
                        if (!isNull(pos)) {
                            long decimal64 = decimal64List[pos];
                            FastDecimalUtils.setLongWithScale(tmpBuffer, resultBuffer, decimal64, scale);
                            sliceOutput.writeBytes(resultBuffer.getDecimalMemorySegment());
                            updateDecimalInfo(resultBuffer);
                        } else {
                            sliceOutput.skipBytes(DECIMAL_MEMORY_SIZE);
                        }
                    }

                    decimal64List = null;
                } else if (state.isDecimal128()) {
                    Preconditions.checkArgument(decimal64List.length == decimal128HighList.length,
                        "Decimal128 lowBits count does not match highBits count");
                    state = UNSET_STATE;
                    DecimalStructure tmpBuffer = getDecimalBuffer();
                    DecimalStructure resultBuffer = getDecimalResult();
                    for (int pos = 0; pos < currentIndex; pos++) {
                        if (!isNull(pos)) {
                            long lowBits = decimal64List[pos];
                            long highBits = decimal128HighList[pos];
                            FastDecimalUtils.setDecimal128WithScale(tmpBuffer, resultBuffer, lowBits, highBits, scale);
                            sliceOutput.writeBytes(resultBuffer.getDecimalMemorySegment());
                            updateDecimalInfo(resultBuffer);
                        } else {
                            sliceOutput.skipBytes(DECIMAL_MEMORY_SIZE);
                        }
                    }

                    decimal64List = null;
                    decimal128HighList = null;
                }
            }
        }

        private void checkNormalDecimalType() {
            if (state.isDecimal64()) {
                throw new AssertionError("DECIMAL_64 store type is inconsistent when writing a Decimal");
            }
        }

        private void checkDecimal64StoreType() {
            if (state.isUnset()) {
                state = DECIMAL_64;
            } else if (state != DECIMAL_64) {
                throw new AssertionError("Unmatched DECIMAL_64 type: " + state);
            }
        }

        public boolean canWriteDecimal64() {
            return state.isUnset() || state.isDecimal64();
        }

        public boolean isUnset() {
            return state.isUnset();
        }

        public boolean isSimple() {
            return state.isSimple();
        }

        public void setContainsNull(boolean containsNull) {
            this.containsNull = containsNull;
        }

        protected DecimalStructure getDecimalBuffer() {
            if (decimalBuffer == null) {
                this.decimalBuffer = new DecimalStructure();
            }
            return decimalBuffer;
        }

        protected DecimalStructure getDecimalResult() {
            if (decimalResult == null) {
                this.decimalResult = new DecimalStructure();
            }
            return decimalResult;
        }
    }
}
