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

import com.alibaba.polardbx.common.charset.CharsetName;
import com.alibaba.polardbx.common.memory.FastMemoryCounter;
import com.alibaba.polardbx.common.memory.FieldMemoryCounter;
import com.alibaba.polardbx.executor.operator.scan.BlockDictionary;
import com.alibaba.polardbx.executor.operator.scan.impl.DictionaryMapping;
import com.alibaba.polardbx.executor.operator.scan.impl.DictionaryMappingImpl;
import com.alibaba.polardbx.executor.operator.scan.impl.LocalBlockDictionary;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.SliceType;
import com.google.common.base.Preconditions;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.openjdk.jol.info.ClassLayout;

import java.util.List;

import static com.alibaba.polardbx.common.charset.MySQLUnicodeUtils.LATIN1_TO_UTF8_BYTES;

public class SliceBlockBuilder extends AbstractBlockBuilder {
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(SliceBlockBuilder.class).instanceSize();

    private static final int EXPECTED_STRING_SIZE_IN_BYTES = 64;
    final MemoryCountableIntArrayList offsets; // records where the bytes end at

    @FieldMemoryCounter(value = false)
    final SliceType dataType;
    final boolean compatible;
    final MemoryCountableIntArrayList values;
    SliceOutput sliceOutput;

    @FieldMemoryCounter(value = false)
    ExecutionContext context;

    // for dictionary
    @FieldMemoryCounter(value = false)
    BlockDictionary blockDictionary;

    @FieldMemoryCounter(value = false)
    DictionaryMapping mapping = null; // todo

    public SliceBlockBuilder(DataType dataType, int initialCapacity, ExecutionContext context, boolean compatible) {
        super(initialCapacity);
        Preconditions.checkArgument(dataType instanceof SliceType);
        this.dataType = (SliceType) dataType;
        this.offsets = new MemoryCountableIntArrayList(initialCapacity);
        this.context = context;
        this.sliceOutput = new DynamicSliceOutput(EXPECTED_STRING_SIZE_IN_BYTES * initialCapacity);
        this.compatible = compatible;
        this.values = new MemoryCountableIntArrayList(4);
    }

    @Override
    public long getMemoryUsage() {
        return INSTANCE_SIZE
            + FastMemoryCounter.sizeOf(offsets)
            + FastMemoryCounter.sizeOf(values)
            + FastMemoryCounter.sizeOf(sliceOutput)
            + FastMemoryCounter.sizeOf(valueIsNull);
    }

    @Override
    public void ensureCapacity(int capacity) {
        super.ensureCapacity(capacity);
        offsets.ensureCapacity(capacity);
        sliceOutput.ensureCapacity(capacity * EXPECTED_STRING_SIZE_IN_BYTES);
    }

    public void setDictionary(BlockDictionary dictionary) {
        if (dictionary != null && this.blockDictionary != null) {
            boolean notSame = dictionary.size() != this.blockDictionary.size();
            notSame &= dictionary.hashCode() != this.blockDictionary.hashCode();
            if (notSame) {
                throw new IllegalArgumentException("Setting a new different dictionary in SliceBlockBuilder");
            }
        }
        if (this.blockDictionary == null && !valueIsNull.isEmpty()) {
            // lazy append nulls in dictIds
            for (int i = 0; i < valueIsNull.size(); i++) {
                if (valueIsNull.getBoolean(i)) {
                    values.add(-1);
                } else {
                    throw new UnsupportedOperationException(
                        "Do not support setting a new dictionary to builder with values");
                }
            }
        }
        this.blockDictionary = dictionary;
    }

    public boolean isEmpty() {
        return valueIsNull.isEmpty();
    }

    @Override
    public Block build() {
        if (blockDictionary == null) {
            Slice data = sliceOutput.slice();

            return new SliceBlock(dataType,
                0,
                getPositionCount(),
                mayHaveNull() ? valueIsNull.elements() : null,
                offsets.elements(),
                data, compatible);
        } else {
            if (mapping == null) {
                return new SliceBlock(dataType, 0, getPositionCount(),
                    mayHaveNull() ? valueIsNull.elements() : null,
                    blockDictionary, values.elements(), compatible
                );
            } else {
                List<Slice> mergedDict = ((DictionaryMappingImpl) mapping).getMergedDict();
                BlockDictionary blockDictionary1 = new LocalBlockDictionary(mergedDict.toArray(new Slice[0]));

                return new SliceBlock(dataType, 0, getPositionCount(),
                    mayHaveNull() ? valueIsNull.elements() : null,
                    blockDictionary1, values.elements(), compatible
                );
            }

        }
    }

    public SliceOutput getSliceOutput() {
        return sliceOutput;
    }

    public IntArrayList getOffsets() {
        return offsets;
    }

    @Override
    public void writeByteArray(byte[] value) {
        writeBytes(value);
    }

    @Override
    public void writeByteArray(byte[] value, int offset, int length) {
        sliceOutput.writeBytes(value, offset, length);
        valueIsNull.add(false);
        offsets.add(sliceOutput.size());
    }

    @Override
    public void writeString(String s) {
        writeBytes(s.getBytes(CharsetName.DEFAULT_STORAGE_CHARSET_IN_CHUNK));
    }

    public void writeBytes(byte[] bytes) {
        sliceOutput.writeBytes(bytes);
        valueIsNull.add(false);
        offsets.add(sliceOutput.size());
    }

    public void writeBytes(byte[] bytes, int offset, int len) {
        sliceOutput.writeBytes(bytes, offset, len);
        valueIsNull.add(false);
        offsets.add(sliceOutput.size());
    }

    public void writeBytesInLatin1(byte[] bytes) {
        writeBytesInLatin1(bytes, 0, bytes.length);
    }

    // fast converter from latin1 to utf8
    public void writeBytesInLatin1(byte[] bytes, int offset, int length) {
        final int limit = Math.min(offset + length, bytes.length);
        for (int i = offset; i < limit; i++) {
            sliceOutput.writeBytes(LATIN1_TO_UTF8_BYTES[((int) bytes[i]) & 0xFF]);
        }
        valueIsNull.add(false);
        offsets.add(sliceOutput.size());
    }

    public void writeSlice(Slice slice) {
        sliceOutput.writeBytes(slice);
        valueIsNull.add(false);
        offsets.add(sliceOutput.size());
    }

    @Override
    public void writeObject(Object value) {
        if (value == null) {
            appendNull();
            return;
        }
        Preconditions.checkArgument(value instanceof Slice);
        writeSlice((Slice) value);
    }

    @Override
    public void appendNull() {
        appendNullInternal();
        offsets.add(sliceOutput.size());
        if (blockDictionary != null) {
            values.add(-1);
        }
    }

    @Override
    public BlockBuilder newBlockBuilder() {
        return new SliceBlockBuilder(dataType, getCapacity(), context, compatible);
    }

    @Override
    public Object getObject(int position) {
        return isNull(position) ? null : copySlice(position);
    }

    public int[] mergeDictionary(BlockDictionary newDict) {
        if (this.mapping == null) {
            this.mapping = new DictionaryMappingImpl();
            this.mapping.merge(blockDictionary);
        }
        return this.mapping.merge(newDict);
    }

    public int[] mergeValue(Slice newValue) {
        if (this.mapping == null) {
            this.mapping = new DictionaryMappingImpl();
            this.mapping.merge(blockDictionary);
        }
        BlockDictionary tmpDict = new LocalBlockDictionary(new Slice[] {newValue});
        return this.mapping.merge(tmpDict);
    }

    /**
     * bad performance
     */
    public int[] mergeValues(Slice[] newValues) {
        if (this.mapping == null) {
            this.mapping = new DictionaryMappingImpl();
            this.mapping.merge(blockDictionary);
        }
        BlockDictionary tmpDict = new LocalBlockDictionary(newValues);
        return this.mapping.merge(tmpDict);
    }

    private Slice copySlice(int position) {
        checkReadablePosition(position);

        int beginOffset = beginOffset(position);
        int endOffset = endOffset(position);
        Slice slice = sliceOutput.slice();
        return Slices.copyOf(slice, beginOffset, endOffset - beginOffset);
    }

    public int beginOffset(int position) {
        return position > 0 ? offsets.getInt(position - 1) : 0;
    }

    public int endOffset(int position) {
        return offsets.getInt(position);
    }

    public Slice getRegion(int position) {
        checkReadablePosition(position);

        int beginOffset = beginOffset(position);
        int endOffset = endOffset(position);
        Slice slice = sliceOutput.slice();
        return slice.slice(beginOffset, endOffset - beginOffset);
    }
}
