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

import com.alibaba.polardbx.common.charset.CharsetName;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.SliceType;
import com.google.common.base.Preconditions;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import static com.alibaba.polardbx.common.charset.MySQLUnicodeUtils.LATIN1_TO_UTF8_BYTES;

public class SliceBlockBuilder extends AbstractBlockBuilder {
    private static final int EXPECTED_STRING_SIZE_IN_BYTES = 32;
    SliceOutput sliceOutput;
    final IntArrayList offsets; // records where the bytes end at
    final SliceType dataType;

    SliceBlockBuilder(DataType dataType, int initialCapacity) {
        super(initialCapacity);
        Preconditions.checkArgument(dataType instanceof SliceType);
        this.dataType = (SliceType) dataType;
        this.offsets = new IntArrayList(initialCapacity);
        this.sliceOutput = new DynamicSliceOutput(EXPECTED_STRING_SIZE_IN_BYTES * initialCapacity);
    }

    @Override
    public void ensureCapacity(int capacity) {
        super.ensureCapacity(capacity);
        offsets.ensureCapacity(capacity);
        sliceOutput.ensureCapacity(capacity * EXPECTED_STRING_SIZE_IN_BYTES);
    }

    @Override
    public Block build() {
        // prevent from memory leak
        Slice slice = sliceOutput.slice();
        Slice data = Slices.copyOf(slice);

        return new SliceBlock(dataType,
            0,
            getPositionCount(),
            mayHaveNull() ? valueIsNull.elements() : null,
            offsets.elements(),
            data);
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
    }

    @Override
    public BlockBuilder newBlockBuilder() {
        return new SliceBlockBuilder(dataType, getCapacity());
    }

    @Override
    public Object getObject(int position) {
        return isNull(position) ? null : copySlice(position);
    }

    private Slice copySlice(int position) {
        checkReadablePosition(position);

        int beginOffset = beginOffset(position);
        int endOffset = endOffset(position);
        Slice slice = sliceOutput.slice();
        return Slices.copyOf(slice, beginOffset, endOffset - beginOffset);
    }

    int beginOffset(int position) {
        return position > 0 ? offsets.getInt(position - 1) : 0;
    }

    int endOffset(int position) {
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
