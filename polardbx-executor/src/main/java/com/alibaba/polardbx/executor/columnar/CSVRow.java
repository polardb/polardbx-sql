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

package com.alibaba.polardbx.executor.columnar;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.io.IOException;
import java.nio.ByteBuffer;

public class CSVRow {
    /**
     * The continuous memory structure for the whole row.
     */
    private final Slice data;

    /**
     * The offsets of each column in the row.
     */
    private final int[] offsets;

    /**
     * The null value of each column in the row.
     */
    private final byte[] nulls;

    /**
     * The count of fields.
     */
    private final int fieldNum;

    public CSVRow(Slice data, int[] offsets, byte[] nulls, int fieldNum) {
        this.data = data;
        this.offsets = offsets;
        this.nulls = nulls;
        this.fieldNum = fieldNum;
    }

    private int beginOffset(int position) {
        return position > 0 ? offsets[position - 1] : 0;
    }

    private int endOffset(int position) {
        return offsets[position];
    }

    public ByteBuffer getBytes(int pos) {
        int beginOffset = beginOffset(pos);
        int endOffset = endOffset(pos);

        // avoid memory allocate and copy
        return data.toByteBuffer(beginOffset, endOffset - beginOffset);
    }

    public boolean isNullAt(int pos) {
        return nulls[pos] == 1;
    }

    public byte[] serialize() throws IOException {
        int size = Integer.BYTES + Byte.BYTES * fieldNum + Integer.BYTES * fieldNum + data.length();
        Slice sliceOutput = Slices.allocate(size);

        int pos = 0;
        //write filedNum
        sliceOutput.setLong(0, fieldNum);
        pos += Integer.BYTES;

        //write nulls
        sliceOutput.setBytes(pos, nulls);
        pos += Byte.BYTES * fieldNum;

        //write offsets
        for (int offset : offsets) {
            sliceOutput.setInt(pos, offset);
            pos += Integer.BYTES;
        }

        //write data
        sliceOutput.setBytes(pos, data.getBytes());
        return sliceOutput.getBytes();
    }

    public static CSVRow deserialize(byte[] values, byte[] nulls, int[] offsets, int length) throws IOException {
        Slice data = Slices.wrappedBuffer(values);
        int pos = 0;

        int filedNum = data.getInt(pos);
        pos += Integer.BYTES;

        data.getBytes(pos, nulls, 0, filedNum);
        pos += (Byte.BYTES * filedNum);

        for (int i = 0; i < filedNum; i++) {
            offsets[i] = data.getInt(pos);
            pos += Integer.BYTES;
        }

        // avoid copy of bytes
        Slice realData = Slices.wrappedBuffer(values, pos, length - pos);

        return new CSVRow(realData, offsets, nulls, filedNum);
    }
}
