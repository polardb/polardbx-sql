/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.executor.columnar;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class ByteCSVReader {
    private final ByteBuffer lengthBuffer = ByteBuffer.allocate(Integer.BYTES);
    private final InputStream inputStream;

    private final String csvFileName;
    private int preAssignedLength = 1024;
    byte[] data = new byte[preAssignedLength];

    private long currentPosition;
    private final long fileEndOffset;

    private final byte[] reusableNulls;
    private final int[] reusableOffsets;

    public ByteCSVReader(String csvFileName, InputStream inputStream, long length,
                         byte[] reusableNulls, int[] reusableOffsets) {
        this.csvFileName = csvFileName;
        this.inputStream = inputStream;
        this.currentPosition = 0;
        this.fileEndOffset = length;
        this.reusableNulls = reusableNulls;
        this.reusableOffsets = reusableOffsets;
    }

    public boolean isReadable() throws IOException {
        return (currentPosition < fileEndOffset || fileEndOffset < 0) && inputStream.available() > 0;
    }

    public CSVRow nextRow() throws IOException {
        lengthBuffer.clear();
        canReadFileLength(Integer.BYTES);
        inputStream.read(lengthBuffer.array(), 0, Integer.BYTES);

        int length = lengthBuffer.getInt();

        if (preAssignedLength < length) {
            preAssignedLength = (int) (length * 1.5);
            data = new byte[preAssignedLength];
        }

        canReadFileLength(length);
        inputStream.read(data, 0, length);

        currentPosition += (Integer.BYTES + length);

        return CSVRow.deserialize(data, reusableNulls, reusableOffsets, length);
    }

    public void close() throws IOException {
        if (inputStream != null) {
            inputStream.close();
        }
    }

    private void canReadFileLength(int needRead) throws IOException {
        if (fileEndOffset >= 0 && currentPosition + needRead > fileEndOffset) {
            throw new IOException(
                String.format("%s need read %d failed! current Position:%d, end Position:%d", csvFileName,
                    needRead, currentPosition, fileEndOffset));
        }
    }

    public long position() throws IOException {
        return currentPosition;
    }

}
