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

import org.apache.hadoop.fs.FSDataInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class ByteCSVReader {
    private final ByteBuffer lengthBuffer = ByteBuffer.allocate(Integer.BYTES);
    private final FSDataInputStream inputStream;

    private final String csvFileName;

    private long currentPosition;
    private final long fileEndOffset;

    public ByteCSVReader(String csvFileName, FSDataInputStream inputStream, long length) throws IOException {
        this.csvFileName = csvFileName;
        this.inputStream = inputStream;
        this.currentPosition = 0;
        this.fileEndOffset = length;
    }

    public boolean isReadable() throws IOException {
        return currentPosition < fileEndOffset && inputStream.available() > 0;
    }

    public CSVRow nextRow() throws IOException {
        lengthBuffer.clear();
        canReadFileLength(Integer.BYTES);
        inputStream.readFully(lengthBuffer.array(), 0, Integer.BYTES);

        int length = lengthBuffer.getInt();
        byte[] data = new byte[length];
        canReadFileLength(length);
        inputStream.readFully(data);

        currentPosition += (Integer.BYTES + length);

        return CSVRow.deserialize(data);
    }

    public void close() throws IOException {
        if (inputStream != null) {
            inputStream.close();
        }
    }

    private void canReadFileLength(int needRead) throws IOException {
        if (currentPosition + needRead > fileEndOffset) {
            throw new IOException(
                String.format("%s need read %d failed! current Position:%d, end Position:%d", csvFileName,
                    needRead, currentPosition, fileEndOffset));
        }
    }

    public long position() throws IOException {
        return currentPosition;
    }

}
