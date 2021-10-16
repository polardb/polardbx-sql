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

package com.alibaba.polardbx.optimizer.core.datatype;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.SQLException;

public class Blob implements java.sql.Blob {

    private byte[] data;

    public Blob(byte[] data) {
        this.data = data;
    }

    @Override
    public synchronized java.io.InputStream getBinaryStream() throws SQLException {
        return new ByteArrayInputStream(this.data);
    }

    @Override
    public synchronized byte[] getBytes(long pos, int length) throws SQLException {
        if (pos < 1) {
            throw new IllegalArgumentException();
        }

        pos = pos - 1;

        if (pos > this.data.length) {
            throw new IllegalArgumentException();
        }

        if (pos + length > this.data.length) {
            throw new IllegalArgumentException();
        }

        byte[] newData = new byte[length];
        System.arraycopy(data, (int) (pos), newData, 0, length);
        return newData;
    }

    @Override
    public synchronized long length() throws SQLException {
        return data.length;
    }

    @Override
    public synchronized long position(byte[] pattern, long start) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public synchronized long position(java.sql.Blob pattern, long start) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public synchronized OutputStream setBinaryStream(long indexToWriteAt) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public synchronized int setBytes(long writeAt, byte[] bytes) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public synchronized int setBytes(long writeAt, byte[] bytes, int offset, int length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public synchronized void truncate(long len) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public synchronized void free() throws SQLException {
        this.data = null;
    }

    @Override
    public synchronized InputStream getBinaryStream(long pos, long length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Slice getSlice() {
        return Slices.wrappedBuffer(this.data);
    }
}