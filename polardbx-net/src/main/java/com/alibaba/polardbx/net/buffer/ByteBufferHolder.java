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

package com.alibaba.polardbx.net.buffer;

import java.nio.ByteBuffer;

/**
 * Created by chensr on 2017/4/22.
 */

/**
 * @author zhouxy adapter from netty bytebuf
 */
public class ByteBufferHolder {

    public static final ByteBufferHolder EMPTY = new ByteBufferHolder(ByteBuffer.allocateDirect(0));
    private ByteBuffer buffer;

    int readerIndex;
    int writerIndex;

    private int maxCapacity;

    public ByteBufferHolder(ByteBuffer buffer) {
        this.buffer = buffer;
        maxCapacity = buffer.capacity();
    }

    public ByteBuffer getBuffer() {
        return buffer;
    }

    public void setBuffer(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    public final int position() {
        return buffer.position();
    }

    public final ByteBufferHolder position(int newPosition) {
        if (buffer != null) {
            buffer.position(newPosition);
        }
        return this;
    }

    public final boolean hasRemaining() {
        return buffer.hasRemaining();
    }

    public ByteBufferHolder get(byte[] dst, int offset, int length) {
        buffer.get(dst, offset, length);
        this.readerIndex += length;
        return this;
    }

    public final ByteBufferHolder clear() {
        if (buffer != null) {
            buffer.clear();
            writerIndex = 0;
            readerIndex = 0;
        }
        return this;
    }

    public final int remaining() {
        return buffer.remaining();
    }

    public ByteBufferHolder put(byte[] src, int offset, int length) {
        buffer.put(src, offset, length);
        this.writerIndex += length;
        return this;
    }

    public ByteBufferHolder put(ByteBufferHolder src) {
        buffer.put(src.array(), src.readerIndex, src.writerIndex);
        this.writerIndex += src.readable();
        return this;
    }

    public byte get(int index) {
        return buffer.get(index);
    }

    public final int capacity() {
        return buffer.capacity();
    }

    public ByteBufferHolder compact() {
        buffer.compact();
        return this;
    }

    public final ByteBufferHolder flip() {
        buffer.flip();
        return this;
    }

    public final byte[] array() {
        return buffer.array();
    }

    public ByteBufferHolder put(byte b) {
        buffer.put(b);
        writerIndex += 1;
        return this;
    }

    // adapter to netty bytebuf

    public short getUnsignedByte(int index) {
        return (short) (buffer.get(index) & 0xFF);
    }

    public short getUnsignedShort(int index) {
        return (short) (buffer.getShort(index) & 0xFFFF);
    }

    public short getShort(int index) {
        return buffer.getShort(index);
    }

    public ByteBuffer nioBuffer(int index, int length) {
        return ((ByteBuffer) buffer.duplicate().position(index).limit(index + length)).slice();
    }

    public int maxCapacity() {
        return maxCapacity;
    }

    public int readerIndex() {
        return readerIndex;
    }

    public ByteBufferHolder readerIndex(int readerIndex) {
        if (readerIndex < 0 || readerIndex > writerIndex) {
            throw new IndexOutOfBoundsException(
                String.format("readerIndex: %d (expected: 0 <= readerIndex <= writerIndex(%d))",
                    readerIndex,
                    writerIndex));
        }
        this.readerIndex = readerIndex;
        return this;
    }

    public int writerIndex() {
        return writerIndex;
    }

    public ByteBufferHolder writerIndex(int writerIndex) {
        if (writerIndex < readerIndex || writerIndex > capacity()) {
            throw new IndexOutOfBoundsException(
                String.format("writerIndex: %d (expected: readerIndex(%d) <= writerIndex <= capacity(%d))",
                    writerIndex,
                    readerIndex,
                    capacity()));
        }
        this.writerIndex = writerIndex;
        return this;
    }

    public ByteBufferHolder setIndex(int readerIndex, int writerIndex) {
        if (readerIndex < 0 || readerIndex > writerIndex || writerIndex > capacity()) {
            throw new IndexOutOfBoundsException(String
                .format("readerIndex: %d, writerIndex: %d (expected: 0 <= readerIndex <= writerIndex <= capacity(%d))",
                    readerIndex,
                    writerIndex,
                    capacity()));
        }
        this.readerIndex = readerIndex;
        this.writerIndex = writerIndex;
        return this;
    }

    public boolean isReadable() {
        return writerIndex > readerIndex;
    }

    public int readable() {
        return writerIndex - readerIndex;
    }

    public ByteBufferHolder writeBytes(ByteBuffer src) {
        int length = src.remaining();
        ensureWritable(length);
        setBytes(writerIndex, src);
        writerIndex += length;
        return this;
    }

    public ByteBufferHolder setBytes(int index, ByteBuffer src) {
        checkIndex(index, src.remaining());
        ByteBuffer tmpBuf = this.buffer;
        if (src == tmpBuf) {
            src = src.duplicate();
        }

        tmpBuf.clear().position(index).limit(index + src.remaining());
        tmpBuf.put(src);
        return this;
    }

    protected final void checkIndex(int index, int fieldLength) {
        if (fieldLength < 0) {
            throw new IllegalArgumentException("length: " + fieldLength + " (expected: >= 0)");
        }
        if (index < 0 || index > capacity() - fieldLength) {
            throw new IndexOutOfBoundsException(String.format("index: %d, length: %d (expected: range(0, %d))",
                index,
                fieldLength,
                capacity()));
        }
    }

    public ByteBufferHolder ensureWritable(int size) {
        if (buffer.capacity() - writerIndex < size) {
            int newCap = writerIndex + size;
            ByteBuffer tmp = ByteBuffer.allocate(newCap);
            buffer.rewind();
            tmp.put(buffer);
            tmp.position(writerIndex);
            tmp.limit(newCap);
            buffer = tmp;
            maxCapacity = buffer.capacity();
        }
        return this;
    }

    public ByteBufferHolder skipBytes(int length) {
        int newReaderIndex = readerIndex + length;
        if (newReaderIndex > writerIndex) {
            throw new IndexOutOfBoundsException(
                String.format("length: %d (expected: readerIndex(%d) + length <= writerIndex(%d))",
                    length,
                    readerIndex,
                    writerIndex));
        }
        readerIndex = newReaderIndex;
        return this;
    }

}
