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
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author xianmao.hexm
 */
public final class BufferPool {

    private final int chunkSize;
    private final ByteBuffer[] items;
    private final ReentrantLock lock;
    private int putIndex;
    private int takeIndex;
    private int count;
    private volatile int newCount;

    public BufferPool(int bufferSize, int chunkSize) {
        this.chunkSize = chunkSize;
        int capacity = bufferSize / chunkSize;
        capacity = (bufferSize % chunkSize == 0) ? capacity : capacity + 1;
        this.items = new ByteBuffer[capacity];
        this.lock = new ReentrantLock();
        for (int i = 0; i < capacity; i++) {
            insert(create(chunkSize));
        }
    }

    public int capacity() {
        return items.length;
    }

    public int size() {
        return count;
    }

    public int getNewCount() {
        return newCount;
    }

    public ByteBufferHolder allocate() {
        ByteBuffer node = null;
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            node = (count == 0) ? null : extract();
        } finally {
            lock.unlock();
        }
        if (node == null) {
            ++newCount;
            node = create(chunkSize);
        }
        return new ByteBufferHolder(node);
    }

    public void recycle(ByteBufferHolder bufferHolder) {
        // 拒绝回收null和容量大于chunkSize的缓存
        if (bufferHolder == null || bufferHolder.getBuffer() == null
            || bufferHolder.getBuffer().capacity() > chunkSize) {
            return;
        }
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            ByteBuffer buffer = bufferHolder.getBuffer();
            if (buffer != null) {
                if (count != items.length) {
                    buffer.clear();
                    insert(buffer);
                }
                bufferHolder.setBuffer(null);
            }
        } finally {
            lock.unlock();
        }
    }

    private void insert(ByteBuffer buffer) {
        items[putIndex] = buffer;
        putIndex = inc(putIndex);
        ++count;
    }

    private ByteBuffer extract() {
        final ByteBuffer[] items = this.items;
        ByteBuffer item = items[takeIndex];
        items[takeIndex] = null;
        takeIndex = inc(takeIndex);
        --count;
        return item;
    }

    private int inc(int i) {
        return (++i == items.length) ? 0 : i;
    }

    private ByteBuffer create(int size) {
        return ByteBuffer.allocate(size);
    }

}
