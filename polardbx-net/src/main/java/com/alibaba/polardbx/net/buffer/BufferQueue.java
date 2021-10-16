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

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author xianmao.hexm
 */
public final class BufferQueue {

    private int takeIndex;
    private int putIndex;
    private int count;
    private final ByteBufferHolder[] items;
    private final ReentrantLock lock;
    private final Condition notFull;
    private ByteBufferHolder attachment;

    public BufferQueue(int capacity) {
        items = new ByteBufferHolder[capacity];
        lock = new ReentrantLock();
        notFull = lock.newCondition();
    }

    public ByteBufferHolder attachment() {
        return attachment;
    }

    public void attach(ByteBufferHolder buffer) {
        this.attachment = buffer;
    }

    public int size() {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            return count;
        } finally {
            lock.unlock();
        }
    }

    public void put(ByteBufferHolder buffer) throws InterruptedException {
        final ByteBufferHolder[] items = this.items;
        final ReentrantLock lock = this.lock;
        lock.lockInterruptibly();
        try {
            try {
                while (count == items.length) {
                    notFull.await();
                }
            } catch (InterruptedException ie) {
                notFull.signal();
                throw ie;
            }
            insert(buffer);
        } finally {
            lock.unlock();
        }
    }

    public ByteBufferHolder poll() {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            if (count == 0) {
                return null;
            }
            return extract();
        } finally {
            lock.unlock();
        }
    }

    private void insert(ByteBufferHolder buffer) {
        items[putIndex] = buffer;
        putIndex = inc(putIndex);
        ++count;
    }

    private ByteBufferHolder extract() {
        final ByteBufferHolder[] items = this.items;
        ByteBufferHolder buffer = items[takeIndex];
        items[takeIndex] = null;
        takeIndex = inc(takeIndex);
        --count;
        notFull.signal();
        return buffer;
    }

    private int inc(int i) {
        return (++i == items.length) ? 0 : i;
    }

}
