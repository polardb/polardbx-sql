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

import java.text.MessageFormat;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class DriverLongArrayPool implements DriverObjectPool<long[]> {
    private LinkedBlockingQueue<long[]> queue;
    private AtomicBoolean isCleared;
    private long recycleTimes;
    private long reuseTimes;

    public DriverLongArrayPool() {
        this.queue = new LinkedBlockingQueue<>();
        this.isCleared = new AtomicBoolean(false);
        this.recycleTimes = 0L;
        this.reuseTimes = 0L;
    }

    @Override
    public void add(long[] object) {
        queue.add(object);
        recycleTimes++;
    }

    @Override
    public long[] poll() {
        long[] res = queue.poll();
        if (res != null) {
            reuseTimes++;
        }
        return res;
    }

    @Override
    public Recycler<long[]> getRecycler(int chunkLimit) {
        return (long[] object) -> {
            if (!isCleared.get() && object != null && object.length >= chunkLimit) {
                add(object);
            }
        };
    }

    @Override
    public void clear() {
        if (isCleared.compareAndSet(false, true)) {
            queue.clear();
        }
    }

    @Override
    public String report() {
        return MessageFormat.format("long array object pool, recycleTimes = {0}, reuseTimes = {1}", recycleTimes,
            reuseTimes);
    }
}
