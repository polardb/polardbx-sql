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

package com.alibaba.polardbx.executor.gms;

import org.roaringbitmap.RoaringBitmap;

import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MultiVersionDelData implements Purgeable {
    // The merged bitmap until the latest tso
    private final RoaringBitmap mergedBitMap = new RoaringBitmap();
    // tso - cache
    private final SortedMap<Long, RoaringBitmap> allBitmaps = new ConcurrentSkipListMap<>();
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private final AtomicLong memoryUsed;

    public MultiVersionDelData(AtomicLong memoryUsed) {
        this.memoryUsed = memoryUsed;
        this.memoryUsed.addAndGet(mergedBitMap.getLongSizeInBytes());
    }

    public void putNewTsoBitMap(long tso, RoaringBitmap bitmap) {
        // this tso already exists, skip
        if (!allBitmaps.isEmpty() && tso <= allBitmaps.lastKey()) {
            return;
        }

        Lock writeLock = lock.writeLock();
        writeLock.lock();
        try {
            // double check
            if (!allBitmaps.isEmpty() && tso <= allBitmaps.lastKey()) {
                return;
            }

            allBitmaps.put(tso, bitmap);
            long prevMemoryUsed = mergedBitMap.getLongSizeInBytes();
            mergedBitMap.or(bitmap);
            memoryUsed.addAndGet(mergedBitMap.getLongSizeInBytes() - prevMemoryUsed);
        } finally {
            writeLock.unlock();
        }
    }

    // TODO(siyun): 优化 RoaringBitmap 对象的创建和销毁，这里为了保证并发安全，即使读的是最新的 bitmap，也进行了内存的拷贝
    public RoaringBitmap buildDeleteBitMap(long tso) {
        if (tso == Long.MIN_VALUE) {
            return new RoaringBitmap();
        }

        Lock readLock = lock.readLock();
        readLock.lock();
        try {
            RoaringBitmap bitmap = mergedBitMap.clone();

            for (RoaringBitmap deltaBitmap : allBitmaps.tailMap(tso + 1).values()) {
                // This takes the same effects as xor in this case
                bitmap.andNot(deltaBitmap);
            }

            return bitmap;
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Merge all old bitmaps whose tso < given tso into the first bitmap whose tso >= given tso,
     * and then remove all old bitmaps.
     *
     * @param tso purged tso value.
     */
    public void purge(long tso) {
        Lock writeLock = lock.writeLock();
        writeLock.lock();
        try {
            allBitmaps.headMap(tso + 1).clear();
        } finally {
            writeLock.unlock();
        }
    }

    public long getCurrentMemoryUsed() {
        return mergedBitMap.getLongSizeInBytes();
    }
}
