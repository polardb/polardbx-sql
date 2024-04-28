package com.alibaba.polardbx.executor.gms;

import org.roaringbitmap.RoaringBitmap;

import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MultiVersionDelData implements Purgeable {
    // The merged bitmap until the latest tso
    private final RoaringBitmap mergedBitMap = new RoaringBitmap();
    // tso - cache
    private final SortedMap<Long, RoaringBitmap> allBitmaps = new ConcurrentSkipListMap<>();
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

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
            mergedBitMap.or(bitmap);
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
}
