package com.alibaba.polardbx.executor.operator.util;

import com.google.common.base.Preconditions;
import it.unimi.dsi.fastutil.Hash;
import org.openjdk.jol.info.ClassLayout;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicIntegerArray;

/**
 * An concurrent hash table implementation for marking position
 */
public class ConcurrentRawDirectHashTable implements Hash {

    private static final long INSTANCE_SIZE = ClassLayout.parseClass(ConcurrentRawDirectHashTable.class).instanceSize();

    public static final int NOT_EXISTS = -1;

    public static final int EXISTS = 1;

    /**
     * The array of keys (buckets)
     */
    private final AtomicIntegerArray keys;
    /**
     * The current table size.
     */
    private final int n;

    public ConcurrentRawDirectHashTable(int size) {
        Preconditions.checkArgument(size >= 0, "The number of elements must be non-negative");

        this.n = size;

        int[] keys = new int[n];
        Arrays.fill(keys, NOT_EXISTS);
        this.keys = new AtomicIntegerArray(keys);
    }

    /**
     * Mark assigned position
     *
     * @return true if this slot was empty, false otherwise
     */
    public boolean markAndGet(int pos) {
        if (!keys.compareAndSet(pos, NOT_EXISTS, EXISTS)) {
            return false;
        } else {
            return true;
        }
    }

    public void rawMark(int pos) {
        keys.set(pos, EXISTS);
    }

    /**
     * Whether this position has been set yet
     *
     * @return return true if this position has been set
     */
    public boolean hasSet(int pos) {
        return keys.get(pos) == EXISTS;
    }

    public long estimateSize() {
        return INSTANCE_SIZE + keys.length() * Integer.BYTES;
    }

    public int size() {
        return keys.length();
    }

    public List<Integer> getNotMarkedPosition() {
        List<Integer> notMarkedPosition = new ArrayList<>();
        int length = keys.length();
        for (int pos = 0; pos < length; ++pos) {
            if (keys.get(pos) == NOT_EXISTS) {
                notMarkedPosition.add(pos);
            }
        }
        return notMarkedPosition;
    }

    public static long estimatedSizeInBytes(int size) {
        return INSTANCE_SIZE + size * Integer.BYTES;
    }
}
