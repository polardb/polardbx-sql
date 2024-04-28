package com.alibaba.polardbx.executor.operator.util;

/**
 * A object pool in executor level
 *
 * @param <T> type of object.
 */
public interface DriverObjectPool<T> {
    static DriverObjectPool<int[]> createIntArrayPool() {
        return new DriverIntArrayPool();
    }

    static DriverObjectPool<long[]> createLongArrayPool() {
        return new DriverLongArrayPool();
    }

    /**
     * Add object into pool.
     */
    void add(T object);

    /**
     * Poll the object from the pool
     */
    T poll();

    /**
     * Get the recycler to recycle object back into the pool.
     */
    Recycler<T> getRecycler(int chunkLimit);

    /**
     * Clear the pool.
     */
    void clear();

    /**
     * Get the message of this pool.
     */
    String report();

    /**
     * To recycle the object back into pool.
     *
     * @param <T> type of the object.
     */
    interface Recycler<T> {
        void recycle(T object);
    }
}
