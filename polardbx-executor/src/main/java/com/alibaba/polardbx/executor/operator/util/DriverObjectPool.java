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
