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
 * Manage different types of object pool.
 */
public interface ObjectPools {
    static ObjectPools create() {
        return new ObjectPoolsImpl();
    }

    DriverObjectPool<int[]> getIntArrayPool();

    DriverObjectPool<long[]> getLongArrayPool();

    void clear();

    class ObjectPoolsImpl implements ObjectPools {
        // object pool
        private DriverObjectPool<int[]> intArrayPool;
        private DriverObjectPool<long[]> longArrayPool;

        ObjectPoolsImpl() {
            this.intArrayPool = DriverObjectPool.createIntArrayPool();
            this.longArrayPool = DriverObjectPool.createLongArrayPool();
        }

        public DriverObjectPool<int[]> getIntArrayPool() {
            return intArrayPool;
        }

        public DriverObjectPool<long[]> getLongArrayPool() {
            return longArrayPool;
        }

        @Override
        public void clear() {
            intArrayPool.clear();
            longArrayPool.clear();
        }
    }
}
