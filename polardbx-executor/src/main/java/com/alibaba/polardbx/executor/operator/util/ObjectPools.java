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
