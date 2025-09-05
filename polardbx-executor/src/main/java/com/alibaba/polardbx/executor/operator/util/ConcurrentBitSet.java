package com.alibaba.polardbx.executor.operator.util;

public interface ConcurrentBitSet {
    void set(int bitIndex);

    void clear(int bitIndex);

    int nextClearBit(int fromIndex);

    boolean get(int bitIndex);
}
