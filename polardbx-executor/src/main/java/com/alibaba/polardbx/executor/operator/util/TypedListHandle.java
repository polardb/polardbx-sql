package com.alibaba.polardbx.executor.operator.util;

import com.alibaba.polardbx.executor.chunk.Chunk;

public interface TypedListHandle {
    long SERIALIZED_MASK = ((long) 0x7fffffff) << 1 | 1;

    long estimatedSize(int fixedSize);

    TypedList[] getTypedLists(int fixedSize);

    void consume(Chunk chunk, int sourceIndex);

    static long serialize(int a, int b) {
        return ((long) a << 32) | (b & SERIALIZED_MASK);
    }
}
