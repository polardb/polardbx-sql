package com.alibaba.polardbx.executor.operator.frame;

import com.alibaba.polardbx.executor.chunk.Chunk;

public class ChunkBuffer {
    protected Chunk chunk;

    void reset(Chunk chunk) {
        this.chunk = chunk;
    }

}
