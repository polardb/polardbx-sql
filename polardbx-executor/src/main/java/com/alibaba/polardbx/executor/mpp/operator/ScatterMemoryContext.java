package com.alibaba.polardbx.executor.mpp.operator;

import com.alibaba.polardbx.executor.chunk.Block;

public class ScatterMemoryContext {

    private int batchSize;
    private int srcStartPosition;

    private int[] partitionsBuffer;

    private int[] destScatterMapBuffer;

    private int[] destFixedWidthScatterMapBuffer;

    private boolean[] scatterBufferNullsBuffer;

    private boolean[] bufferNullsBuffer;

    private int[] intScatterBufferDataBuffer;

    private long[] longScatterBufferDataBuffer;

    private long[] longBufferDataBuffer;

    private short[] shortScatterBufferDataBuffer;

    private short[] shortBufferDataBuffer;

    private int[] destLengthsPerPartitionInBatch;

    private int[] intBufferDataBuffer;

    public void init(Block[] blocks) {
        int count = blocks[0].getPositionCount();
        partitionsBuffer = new int[count];
        destScatterMapBuffer = new int[count];
        destFixedWidthScatterMapBuffer = new int[count];
        scatterBufferNullsBuffer = new boolean[count];
        bufferNullsBuffer = new boolean[count];
        intScatterBufferDataBuffer = new int[count];
        longScatterBufferDataBuffer = new long[count];
        longBufferDataBuffer = new long[count];
        shortScatterBufferDataBuffer = new short[count];
        shortBufferDataBuffer = new short[count];
        intBufferDataBuffer = new int[count];
    }

    public int[] getPartitionsBuffer() {
        return partitionsBuffer;
    }

    public int[] getDestScatterMapBuffer() {
        return destScatterMapBuffer;
    }

    public int[] getDestFixedWidthScatterMapBuffer() {
        return destFixedWidthScatterMapBuffer;
    }

    public boolean[] getScatterBufferNullsBuffer() {
        return scatterBufferNullsBuffer;
    }

    public boolean[] getBufferNullsBuffer() {
        return bufferNullsBuffer;
    }

    public int[] getIntScatterBufferDataBuffer() {
        return intScatterBufferDataBuffer;
    }

    public int[] getIntBufferDataBuffer() {
        return intBufferDataBuffer;
    }

    public long[] getLongScatterBufferDataBuffer() {
        return longScatterBufferDataBuffer;
    }

    public long[] getLongBufferDataBuffer() {
        return longBufferDataBuffer;
    }

    public short[] getShortScatterBufferDataBuffer() {
        return shortScatterBufferDataBuffer;
    }

    public short[] getShortBufferDataBuffer() {
        return shortBufferDataBuffer;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public int getSrcStartPosition() {
        return srcStartPosition;
    }

    public void setSrcStartPosition(int srcStartPosition) {
        this.srcStartPosition = srcStartPosition;
    }

    public int[] getDestLengthsPerPartitionInBatch() {
        return destLengthsPerPartitionInBatch;
    }

    public void setDestLengthsPerPartitionInBatch(int[] destLengthsPerPartitionInBatch) {
        this.destLengthsPerPartitionInBatch = destLengthsPerPartitionInBatch;
    }
}
