package com.alibaba.polardbx.executor.operator.scan.impl;

import com.google.common.base.Preconditions;
import io.airlift.slice.SliceOutput;
import org.apache.orc.customized.ORCDataOutput;

import java.nio.ByteBuffer;

/**
 * A ORCDataOutput wrapped with SliceOutput object.
 * It will automatically maintain the current offset of bytes.
 */
public class SliceOutputWrapper implements ORCDataOutput {
    private final SliceOutput sliceOutput;

    public SliceOutputWrapper(SliceOutput sliceOutput) {
        this.sliceOutput = sliceOutput;
    }

    public void read(ByteBuffer buffer, int bytesToRead) {
        Preconditions.checkArgument(bytesToRead <= buffer.remaining());

        // must be an instance of HeapByteBuffer
        Preconditions.checkArgument(buffer.array() != null);

        // NOTE:
        // HeapByteBuffer.get(byte[] dst, int offset, int length)
        // ix(position()) = position + offset
        sliceOutput.write(buffer.array(), buffer.arrayOffset() + buffer.position(), bytesToRead);
        buffer.position(buffer.position() + bytesToRead);
    }
}
