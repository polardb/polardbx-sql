package org.apache.orc.customized;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

import java.nio.ByteBuffer;

public class DefaultORCMemoryAllocator implements ORCMemoryAllocator {
    private BufferAllocator allocator = new RootAllocator();

    @Override
    public ByteBuffer allocateOnHeap(int bufferSize) {
        return allocateOnHeap(bufferSize, null);
    }

    @Override
    public ByteBuffer allocateOnHeap(int bufferSize, ORCProfile profile) {
        ByteBuffer result = ByteBuffer.allocate(bufferSize);

        if (profile != null) {
            profile.update(bufferSize);
        }

        return result;
    }

    @Override
    public ByteBuffer allocateOffHeap(int bufferSize) {
        return ByteBuffer.allocateDirect(bufferSize);
    }

    @Override
    public Recyclable<ByteBuffer> pooledDirect(int bufferSize) {
        ArrowBuf arrowBuf = allocator.buffer(bufferSize);
        return new Recyclable<ByteBuffer>() {
            @Override
            public ByteBuffer get() {
                return arrowBuf.nioBuffer();
            }

            @Override
            public void recycle() {
                arrowBuf.close();
            }
        };
    }
}
