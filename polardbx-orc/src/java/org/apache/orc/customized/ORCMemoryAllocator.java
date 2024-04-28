package org.apache.orc.customized;

import java.nio.ByteBuffer;

/**
 * The Memory allocator to unify the management of memory during orc reading.
 * <p>
 * There are three types of memory in ORC SDK:
 * 1. The byte[] or ByteBuffer object allocated within the method stack.
 * 2. The memory for IO buffer e.g. results of FileInputStream, buffer of PostScript / BloomFilter.
 * 3. The memory for uncompressed bytes without decoding.
 * <p>
 * Only in case 3 we should use pooled ArrowBuf to manage the memory.
 */
public interface ORCMemoryAllocator {
    boolean USE_ARROW = false;

    ORCMemoryAllocator ALLOCATOR = new DefaultORCMemoryAllocator();

    static ORCMemoryAllocator getInstance() {
        return ALLOCATOR;
    }

    static boolean useArrow() {
        return USE_ARROW;
    }

    /**
     * Allocate un-pooled on-heap memory
     *
     * @param bufferSize memory size
     * @return ByteBuffer
     */
    ByteBuffer allocateOnHeap(int bufferSize);

    ByteBuffer allocateOnHeap(int bufferSize, ORCProfile profile);

    /**
     * Allocate un-pooled off-heap memory
     *
     * @param bufferSize memory size
     * @return ByteBuffer
     */
    ByteBuffer allocateOffHeap(int bufferSize);

    /**
     * Get ByteBuffer whose memory segment is held by ArrowBuf Object.
     *
     * @param bufferSize memory size.
     * @return A recyclable memory buffer.
     */
    Recyclable<ByteBuffer> pooledDirect(int bufferSize);
}
