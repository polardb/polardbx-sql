package com.alibaba.polardbx.executor.operator.util;

public class ConcurrentBitSetImpl implements ConcurrentBitSet {
    private final AtomicIntegerArray bitArray;
    private final int size;

    public ConcurrentBitSetImpl(int size) {
        this.size = size;
        // Create an AtomicIntegerArray of the required size, with each int storing information for 32 bits.
        this.bitArray = new AtomicIntegerArray((size + 31) / 32);
    }

    @Override
    public void set(int bitIndex) {
        if (bitIndex < 0 || bitIndex >= size) {
            throw new IndexOutOfBoundsException("Index out of bounds: " + bitIndex);
        }
        int arrayIndex = bitIndex / 32;
        int bitPosition = bitIndex % 32;
        bitArray.getAndUpdate(arrayIndex, v -> v | (1 << bitPosition));
    }

    @Override
    public void clear(int bitIndex) {
        if (bitIndex < 0 || bitIndex >= size) {
            throw new IndexOutOfBoundsException("Index out of bounds: " + bitIndex);
        }
        int arrayIndex = bitIndex / 32;
        int bitPosition = bitIndex % 32;
        bitArray.getAndUpdate(arrayIndex, v -> v & ~(1 << bitPosition));
    }

    @Override
    public int nextClearBit(int fromIndex) {
        if (fromIndex < 0 || fromIndex >= size) {
            throw new IndexOutOfBoundsException("Index out of bounds: " + fromIndex);
        }

        int arrayIndex = fromIndex / 32;
        int bitPosition = fromIndex % 32;

        // Loop through the current array elements.
        while (arrayIndex < bitArray.length()) {
            int currentBits = bitArray.get(arrayIndex);

            // Find the first cleared bit within the current element.
            for (int i = bitPosition; i < 32; i++) {
                if ((currentBits & (1 << i)) == 0) {
                    return arrayIndex * 32 + i;
                }
            }

            // Check the next array element.
            bitPosition = 0;
            arrayIndex++;
        }

        // If not found, return out of range.
        return size;
    }

    @Override
    public boolean get(int bitIndex) {
        if (bitIndex < 0 || bitIndex >= size) {
            throw new IndexOutOfBoundsException("Index out of bounds: " + bitIndex);
        }
        int arrayIndex = bitIndex / 32;
        int bitPosition = bitIndex % 32;
        return (bitArray.get(arrayIndex) & (1 << bitPosition)) != 0;
    }
}
