package com.alibaba.polardbx.executor.operator.util;

import com.alibaba.polardbx.common.utils.memory.SizeOf;

public interface TypedList {
    int INITIAL_SIZE = 1024;

    static TypedList createLong(int fixedSize) {
        return new LongTypedList(fixedSize);
    }

    static TypedList createInt(int fixedSize) {
        return new IntTypedList(fixedSize);
    }

    default void setInt(int index, int value) {
        throw new UnsupportedOperationException();
    }

    default void setLong(int index, long value) {
        throw new UnsupportedOperationException();
    }

    default void setIntArray(int sourceIndex, int[] fromArray, int startIndex, int endIndex) {
        throw new UnsupportedOperationException();
    }

    default void setLongArray(int sourceIndex, long[] fromArray, int startIndex, int endIndex) {
        throw new UnsupportedOperationException();
    }

    default long getLong(int position) {
        throw new UnsupportedOperationException();
    }

    default int getInt(int position) {
        throw new UnsupportedOperationException();
    }

    default void close() {
        throw new UnsupportedOperationException();
    }

    class LongTypedList implements TypedList {
        long[] array;

        public LongTypedList(int fixedSize) {
            this.array = new long[fixedSize];
        }

        @Override
        public void setLong(int index, long value) {
            array[index] = value;
        }

        @Override
        public void setLongArray(int sourceIndex, long[] fromArray, int startIndex, int endIndex) {
            System.arraycopy(fromArray, startIndex, array, sourceIndex, endIndex - startIndex);
        }

        @Override
        public long getLong(int position) {
            return array[position];
        }

        @Override
        public void close() {
            array = null;
        }

        public static long estimatedSizeInBytes(int fixedSize) {
            return SizeOf.sizeOfLongArray(fixedSize);
        }
    }

    class IntTypedList implements TypedList {
        int[] array;

        public IntTypedList(int fixedSize) {
            array = new int[fixedSize];
        }

        @Override
        public void setInt(int index, int value) {
            array[index] = value;
        }

        @Override
        public void setIntArray(int sourceIndex, int[] fromArray, int startIndex, int endIndex) {
            System.arraycopy(fromArray, startIndex, array, sourceIndex, endIndex - startIndex);
        }

        @Override
        public int getInt(int position) {
            return array[position];
        }

        @Override
        public void close() {
            array = null;
        }

        public static long estimatedSizeInBytes(int fixedSize) {
            return SizeOf.sizeOfIntArray(fixedSize);
        }
    }
}
