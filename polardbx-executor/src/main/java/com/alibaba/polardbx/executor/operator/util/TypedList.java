/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
