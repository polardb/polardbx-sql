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

package com.alibaba.polardbx.executor.chunk;

import it.unimi.dsi.fastutil.booleans.BooleanArrayList;
import it.unimi.dsi.fastutil.booleans.BooleanArrays;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongArrays;

public interface BatchedArrayList<T> {

    void add(T array, int[] selection, int offsetInSelection, int positionCount);

    class BatchLongArrayList extends LongArrayList implements BatchedArrayList<long[]> {
        public BatchLongArrayList(int capacity) {
            super(capacity);
        }

        @Override
        public void add(long[] array, int[] selection, int offsetInSelection, int positionCount) {
            // grow to prevent that array index out of bound.
            this.a = LongArrays.ensureCapacity(this.a, this.size + positionCount, this.size);
            for (int i = 0; i < positionCount; i++) {
                int j = selection[i + offsetInSelection];
                this.a[this.size++] = array[j];
            }
        }
    }

    class BatchIntArrayList extends IntArrayList implements BatchedArrayList<int[]> {
        public BatchIntArrayList(int capacity) {
            super(capacity);
        }

        @Override
        public void add(int[] array, int[] selection, int offsetInSelection, int positionCount) {
            // grow to prevent that array index out of bound.
            this.a = IntArrays.ensureCapacity(this.a, this.size + positionCount, this.size);
            for (int i = 0; i < positionCount; i++) {
                int j = selection[i + offsetInSelection];
                this.a[this.size++] = array[j];
            }
        }
    }

    class BatchBooleanArrayList extends BooleanArrayList implements BatchedArrayList<boolean[]> {
        public BatchBooleanArrayList(int capacity) {
            super(capacity);
        }

        @Override
        public void add(boolean[] array, int[] selection, int offsetInSelection, int positionCount) {
            this.a = BooleanArrays.ensureCapacity(this.a, this.size + positionCount, this.size);
            for (int i = 0; i < positionCount; i++) {
                int j = selection[i + offsetInSelection];
                this.a[this.size++] = array[j];
            }
        }

        public void add(boolean booleanValue, int positionCount) {
            this.a = BooleanArrays.ensureCapacity(this.a, this.size + positionCount, this.size);
            for (int i = 0; i < positionCount; i++) {
                this.a[this.size++] = booleanValue;
            }
        }
    }
}
