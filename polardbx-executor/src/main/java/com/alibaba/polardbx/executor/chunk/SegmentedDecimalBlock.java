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

import com.alibaba.polardbx.common.datatype.DecimalStructure;
import com.alibaba.polardbx.common.memory.FastMemoryCounter;
import io.airlift.slice.Slice;
import org.openjdk.jol.info.ClassLayout;

import static com.alibaba.polardbx.common.datatype.DecimalTypeBase.DECIMAL_MEMORY_SIZE;
import static com.alibaba.polardbx.common.datatype.DecimalTypeBase.FRACTIONS_OFFSET;
import static com.alibaba.polardbx.common.datatype.DecimalTypeBase.INTEGERS_OFFSET;
import static com.alibaba.polardbx.common.datatype.DecimalTypeBase.IS_NEG_OFFSET;
import static com.alibaba.polardbx.common.datatype.DecimalTypeBase.roundUp;

public interface SegmentedDecimalBlock {
    int UNSET = -1;

    Slice segmentUncheckedAt(int position);

    boolean isDecimal64();

    boolean isDecimal128();

    long getLong(int position);

    long getDecimal128Low(int position);

    long getDecimal128High(int position);

    int getScale();

    /**
     * State of decimal block
     */
    enum DecimalBlockState {
        UNALLOC_STATE(false, UNSET, UNSET, UNSET),

        UNSET_STATE(false, UNSET, UNSET, UNSET),

        // 8 bytes
        DECIMAL_64(false, UNSET, UNSET, UNSET),
        // 16 bytes
        DECIMAL_128(false, UNSET, UNSET, UNSET),

        // 40 bytes
        FULL(false, UNSET, UNSET, UNSET),

        // (12 bytes) frac * 10^-9
        SIMPLE_MODE_1(true, UNSET, UNSET, 0),

        // (12 bytes) int1 + frac * 10^-9
        SIMPLE_MODE_2(true, UNSET, 0, 1),

        // (12 bytes) int2 * 10^9 + int1 + frac * 10^-9
        SIMPLE_MODE_3(true, 0, 1, 2);

        public static int INSTANCE_SIZE = ClassLayout.parseClass(DecimalBlockState.class).instanceSize();
        private final boolean isSimple;
        private final int int2Pos;
        private final int int1Pos;
        private final int fracPos;

        DecimalBlockState(boolean isSimple, int int2Pos, int int1Pos, int fracPos) {
            this.isSimple = isSimple;
            this.int2Pos = int2Pos;
            this.int1Pos = int1Pos;
            this.fracPos = fracPos;
        }

        public long memorySize() {
            return INSTANCE_SIZE + FastMemoryCounter.sizeOf(name());
        }

        public static DecimalBlockState stateOf(Slice memorySegments, int position) {
            int isNeg = memorySegments.getByte(position * DECIMAL_MEMORY_SIZE + IS_NEG_OFFSET) & 0xFF;
            if (isNeg == 1) {
                return FULL;
            }

            int integers = memorySegments.getByte(position * DECIMAL_MEMORY_SIZE + INTEGERS_OFFSET) & 0xFF;
            int fractions = memorySegments.getByte(position * DECIMAL_MEMORY_SIZE + FRACTIONS_OFFSET) & 0xFF;

            int intWord = roundUp(integers);
            int fracWord = roundUp(fractions);

            if (intWord == 0 && fracWord == 1) {
                // frac * 10^-9
                return SIMPLE_MODE_1;
            } else if (intWord == 1 && fracWord == 1) {
                // int1 + frac * 10^-9
                return SIMPLE_MODE_2;
            } else if (intWord == 2 && fracWord == 1) {
                // int2 * 10^9 + int1 + frac * 10^-9
                return SIMPLE_MODE_3;
            }

            return FULL;
        }

        public static DecimalBlockState stateOf(DecimalStructure decimalStructure) {
            if (decimalStructure == null || decimalStructure.isNeg()) {
                return FULL;
            }

            int integers = decimalStructure.getIntegers();
            int fractions = decimalStructure.getFractions();

            int intWord = roundUp(integers);
            int fracWord = roundUp(fractions);

            if (intWord == 0 && fracWord == 1) {
                // frac * 10^-9
                return SIMPLE_MODE_1;
            } else if (intWord == 1 && fracWord == 1) {
                // int1 + frac * 10^-9
                return SIMPLE_MODE_2;
            } else if (intWord == 2 && fracWord == 1) {
                // int2 * 10^9 + int1 + frac * 10^-9
                return SIMPLE_MODE_3;
            }

            return FULL;
        }

        public DecimalBlockState merge(DecimalBlockState that) {
            if (this == UNALLOC_STATE || this == UNSET_STATE) {
                return that;
            }

            if (that == UNSET_STATE || that == UNALLOC_STATE) {
                return this;
            }

            if (this == that) {
                return this;
            }
            return FULL;
        }

        public boolean isSimple() {
            return isSimple;
        }

        public boolean isUnset() {
            return this == UNSET_STATE;
        }

        public boolean isFull() {
            return this == FULL;
        }

        public boolean isDecimal64() {
            return this == DECIMAL_64;
        }

        public boolean isDecimal128() {
            return this == DECIMAL_128;
        }

        public boolean isDecimal64Or128() {
            return this == DECIMAL_64 || this == DECIMAL_128;
        }

        public boolean isNormal() {
            return this == FULL || this.isSimple;
        }

        public int getInt2Pos() {
            return int2Pos;
        }

        public int getInt1Pos() {
            return int1Pos;
        }

        public int getFracPos() {
            return fracPos;
        }
    }
}
