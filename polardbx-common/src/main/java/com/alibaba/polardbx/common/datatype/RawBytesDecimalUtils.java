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

package com.alibaba.polardbx.common.datatype;

import com.google.common.base.Preconditions;
import io.airlift.slice.Slice;

import static com.alibaba.polardbx.common.datatype.DecimalTypeBase.*;


public class RawBytesDecimalUtils {

    public static int hashCode(Slice memorySegment) {

        Preconditions.checkPositionIndexes(FRACTIONS_OFFSET, FRACTIONS_OFFSET + 1, memorySegment.length());

        int integers = ((int) memorySegment.getByteUnchecked(INTEGERS_OFFSET)) & 0xFF;
        int fractions = ((int) memorySegment.getByteUnchecked(FRACTIONS_OFFSET)) & 0xFF;

        int fromIndex = 0;
        int endIndex = roundUp(integers) + roundUp(fractions);

        if (memorySegment.getIntUnchecked(BUFF_OFFSETS[fromIndex]) == 0) {
            int i = ((integers - 1) % DIG_PER_DEC1) + 1;
            while (integers > 0 && memorySegment.getIntUnchecked(BUFF_OFFSETS[fromIndex]) == 0) {
                integers -= i;
                i = DIG_PER_DEC1;
                fromIndex++;
            }
        }

        if (endIndex > 0 && memorySegment.getIntUnchecked(BUFF_OFFSETS[endIndex - 1]) == 0) {
            int i = ((fractions - 1) % DIG_PER_DEC1) + 1;
            while (fractions > 0 && memorySegment.getIntUnchecked(BUFF_OFFSETS[endIndex - 1]) == 0) {
                fractions -= i;
                i = DIG_PER_DEC1;
                endIndex--;
            }
        }

        int result = 1;
        for (int pos = fromIndex; pos != endIndex; pos++) {
            int element = memorySegment.getIntUnchecked(BUFF_OFFSETS[pos]);
            result = 31 * result + element;
        }

        return memorySegment.getByte(IS_NEG_OFFSET) == NEGATIVE_FLAG
            ? -result : result;
    }

    public static int hashCode(long decimal64, int scale) {
        if (decimal64 >= DIG_MASK || decimal64 < 0 || scale >= 9) {
            // go to normal method
            return 0;
        }

        int fractionVal = (int) ((decimal64 % POW_10[scale]) * POW_10[9 - scale]);
        int integerVal = (int) (decimal64 / POW_10[scale]);
        int result = 1;
        if (integerVal != 0) {
            result = 31 * result + integerVal;
        }
        if (fractionVal != 0) {
            result = 31 * result + fractionVal;
        }
        return result;
    }

    /**
     * compare two decimal values in format of raw bytes.
     */
    public static boolean equals(Slice left, Slice right) {

        Preconditions.checkPositionIndexes(FRACTIONS_OFFSET, FRACTIONS_OFFSET + 1, left.length());
        Preconditions.checkPositionIndexes(FRACTIONS_OFFSET, FRACTIONS_OFFSET + 1, right.length());

        if (left.getByte(IS_NEG_OFFSET) != right.getByte(IS_NEG_OFFSET)) {
            return false;
        }

        int leftIntWords = roundUp(((int) left.getByteUnchecked(INTEGERS_OFFSET) & 0xFF));
        int leftFracWords = roundUp(((int) left.getByteUnchecked(FRACTIONS_OFFSET) & 0xFF));
        int rightIntWords = roundUp(((int) right.getByteUnchecked(INTEGERS_OFFSET) & 0xFF));
        int rightFracWords = roundUp(((int) right.getByteUnchecked(FRACTIONS_OFFSET) & 0xFF));

        int endPos1 = leftIntWords, endPos2 = rightIntWords;
        int bufPos1 = 0, bufPos2 = 0;

        if (left.getIntUnchecked(BUFF_OFFSETS[bufPos1]) == 0) {
            for (; bufPos1 < endPos1 && left.getIntUnchecked(BUFF_OFFSETS[bufPos1]) == 0; ) {
                bufPos1++;
            }
            leftIntWords = endPos1 - bufPos1;
        }

        if (right.getIntUnchecked(BUFF_OFFSETS[bufPos2]) == 0) {
            for (; bufPos2 < endPos2 && right.getIntUnchecked(BUFF_OFFSETS[bufPos2]) == 0; ) {
                bufPos2++;
            }
            rightIntWords = endPos2 - bufPos2;
        }

        if (rightIntWords == leftIntWords) {

            int end1 = endPos1 + leftFracWords - 1;
            while (bufPos1 <= end1 && left.getIntUnchecked(BUFF_OFFSETS[end1]) == 0) {
                end1--;
            }

            int end2 = endPos2 + rightFracWords - 1;
            while (bufPos2 <= end2 && right.getIntUnchecked(BUFF_OFFSETS[end2]) == 0) {
                end2--;
            }

            while (bufPos1 <= end1 && bufPos2 <= end2 &&
                left.getIntUnchecked(BUFF_OFFSETS[bufPos1]) == right.getIntUnchecked(BUFF_OFFSETS[bufPos2])) {
                bufPos1++;
                bufPos2++;
            }
            return bufPos1 > end1 && bufPos2 > end2;
        }

        return false;
    }
}