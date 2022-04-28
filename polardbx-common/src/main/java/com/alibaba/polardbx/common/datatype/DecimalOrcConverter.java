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

import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;

public class DecimalOrcConverter {
    private static class UnsafeHolder {
        private static final sun.misc.Unsafe unsafe;

        // -1 when negative; 0 when decimal is zero; 1 when positive.
        private static final long fastSignum;

        // Decimal longwords.
        private static final long fast2;
        private static final long fast1;
        private static final long fast0;

        // The number of integer digits in the decimal.  When the integer portion is zero, this is 0.
        private static final long fastIntegerDigitCount;

        // The scale of the decimal.
        private static final long fastScale;

        // Used for legacy HiveDecimalV1 setScale compatibility for binary / display serialization of
        // trailing zeroes (or rounding).
        private static final long fastSerializationScale;

        static {
            try {
                java.lang.reflect.Field f = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
                f.setAccessible(true);
                unsafe = (sun.misc.Unsafe) f.get(null);

                fastSignum = unsafe.objectFieldOffset(HiveDecimalWritable.class.getSuperclass().getDeclaredField("fastSignum"));
                // Decimal longwords.
                fast2 = unsafe.objectFieldOffset(HiveDecimalWritable.class.getSuperclass().getDeclaredField("fast2"));
                fast1 = unsafe.objectFieldOffset(HiveDecimalWritable.class.getSuperclass().getDeclaredField("fast1"));
                fast0 = unsafe.objectFieldOffset(HiveDecimalWritable.class.getSuperclass().getDeclaredField("fast0"));

                // The number of integer digits in the decimal.  When the integer portion is zero, this is 0.
                fastIntegerDigitCount = unsafe.objectFieldOffset(HiveDecimalWritable.class.getSuperclass().getDeclaredField("fastIntegerDigitCount"));

                // The scale of the decimal.
                fastScale = unsafe.objectFieldOffset(HiveDecimalWritable.class.getSuperclass().getDeclaredField("fastScale"));

                // Used for legacy HiveDecimalV1 setScale compatibility for binary / display serialization of
                // trailing zeroes (or rounding).
                fastSerializationScale = unsafe.objectFieldOffset(HiveDecimalWritable.class.getSuperclass().getDeclaredField("fastSerializationScale"));
            } catch (Exception ex) {
                throw new ExceptionInInitializerError(ex);
            }
        }

        public static int fastSignum(HiveDecimalWritable h) {
            return unsafe.getInt(h, fastSignum);
        }

        public static long fast2(HiveDecimalWritable h) {
            return unsafe.getLong(h, fast2);
        }

        public static long fast1(HiveDecimalWritable h) {
            return unsafe.getLong(h, fast1);
        }

        public static long fast0(HiveDecimalWritable h) {
            return unsafe.getLong(h, fast0);
        }

        public static int fastIntegerDigitCount(HiveDecimalWritable h) {
            return unsafe.getInt(h, fastIntegerDigitCount);
        }

        public static int fastScale(HiveDecimalWritable h) {
            return unsafe.getInt(h, fastScale);
        }

        public static int fastSerializationScale(HiveDecimalWritable h) {
            return unsafe.getInt(h, fastSerializationScale);
        }
    }

    private static final long[] pows = new long[]{
        1,
        10,
        100,
        1000,
        10000,
        100000,
        1000000,
        10000000,
        100000000,
        1000000000,
        10000000000L,
        100000000000L,
        1000000000000L,
        10000000000000L,
        100000000000000L,
        1000000000000000L,
        10000000000000000L,
        100000000000000000L,
        1000000000000000000L
    };

    public static void transform(DecimalStructure decimalStructure, HiveDecimalWritable h) {
        boolean isNeg = UnsafeHolder.fastSignum(h) < 0;
        int integers = UnsafeHolder.fastIntegerDigitCount(h);
        int fractions = UnsafeHolder.fastScale(h);
        int intPart = DecimalTypeBase.roundUp(integers);
        int fracPart = DecimalTypeBase.roundUp(fractions);

        long f0 = UnsafeHolder.fast0(h);
        long f1 = UnsafeHolder.fast1(h);
        long f2 = UnsafeHolder.fast2(h);

        long[] fs = new long[]{f0, f1, f2};


        int pointWord = fractions / 16;
        int pointDigitPos = 16 - (fractions % 16);

        final int originalPointWord = pointWord;
        final int originalPointDigitPos = pointDigitPos;

        // get digits before point
        int pointDigitBeginPos = pointDigitPos - 9;
        int pointWordBeg = pointWord;
        if (pointDigitBeginPos < 0) {
            if (pointWordBeg < 2) {
                pointWordBeg++;
                pointDigitBeginPos += 16;
            } else {
                pointDigitBeginPos = 0;
            }
        }
        for (int buffPos = intPart - 1; buffPos >= 0; buffPos--) {
            // get 9 digits before point
            if (pointWordBeg != pointWord) {
                // span
                int highCoefficient = 16 - pointDigitBeginPos;
                int lowCoefficient = pointDigitPos;

                long highBufVal = fs[pointWordBeg] % pows[highCoefficient];
                long lowBufVal = fs[pointWord] / pows[16 - lowCoefficient];
                int bufVal = (int) (highBufVal * pows[lowCoefficient] + lowBufVal);
                decimalStructure.setBuffValAt(buffPos, bufVal);
            } else {
                // not span
                // cut 9 digits before point
                long bufVal = (fs[pointWord] % pows[16 - pointDigitBeginPos]) / pows[16 - pointDigitPos];
                decimalStructure.setBuffValAt(buffPos, (int) bufVal);
            }
            pointWord = pointWordBeg;
            pointDigitPos = pointDigitBeginPos;
            pointDigitBeginPos = pointDigitPos - 9;
            if (pointDigitBeginPos < 0) {
                if (pointWordBeg < 2) {
                    pointWordBeg++;
                    pointDigitBeginPos += 16;
                } else {
                    pointDigitBeginPos = 0;
                }
            }
        }

        // get digits after point
        pointWord = originalPointWord;
        pointDigitPos = originalPointDigitPos;
        int pointDigitEndPos = pointDigitPos + 9;
        int pointWordEnd = pointWord;
        if (pointDigitEndPos > 16) {
            if (pointWordEnd > 0) {
                pointWordEnd--;
                pointDigitEndPos -= 16;
            } else {
                pointDigitEndPos = 16;
            }
        }
        for (int buffPos = intPart; buffPos < intPart + fracPart; buffPos++) {
            // get 9 digits after point
            if (pointWordEnd != pointWord) {
                // span
                int highCoefficient = 16 - pointDigitPos;
                int lowCoefficient = pointDigitEndPos;

                long highBufVal = fs[pointWord] % pows[highCoefficient];
                long lowBufVal = fs[pointWordEnd] / pows[16 - lowCoefficient];
                long bufVal = highBufVal * pows[lowCoefficient] + lowBufVal;


                // digits < 9
                if (highCoefficient + lowCoefficient < 9) {
                    bufVal *= pows[9 - (highCoefficient + lowCoefficient)];
                }

                decimalStructure.setBuffValAt(buffPos, (int) bufVal);
            } else {
                // not span
                // cut 9 digits from point
                long bufVal = (fs[pointWord] % pows[16 - pointDigitPos]) / pows[16 - pointDigitEndPos];

                // digits < 9
                if (pointDigitEndPos - pointDigitPos < 9) {
                    bufVal *= pows[9 - (pointDigitEndPos - pointDigitPos)];
                }

                decimalStructure.setBuffValAt(buffPos, (int) bufVal);
            }
            pointWord = pointWordEnd;
            pointDigitPos = pointDigitEndPos;
            pointDigitEndPos = pointDigitPos + 9;
            if (pointDigitEndPos > 16) {
                if (pointWordEnd > 0) {
                    pointWordEnd--;
                    pointDigitEndPos -= 16;
                } else {
                    pointDigitEndPos = 16;
                }
            }
        }

        decimalStructure.setNeg(isNeg);
        decimalStructure.setFractions(fractions);
        decimalStructure.setIntegers(integers);
        decimalStructure.setDerivedFractions(fractions);
    }

    public static DecimalStructure transform(HiveDecimalWritable h) {
        DecimalStructure decimalStructure = new DecimalStructure();
        transform(decimalStructure, h);
        return decimalStructure;
    }
}
