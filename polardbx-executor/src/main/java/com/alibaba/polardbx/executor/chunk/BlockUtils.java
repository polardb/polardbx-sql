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

import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.common.datatype.UInt64;
import com.alibaba.polardbx.executor.operator.util.ObjectPools;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;

import java.util.Arrays;
import java.util.TimeZone;

public class BlockUtils {
    public static <T> RandomAccessBlock createBlock(DataType<T> dataType, int positionCount) {
        Class<?> clazz = dataType.getDataClass();
        if (clazz == Byte.class) {
            return new ByteBlock(dataType, positionCount);
        } else if (clazz == Short.class) {
            return new ShortBlock(dataType, positionCount);
        } else if (clazz == Integer.class) {
            return new IntegerBlock(dataType, positionCount);
        } else if (clazz == Long.class) {
            return new LongBlock(dataType, positionCount);
        } else if (clazz == Float.class) {
            return new FloatBlock(dataType, positionCount);
        } else if (clazz == Double.class) {
            return new DoubleBlock(dataType, positionCount);
        } else if (clazz == Boolean.class) {
            return new BooleanBlock(dataType, positionCount);
        } else if (clazz == Decimal.class) {
            return new DecimalBlock(dataType, positionCount);
        } else if (clazz == UInt64.class) {
            return new ULongBlock(dataType, positionCount);
        } else {
            return new ReferenceBlock<T>(dataType, positionCount);
        }
    }

    public static <T> RandomAccessBlock createBlock(DataType<T> dataType, int positionCount, ObjectPools objectPools,
                                                    int chunkLimit) {
        Class<?> clazz = dataType.getDataClass();
        if (clazz == Byte.class) {
            return new ByteBlock(dataType, positionCount);
        } else if (clazz == Short.class) {
            return new ShortBlock(dataType, positionCount);
        } else if (clazz == Integer.class) {
            return new IntegerBlock(dataType, positionCount, objectPools.getIntArrayPool(), chunkLimit);
        } else if (clazz == Long.class) {
            return new LongBlock(dataType, positionCount, objectPools.getLongArrayPool(), chunkLimit);
        } else if (clazz == Float.class) {
            return new FloatBlock(dataType, positionCount);
        } else if (clazz == Double.class) {
            return new DoubleBlock(dataType, positionCount);
        } else if (clazz == Boolean.class) {
            return new BooleanBlock(dataType, positionCount);
        } else if (clazz == Decimal.class) {
            return new DecimalBlock(dataType, positionCount, objectPools.getLongArrayPool(), chunkLimit);
        } else if (clazz == UInt64.class) {
            return new ULongBlock(dataType, positionCount);
        } else {
            return new ReferenceBlock<T>(dataType, positionCount);
        }
    }

    public static void copySelectedInCommon(boolean selectedInUse, int[] sel, int size, RandomAccessBlock srcVector,
                                            RandomAccessBlock dstVector) {
        DataType dataType = dstVector.getType();
        if (selectedInUse) {
            for (int i = 0; i < size; i++) {
                int j = sel[i];
                dstVector.setElementAt(j, dataType.convertFrom(srcVector.elementAt(j)));
            }
        } else {
            for (int i = 0; i < size; i++) {
                dstVector.setElementAt(i, dataType.convertFrom(srcVector.elementAt(i)));
            }
        }
    }

    public static AbstractBlock fillSelection(Block result, int[] selection, int selSize,
                                              boolean useSelection, boolean enableCompatible, TimeZone timeZone) {
        if (result instanceof SliceBlock) {

            // case 1: for varchar / char type with dictionary / direct encoding.
            // need compatible form execution context
            return SliceBlock.from(result.cast(SliceBlock.class), selSize, selection, enableCompatible, useSelection);
        } else if (result instanceof DateBlock) {

            // case 2: for date type.
            return DateBlock.from(result.cast(DateBlock.class), selSize, selection, useSelection);
        } else if (result instanceof IntegerBlock) {

            // case 3: for integer / short unsigned / int24 unsigned type.
            return IntegerBlock.from(result.cast(IntegerBlock.class), selSize, selection, useSelection);

        } else if (result instanceof DecimalBlock) {

            // case 4. for decimal type in decimal_64 mode and normal mode.
            return DecimalBlock.from(result.cast(DecimalBlock.class), selSize, selection, useSelection);
        } else if (result instanceof LongBlock) {

            // case 5. for bigint / int unsigned type.
            return LongBlock.from(result.cast(LongBlock.class), selSize, selection, useSelection);

        } else if (result instanceof TimestampBlock) {

            // case 6. timestamp type
            return TimestampBlock.from(result.cast(TimestampBlock.class), selSize, selection, useSelection, timeZone);
        } else {
            if (useSelection) {
                throw new UnsupportedOperationException("Unsupported block "
                    + result.getClass() + " with useSelection");
            }
            if (result instanceof BlobBlock) {
                return BlobBlock.from(result.cast(BlobBlock.class), selSize, selection);
            }
            if (result instanceof ByteBlock) {
                return ByteBlock.from(result.cast(ByteBlock.class), selSize, selection);
            }
            if (result instanceof DoubleBlock) {
                return DoubleBlock.from(result.cast(DoubleBlock.class), selSize, selection);
            }
            if (result instanceof EnumBlock) {
                return EnumBlock.from(result.cast(EnumBlock.class), selSize, selection);
            }
            if (result instanceof FloatBlock) {
                return FloatBlock.from(result.cast(FloatBlock.class), selSize, selection);
            }
            if (result instanceof ShortBlock) {
                return ShortBlock.from(result.cast(ShortBlock.class), selSize, selection);
            }
            if (result instanceof StringBlock) {
                return StringBlock.from(result.cast(StringBlock.class), selSize, selection);
            }
            if (result instanceof TimeBlock) {
                return TimeBlock.from(result.cast(TimeBlock.class), selSize, selection);
            }
            if (result instanceof ULongBlock) {
                return ULongBlock.from(result.cast(ULongBlock.class), selSize, selection);
            }
            if (result instanceof BigIntegerBlock) {
                return BigIntegerBlock.from(result.cast(BigIntegerBlock.class), selSize, selection);
            }
            if (result instanceof ByteArrayBlock) {
                return ByteArrayBlock.from(result.cast(ByteArrayBlock.class), selSize, selection);
            }
            throw new UnsupportedOperationException("Unsupported block "
                + result.getClass() + " with selection copy");
        }
    }

    public static AbstractBlock wrapNullSelection(AbstractBlock result, boolean useSelection,
                                                  boolean enableCompatible, TimeZone timeZone) {
        if (result instanceof SliceBlock) {
            // case 1: for varchar / char type with dictionary / direct encoding.
            // need compatible form execution context
            return SliceBlock.from(result.cast(SliceBlock.class), result.getPositionCount(), null, enableCompatible,
                useSelection);
        } else if (result instanceof DateBlock) {
            // case 2: for date type.
            return DateBlock.from(result.cast(DateBlock.class), result.getPositionCount(), null, useSelection);
        } else if (result instanceof IntegerBlock) {
            // case 3: for integer / short unsigned / int24 unsigned type.
            return IntegerBlock.from(result.cast(IntegerBlock.class), result.getPositionCount(), null, useSelection);
        } else if (result instanceof DecimalBlock) {
            // case 4. for decimal type in decimal_64 mode and normal mode.
            return DecimalBlock.from(result.cast(DecimalBlock.class), result.getPositionCount(), null, useSelection);
        } else if (result instanceof LongBlock) {
            // case 5. for bigint / int unsigned type.
            return LongBlock.from(result.cast(LongBlock.class), result.getPositionCount(), null, useSelection);
        } else if (result instanceof TimestampBlock) {
            // case 6. timestamp type
            return TimestampBlock.from(result.cast(TimestampBlock.class), result.getPositionCount(), null,
                useSelection, timeZone);
        } else {
            return result;
        }
    }

    public static int[] copyIntArray(int[] values, int[] selection, int positionCount) {
        if (values == null) {
            return null;
        }
        if (selection == null) {
            return Arrays.copyOf(values, positionCount);
        } else {
            int[] target = new int[positionCount];
            for (int i = 0; i < positionCount; i++) {
                target[i] = values[selection[i]];
            }
            return target;
        }
    }

    public static boolean[] copyNullArray(boolean[] values, int[] selection, int positionCount) {
        if (values == null) {
            return null;
        }
        if (selection == null) {
            return Arrays.copyOf(values, positionCount);
        } else {
            boolean[] target = new boolean[positionCount];
            boolean hasNull = false;
            for (int i = 0; i < positionCount; i++) {
                target[i] = values[selection[i]];
                hasNull |= target[i];
            }
            // NOTE: destroy the boolean array if it does not have null.
            return hasNull ? target : null;
        }
    }

    public static long[] copyLongArray(long[] values, int[] selection, int positionCount) {
        if (values == null) {
            return null;
        }
        if (selection == null) {
            return Arrays.copyOf(values, positionCount);
        } else {
            long[] target = new long[positionCount];
            for (int i = 0; i < positionCount; i++) {
                target[i] = values[selection[i]];
            }
            return target;
        }
    }

    public static byte[] copyByteArray(byte[] values, int[] selection, int positionCount) {
        if (values == null) {
            return null;
        }
        if (selection == null) {
            return Arrays.copyOf(values, positionCount);
        } else {
            byte[] target = new byte[positionCount];
            for (int i = 0; i < positionCount; i++) {
                target[i] = values[selection[i]];
            }
            return target;
        }
    }

    public static double[] copyDoubleArray(double[] values, int[] selection, int positionCount) {
        if (values == null) {
            return null;
        }
        if (selection == null) {
            return Arrays.copyOf(values, positionCount);
        } else {
            double[] target = new double[positionCount];
            for (int i = 0; i < positionCount; i++) {
                target[i] = values[selection[i]];
            }
            return target;
        }
    }

    public static float[] copyFloatArray(float[] values, int[] selection, int positionCount) {
        if (values == null) {
            return null;
        }
        if (selection == null) {
            return Arrays.copyOf(values, positionCount);
        } else {
            float[] target = new float[positionCount];
            for (int i = 0; i < positionCount; i++) {
                target[i] = values[selection[i]];
            }
            return target;
        }
    }

    public static short[] copyShortArray(short[] values, int[] selection, int positionCount) {
        if (values == null) {
            return null;
        }
        if (selection == null) {
            return Arrays.copyOf(values, positionCount);
        } else {
            short[] target = new short[positionCount];
            for (int i = 0; i < positionCount; i++) {
                target[i] = values[selection[i]];
            }
            return target;
        }
    }
}