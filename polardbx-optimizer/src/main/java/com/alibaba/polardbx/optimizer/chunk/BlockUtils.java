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

package com.alibaba.polardbx.optimizer.chunk;

import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.common.datatype.UInt64;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;

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

}