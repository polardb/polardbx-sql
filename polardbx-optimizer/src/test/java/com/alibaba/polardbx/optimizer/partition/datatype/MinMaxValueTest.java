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

package com.alibaba.polardbx.optimizer.partition.datatype;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.IntegerType;
import com.alibaba.polardbx.optimizer.core.datatype.LongType;
import com.alibaba.polardbx.optimizer.core.datatype.UIntegerType;
import com.alibaba.polardbx.optimizer.core.datatype.ULongType;
import org.junit.Test;

import java.util.Random;

public class MinMaxValueTest {
    private static final Random R = new Random();

    @Test
    public void testBigInt() {
        DataType longType = new LongType();
        PartitionField min = PartitionDataTypeUtils.minValueOf(longType);
        PartitionField max = PartitionDataTypeUtils.maxValueOf(longType);
        PartitionField f = new BigIntPartitionField(longType);

        R.longs(1 << 10)
            .boxed()
            .forEach(
                l -> {
                    f.reset();
                    f.store(l, longType);

                    Assert.assertTrue(f.compareTo(min) > 0);
                    Assert.assertTrue(f.compareTo(max) < 0);
                });
    }

    @Test
    public void testBigIntUnsigned() {
        DataType unsignedLongType = new ULongType();
        PartitionField min = PartitionDataTypeUtils.minValueOf(unsignedLongType);
        PartitionField max = PartitionDataTypeUtils.maxValueOf(unsignedLongType);
        PartitionField f = new BigIntPartitionField(unsignedLongType);

        R.longs(1 << 10)
            .boxed()
            .forEach(
                l -> {
                    f.reset();
                    f.store(l, unsignedLongType);

                    Assert.assertTrue(f.compareTo(min) > 0);
                    Assert.assertTrue(f.compareTo(max) < 0);
                });
    }

    @Test
    public void testInt() {
        DataType intType = new IntegerType();
        PartitionField min = PartitionDataTypeUtils.minValueOf(intType);
        PartitionField max = PartitionDataTypeUtils.maxValueOf(intType);
        PartitionField f = PartitionFieldBuilder.createField(intType);

        R.ints(1 << 10)
            .boxed()
            .forEach(
                l -> {
                    f.reset();
                    f.store(l, intType);

                    Assert.assertTrue(f.compareTo(min) > 0);
                    Assert.assertTrue(f.compareTo(max) < 0);
                });
    }

    @Test
    public void testIntUnsigned() {
        DataType uintType = new UIntegerType();
        PartitionField min = PartitionDataTypeUtils.minValueOf(uintType);
        PartitionField max = PartitionDataTypeUtils.maxValueOf(uintType);
        PartitionField f = PartitionFieldBuilder.createField(uintType);

        R.ints(1 << 20)
            .boxed()
            .forEach(
                l -> {
                    l = Math.abs(l);
                    f.reset();
                    f.store(l, uintType);

                    Assert.assertTrue(f.compareTo(min) > 0);
                    Assert.assertTrue(f.compareTo(max) < 0, f.longValue() + "");
                });
    }
}
