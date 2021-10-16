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

import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.IntegerType;
import com.alibaba.polardbx.optimizer.core.datatype.LongType;
import com.alibaba.polardbx.optimizer.core.datatype.MediumIntType;
import com.alibaba.polardbx.optimizer.core.datatype.SmallIntType;
import com.alibaba.polardbx.optimizer.core.datatype.TinyIntType;
import com.alibaba.polardbx.optimizer.core.datatype.UIntegerType;
import com.alibaba.polardbx.optimizer.core.datatype.UMediumIntType;
import com.alibaba.polardbx.optimizer.core.datatype.USmallIntType;
import com.alibaba.polardbx.optimizer.core.datatype.UTinyIntType;
import org.junit.Assert;
import org.junit.Test;

import java.util.Random;
import java.util.function.BiFunction;
import java.util.stream.IntStream;

import static com.alibaba.polardbx.optimizer.core.field.AbstractNumericField.INT_16_MAX;
import static com.alibaba.polardbx.optimizer.core.field.AbstractNumericField.INT_16_MIN;
import static com.alibaba.polardbx.optimizer.core.field.AbstractNumericField.INT_24_MAX;
import static com.alibaba.polardbx.optimizer.core.field.AbstractNumericField.INT_24_MIN;
import static com.alibaba.polardbx.optimizer.core.field.AbstractNumericField.INT_32_MAX;
import static com.alibaba.polardbx.optimizer.core.field.AbstractNumericField.INT_32_MIN;
import static com.alibaba.polardbx.optimizer.core.field.AbstractNumericField.INT_8_MAX;
import static com.alibaba.polardbx.optimizer.core.field.AbstractNumericField.INT_8_MIN;
import static com.alibaba.polardbx.optimizer.core.field.AbstractNumericField.UNSIGNED_INT_16_MAX;
import static com.alibaba.polardbx.optimizer.core.field.AbstractNumericField.UNSIGNED_INT_24_MAX;
import static com.alibaba.polardbx.optimizer.core.field.AbstractNumericField.UNSIGNED_INT_32_MAX;
import static com.alibaba.polardbx.optimizer.core.field.AbstractNumericField.UNSIGNED_INT_8_MAX;

public class CmpPartitionFieldTest {
    private static final Random R = new Random();
    private BiFunction<Long, Long, Long> r1 = (min, max) ->
        Long.valueOf(R.nextInt(max.intValue() - min.intValue() + 1) + min.intValue());

    private BiFunction<Long, Long, Long> r2 = (min, max) ->
        min + (long) (Math.random() * (max - min));

    @Test
    public void testIntSigned() {
        checkRandom(INT_32_MIN, INT_32_MAX, new IntPartitionField(new IntegerType()), new LongType(), r2);
    }

    @Test
    public void testIntUnSigned() {
        checkRandom(0, UNSIGNED_INT_32_MAX, new IntPartitionField(new UIntegerType()), new LongType(), r2);
    }

    @Test
    public void testMediumIntSigned() {
        checkRandom(INT_24_MIN, INT_24_MAX, new MediumIntPartitionField(new MediumIntType()), new LongType(), r1);
    }

    @Test
    public void testMediumIntUnSigned() {
        checkRandom(0, UNSIGNED_INT_24_MAX, new MediumIntPartitionField(new UMediumIntType()), new LongType(), r1);
    }

    @Test
    public void testSmallIntSigned() {
        checkRandom(INT_16_MIN, INT_16_MAX, new SmallIntPartitionField(new SmallIntType()), new LongType(), r1);
    }

    @Test
    public void testSmallIntUnSigned() {
        checkRandom(0, UNSIGNED_INT_16_MAX, new SmallIntPartitionField(new USmallIntType()), new LongType(), r1);
    }

    @Test
    public void testTinyIntSigned() {
        checkRandom(INT_8_MIN, INT_8_MAX, new TinyIntPartitionField(new TinyIntType()), new LongType(), r1);
    }

    @Test
    public void testTinyIntUnSigned() {
        checkRandom(0, UNSIGNED_INT_8_MAX, new TinyIntPartitionField(new UTinyIntType()), new LongType(), r1);
    }

    private void checkRandom(long min, long max, PartitionField f, DataType t, BiFunction<Long, Long, Long> r) {
        PartitionField f1 = PartitionFieldBuilder.createField(f.dataType());
        IntStream.range(0, 1 << 20).forEach(
            i -> {
                f.reset();
                long val = r.apply(min, max);
                f.store(val, t);

                f1.reset();
                long val1 = r.apply(min, max);
                f1.store(val1, t);

                boolean b1 = val <= val1;
                boolean b2 = f.compareTo(f1) <= 0;

                Assert.assertTrue(b1 == b2);
            }
        );
    }
}
