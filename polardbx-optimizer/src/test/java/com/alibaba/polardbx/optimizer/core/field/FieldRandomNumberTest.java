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

package com.alibaba.polardbx.optimizer.core.field;

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

public class FieldRandomNumberTest {
    private static final Random R = new Random();
    private BiFunction<Long, Long, Long> r1 = (min, max) ->
        Long.valueOf(R.nextInt(max.intValue() - min.intValue() + 1) + min.intValue());

    private BiFunction<Long, Long, Long> r2 = (min, max) ->
        min + (long) (Math.random() * (max - min));

    @Test
    public void testIntSigned() {
        checkRandom(INT_32_MIN, INT_32_MAX, new IntField(new IntegerType()), new LongType(), r2);
    }

    @Test
    public void testIntUnSigned() {
        checkRandom(0, UNSIGNED_INT_32_MAX, new IntField(new UIntegerType()), new LongType(), r2);
    }

    @Test
    public void testMediumIntSigned() {
        checkRandom(INT_24_MIN, INT_24_MAX, new MediumIntField(new MediumIntType()), new LongType(), r1);
    }

    @Test
    public void testMediumIntUnSigned() {
        checkRandom(0, UNSIGNED_INT_24_MAX, new MediumIntField(new UMediumIntType()), new LongType(), r1);
    }

    @Test
    public void testSmallIntSigned() {
        checkRandom(INT_16_MIN, INT_16_MAX, new SmallIntField(new SmallIntType()), new LongType(), r1);
    }

    @Test
    public void testSmallIntUnSigned() {
        checkRandom(0, UNSIGNED_INT_16_MAX, new SmallIntField(new USmallIntType()), new LongType(), r1);
    }

    @Test
    public void testTinyIntSigned() {
        checkRandom(INT_8_MIN, INT_8_MAX, new TinyIntField(new TinyIntType()), new LongType(), r1);
    }

    @Test
    public void testTinyIntUnSigned() {
        checkRandom(0, UNSIGNED_INT_8_MAX, new TinyIntField(new UTinyIntType()), new LongType(), r1);
    }

    private void checkRandom(long min, long max, StorageField f, DataType t, BiFunction<Long, Long, Long> r) {
        IntStream.range(0, 1 << 20).forEach(
            i -> {
                f.reset();
                long val = r.apply(min, max);

                // test store
                TypeConversionStatus conversionStatus = f.store(val, t);
                Assert.assertTrue(conversionStatus == TypeConversionStatus.TYPE_OK);

                // test long value
                long l = f.longValue();
                Assert.assertTrue(l == val);
            }
        );
    }
}
