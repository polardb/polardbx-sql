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

package com.alibaba.polardbx.optimizer.core.datatype;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.optimizer.core.expression.build.Rex2ExprUtil;
import org.junit.Test;

import static com.alibaba.polardbx.optimizer.core.datatype.DataTypes.*;

public class CompareTypeTest {
    @Test
    public void testBinaryComparison() {
        // signed integer numeric -> comparison by long
        doTest(LongType, IntegerType, LongType);
        doTest(SmallIntType, IntegerType, LongType);
        doTest(MediumIntType, SmallIntType, LongType);
        doTest(TinyIntType, IntegerType, LongType);
        doTest(LongType, IntegerType, LongType);

        // at least one unsigned numeric -> comparison by unsigned long
        doTest(ULongType, IntegerType, ULongType);
        doTest(SmallIntType, ULongType, ULongType);
        doTest(MediumIntType, ULongType, ULongType);
        doTest(TinyIntType, ULongType, ULongType);
        doTest(LongType, ULongType, ULongType);

        // string - numeric -> double
        doTest(LongType, VarcharType, DoubleType);
        doTest(IntegerType, VarcharType, DoubleType);
        doTest(SmallIntType, VarcharType, DoubleType);
        doTest(MediumIntType, VarcharType, DoubleType);
        doTest(TinyIntType, VarcharType, DoubleType);
        doTest(ULongType, VarcharType, DoubleType);
        doTest(DecimalType, VarcharType, DoubleType);
        doTest(DoubleType, VarcharType, DoubleType);
        doTest(FloatType, VarcharType, DoubleType);

        // string const - decimal col -> decimal
        // string col - decimal const -> double
        doTest(VarcharType, DecimalType, true, false, DecimalType);
        doTest(VarcharType, DecimalType, false, true, DoubleType);

        // numeric - temporal -> comparison by double
        doTest(TimeType, LongType, DoubleType);
        doTest(DatetimeType, LongType, DoubleType);
        doTest(DateType, LongType, DoubleType);
        doTest(TimeType, DecimalType, DoubleType);
        doTest(DatetimeType, DecimalType, DoubleType);
        doTest(DateType, DecimalType, DoubleType);
        doTest(TimeType, DoubleType, DoubleType);
        doTest(DatetimeType, DoubleType, DoubleType);
        doTest(DateType, DoubleType, DoubleType);

        // temporal - temporal (at least one temporal with date) -> comparison by datetime
        doTest(TimeType, DatetimeType, DatetimeType);
        doTest(TimeType, DateType, DatetimeType);
        doTest(DateType, DatetimeType, DatetimeType);

        // same temporal type -> temporal with or without date
        doTest(TimeType, TimeType, TimeType);
        doTest(DateType, DateType, DatetimeType);
        doTest(DatetimeType, DatetimeType, DatetimeType);

        // string - time -> string
        doTest(TimeType, VarcharType, VarcharType);

        // string - temporal with date -> temporal
        doTest(DateType, VarcharType, DatetimeType);
        doTest(DatetimeType, VarcharType, DatetimeType);
    }

    private void doTest(DataType leftType, DataType rightType, boolean isLeftConst, boolean isRightConst, DataType expectedType) {
        DataType actualType = Rex2ExprUtil.compareTypeOf(leftType, rightType, isLeftConst, isRightConst);
        Assert.assertTrue(
            DataTypeUtil.equalsSemantically(expectedType, actualType),
            "expect = " + expectedType + ", actual = " + actualType);

        actualType = Rex2ExprUtil.compareTypeOf(rightType, leftType, isRightConst, isLeftConst);
        Assert.assertTrue(
            DataTypeUtil.equalsSemantically(expectedType, actualType),
            "expect = " + expectedType + ", actual = " + actualType);
    }

    private void doTest(DataType leftType, DataType rightType, DataType expectedType) {
        DataType actualType = Rex2ExprUtil.compareTypeOf(leftType, rightType);
        Assert.assertTrue(
            DataTypeUtil.equalsSemantically(expectedType, actualType),
            "expect = " + expectedType + ", actual = " + actualType);

        actualType = Rex2ExprUtil.compareTypeOf(rightType, leftType);
        Assert.assertTrue(
            DataTypeUtil.equalsSemantically(expectedType, actualType),
            "expect = " + expectedType + ", actual = " + actualType);
    }
}
