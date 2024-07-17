package com.alibaba.polardbx.executor.vectorized.binding;

import org.junit.Test;

public class CastBindingTest extends BindingTestBase {

    @Test
    public void testCastToDecimal() {
        testProject("select cast(99 as decimal) from test")
            .tree("CastLongConstToDecimalVectorizedExpression, { DecimalType, 26 }\n"
                + "   └ LiteralVectorizedExpression, { LongType, 25 }\n");
    }

    @Test
    public void testIfNullCast() {
        testProject("select ifnull(decimal_test, 0) from test")
            .tree("CoalesceVectorizedExpression, { DecimalType, 27 }\n"
                + "   └ InputRefVectorizedExpression, { DecimalType, 18 }\n"
                + "   └ CastLongConstToDecimalVectorizedExpression, { DecimalType, 26 }\n"
                + "      └ LiteralVectorizedExpression, { LongType, 25 }\n");
    }
}
