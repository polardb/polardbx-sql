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

package com.alibaba.polardbx.executor.vectorized.binding;

import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class ComplexLogicalOperatorBindingTest extends BindingTestBase {
    @Test
    public void testNull() {
        testFilter("select * from test where integer_test = null")
            .showTrees();
    }

    @Test
    public void testNest() {
        testFilter("select * from test where tinyint_test > 1 and varchar_test is not null and bigint_test < 1")
            .tree("└ AndVectorizedExpression, { [Filter], -1 }\n"
                + "   └ FilterGTTinyIntColLongConstVectorizedExpression, { [Filter], -1 }\n"
                + "      └ InputRefVectorizedExpression, { TinyIntType, 6 }\n"
                + "      └ LiteralVectorizedExpression, { LongType, 25 }\n"
                + "   └ FilterIsTrueLongColVectorizedExpression, { [Filter], -1 }\n"
                + "      └ [BuiltIn].IsNotNull, { LongType, 26 }\n"
                + "         └ InputRefVectorizedExpression, { StringType, 3 }\n"
                + "   └ FilterLTLongColLongConstVectorizedExpression, { [Filter], -1 }\n"
                + "      └ InputRefVectorizedExpression, { LongType, 14 }\n"
                + "      └ LiteralVectorizedExpression, { LongType, 27 }\n");

        testFilter("select * from test where tinyint_test > 1 and varchar_test is true and bigint_test < 1")
            .tree("└ AndVectorizedExpression, { [Filter], -1 }\n"
                + "   └ FilterGTTinyIntColLongConstVectorizedExpression, { [Filter], -1 }\n"
                + "      └ InputRefVectorizedExpression, { TinyIntType, 6 }\n"
                + "      └ LiteralVectorizedExpression, { LongType, 25 }\n"
                + "   └ FilterIsTrueLongColVectorizedExpression, { [Filter], -1 }\n"
                + "      └ [BuiltIn].IsTrue, { LongType, 26 }\n"
                + "         └ InputRefVectorizedExpression, { StringType, 3 }\n"
                + "   └ FilterLTLongColLongConstVectorizedExpression, { [Filter], -1 }\n"
                + "      └ InputRefVectorizedExpression, { LongType, 14 }\n"
                + "      └ LiteralVectorizedExpression, { LongType, 27 }\n");

    }

    @Test
    public void testCommonExpr() {
        testFilter("select * from test where integer_test + 1 and bigint_test - 1")
            .tree("└ AndVectorizedExpression, { [Filter], -1 }\n"
                + "   └ FilterIsTrueLongColVectorizedExpression, { [Filter], -1 }\n"
                + "      └ AddIntegerColLongConstVectorizedExpression, { LongType, 25 }\n"
                + "         └ InputRefVectorizedExpression, { IntegerType, 1 }\n"
                + "         └ LiteralVectorizedExpression, { LongType, 26 }\n"
                + "   └ FilterIsTrueLongColVectorizedExpression, { [Filter], -1 }\n"
                + "      └ SubtractLongColLongConstVectorizedExpression, { LongType, 27 }\n"
                + "         └ InputRefVectorizedExpression, { LongType, 14 }\n"
                + "         └ LiteralVectorizedExpression, { LongType, 28 }\n");
    }

    @Test
    public void testIn() {
        testFilter("select * from test where varchar_test in ('a', 'b', 'c') and integer_test < 1")
            .tree("└ AndVectorizedExpression, { [Filter], -1 }\n"
                + "   └ FilterIsTrueLongColVectorizedExpression, { [Filter], -1 }\n"
                + "      └ [BuiltIn].In, { LongType, 29 }\n"
                + "         └ InputRefVectorizedExpression, { StringType, 3 }\n"
                + "         └ [BuiltIn].Row, { RowType, 28 }\n"
                + "            └ LiteralVectorizedExpression, { StringType, 25 }\n"
                + "            └ LiteralVectorizedExpression, { StringType, 26 }\n"
                + "            └ LiteralVectorizedExpression, { StringType, 27 }\n"
                + "   └ FilterLTIntegerColLongConstVectorizedExpression, { [Filter], -1 }\n"
                + "      └ InputRefVectorizedExpression, { IntegerType, 1 }\n"
                + "      └ LiteralVectorizedExpression, { LongType, 30 }\n");

        testFilter("select * from test where varchar_test in ('a', 'b', 'c') or integer_test < 1")
            .tree("└ OrVectorizedExpression, { [Filter], -1 }\n"
                + "   └ FilterIsTrueLongColVectorizedExpression, { [Filter], -1 }\n"
                + "      └ [BuiltIn].In, { LongType, 29 }\n"
                + "         └ InputRefVectorizedExpression, { StringType, 3 }\n"
                + "         └ [BuiltIn].Row, { RowType, 28 }\n"
                + "            └ LiteralVectorizedExpression, { StringType, 25 }\n"
                + "            └ LiteralVectorizedExpression, { StringType, 26 }\n"
                + "            └ LiteralVectorizedExpression, { StringType, 27 }\n"
                + "   └ FilterLTIntegerColLongConstVectorizedExpression, { [Filter], -1 }\n"
                + "      └ InputRefVectorizedExpression, { IntegerType, 1 }\n"
                + "      └ LiteralVectorizedExpression, { LongType, 30 }\n");
    }

    @Test
    public void testBetween() {
        testFilter("select * from test where varchar_test between 'a' and 'b' and integer_test < 1")
            .tree("└ AndVectorizedExpression, { [Filter], -1 }\n"
                + "   └ FilterIsTrueLongColVectorizedExpression, { [Filter], -1 }\n"
                + "      └ [BuiltIn].Between, { LongType, 27 }\n"
                + "         └ InputRefVectorizedExpression, { StringType, 3 }\n"
                + "         └ LiteralVectorizedExpression, { StringType, 25 }\n"
                + "         └ LiteralVectorizedExpression, { StringType, 26 }\n"
                + "   └ FilterLTIntegerColLongConstVectorizedExpression, { [Filter], -1 }\n"
                + "      └ InputRefVectorizedExpression, { IntegerType, 1 }\n"
                + "      └ LiteralVectorizedExpression, { LongType, 28 }\n");

        testFilter("select * from test where varchar_test between 'a' and 'b' or integer_test < 1")
            .tree("└ OrVectorizedExpression, { [Filter], -1 }\n"
                + "   └ FilterIsTrueLongColVectorizedExpression, { [Filter], -1 }\n"
                + "      └ [BuiltIn].Between, { LongType, 27 }\n"
                + "         └ InputRefVectorizedExpression, { StringType, 3 }\n"
                + "         └ LiteralVectorizedExpression, { StringType, 25 }\n"
                + "         └ LiteralVectorizedExpression, { StringType, 26 }\n"
                + "   └ FilterLTIntegerColLongConstVectorizedExpression, { [Filter], -1 }\n"
                + "      └ InputRefVectorizedExpression, { IntegerType, 1 }\n"
                + "      └ LiteralVectorizedExpression, { LongType, 28 }\n");
    }
}