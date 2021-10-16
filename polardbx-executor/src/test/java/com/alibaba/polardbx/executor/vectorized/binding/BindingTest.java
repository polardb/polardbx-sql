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

import org.junit.Test;

public class BindingTest extends BindingTestBase {

    @Test
    public void testCaseWhen() {
        initTable("test_case_when",
            "CREATE TABLE `test_case_when` (\n"
                + "  `pk` bigint(11) NOT NULL auto_increment,\n"
                + "  `a` double DEFAULT NULL,\n"
                + "  `b` double DEFAULT NULL,\n"
                + "  `c` double DEFAULT NULL,\n"
                + "  `d` double DEFAULT NULL,\n"
                + "  `e` double DEFAULT NULL,\n"
                + "  PRIMARY KEY (`pk`)\n"
                + ");");

        // select case when a > b then a + b when c < d then c - d else e end from test_case_when
        testProject("select case when a > b then a + b when c < d then c - d else e end from test_case_when")
            .tree("CaseVectorizedExpression, { DoubleType, 6 }\n"
                + "   └ FilterGTDoubleColDoubleColVectorizedExpression, { [Filter], -1 }\n"
                + "      └ InputRefVectorizedExpression, { DoubleType, 1 }\n"
                + "      └ InputRefVectorizedExpression, { DoubleType, 2 }\n"
                + "   └ AddDoubleColDoubleColVectorizedExpression, { DoubleType, 7 }\n"
                + "      └ InputRefVectorizedExpression, { DoubleType, 1 }\n"
                + "      └ InputRefVectorizedExpression, { DoubleType, 2 }\n"
                + "   └ FilterLTDoubleColDoubleColVectorizedExpression, { [Filter], -1 }\n"
                + "      └ InputRefVectorizedExpression, { DoubleType, 3 }\n"
                + "      └ InputRefVectorizedExpression, { DoubleType, 4 }\n"
                + "   └ SubtractDoubleColDoubleColVectorizedExpression, { DoubleType, 8 }\n"
                + "      └ InputRefVectorizedExpression, { DoubleType, 3 }\n"
                + "      └ InputRefVectorizedExpression, { DoubleType, 4 }\n"
                + "   └ InputRefVectorizedExpression, { DoubleType, 5 }\n");

        // select case a when 1 then b when 2 then c else d end from test_case_when
        testProject("select case a when 1 then b when 2 then c else d end from test_case_when")
            .tree("CaseVectorizedExpression, { DoubleType, 6 }\n"
                + "   └ FilterEQDoubleColLongConstVectorizedExpression, { [Filter], -1 }\n"
                + "      └ InputRefVectorizedExpression, { DoubleType, 1 }\n"
                + "      └ LiteralVectorizedExpression, { LongType, 7 }\n"
                + "   └ InputRefVectorizedExpression, { DoubleType, 2 }\n"
                + "   └ FilterEQDoubleColLongConstVectorizedExpression, { [Filter], -1 }\n"
                + "      └ InputRefVectorizedExpression, { DoubleType, 1 }\n"
                + "      └ LiteralVectorizedExpression, { LongType, 8 }\n"
                + "   └ InputRefVectorizedExpression, { DoubleType, 3 }\n"
                + "   └ InputRefVectorizedExpression, { DoubleType, 4 }\n");

        // select case when a then a when b then b else c end from test_case_when
        testProject("select case when a then a when b then b else c end from test_case_when")
            .tree("CaseVectorizedExpression, { DoubleType, 6 }\n"
                + "   └ FilterIsTrueDoubleColVectorizedExpression, { [Filter], -1 }\n"
                + "      └ InputRefVectorizedExpression, { DoubleType, 1 }\n"
                + "   └ InputRefVectorizedExpression, { DoubleType, 1 }\n"
                + "   └ FilterIsTrueDoubleColVectorizedExpression, { [Filter], -1 }\n"
                + "      └ InputRefVectorizedExpression, { DoubleType, 2 }\n"
                + "   └ InputRefVectorizedExpression, { DoubleType, 2 }\n"
                + "   └ InputRefVectorizedExpression, { DoubleType, 3 }\n");
    }
}
