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

package com.alibaba.polardbx.qatest.dml.auto.basecrud.function;

import com.alibaba.polardbx.qatest.ReadBaseTestCase;
import org.junit.Test;

import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

public class ConvTest extends ReadBaseTestCase {
    /**
     * bug fix: fix conv function: conv(N, from_base, to_base)
     * conv should return 0 rather than null when to_string(N, from_base) = null,
     *
     * @see com.alibaba.polardbx.optimizer.core.function.calc.scalar.math.Conv
     */
    @Test
    public void emptyStringReturn0() {
        String sql = "select conv(37, 2, 10);";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }
}