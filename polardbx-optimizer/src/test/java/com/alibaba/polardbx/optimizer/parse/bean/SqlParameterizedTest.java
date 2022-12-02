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

package com.alibaba.polardbx.optimizer.parse.bean;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.optimizer.parse.SqlParameterizeUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class SqlParameterizedTest {
    @Test
    public void test() {
        doTest(
            "select * from t where integer_test in (1, '2', 9223372036854775807, 18446744073709551615, 18446744073709551616, 8.8)",
            "SELECT *\n"
                + "FROM t\n"
                + "WHERE integer_test IN (?)",
            72098196L);

        doTest(
            "select integer_test + 1 from t where varchar_test = '2' and decimal_test = concat('CED4') and integer_test in (1, '2', 5.5)",
            "SELECT integer_test + ? AS 'integer_test + 1'\n"
                + "FROM t\n"
                + "WHERE varchar_test = ?\n"
                + "\tAND decimal_test = concat(?)\n"
                + "\tAND integer_test IN (?)",
            72513720L);

        // test int, varchar, bigint, bigint_unsigned, decimal.
        doTest(
            "select * from t where integer_test = 1 and varchar_test = '2' and bigint_test = 9223372036854775807 and unsigned_test = 18446744073709551615 and decimal_test = 18446744073709551616 ",
            "SELECT *\n"
                + "FROM t\n"
                + "WHERE integer_test = ?\n"
                + "\tAND varchar_test = ?\n"
                + "\tAND bigint_test = ?\n"
                + "\tAND unsigned_test = ?\n"
                + "\tAND decimal_test = ?",
            2325752L);

        doTest(
            "select * from t where integer_test = 1 and varchar_test = '2' and bigint_test = 18446744073709551615 and unsigned_test = 18446744073709551615 and decimal_test = 18446744073709551616 ",
            "SELECT *\n"
                + "FROM t\n"
                + "WHERE integer_test = ?\n"
                + "\tAND varchar_test = ?\n"
                + "\tAND bigint_test = ?\n"
                + "\tAND unsigned_test = ?\n"
                + "\tAND decimal_test = ?",
            2327674L);

        doTest(
            "select * from t where integer_test = 1 and varchar_test = '2' and bigint_test = 9223372036854775807 and unsigned_test = 18446744073709551615 and decimal_test = 18446744073709551615 ",
            "SELECT *\n"
                + "FROM t\n"
                + "WHERE integer_test = ?\n"
                + "\tAND varchar_test = ?\n"
                + "\tAND bigint_test = ?\n"
                + "\tAND unsigned_test = ?\n"
                + "\tAND decimal_test = ?",
            2325748L);

    }

    private void doTest(String sql, String expectedParameterizedSql, long expectTypeDigest) {
        Map<Integer, ParameterContext> currentParameter = new HashMap<>();
        SqlParameterized sqlParameterized = SqlParameterizeUtils.parameterize(sql, currentParameter, false);
        Assert.assertEquals(expectedParameterizedSql, sqlParameterized.getSql());
        Assert.assertEquals(expectTypeDigest, sqlParameterized.getDigest());
    }
}
