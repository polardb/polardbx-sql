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

package com.alibaba.polardbx.qatest.dql.sharding.charset;

import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import com.google.common.collect.ImmutableMultimap;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.util.List;

import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

public class CollationCompareTest extends CharsetTestBase {
    @Parameterized.Parameters(name = "{index}:table0={0},table1={1}")
    public static List<String[]> prepare() {
        return PARAMETERS;
    }

    public CollationCompareTest(String table, String suffix) {
        this.table = randomTableName(table, 4);
        this.suffix = suffix;
    }

    private static final String COMPARE_CASE_FORMAT = "select _%s'abc' collate %s %s _%s'AbC' collate %s";
    private static final String COMPARE_SPACE_FORMAT = "select _%s'abc  ' collate %s %s _%s'abc ' collate %s";
    private static final String[] COMPARE_OPERATORS = {"=", ">", ">=", "<", "<=", "<=>", "<>"};
    private static final ImmutableMultimap<String, String> MAP = ImmutableMultimap.<String, String>builder()
        .put("utf8mb4", "utf8mb4_general_ci")
        .put("utf8mb4", "utf8mb4_unicode_ci")
        .put("utf8mb4", "utf8mb4_bin")
        .put("ascii", "ascii_bin")
        .put("ascii", "ascii_general_ci")
        .put("utf8", "utf8_bin")
        .put("utf8", "utf8_general_ci")
        .put("utf8", "utf8_unicode_ci")
        .put("latin1", "latin1_swedish_ci")
        .put("latin1", "latin1_german1_ci")
        .put("latin1", "latin1_danish_ci")
        .put("latin1", "latin1_bin")
        .put("latin1", "latin1_general_ci")
        .put("latin1", "latin1_general_cs")
        .put("latin1", "latin1_spanish_ci")
        .build();

    @Test
    public void testCase() {
        if (PropertiesUtil.usePrepare()) {
            // current prepare mode does not support collation
            return;
        }
        MAP.forEach(
            (charset, collation) -> {
                for (String op : COMPARE_OPERATORS) {
                    String sql = String.format(COMPARE_CASE_FORMAT, charset, collation, op, charset, collation);
                    selectContentSameAssert(sql, null, tddlConnection, mysqlConnection);
                }
            }
        );
    }

    @Test
    public void testSpace() {
        MAP.forEach(
            (charset, collation) -> {
                for (String op : COMPARE_OPERATORS) {
                    String sql = String.format(COMPARE_SPACE_FORMAT, charset, collation, op, charset, collation);
                    selectContentSameAssert(sql, null, tddlConnection, mysqlConnection);
                }
            }
        );
    }

}
