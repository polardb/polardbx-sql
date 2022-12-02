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
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.util.List;

import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

public class CharsetFunctionTest extends CharsetTestBase {
    @Parameterized.Parameters(name = "{index}:table0={0},table1={1}")
    public static List<String[]> prepare() {
        return PARAMETERS;
    }

    public CharsetFunctionTest(String table, String suffix) {
        this.table = randomTableName(table, 4);
        this.suffix = suffix;
    }

    @Test
    public void testCharset() {
        if (PropertiesUtil.usePrepare()) {
            // current prepare mode does not support charset
            return;
        }
        String sql = "select charset(_latin1'abc' collate latin1_general_cs)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void testCollation() {
        if (PropertiesUtil.usePrepare()) {
            // current prepare mode does not support collation
            return;
        }
        String sql = "select collation(_latin1'abc' collate latin1_general_cs)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }
}
