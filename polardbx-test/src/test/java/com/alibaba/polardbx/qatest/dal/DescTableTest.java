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

package com.alibaba.polardbx.qatest.dal;

import com.alibaba.polardbx.qatest.ReadBaseTestCase;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.qatest.data.ExecuteTableSelect.selectBaseOneTable;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

/**
 * @author bairui
 * @since 5.4.9
 */

public class DescTableTest extends ReadBaseTestCase {
    private final String table;

    public DescTableTest(String table) {
        this.table = table;
    }

    @Parameterized.Parameters(name = "{index}:table={0}")
    public static List<String[]> generateParameters() {
        return Arrays.asList(selectBaseOneTable());
    }

    @Test
    public void testWithoutSchema() {
        String sql = "desc `" + table + "`";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void testWithEmptyCurrentSchema() {
        useDb(tddlConnection, polardbxOneDB2);
        String tddlSql = String.format("desc `%s`.`%s`", polardbxOneDB, table);
        String mysqlSql = String.format("desc `%s`.`%s`", mysqlOneDB, table);
        selectContentSameAssert(mysqlSql, tddlSql, null, mysqlConnection,
            tddlConnection);
    }
}
