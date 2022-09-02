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

package com.alibaba.polardbx.qatest.ddl.sharding.ddl;

import com.alibaba.polardbx.qatest.AsyncDDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.dql.sharding.join.JoinUtils;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.util.List;

/**
 * 建表测试
 *
 * @author simiao
 * @since 14-11-27
 */

public class CreateSpecialTableTest extends AsyncDDLBaseNewDBTestCase {

    private static String testTableName = "gxw_test";

    static final String[] tableNames = {
        "`" + testTableName + "-minus`",
        "`" + testTableName + "``backtick`",
        "```" + testTableName + "-minus`",
        "```" + testTableName + "``backtick`",
        "```" + testTableName + "-minus```",
        "```" + testTableName + "``backtick```",
    };

    static final String[] colNames = {
        "`col-minus`",
        "`col``backtick`",
        "```col-minus`",
        "```col``backtick`",
        "```col-minus```",
        "```col``backtick```",
    };

    String tableName;
    String colName;

    public CreateSpecialTableTest(String tableName, String columnName) {
        this.tableName = tableName;
        this.colName = columnName;
    }

    @Parameterized.Parameters(name = "{index}:tableName={0}, colName={1}")
    public static List<Object[]> initParameters() {
        return JoinUtils.cartesianProduct(tableNames, colNames);
    }

    @Test
    public void testTable() {
        dropTableIfExists(tableName);

        String insert = "insert into " + tableName + " (" + colName + ",c2) values (1,2)";
        String sel = "select " + colName + " from " + tableName;
        String alterSql = "ALTER TABLE " + tableName + " add c3 int";

        String createSql = "CREATE TABLE IF NOT EXISTS " + tableName + " (" + colName + " int, c2 int)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);
        JdbcUtil.executeUpdateSuccess(tddlConnection, insert);
        JdbcUtil.executeQuerySuccess(tddlConnection, sel);
        JdbcUtil.executeUpdateSuccess(tddlConnection, alterSql);
        dropTableIfExists(tableName);

        createSql = "CREATE TABLE IF NOT EXISTS " + tableName + " (" + colName + " int, c2 int) broadcast";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);
        JdbcUtil.executeUpdateSuccess(tddlConnection, insert);
        JdbcUtil.executeQuerySuccess(tddlConnection, sel);
        JdbcUtil.executeUpdateSuccess(tddlConnection, alterSql);
        dropTableIfExists(tableName);

        createSql =
            "CREATE TABLE IF NOT EXISTS " + tableName + " (" + colName
                + " int, c2 int) dbpartition by hash(" + colName + ")";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);
        JdbcUtil.executeUpdateSuccess(tddlConnection, insert);
        JdbcUtil.executeQuerySuccess(tddlConnection, sel);
        JdbcUtil.executeUpdateSuccess(tddlConnection, alterSql);
        dropTableIfExists(tableName);

        createSql =
            "CREATE TABLE IF NOT EXISTS " + tableName + " (" + colName + " int, c2 int) dbpartition by hash("
                + colName + ")"
                + " tbpartition by hash(" + colName + ") tbpartitions 2";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);
        JdbcUtil.executeUpdateSuccess(tddlConnection, insert);
        JdbcUtil.executeQuerySuccess(tddlConnection, sel);
        JdbcUtil.executeUpdateSuccess(tddlConnection, alterSql);
        dropTableIfExists(tableName);
    }
}
