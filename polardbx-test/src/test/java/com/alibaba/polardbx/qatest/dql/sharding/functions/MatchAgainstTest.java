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

package com.alibaba.polardbx.qatest.dql.sharding.functions;

import com.alibaba.polardbx.qatest.CrudBasedLockTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;


public class MatchAgainstTest extends CrudBasedLockTestCase {
    private String suffix;
    private static final String TABLE_PREFIX = "articles";
    private static final String CREATE_TABLE = "CREATE TABLE IF NOT EXISTS %s (\n"
        + "    pk INT UNSIGNED AUTO_INCREMENT NOT NULL PRIMARY KEY,\n"
        + "    title VARCHAR(200),\n"
        + "    body TEXT,\n"
        + "    FULLTEXT (title, body)\n"
        + ") %s";
    private static final String INSERT_SQL_FORMAT = "INSERT INTO\n"
        + "    %s (title, body)\n"
        + "VALUES\n"
        + "    ('MySQL Tutorial', 'DBMS stands for DataBase ...'),\n"
        + "    (\n"
        + "        'How To Use MySQL Well',\n"
        + "        'After you went through a ...'\n"
        + "    ),\n"
        + "    (\n"
        + "        'Optimizing MySQL',\n"
        + "        'In this tutorial we will show ...'\n"
        + "    ),\n"
        + "    (\n"
        + "        '1001 MySQL Tricks',\n"
        + "        '1. Never run mysqld as root. 2. ...'\n"
        + "    ),\n"
        + "    (\n"
        + "        'MySQL vs. YourSQL',\n"
        + "        'In the following database comparison ...'\n"
        + "    ),\n"
        + "    (\n"
        + "        'MySQL Security',\n"
        + "        'When configured properly, MySQL ...'\n"
        + "    )";
    private static final String[] QUERY_SQL_FORMATS = new String[] {
        "SELECT title, body FROM %s WHERE MATCH (title, body) AGAINST ('database')",
        "SELECT title, body FROM %s WHERE MATCH (title, body) AGAINST ('database' IN NATURAL LANGUAGE MODE)",
        "SELECT title, body FROM %s WHERE MATCH (title, body) AGAINST ('database' IN BOOLEAN MODE)"
    };

    @Parameterized.Parameters(name = "{index}:table0={0},table1={1}")
    public static List<String[]> prepare() {
        return Arrays.asList(
            new String[] {TABLE_PREFIX + "_one_db_one_tb", ""},
            new String[] {TABLE_PREFIX + "_one_db_multi_tb", "tbpartition by hash(`pk`) tbpartitions 4"},
            new String[] {TABLE_PREFIX + "_multi_db_one_tb", "dbpartition by hash(`pk`)"},
            new String[] {
                TABLE_PREFIX + "_multi_db_multi_tb",
                "dbpartition by hash(`pk`) tbpartition by hash(`pk`) tbpartitions 4"},
            new String[] {TABLE_PREFIX + "_broadcast", "broadcast"}
        );
    }

    public MatchAgainstTest(String baseOneTableName, String suffix) {
        this.baseOneTableName = baseOneTableName;
        this.suffix = suffix;
    }

    @Before
    public void preparMySQLTable() {
        String mysqlSql = String.format(CREATE_TABLE, baseOneTableName, "");
        JdbcUtil.executeSuccess(mysqlConnection, mysqlSql);
        JdbcUtil.executeSuccess(mysqlConnection, String.format("truncate table %s", baseOneTableName));
    }

    @Before
    public void preparePolarDBXTable() {
        String tddlSql = String.format(CREATE_TABLE, baseOneTableName, suffix);
        JdbcUtil.executeSuccess(tddlConnection, tddlSql);
        JdbcUtil.executeSuccess(tddlConnection, String.format("truncate table %s", baseOneTableName));
    }

    @Test
    public void test() throws SQLException {
        JdbcUtil.executeUpdate(mysqlConnection, String.format(INSERT_SQL_FORMAT, baseOneTableName));
        JdbcUtil.executeUpdate(tddlConnection, String.format(INSERT_SQL_FORMAT, baseOneTableName));
        Arrays.stream(QUERY_SQL_FORMATS)
            .map(format -> String.format(format, baseOneTableName))
            .forEach(sql -> selectContentSameAssert(sql, null, mysqlConnection, tddlConnection));
    }
}
