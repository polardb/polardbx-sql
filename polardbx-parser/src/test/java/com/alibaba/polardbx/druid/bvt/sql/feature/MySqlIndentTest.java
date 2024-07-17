/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.polardbx.druid.bvt.sql.feature;

import com.alibaba.polardbx.druid.sql.MysqlTest;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateTableStatement;
import com.alibaba.polardbx.druid.sql.visitor.VisitorFeature;
import org.junit.Test;

public class MySqlIndentTest extends MysqlTest {

    @Test
    public void test_one() throws Exception {
        String sql = "create table tmp_eric (pk int key, ia int unique);";

        SQLStatement stmt = SQLUtils.parseSingleMysqlStatement(sql);
        assertEquals("CREATE TABLE tmp_eric (\n" +
            "  pk int PRIMARY KEY,\n" +
            "  ia int UNIQUE\n" +
            ");", stmt.toString(VisitorFeature.OutputMySQLIndentString));
    }

    @Test
    public void test_two() throws Exception {
        String sql = "create table tmp_eric (pk int key, ia int unique);";

        SQLCreateTableStatement stmt = (SQLCreateTableStatement) SQLUtils.parseSingleMysqlStatement(sql);
        assertEquals("CREATE TABLE tmp_eric (\n" +
            "  pk int PRIMARY KEY,\n" +
            "  ia int UNIQUE\n" +
            ");", stmt.toSqlString(true, true));

        assertEquals("CREATE TABLE tmp_eric (\n" +
            "\tpk int PRIMARY KEY,\n" +
            "\tia int UNIQUE\n" +
            ");", stmt.toSqlString(false, false));
    }
}
