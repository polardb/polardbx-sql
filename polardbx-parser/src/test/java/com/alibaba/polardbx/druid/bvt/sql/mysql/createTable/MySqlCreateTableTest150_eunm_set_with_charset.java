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

package com.alibaba.polardbx.druid.bvt.sql.mysql.createTable;

import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import junit.framework.TestCase;

import java.util.List;

/**
 * @version 1.0
 * @ClassName MySqlCreateTableTest150_eunm_set_with_charset
 * @description
 * @Author zzy
 * @Date 2019-05-15 10:30
 */
public class MySqlCreateTableTest150_eunm_set_with_charset extends TestCase {

    public void test_0() {
        String sql = "create temporary table `tb_xx` (\n" +
                "`col_ttuap` enum('value1','value2') character set utf8  collate utf8_unicode_ci generated always as ( 1+2 ) virtual unique comment 'comment' references tb_fn ( `col_qzqnqrrfyv` ) match partial\n" +
                ") min_rows = 1 , checksum = 1 , compression 'NONE'";

        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();
        MySqlCreateTableStatement stmt = (MySqlCreateTableStatement)statementList.get(0);

        assertEquals(1, statementList.size());

        assertEquals("CREATE TEMPORARY TABLE `tb_xx` (\n" +
                "\t`col_ttuap` enum('value1', 'value2') GENERATED ALWAYS AS (1 + 2) VIRTUAL CHARACTER SET utf8 COLLATE utf8_unicode_ci UNIQUE COMMENT 'comment' REFERENCES tb_fn (`col_qzqnqrrfyv`) MATCH PARTIAL\n" +
                ") MIN_ROWS = 1 CHECKSUM = 1 COMPRESSION = 'NONE'", stmt.toString());

        assertEquals("create temporary table `tb_xx` (\n" +
                "\t`col_ttuap` enum('value1', 'value2') generated always as (1 + 2) virtual character set utf8 collate utf8_unicode_ci unique comment 'comment' references tb_fn (`col_qzqnqrrfyv`) match partial\n" +
                ") min_rows = 1 checksum = 1 compression = 'NONE'", stmt.toLowerCaseString());
    }

    public void test_1() {
        String sql = "create temporary table `tb_xx` (\n" +
                "`col_ttuap` set('value1','value2') character set utf8  collate utf8_unicode_ci generated always as ( 1+2 ) virtual unique comment 'comment' references tb_fn ( `col_qzqnqrrfyv` ) match partial\n" +
                ") min_rows = 1 , checksum = 1 , compression 'NONE'";

        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();
        MySqlCreateTableStatement stmt = (MySqlCreateTableStatement)statementList.get(0);

        assertEquals(1, statementList.size());

        assertEquals("CREATE TEMPORARY TABLE `tb_xx` (\n" +
                "\t`col_ttuap` set('value1', 'value2') GENERATED ALWAYS AS (1 + 2) VIRTUAL CHARACTER SET utf8 COLLATE utf8_unicode_ci UNIQUE COMMENT 'comment' REFERENCES tb_fn (`col_qzqnqrrfyv`) MATCH PARTIAL\n" +
                ") MIN_ROWS = 1 CHECKSUM = 1 COMPRESSION = 'NONE'", stmt.toString());

        assertEquals("create temporary table `tb_xx` (\n" +
                "\t`col_ttuap` set('value1', 'value2') generated always as (1 + 2) virtual character set utf8 collate utf8_unicode_ci unique comment 'comment' references tb_fn (`col_qzqnqrrfyv`) match partial\n" +
                ") min_rows = 1 checksum = 1 compression = 'NONE'", stmt.toLowerCaseString());
    }

}
