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
 * @ClassName MySqlCreateTableTest149_collate_before_generated
 * @description
 *
 *   | data_type
 *       [COLLATE collation_name]
 *       [GENERATED ALWAYS] AS (expr)
 *       [VIRTUAL | STORED] [NOT NULL | NULL]
 *       [UNIQUE [KEY]] [[PRIMARY] KEY]
 *       [COMMENT 'string']
 *       [reference_definition]
 *
 * @Author zzy
 * @Date 2019-05-14 17:41
 */
public class MySqlCreateTableTest149_collate_before_generated extends TestCase {

    public void test_0() {
        String sql = "create temporary table `tb_dhma` (col_oxqagw int collate utf8_unicode_ci generated always as ( 1+2 ))";

        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();
        MySqlCreateTableStatement stmt = (MySqlCreateTableStatement)statementList.get(0);

        assertEquals(1, statementList.size());

        assertEquals("CREATE TEMPORARY TABLE `tb_dhma` (\n" +
                "\tcol_oxqagw int GENERATED ALWAYS AS (1 + 2) COLLATE utf8_unicode_ci\n" +
                ")", stmt.toString());

        assertEquals("create temporary table `tb_dhma` (\n" +
                "\tcol_oxqagw int generated always as (1 + 2) collate utf8_unicode_ci\n" +
                ")", stmt.toLowerCaseString());
    }

}
