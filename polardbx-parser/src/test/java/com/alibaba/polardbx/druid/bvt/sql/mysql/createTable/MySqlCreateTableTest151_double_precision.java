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
 * @ClassName MySqlCreateTableTest151_double_precision
 * @description
 * @Author zzy
 * @Date 2019-05-15 14:07
 */
public class MySqlCreateTableTest151_double_precision extends TestCase {

    public void test_0() {
        String sql = "create temporary table tb_etaqf (\n" +
                "\t `col_mcdw` double precision(10,2)\n" +
                ")";

        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();
        MySqlCreateTableStatement stmt = (MySqlCreateTableStatement)statementList.get(0);

        assertEquals(1, statementList.size());

        assertEquals("CREATE TEMPORARY TABLE tb_etaqf (\n" +
                "\t`col_mcdw` double precision(10, 2)\n" +
                ")", stmt.toString());

        assertEquals("create temporary table tb_etaqf (\n" +
                "\t`col_mcdw` double precision(10, 2)\n" +
                ")", stmt.toLowerCaseString());
    }

}
