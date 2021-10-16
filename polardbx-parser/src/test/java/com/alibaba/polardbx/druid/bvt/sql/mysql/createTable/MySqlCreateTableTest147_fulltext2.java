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

public class MySqlCreateTableTest147_fulltext2 extends TestCase {

    public void test_0() throws Exception {
        String sql = "CREATE TABLE aliyun_poc_db.tbl_custom_analyzer2 (\n" +
                "  `id` int COMMENT '',\n" +
                "  `title` varchar COMMENT '',\n" +
                "  FULLTEXT INDEX title_fulltext_idx (title) WITH INDEX ANALYZER index_analyzer2 WITH QUERY ANALYZER query_analyzer2 WITH DICT user_dict,\n" +
                "  PRIMARY KEY (`id`)\n" +
                ")DISTRIBUTED BY HASH(`id`);";

        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();
        MySqlCreateTableStatement stmt = (MySqlCreateTableStatement)statementList.get(0);

        assertEquals(1, statementList.size());

        assertEquals("CREATE TABLE aliyun_poc_db.tbl_custom_analyzer2 (\n" +
                "\t`id` int COMMENT '',\n" +
                "\t`title` varchar COMMENT '',\n" +
                "\tFULLTEXT INDEX title_fulltext_idx(title) WITH INDEX ANALYZER index_analyzer2 WITH QUERY ANALYZER query_analyzer2 WITH DICT user_dict,\n" +
                "\tPRIMARY KEY (`id`)\n" +
                ")\n" +
                "DISTRIBUTE BY HASH(`id`);", stmt.toString());

        assertEquals("create table aliyun_poc_db.tbl_custom_analyzer2 (\n" +
                "\t`id` int comment '',\n" +
                "\t`title` varchar comment '',\n" +
                "\tfulltext index title_fulltext_idx(title) with index analyzer index_analyzer2 with query analyzer query_analyzer2 with dict user_dict,\n" +
                "\tprimary key (`id`)\n" +
                ")\n" +
                "distribute by hash(`id`);", stmt.toLowerCaseString());
    }

}
