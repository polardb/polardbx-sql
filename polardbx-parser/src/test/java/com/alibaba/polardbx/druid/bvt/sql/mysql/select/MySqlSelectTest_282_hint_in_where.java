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

package com.alibaba.polardbx.druid.bvt.sql.mysql.select;

import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.MysqlTest;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.parser.SQLParserFeature;

/**
 * @version 1.0
 * @ClassName MySqlSelectTest_282_hint_in_where
 * @description
 * @Author zzy
 * @Date 2019-05-23 15:28
 */
public class MySqlSelectTest_282_hint_in_where extends MysqlTest {

    public void test_0() throws Exception {
        String sql = "select `api_menu_groups`.* from `api_menu_groups` where 1 = 1 /*TDDL:MASTER*/ and `api_menu_groups`.`project_id` = 3 order by `api_menu_groups`.`sort` asc limit 10000\n";

        SQLStatement stmt = SQLUtils
                .parseSingleStatement(sql, DbType.mysql, SQLParserFeature.TDDLHint);

        assertEquals("SELECT /*TDDL:MASTER*/ `api_menu_groups`.*\n" +
                "FROM `api_menu_groups`\n" +
                "WHERE 1 = 1\n" +
                "\tAND `api_menu_groups`.`project_id` = 3\n" +
                "ORDER BY `api_menu_groups`.`sort` ASC\n" +
                "LIMIT 10000", stmt.toString());
    }

    public void test_1() throws Exception {
        String sql = "select `api_menu_groups`.* from `api_menu_groups` where 1 = 1 /!TDDL:MASTER*/ and `api_menu_groups`.`project_id` = 3 order by `api_menu_groups`.`sort` asc limit 10000\n";

        SQLStatement stmt = SQLUtils
                .parseSingleStatement(sql, DbType.mysql, SQLParserFeature.TDDLHint);

        assertEquals("SELECT /*TDDL:MASTER*/ `api_menu_groups`.*\n" +
                "FROM `api_menu_groups`\n" +
                "WHERE 1 = 1\n" +
                "\tAND `api_menu_groups`.`project_id` = 3\n" +
                "ORDER BY `api_menu_groups`.`sort` ASC\n" +
                "LIMIT 10000", stmt.toString());
    }

    public void test_2() throws Exception {
        String sql = "select `api_menu_groups`.* from `api_menu_groups` where 1 = 1 /*+TDDL:MASTER*/ and `api_menu_groups`.`project_id` = 3 order by `api_menu_groups`.`sort` asc limit 10000\n";

        SQLStatement stmt = SQLUtils
                .parseSingleStatement(sql, DbType.mysql, SQLParserFeature.TDDLHint);

        assertEquals("SELECT /*+TDDL:MASTER*/ `api_menu_groups`.*\n" +
                "FROM `api_menu_groups`\n" +
                "WHERE 1 = 1\n" +
                "\tAND `api_menu_groups`.`project_id` = 3\n" +
                "ORDER BY `api_menu_groups`.`sort` ASC\n" +
                "LIMIT 10000", stmt.toString());
    }

    public void test_3() throws Exception {
        String sql = "select `api_menu_groups`.* from `api_menu_groups` where 1 = 1 /!+TDDL:MASTER*/ and `api_menu_groups`.`project_id` = 3 order by `api_menu_groups`.`sort` asc limit 10000\n";

        SQLStatement stmt = SQLUtils
                .parseSingleStatement(sql, DbType.mysql, SQLParserFeature.TDDLHint);

        assertEquals("SELECT /*+TDDL:MASTER*/ `api_menu_groups`.*\n" +
                "FROM `api_menu_groups`\n" +
                "WHERE 1 = 1\n" +
                "\tAND `api_menu_groups`.`project_id` = 3\n" +
                "ORDER BY `api_menu_groups`.`sort` ASC\n" +
                "LIMIT 10000", stmt.toString());
    }
}
