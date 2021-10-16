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

import com.alibaba.polardbx.druid.sql.MysqlTest;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.polardbx.druid.util.JdbcConstants;

import java.util.List;

public class MySqlSelectTest_166_xor extends MysqlTest {

    public void test_0() throws Exception {
        String sql = "select 1 from corona_select_multi_db_multi_tb where (1 XOR 2 ) between 3 and 4";
//
        List<SQLStatement> statementList = SQLUtils.parseStatements(sql, JdbcConstants.MYSQL);
        SQLSelectStatement stmt = (SQLSelectStatement)statementList.get(0);

        assertEquals(1, statementList.size());

        assertEquals("SELECT 1\n" +
                "FROM corona_select_multi_db_multi_tb\n" +
                "WHERE (1 XOR 2) BETWEEN 3 AND 4", stmt.toString());
    }

    public void test_1() throws Exception {
        String sql = "select * from t where id != 4 or id = 4 xor (id = 4 or id < 4)";
//
        List<SQLStatement> statementList = SQLUtils.parseStatements(sql, JdbcConstants.MYSQL);
        SQLSelectStatement stmt = (SQLSelectStatement)statementList.get(0);

        assertEquals(1, statementList.size());

        assertEquals("SELECT *\n" +
                "FROM t\n" +
                "WHERE id != 4\n" +
                "\tOR id = 4 XOR (id = 4\n" +
                "\t\tOR id < 4)", stmt.toString());
    }

    public void test_2() throws Exception {
        String sql = "SELECT ((10)!=(( (HEX ('abc' )) is FALSE )) ),( (( (( (( 'a')&& (null )) not in(layer_1_left_tb.bigint_test, layer_0_right_tb.smallint_test,layer_1_left_tb.datetime_test,( EXPORT_SET (6, layer_1_left_tb.year_test,'0', ':', 66 ))) )) <(layer_1_left_tb.decimal_test) )) not in(layer_1_left_tb.mediumint_test, layer_1_left_tb.double_test,layer_1_left_tb.year_test,( 1+'1')) )FROM corona_select_one_db_multi_tb AS layer_1_left_tb RIGHT JOIN corona_select_multi_db_one_tb AS layer_1_right_tb ON layer_1_right_tb.datetime_test=layer_1_left_tb.datetime_test RIGHT JOIN corona_select_one_db_multi_tb AS layer_0_right_tb ON layer_0_right_tb.tinyint_1bit_test=layer_1_left_tb.tinyint_1bit_test;";
//
        List<SQLStatement> statementList = SQLUtils.parseStatements(sql, JdbcConstants.MYSQL);
        SQLSelectStatement stmt = (SQLSelectStatement)statementList.get(0);

        assertEquals(1, statementList.size());

        assertEquals("SELECT 10 != (HEX('abc') IS false), ((('a'\n" +
                "\tAND NULL) NOT IN (layer_1_left_tb.bigint_test, layer_0_right_tb.smallint_test, layer_1_left_tb.datetime_test, EXPORT_SET(6, layer_1_left_tb.year_test, '0', ':', 66))) < layer_1_left_tb.decimal_test) NOT IN (layer_1_left_tb.mediumint_test, layer_1_left_tb.double_test, layer_1_left_tb.year_test, 1 + '1')\n" +
                "FROM corona_select_one_db_multi_tb layer_1_left_tb\n" +
                "\tRIGHT JOIN corona_select_multi_db_one_tb layer_1_right_tb ON layer_1_right_tb.datetime_test = layer_1_left_tb.datetime_test\n" +
                "\tRIGHT JOIN corona_select_one_db_multi_tb layer_0_right_tb ON layer_0_right_tb.tinyint_1bit_test = layer_1_left_tb.tinyint_1bit_test;", stmt.toString());

    }

}