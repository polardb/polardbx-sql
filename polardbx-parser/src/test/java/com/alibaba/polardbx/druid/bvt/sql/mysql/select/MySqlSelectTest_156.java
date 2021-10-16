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
import com.alibaba.polardbx.druid.sql.parser.SQLParserFeature;
import com.alibaba.polardbx.druid.sql.visitor.ParameterizedOutputVisitorUtils;
import com.alibaba.polardbx.druid.sql.visitor.VisitorFeature;
import com.alibaba.polardbx.druid.util.JdbcConstants;

import java.util.List;

public class MySqlSelectTest_156 extends MysqlTest {

    public void test_0() throws Exception {
        String sql = "SELECT SQL_SMALL_RESULT ((NULL) is  not  FALSE) \n" +
                "FROM corona_select_multi_db_one_tb AS layer_0_left_tb \n" +
                "RIGHT JOIN corona_select_one_db_multi_tb AS layer_0_right_tb \n" +
                "   ON layer_0_right_tb.smallint_test=layer_0_right_tb.date_test \n" +
                "WHERE layer_0_right_tb.time_test='x6' NOT BETWEEN 96 AND layer_0_right_tb.bigint_test;\n";
//
        List<SQLStatement> statementList = SQLUtils.parseStatements(sql, JdbcConstants.MYSQL, SQLParserFeature.TDDLHint);
        SQLSelectStatement stmt = (SQLSelectStatement)statementList.get(0);

        assertEquals(1, statementList.size());

        assertEquals("SELECT SQL_SMALL_RESULT NULL IS NOT false\n" +
                "FROM corona_select_multi_db_one_tb layer_0_left_tb\n" +
                "\tRIGHT JOIN corona_select_one_db_multi_tb layer_0_right_tb ON layer_0_right_tb.smallint_test = layer_0_right_tb.date_test\n" +
                "WHERE layer_0_right_tb.time_test = 'x6' NOT BETWEEN 96 AND layer_0_right_tb.bigint_test;", stmt.toString());

        assertEquals("SELECT SQL_SMALL_RESULT NULL IS NOT false\n" +
                        "FROM corona_select_multi_db_one_tb layer_0_left_tb\n" +
                        "\tRIGHT JOIN corona_select_one_db_multi_tb layer_0_right_tb ON layer_0_right_tb.smallint_test = layer_0_right_tb.date_test\n" +
                        "WHERE layer_0_right_tb.time_test = ? NOT BETWEEN ? AND layer_0_right_tb.bigint_test;"
                , ParameterizedOutputVisitorUtils.parameterize(sql, JdbcConstants.MYSQL, VisitorFeature.OutputParameterizedZeroReplaceNotUseOriginalSql));


    }

}