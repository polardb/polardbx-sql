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

public class MySqlSelectTest_153 extends MysqlTest {

    public void test_0() throws Exception {
        String sql = "SELECT ((layer_1_column_0)|(NULLIF(NULL,null )))FROM (SELECT NULL is NULL AS layer_1_column_0 FROM corona_select_multi_db_one_tb WHERE 'a' AND 'b') AS layer_0_table WHERE ! ~ 25 IS NULL;";
//
        List<SQLStatement> statementList = SQLUtils.parseStatements(sql, JdbcConstants.MYSQL, SQLParserFeature.TDDLHint);
        SQLSelectStatement stmt = (SQLSelectStatement)statementList.get(0);

        assertEquals(1, statementList.size());

        assertEquals("SELECT layer_1_column_0 | NULLIF(NULL, NULL)\n" +
                "FROM (\n" +
                "\tSELECT NULL IS NULL AS layer_1_column_0\n" +
                "\tFROM corona_select_multi_db_one_tb\n" +
                "\tWHERE 'a'\n" +
                "\t\tAND 'b'\n" +
                ") layer_0_table\n" +
                "WHERE (!(~25)) IS NULL;", stmt.toString());

        assertEquals("SELECT layer_1_column_0 | NULLIF(NULL, NULL)\n" +
                        "FROM (\n" +
                        "\tSELECT NULL IS NULL AS layer_1_column_0\n" +
                        "\tFROM corona_select_multi_db_one_tb\n" +
                        "\tWHERE ?\n" +
                        "\t\tAND ?\n" +
                        ") layer_0_table\n" +
                        "WHERE (!(~?)) IS NULL;"
                , ParameterizedOutputVisitorUtils.parameterize(sql, JdbcConstants.MYSQL, VisitorFeature.OutputParameterizedZeroReplaceNotUseOriginalSql));


    }

}