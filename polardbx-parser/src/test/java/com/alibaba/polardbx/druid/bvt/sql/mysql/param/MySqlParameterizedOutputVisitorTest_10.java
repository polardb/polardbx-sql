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

package com.alibaba.polardbx.druid.bvt.sql.mysql.param;

import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlExportParameterVisitor;
import com.alibaba.polardbx.druid.sql.parser.SQLParserUtils;
import com.alibaba.polardbx.druid.sql.parser.SQLStatementParser;
import com.alibaba.polardbx.druid.sql.visitor.ExportParameterVisitor;
import com.alibaba.polardbx.druid.sql.visitor.ParameterizedOutputVisitorUtils;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import junit.framework.TestCase;

import java.util.List;

/**
 * Created by wenshao on 16/8/23.
 */
public class MySqlParameterizedOutputVisitorTest_10 extends TestCase {
    public void test_for_parameterize() throws Exception {
         /*String instance = "100.81.152.9"+"_"+3314;
        int urlNum = Math.abs(instance.hashCode()) % 2;
        System.out.println(urlNum);*/
      /* String formattedSql = SQLUtils.rowFormat("select * from ? where id = ?", JdbcConstants.MYSQL,
                Arrays.<Object> asList("abc,a"));
        System.out.println(formattedSql);*/

        final DbType dbType = JdbcConstants.MYSQL;

        String sql = "SELECT `SURVEY_ANSWER`.`TIME_UPDATED`, `SURVEY_ANSWER`.`ANSWER_VALUE` FROM `S_ANSWER_P0115` `SURVEY_ANSWER` WHERE `SURVEY_ANSWER`.`SURVEY_ID` = 11 AND `SURVEY_ANSWER`.`QUESTION_CODE` = 'qq' ORDER BY `SURVEY_ANSWER`.`TIME_UPDATED` DESC LIMIT 1, 2";
        String psql = ParameterizedOutputVisitorUtils.parameterize(sql, dbType);
       /* Assert.assertEquals("SELECT *\n" +
                "FROM t\n" +
                "LIMIT ?, ?", psql);*/

        System.out.println(psql);

        SQLStatementParser parser = SQLParserUtils.createSQLStatementParser(sql, dbType);
        List<SQLStatement> stmtList = parser.parseStatementList();

        StringBuilder out = new StringBuilder();
        ExportParameterVisitor visitor = new MySqlExportParameterVisitor(out);
        for (SQLStatement stmt : stmtList) {
            stmt.accept(visitor);
        }

        System.out.println(visitor.getParameters());

        stmtList = SQLUtils.parseStatements(sql, JdbcConstants.MYSQL);

        visitor = new MySqlExportParameterVisitor();
        for (SQLStatement stmt : stmtList) {
            stmt.accept(visitor);
        }
        System.out.println(visitor.getParameters());
      /*  Assert.assertEquals(2, visitor.getArguments().size());
        Assert.assertEquals(3, visitor.getArguments().get(0));
        Assert.assertEquals(4, visitor.getArguments().get(1));*/
    }
}
