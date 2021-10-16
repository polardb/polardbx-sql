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
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlExportParameterVisitor;
import com.alibaba.polardbx.druid.sql.parser.SQLParserUtils;
import com.alibaba.polardbx.druid.sql.parser.SQLStatementParser;
import com.alibaba.polardbx.druid.sql.visitor.ExportParameterVisitor;
import com.alibaba.polardbx.druid.sql.visitor.ParameterizedOutputVisitorUtils;
import com.alibaba.polardbx.druid.util.JdbcConstants;

import java.util.List;

/**
 * Created by wenshao on 16/8/23.
 */
public class MySqlParameterizedOutputVisitorTest_17 extends MySQLParameterizedTest {
    public void test_for_parameterize() throws Exception {
        final DbType dbType = JdbcConstants.MYSQL;

        String sql = "replace into `mytable_0228` " +
                "( `user_id`, `c_level`, `l_level`, `t_level`, `v_level`, `tag`) " +
                "values ( 2272895716, 'C1', null, 'T1', 'V0', '0') ";
        String psql = ParameterizedOutputVisitorUtils.parameterize(sql, dbType);
        assertEquals("REPLACE INTO mytable (`user_id`, `c_level`, `l_level`, `t_level`, `v_level`, `tag`)\n" +
                "VALUES (?, ?, ?, ?, ?\n" +
                "\t, ?)", psql);

        paramaterizeAST(sql, "REPLACE INTO `mytable_0228` (`user_id`, `c_level`, `l_level`, `t_level`, `v_level`, `tag`)\n" +
                "VALUES (?, ?, NULL, ?, ?\n" +
                "\t, ?)");

        SQLStatementParser parser = SQLParserUtils.createSQLStatementParser(sql, dbType);
        List<SQLStatement> stmtList = parser.parseStatementList();

        StringBuilder out = new StringBuilder();
        ExportParameterVisitor visitor = new MySqlExportParameterVisitor(out);
        for (SQLStatement stmt : stmtList) {
            stmt.accept(visitor);
        }
        assertEquals(6, visitor.getParameters().size());
    }
}
