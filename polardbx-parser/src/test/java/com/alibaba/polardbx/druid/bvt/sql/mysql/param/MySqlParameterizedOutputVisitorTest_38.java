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
import com.alibaba.polardbx.druid.sql.parser.SQLParserUtils;
import com.alibaba.polardbx.druid.sql.parser.SQLStatementParser;
import com.alibaba.polardbx.druid.sql.visitor.SQLASTOutputVisitor;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import junit.framework.TestCase;

import java.util.List;

/**
 * Created by wenshao on 16/8/23.
 */
public class MySqlParameterizedOutputVisitorTest_38 extends TestCase {
    public void test_for_parameterize() throws Exception {
        final DbType dbType = JdbcConstants.MYSQL;
        String sql = "SELECT lower(hex(file_md5)) as file_md5,\n" +
                "        lower(hex(thumb)) as thumb,st\n" +
                "        FROM t_f_p_thumb\n" +
                "        WHERE file_md5 = x'84C1F969587F5FD1942148EE9D36A0FB'";

        SQLStatementParser parser = SQLParserUtils.createSQLStatementParser(sql, dbType);
        List<SQLStatement> stmtList = parser.parseStatementList();
        SQLStatement statement = stmtList.get(0);

        StringBuilder out = new StringBuilder();
        //  List<Object> parameters = new ArrayList<Object>();
        SQLASTOutputVisitor visitor = SQLUtils.createOutputVisitor(out, JdbcConstants.MYSQL);
        visitor.setParameterized(true);
        visitor.setParameterizedMergeInList(true);
        //   visitor.setParameters(parameters);
        visitor.setExportTables(true);
        visitor.setPrettyFormat(true);
        statement.accept(visitor);
        assertEquals("SELECT lower(hex(file_md5)) AS file_md5\n" +
                "\t, lower(hex(thumb)) AS thumb, st\n" +
                "FROM t_f_p_thumb\n" +
                "WHERE file_md5 = ?", out.toString());
    }
}
