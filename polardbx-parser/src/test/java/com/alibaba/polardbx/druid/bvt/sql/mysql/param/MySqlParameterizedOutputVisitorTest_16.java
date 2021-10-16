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
import junit.framework.TestCase;

import java.util.List;

/**
 * Created by wenshao on 16/8/23.
 */
public class MySqlParameterizedOutputVisitorTest_16 extends TestCase {
    public void test_for_parameterize() throws Exception {
        final DbType dbType = JdbcConstants.MYSQL;

        String sql = "/* 0bfacfa414829200086238910e/0.3// */" +
                "insert into `t1` (" +
                " `f0`, `f1`, `f2`, `f3`, `f4`, " +
                "`f5`, `f6`, `f7`, `f8`, `f9`, " +
                "`destination`, `start_standard`, `start_fee`, `add_standard`, `add_fee`, " +
                "`region_fee_standard`, `region_fee_add`, `cell_fee`, `way_day`, `version`)" +
                " values ( 1, 2, 2, 3, 0, -4, 1, null, '2016-12-28 18:13:28.825', '2016-12-28 18:13:28.825', 1, 1, 0, 1, 0, null, null, null, null, 0)\n";
        String psql = ParameterizedOutputVisitorUtils.parameterize(sql, dbType);

        SQLStatementParser parser = SQLParserUtils.createSQLStatementParser(sql, dbType);
        List<SQLStatement> stmtList = parser.parseStatementList();

        assertEquals("INSERT INTO `t1` (`f0`, `f1`, `f2`, `f3`, `f4`\n" +
                "\t, `f5`, `f6`, `f7`, `f8`, `f9`\n" +
                "\t, `destination`, `start_standard`, `start_fee`, `add_standard`, `add_fee`\n" +
                "\t, `region_fee_standard`, `region_fee_add`, `cell_fee`, `way_day`, `version`)\n" +
                "VALUES (?, ?, ?, ?, ?\n" +
                "\t, ?, ?, ?, ?, ?\n" +
                "\t, ?, ?, ?, ?, ?\n" +
                "\t, ?, ?, ?, ?, ?)", psql);



        StringBuilder out = new StringBuilder();
        ExportParameterVisitor visitor = new MySqlExportParameterVisitor(out);
        for (SQLStatement stmt : stmtList) {
            stmt.accept(visitor);
        }
        assertEquals(20, visitor.getParameters().size());
    }
}
