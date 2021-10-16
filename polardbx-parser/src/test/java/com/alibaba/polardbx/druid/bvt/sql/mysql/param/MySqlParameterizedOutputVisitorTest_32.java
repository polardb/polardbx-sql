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
import com.alibaba.polardbx.druid.sql.visitor.ParameterizedOutputVisitorUtils;
import com.alibaba.polardbx.druid.sql.visitor.SQLASTOutputVisitor;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import junit.framework.TestCase;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by wenshao on 16/8/23.
 */
public class MySqlParameterizedOutputVisitorTest_32 extends TestCase {
    public void test_for_parameterize() throws Exception {
        final DbType dbType = JdbcConstants.MYSQL;

        String sql = "/* cds internal mark */select count(*) as count  from (" +
                "( select env_type from `tmall_miaoscm`.`miao_sale_ledger_0060` where `id` > 1 limit 136 )" +
                " union all ( select env_type from `tmall_miaoscm`.`miao_sale_ledger_0060` where `id` > 2331 limit 136 )" +
                ") as miao_sale_ledger_0060 where `miao_sale_ledger_0060`.`env_type` = 3";

        String psql = ParameterizedOutputVisitorUtils.parameterize(sql, dbType);
        assertEquals("SELECT count(*) AS count\n" +
                "FROM (\n" +
                "\t(SELECT env_type\n" +
                "\tFROM `tmall_miaoscm`.miao_sale_ledger\n" +
                "\tWHERE `id` > ?\n" +
                "\tLIMIT ?)\n" +
                "\tUNION ALL\n" +
                "\t(SELECT env_type\n" +
                "\tFROM `tmall_miaoscm`.miao_sale_ledger\n" +
                "\tWHERE `id` > ?\n" +
                "\tLIMIT ?)\n" +
                ") miao_sale_ledger_0060\n" +
                "WHERE `miao_sale_ledger_0060`.`env_type` = ?", psql);

        SQLStatementParser parser = SQLParserUtils.createSQLStatementParser(sql, dbType);
        List<SQLStatement> stmtList = parser.parseStatementList();

        StringBuilder out = new StringBuilder();
        SQLASTOutputVisitor visitor = SQLUtils.createOutputVisitor(out, JdbcConstants.MYSQL);
        List<Object> parameters = new ArrayList<Object>();
        visitor.setParameterized(true);
        visitor.setParameterizedMergeInList(true);
        visitor.setParameters(parameters);
        visitor.setExportTables(true);
        /*visitor.setPrettyFormat(false);*/

        SQLStatement stmt = stmtList.get(0);
        stmt.accept(visitor);

        // System.out.println(parameters);
        assertEquals(5, parameters.size());

        //SQLStatementParser parser = SQLParserUtils.createSQLStatementParser(psql, dbType);
        // List<SQLStatement> stmtList = parser.parseStatementList();
        SQLStatement pstmt = SQLUtils.parseStatements(psql, dbType).get(0);

        StringBuilder buf = new StringBuilder();
        SQLASTOutputVisitor visitor1 = SQLUtils.createOutputVisitor(buf, dbType);
        visitor1.addTableMapping("udata", "udata_0888");
        visitor1.setInputParameters(visitor.getParameters());
        pstmt.accept(visitor1);

        assertEquals("SELECT count(*) AS count\n" +
                "FROM (\n" +
                "\t(SELECT env_type\n" +
                "\tFROM `tmall_miaoscm`.miao_sale_ledger\n" +
                "\tWHERE `id` > 1\n" +
                "\tLIMIT 136)\n" +
                "\tUNION ALL\n" +
                "\t(SELECT env_type\n" +
                "\tFROM `tmall_miaoscm`.miao_sale_ledger\n" +
                "\tWHERE `id` > 2331\n" +
                "\tLIMIT 136)\n" +
                ") miao_sale_ledger_0060\n" +
                "WHERE `miao_sale_ledger_0060`.`env_type` = 3", buf.toString());
    }
}
