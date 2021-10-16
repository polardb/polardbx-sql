/*
 * Copyright 1999-2017 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.polardbx.druid.bvt.sql.mysql.insert;

import com.alibaba.fastjson.JSON;
import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlSchemaStatVisitor;
import com.alibaba.polardbx.druid.sql.parser.SQLParserFeature;
import com.alibaba.polardbx.druid.sql.visitor.ParameterizedOutputVisitorUtils;
import com.alibaba.polardbx.druid.sql.visitor.VisitorFeature;
import junit.framework.TestCase;

import java.util.ArrayList;
import java.util.List;

public class MySqlInsertTest_33_hint extends TestCase {

    public void test_insert_0() throws Exception {
        String sql = "/*TDDL:node('node_name')*/insert/*+TDDL:node('node_name')*/ into table_1 values(1);";

        List<SQLStatement> statementList = SQLUtils.parseStatements(sql, DbType.mysql, SQLParserFeature.TDDLHint);
        SQLStatement stmt = statementList.get(0);

        MySqlInsertStatement insertStmt = (MySqlInsertStatement) stmt;

        assertEquals(1, insertStmt.getValuesList().size());
        assertEquals(1, insertStmt.getValues().getValues().size());
        assertEquals(0, insertStmt.getColumns().size());
        assertEquals(1, statementList.size());

        MySqlSchemaStatVisitor visitor = new MySqlSchemaStatVisitor();
        stmt.accept(visitor);

        String formatSql = "/*TDDL:node('node_name')*/\n" +
                "INSERT /*+TDDL:node('node_name')*/ INTO table_1\n" +
                "VALUES (1);";
        assertEquals(formatSql, SQLUtils.toMySqlString(insertStmt));

        List<Object> params = new ArrayList<Object>();
        String psql = ParameterizedOutputVisitorUtils.parameterizeForTDDL(sql, DbType.mysql, params, VisitorFeature.OutputParameterizedQuesUnMergeValuesList);
        assertEquals("/*TDDL:node('node_name')*/\n" +
                "INSERT /*+TDDL:node('node_name')*/ INTO table\n" +
                "VALUES (?);", psql);
        assertEquals("[1]", JSON.toJSONString(params));
    }

    public void test_insert_1() throws Exception {
        String sql = "insert /*+TDDL:CMD_EXTRA(ALWAYS_REBUILD_PLAN=true)*/ ignore into gsi_dml_unique_one_index_base (pk,integer_test,bigint_test,varchar_test,datetime_test,year_test,char_test) values (1,10,12,'feed32feed','2013-04-05 06:34:12',2014,'word23'), (1,10,12,'feed32feed','2013-04-05 06:34:12',2014,'word23');";

        List<SQLStatement> statementList = SQLUtils.parseStatements(sql, DbType.mysql, SQLParserFeature.TDDLHint);
        SQLStatement stmt = statementList.get(0);

        MySqlInsertStatement insertStmt = (MySqlInsertStatement) stmt;

        assertEquals("INSERT /*+TDDL:CMD_EXTRA(ALWAYS_REBUILD_PLAN=true)*/ IGNORE INTO gsi_dml_unique_one_index_base (pk, integer_test, bigint_test, varchar_test, datetime_test\n"
            + "\t, year_test, char_test)\n"
            + "VALUES (1, 10, 12, 'feed32feed', '2013-04-05 06:34:12'\n"
            + "\t\t, 2014, 'word23'),\n"
            + "\t(1, 10, 12, 'feed32feed', '2013-04-05 06:34:12'\n"
            + "\t\t, 2014, 'word23');", SQLUtils.toMySqlString(insertStmt));
    }

}
