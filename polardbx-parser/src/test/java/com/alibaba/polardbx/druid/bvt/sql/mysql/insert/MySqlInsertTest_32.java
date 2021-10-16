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
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlSchemaStatVisitor;
import com.alibaba.polardbx.druid.sql.parser.SQLParserFeature;
import com.alibaba.polardbx.druid.sql.visitor.ParameterizedOutputVisitorUtils;
import com.alibaba.polardbx.druid.sql.visitor.VisitorFeature;
import junit.framework.TestCase;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class MySqlInsertTest_32 extends TestCase {

    public void test_insert_0() throws Exception {
        String sql = "insert into t values(1,2),(3,4);";

        MySqlStatementParser parser = new MySqlStatementParser(sql, false, true);
        parser.config(SQLParserFeature.KeepInsertValueClauseOriginalString, true);

        List<SQLStatement> statementList = parser.parseStatementList();
        SQLStatement stmt = statementList.get(0);

        MySqlInsertStatement insertStmt = (MySqlInsertStatement) stmt;

        assertEquals(2, insertStmt.getValuesList().size());
        assertEquals(2, insertStmt.getValues().getValues().size());
        assertEquals(0, insertStmt.getColumns().size());
        assertEquals(1, statementList.size());

        MySqlSchemaStatVisitor visitor = new MySqlSchemaStatVisitor();
        stmt.accept(visitor);

        String formatSql = "INSERT INTO t\n" +
                "VALUES (1, 2),\n" +
                "\t(3, 4);";
        assertEquals(formatSql, SQLUtils.toMySqlString(insertStmt));

        List<Object> params = new ArrayList<Object>();
        String psql = ParameterizedOutputVisitorUtils.parameterize(sql, DbType.mysql, params, VisitorFeature.OutputParameterizedQuesUnMergeValuesList);
        assertEquals("INSERT INTO t\n" +
                "VALUES (?, ?),\n" +
                "\t(?, ?);", psql);
        assertEquals("[1,2,3,4]", JSON.toJSONString(params));
    }

    @Test public void test35()  {
        String statement = "/*+ hint=1 */ INSERT INTO TPCH.CTAS_TEST_9 (p_brand,p_type,p_size,supplier_cnt) "
                           + "SELECT p_brand, p_type, p_size, count(DISTINCT ps_suppkey) AS supplier_cnt "
                           + "FROM PARTSUPP AS ps, PART AS p "
                           + "WHERE p_partkey = ps_partkey AND p_brand <> 'Brand#45' AND p_type NOT LIKE 'MEDIUM POLISHED%' "
                           + "AND p_size IN (49, 14, 23, 45, 19, 3, 36, 9) " + "AND ps_suppkey NOT IN "
                           + "  (SELECT /*+ broadcast = true */ s_suppkey FROM SUPPLIER AS s WHERE s_comment LIKE '%Customer%Complaints%') "
                           + "GROUP BY p_brand, p_type, p_size ORDER BY supplier_cnt DESC, p_brand, p_type, p_size";
        List<SQLStatement> stmts = null;

        MySqlStatementParser parser = new MySqlStatementParser(statement, false, true);
        parser.config(SQLParserFeature.KeepInsertValueClauseOriginalString, true);
        stmts = parser.parseStatementList();

        assertEquals(1, stmts.size());
        SQLStatement stmt = stmts.get(0);
        assertTrue(stmt instanceof MySqlInsertStatement);
        System.out.println(SQLUtils.toMySqlString(stmt));
        assertEquals("+ hint=1 ", stmt.getHeadHintsDirect().get(0).getText());
        assertTrue(SQLUtils.toMySqlString(stmt).contains("/*+ broadcast = true */"));
    }

    @Test public void test36() {
        String statement = "/*+ hint=1 */ INSERT INTO TPCH.CTAS_TEST_9 (p_brand,p_type,p_size,supplier_cnt) "
                           + "/*+ hint=2 */ SELECT p_brand, p_type, p_size, count(DISTINCT ps_suppkey) AS supplier_cnt "
                           + "FROM PARTSUPP AS ps, PART AS p "
                           + "WHERE p_partkey = ps_partkey AND p_brand <> 'Brand#45' AND p_type NOT LIKE 'MEDIUM POLISHED%' "
                           + "AND p_size IN (49, 14, 23, 45, 19, 3, 36, 9) "
                           + "GROUP BY p_brand, p_type, p_size ORDER BY supplier_cnt DESC, p_brand, p_type, p_size";
        List<SQLStatement> stmts = null;

        MySqlStatementParser parser = new MySqlStatementParser(statement, false, true);
        parser.config(SQLParserFeature.KeepInsertValueClauseOriginalString, true);
        stmts = parser.parseStatementList();

        assertEquals(1, stmts.size());
        assertTrue(stmts.get(0) instanceof MySqlInsertStatement);

        assertEquals("/*+ hint=1 */\n"
                     + "INSERT INTO TPCH.CTAS_TEST_9 (p_brand, p_type, p_size, supplier_cnt)\n"
                     + "/*+ hint=2 */\n"
                     + "SELECT p_brand, p_type, p_size, count(DISTINCT ps_suppkey) AS supplier_cnt\n"
                     + "FROM PARTSUPP ps, PART p\n"
                     + "WHERE p_partkey = ps_partkey\n"
                     + "\tAND p_brand <> 'Brand#45'\n"
                     + "\tAND p_type NOT LIKE 'MEDIUM POLISHED%'\n" + "\tAND p_size IN (49, 14, 23, 45, 19, 3, 36, 9)\n"
                     + "GROUP BY p_brand, p_type, p_size\n"
                     + "ORDER BY supplier_cnt DESC, p_brand, p_type, p_size", SQLUtils.toMySqlString(stmts.get(0)));
    }
}
