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
package com.alibaba.polardbx.druid.bvt.sql.mysql.rename;

import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLCommentHint;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.SQLStatementImpl;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlRenameTableStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.polardbx.druid.sql.parser.SQLParserFeature;
import com.alibaba.polardbx.druid.sql.parser.Token;
import junit.framework.TestCase;
import org.junit.Assert;

import java.util.List;

public class MySqlRenameTableTest1 extends TestCase {

    public void test_rename_1() throws Exception {
        String sql = "/*TDDL:SCAN*/rename table a to b";
        MySqlStatementParser parser = new MySqlStatementParser(sql, SQLParserFeature.TDDLHint);
        SQLStatement stmt = parser.parseStatementList().get(0);
        parser.match(Token.EOF);
        final List<SQLCommentHint> headHintsDirect = ((MySqlRenameTableStatement) stmt).getHeadHintsDirect();
        final int size = headHintsDirect.size();
        System.out.println(headHintsDirect.get(0));
        String output = SQLUtils.toMySqlString(stmt);
        assertEquals("/*TDDL:SCAN*/", headHintsDirect.get(0).toString());
        Assert.assertEquals("RENAME TABLE a TO b", output);
    }

    public void test_create_1() throws Exception {
        String sql = "/*TDDL:SCAN*/create table a(a int)";
        MySqlStatementParser parser = new MySqlStatementParser(sql, SQLParserFeature.TDDLHint);
        SQLStatement stmt = parser.parseStatementList().get(0);
        parser.match(Token.EOF);
        final List<SQLCommentHint> headHintsDirect = ((SQLStatementImpl) stmt).getHeadHintsDirect();
        final int size = headHintsDirect.size();
        System.out.println(headHintsDirect.get(0));
        String output = SQLUtils.toMySqlString(stmt);
        Assert.assertEquals("/*TDDL:SCAN*/",(headHintsDirect.get(0).toString()));
    }

    public void test_alter_1() throws Exception {
        String sql = "/*TDDL:SCAN*/alter table dd modify aa int";
        MySqlStatementParser parser = new MySqlStatementParser(sql, SQLParserFeature.TDDLHint);
        SQLStatement stmt = parser.parseStatementList().get(0);
        parser.match(Token.EOF);
        final List<SQLCommentHint> headHintsDirect = ((SQLStatementImpl) stmt).getHeadHintsDirect();
        final int size = headHintsDirect.size();
        System.out.println(headHintsDirect.get(0));
        String output = SQLUtils.toMySqlString(stmt);
        Assert.assertEquals("/*TDDL:SCAN*/",(headHintsDirect.get(0).toString()));
    }
}
